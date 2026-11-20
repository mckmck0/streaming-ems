"""Restate spike for the prosumer EMS thesis (deployment #1, port 9080).

Demonstrates the four mechanisms requested by the supervisor:
  1. N strategies on the same stream, each a separate entity with its own virtual battery
     and settlement (counterfactual evaluation); targets are data, so strategies can be
     added at runtime (dynamic routing, see extra_app.py + Registry.configure).
  2. Closed feedback loop across entities: Ems -> Battery -> Ems (+ Meter), one-way messages.
  3. Entity decomposition + durable timers (one-shot "charge at 02:00", periodic refresh).
  4. Fleet scale: one Virtual Object instance per household and entity type.
"""
import json
import os
from datetime import timedelta

import restate
from restate import InvocationRetryPolicy, ObjectContext, ObjectSharedContext, VirtualObject

from ems_spike import model

DEFAULT_STRATEGIES = ["NoBattery", "SelfConsumption", "PriceAware"]

# Optional: cap the retry back-off so work resumes quickly after an outage (see experiment D2).
_MAX_S = os.environ.get("EMS_RETRY_MAX_INTERVAL_S")
OBJ_KW = {"invocation_retry_policy": InvocationRetryPolicy(
    initial_interval=timedelta(milliseconds=100), exponentiation_factor=2.0,
    max_interval=timedelta(seconds=float(_MAX_S)))} if _MAX_S else {}


def strategy_object(name: str) -> VirtualObject:
    """One Virtual Object type per strategy; key = household id."""
    policy, uses_plan = model.STRATEGIES[name]
    obj = VirtualObject(name, **OBJ_KW)

    @obj.handler()
    async def on_tick(ctx: ObjectContext, m: dict) -> None:
        st = await ctx.get("st") or model.new_state(m["t"])
        day_ahead = None
        if uses_plan and model.needs_plan(st, m):
            # request-response to a *shared* (read-only) handler: no deadlock risk
            raw = await ctx.generic_call("Tariff", "day_ahead", key="PL",
                                         arg=json.dumps(m["t"] // model.STEPS_PER_DAY).encode())
            day_ahead = json.loads(raw)
        ctx.set("st", model.step(st, m, policy, day_ahead))

    @obj.handler(kind="shared")
    async def summary(ctx: ObjectSharedContext) -> dict:
        return await ctx.get("st") or {}

    return obj


# ------------------------------- routing / control plane -------------------------------
household = VirtualObject("Household", **OBJ_KW)


@household.handler()
async def on_measurement(ctx: ObjectContext, m: dict) -> None:
    names = await ctx.get("strategies") or DEFAULT_STRATEGIES
    payload = json.dumps(m).encode()
    for name in names:                       # dynamic routing: the target list is state
        ctx.generic_send(name, "on_tick", arg=payload, key=ctx.key())
    ctx.object_send(ems_on_measurement, key=ctx.key(), arg=m)   # live control loop


@household.handler()
async def set_strategies(ctx: ObjectContext, names: list) -> None:
    ctx.set("strategies", names)


registry = VirtualObject("Registry", **OBJ_KW)         # key = "fleet"


@registry.handler()
async def configure(ctx: ObjectContext, req: dict) -> dict:
    households = sorted(set((await ctx.get("households") or []) + req.get("households", [])))
    ctx.set("households", households)
    if "strategies" in req:
        ctx.set("strategies", req["strategies"])
        for h in households:                 # broadcast control message (cf. Flink broadcast state)
            ctx.object_send(set_strategies, key=h, arg=req["strategies"])
    return {"households": len(households), "strategies": req.get("strategies")}


# ------------------------- live closed loop: Ems <-> Battery -> Meter -------------------------
ems = VirtualObject("Ems", **OBJ_KW)


@ems.handler(name="on_measurement")
async def ems_on_measurement(ctx: ObjectContext, m: dict) -> None:
    s = await ctx.get("s") or {"soc_seen": model.SOC0, "soc_seen_t": m["t"] - 1,
                               "n": 0, "stale": 0, "lag_sum_s": 0.0, "lag_max_s": 0.0}
    if s["soc_seen_t"] != m["t"] - 1:
        s["stale"] += 1                      # battery feedback for t-1 had not arrived yet
    if "sent_at" in m:
        lag = await ctx.time() - m["sent_at"]
        s["lag_sum_s"] += lag
        s["lag_max_s"] = max(s["lag_max_s"], lag)
    s["n"] += 1
    ctx.set("s", s)
    ctx.object_send(battery_apply, key=ctx.key(), arg={"m": m, "p_cmd": m["pv_kw"] - m["load_kw"]})


@ems.handler(name="on_battery_state")
async def ems_on_battery_state(ctx: ObjectContext, b: dict) -> None:
    s = await ctx.get("s")
    s["soc_seen"], s["soc_seen_t"] = b["soc"], b["t"]
    ctx.set("s", s)


@ems.handler(name="schedule_grid_charge")
async def ems_schedule_grid_charge(ctx: ObjectContext, req: dict) -> dict:
    """Durable one-shot timer ("charge at 02:00"): survives crashes of SDK and server."""
    now = await ctx.time()
    ctx.object_send(battery_force_charge, key=ctx.key(), send_delay=timedelta(seconds=req["delay_s"]),
                    arg={"kw": req["kw"], "scheduled_at": now, "due_at": now + req["delay_s"]})
    return {"scheduled_at": now, "due_at": now + req["delay_s"]}


@ems.handler(name="summary", kind="shared")
async def ems_summary(ctx: ObjectSharedContext) -> dict:
    return await ctx.get("s") or {}


battery = VirtualObject("Battery", **OBJ_KW)


@battery.handler(name="apply")
async def battery_apply(ctx: ObjectContext, cmd: dict) -> None:
    b = await ctx.get("b") or {"soc": model.SOC0, "forced": []}
    b["soc"], p = model.apply_battery(b["soc"], cmd["p_cmd"])
    ctx.set("b", b)
    m = cmd["m"]
    ctx.object_send(meter_record, key=ctx.key(), arg={"m": m, "p": p})
    ctx.object_send(ems_on_battery_state, key=ctx.key(), arg={"t": m["t"], "soc": b["soc"]})  # loop edge


@battery.handler(name="force_charge")
async def battery_force_charge(ctx: ObjectContext, req: dict) -> None:
    b = await ctx.get("b") or {"soc": model.SOC0, "forced": []}
    b["forced"].append({**req, "fired_at": await ctx.time()})
    ctx.set("b", b)


@battery.handler(name="summary", kind="shared")
async def battery_summary(ctx: ObjectSharedContext) -> dict:
    return await ctx.get("b") or {}


meter = VirtualObject("Meter", **OBJ_KW)


@meter.handler(name="record")
async def meter_record(ctx: ObjectContext, r: dict) -> None:
    acc = await ctx.get("acc") or {"cost_pln": 0.0, "n": 0}
    m = r["m"]
    grid = m["load_kw"] - m["pv_kw"] + r["p"]
    acc["cost_pln"] += max(grid, 0) * model.DT_H * m["price_buy"] - max(-grid, 0) * model.DT_H * m["price_sell"]
    acc["n"] += 1
    ctx.set("acc", acc)


@meter.handler(name="summary", kind="shared")
async def meter_summary(ctx: ObjectSharedContext) -> dict:
    return await ctx.get("acc") or {}


# ------------------------------ tariff service + periodic timer ------------------------------
tariff = VirtualObject("Tariff", **OBJ_KW)             # key = bidding zone, e.g. "PL"


@tariff.handler(name="day_ahead", kind="shared")
async def tariff_day_ahead(ctx: ObjectSharedContext, day: int) -> list:
    return model.day_ahead_curve(day)        # in the thesis: data fetched from ENTSO-E / PSE


@tariff.handler(name="start_refresh")
async def tariff_start_refresh(ctx: ObjectContext, period_s: int) -> str:
    if await ctx.get("period_s"):
        return "already running"
    gen = (await ctx.get("gen") or 0) + 1    # generation guard: a stale chain dies out
    ctx.set("gen", gen)
    ctx.set("period_s", period_s)
    ctx.object_send(tariff_refresh, key=ctx.key(), arg=gen)
    return "started"


@tariff.handler(name="refresh")
async def tariff_refresh(ctx: ObjectContext, gen: int) -> None:
    """Cron-like pattern: do the work, then re-schedule itself with a durable delayed message."""
    period = await ctx.get("period_s")
    if not period or gen != await ctx.get("gen"):
        return                               # stopped, or a chain from an older start
    log = await ctx.get("refresh_log") or []
    log.append(await ctx.time())             # in the thesis: poll the price/forecast API here
    ctx.set("refresh_log", log[-50:])
    ctx.object_send(tariff_refresh, key=ctx.key(), arg=gen, send_delay=timedelta(seconds=period))


@tariff.handler(name="stop_refresh")
async def tariff_stop_refresh(ctx: ObjectContext) -> None:
    ctx.clear("period_s")
    ctx.set("gen", (await ctx.get("gen") or 0) + 1)


@tariff.handler(name="summary", kind="shared")
async def tariff_summary(ctx: ObjectSharedContext) -> dict:
    return {"period_s": await ctx.get("period_s"), "refresh_log": await ctx.get("refresh_log") or []}


app = restate.app(services=[household, registry, ems, battery, meter, tariff,
                            *[strategy_object(n) for n in DEFAULT_STRATEGIES]])
