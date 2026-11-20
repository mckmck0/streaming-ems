"""Pure domain logic (no Restate imports).

Used both by the Restate services and by the offline reference simulation, so the
streaming results can be checked bit-for-bit (exactly-once verification).
"""
import math
import random

DT_H = 0.25            # 15-minute market time unit (EU day-ahead MTU)
STEPS_PER_DAY = 96

BATTERY = {"cap_kwh": 10.0, "p_max_kw": 5.0, "eta": 0.95, "soc_min": 1.0, "soc_max": 9.5}
SOC0 = 5.0


def price(t: int) -> dict:
    """Zone-level day-ahead-like price curve (PLN/kWh). Sell < buy (net-billing asymmetry)."""
    hour = (t % STEPS_PER_DAY) * DT_H
    rnd = random.Random(f"price:{t // STEPS_PER_DAY}")
    base = 0.55 + 0.1 * rnd.random()
    buy = (base
           - 0.25 * math.exp(-((hour - 3.0) ** 2) / 4.0)     # cheap night
           - 0.20 * math.exp(-((hour - 13.0) ** 2) / 6.0)    # solar dip
           + 0.60 * math.exp(-((hour - 19.5) ** 2) / 2.0))   # evening peak
    return {"price_buy": round(buy, 4), "price_sell": round(max(0.0, 0.6 * buy - 0.1), 4)}


def measurement(household: str, t: int) -> dict:
    """Deterministic synthetic PV/load sample for one household at step t."""
    cloud = 0.3 + 0.7 * random.Random(f"{household}:day:{t // STEPS_PER_DAY}").random()
    noise = random.Random(f"{household}:{t}")
    hour = (t % STEPS_PER_DAY) * DT_H
    pv = 0.0
    if 6.0 <= hour <= 20.0:
        pv = 5.0 * math.sin(math.pi * (hour - 6.0) / 14.0) ** 2 * cloud * (0.9 + 0.2 * noise.random())
    load = (0.3 + 1.2 * math.exp(-((hour - 7.5) ** 2) / 1.5)
            + 2.0 * math.exp(-((hour - 19.0) ** 2) / 3.0) + 0.3 * noise.random())
    return {"t": t, "pv_kw": round(pv, 4), "load_kw": round(load, 4), **price(t)}


def apply_battery(soc: float, p_cmd_kw: float) -> tuple:
    """Battery physics. p > 0 charge, p < 0 discharge (AC side). Returns (new_soc, actual_p)."""
    b = BATTERY
    if p_cmd_kw >= 0:
        p = min(p_cmd_kw, b["p_max_kw"], (b["soc_max"] - soc) / (b["eta"] * DT_H))
        return soc + p * b["eta"] * DT_H, p
    p = -min(-p_cmd_kw, b["p_max_kw"], (soc - b["soc_min"]) * b["eta"] / DT_H)
    return soc + p / b["eta"] * DT_H, p


def new_state(first_t: int) -> dict:
    return {"soc": SOC0, "cost_pln": 0.0, "import_kwh": 0.0, "export_kwh": 0.0,
            "first_t": first_t, "last_t": first_t - 1, "n": 0, "out_of_order": 0, "plan": None}


def make_plan(day_ahead_buy: list) -> dict:
    """Day-ahead plan: grid-charge in the 8 cheapest night slots, cover load in the 16 priciest."""
    order = sorted(range(STEPS_PER_DAY), key=lambda i: day_ahead_buy[i])
    night = [i for i in order if i < 24][:8]           # 00:00-06:00
    return {"charge": sorted(night), "peak": sorted(order[-16:])}


# ---------------- strategies: (state, measurement) -> battery power command ----------------
def no_battery(st, m):
    return 0.0


def self_consumption(st, m):
    return m["pv_kw"] - m["load_kw"]


def price_aware(st, m):
    step, net = m["t"] % STEPS_PER_DAY, m["pv_kw"] - m["load_kw"]
    if net > 0:
        return net                                  # store PV surplus
    if step in st["plan"]["charge"]:
        return BATTERY["p_max_kw"]                  # "charge at 2:00, because it is cheapest"
    if step in st["plan"]["peak"]:
        return net                                  # discharge to cover load at peak price
    return 0.0                                      # hold energy for the evening peak


def self_consumption_reserve(st, m):
    """Added at runtime from a second deployment (dynamic routing demo)."""
    net = m["pv_kw"] - m["load_kw"]
    if net < 0 and (m["t"] % STEPS_PER_DAY) * DT_H < 17.0 and st["soc"] < 0.5 * BATTERY["cap_kwh"]:
        return 0.0                                  # keep a reserve for the evening peak
    return net


STRATEGIES = {"NoBattery": (no_battery, False), "SelfConsumption": (self_consumption, False),
              "PriceAware": (price_aware, True), "SelfConsumptionReserve": (self_consumption_reserve, False)}


def step(st: dict, m: dict, policy, day_ahead=None) -> dict:
    """One decision + settlement step. Identical code path online (Restate) and offline (reference)."""
    if m["t"] != st["last_t"] + 1:
        st["out_of_order"] += 1
    if day_ahead is not None:
        st["plan"] = make_plan(day_ahead)
    st["soc"], p = apply_battery(st["soc"], policy(st, m))
    grid = m["load_kw"] - m["pv_kw"] + p              # > 0 import, < 0 export
    imp, exp = max(grid, 0.0) * DT_H, max(-grid, 0.0) * DT_H
    st["import_kwh"] += imp
    st["export_kwh"] += exp
    st["cost_pln"] += imp * m["price_buy"] - exp * m["price_sell"]
    st["last_t"], st["n"] = m["t"], st["n"] + 1
    return st


def needs_plan(st: dict, m: dict) -> bool:
    return st["plan"] is None or m["t"] % STEPS_PER_DAY == 0


def day_ahead_curve(day: int) -> list:
    return [price(day * STEPS_PER_DAY + i)["price_buy"] for i in range(STEPS_PER_DAY)]


def reference(household: str, name: str, first_t: int, last_t: int) -> dict:
    """Offline simulation used to verify the streaming run."""
    policy, plan = STRATEGIES[name]
    st = new_state(first_t)
    for t in range(first_t, last_t + 1):
        m = measurement(household, t)
        da = day_ahead_curve(t // STEPS_PER_DAY) if plan and needs_plan(st, m) else None
        st = step(st, m, policy, da)
    return st
