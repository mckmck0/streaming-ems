"""Experiments for the Restate EMS spike. Usage: python3 experiments.py A B C D [E:200]

A  baseline        : H households x 1 day (96 x 15 min), 3 strategies + live loop
B  crash           : same, but kill -9 the SDK service and later the Restate server mid-stream
C  dynamic routing : register a 2nd deployment with a new strategy mid-stream, no restarts
D  durable timers  : one-shot "charge in 6 s / 20 s" + periodic refresh, survive kill -9 of everything
D2 same as D, with retry back-off capped at 1 s (EMS_RETRY_MAX_INTERVAL_S=1)
E:<H> fleet scale  : like A with H households
"""
import asyncio
import json
import subprocess
import sys
import time

import httpx

import loadgen as lg
from ems_spike.restate_app import DEFAULT_STRATEGIES

STEPS = 96
RESULTS = "results.json"


def ctl(*args):
    subprocess.run(["./ctl.sh", *args], check=False, capture_output=True)


def wait_healthy(timeout=60):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            if httpx.get("http://127.0.0.1:8080/restate/health", timeout=2).status_code == 200:
                return time.time() - t0
        except Exception:
            pass
        time.sleep(0.5)
    raise RuntimeError("server not healthy")


def save(name, res):
    try:
        allres = json.load(open(RESULTS))
    except Exception:
        allres = {}
    allres[name] = res
    json.dump(allres, open(RESULTS, "w"), indent=2)
    print(name, json.dumps(res, indent=2))


async def run_stream(prefix, h, chaos=None):
    hs = lg.households(prefix, h)
    stats = {"sent": 0, "retries": 0, "http_errors": 0}
    t0 = time.time()
    tasks = [lg.ingest(hs, 0, STEPS - 1, concurrency=50, stats=stats)]
    if chaos:
        tasks.append(chaos(stats, h * STEPS))
    await asyncio.gather(*tasks)
    ingest_s = time.time() - t0
    return hs, stats, ingest_s, t0


def finish(name, prefix, hs, strategies, stats, ingest_s, t0, extra=None):
    _, st = lg.wait_done(prefix, hs, strategies, STEPS - 1)
    total_s = time.time() - t0
    rep = lg.verify(st, hs, strategies, STEPS - 1)
    events = len(hs) * STEPS
    invocations = events * (1 + 4) + sum(st[(s, h)]["n"] for s in strategies for h in hs)
    res = {"households": len(hs), "events": events, "strategies": strategies,
           "ingest_s": round(ingest_s, 1), "end_to_end_s": round(total_s, 1),
           "events_per_s": round(events / total_s, 1),
           "handler_invocations": invocations, "invocations_per_s": round(invocations / total_s, 1),
           "ingest_retries": stats["retries"], "http_errors": stats["http_errors"],
           "verification": rep, "mean_daily_cost_pln": lg.costs_table(st, hs, strategies)}
    if extra:
        res.update(extra)
    save(name, res)


def exp_baseline(name="A_baseline", prefix="a", h=50):
    hs, stats, ingest_s, t0 = asyncio.run(run_stream(prefix, h))
    finish(name, prefix, hs, DEFAULT_STRATEGIES, stats, ingest_s, t0)


def exp_crash(prefix="b", h=50, name="B_crash"):
    events_log = []

    async def chaos(stats, total):
        while stats["sent"] < 0.3 * total:
            await asyncio.sleep(0.2)
        events_log.append(("kill -9 SDK service", stats["sent"]))
        await asyncio.to_thread(ctl, "kill-svc1")
        await asyncio.sleep(4)
        await asyncio.to_thread(ctl, "svc1")
        while stats["sent"] < 0.6 * total:
            await asyncio.sleep(0.2)
        events_log.append(("kill -9 restate-server", stats["sent"]))
        await asyncio.to_thread(ctl, "kill-server")
        await asyncio.sleep(4)
        await asyncio.to_thread(ctl, "server")
        rec = await asyncio.to_thread(wait_healthy)
        events_log.append(("server healthy again after s", round(rec, 1)))

    hs, stats, ingest_s, t0 = asyncio.run(run_stream(prefix, h, chaos))
    finish(name, prefix, hs, DEFAULT_STRATEGIES, stats, ingest_s, t0,
           {"chaos": events_log})


def exp_dynamic(prefix="c", h=50):
    new = DEFAULT_STRATEGIES + ["SelfConsumptionReserve"]
    log = []

    async def add_strategy(stats, total):
        while stats["sent"] < 0.5 * total:
            await asyncio.sleep(0.2)
        log.append(("start + register deployment #2", stats["sent"]))
        await asyncio.to_thread(ctl, "svc2")
        await asyncio.sleep(2)
        await asyncio.to_thread(ctl, "register", "http://127.0.0.1:9081")
        r = await asyncio.to_thread(lg.call, "/Registry/fleet/configure",
                                    {"households": lg.households(prefix, h), "strategies": new})
        log.append(("registry configured", stats["sent"], r))

    hs, stats, ingest_s, t0 = asyncio.run(run_stream(prefix, h, add_strategy))
    finish("C_dynamic_routing", prefix, hs, new, stats, ingest_s, t0, {"steps": log})


def exp_timers(key="d0000", name="D_durable_timers", zone="PL"):
    t_start = time.time()
    lg.call(f"/Tariff/{zone}/start_refresh", 3)
    time.sleep(7)                                    # let the periodic chain tick a few times
    lg.call(f"/Ems/{key}/schedule_grid_charge", {"delay_s": 6, "kw": 3.0})    # due DURING outage
    sched = lg.call(f"/Ems/{key}/schedule_grid_charge", {"delay_s": 20, "kw": 3.0})  # due AFTER
    time.sleep(2)
    kill_at = time.time()
    for target in ("kill-server", "kill-svc1"):
        ctl(target)
    ctl("kill-svc2")
    time.sleep(8)
    ctl("server")
    ctl("svc1")
    ctl("svc2")
    wait_healthy()
    restart_at = time.time()
    time.sleep(max(0, sched["due_at"] - time.time()) + 8)
    forced = lg.call(f"/Battery/{key}/summary")["forced"]
    refresh = [x for x in lg.call(f"/Tariff/{zone}/summary")["refresh_log"] if x >= t_start]
    lg.call(f"/Tariff/{zone}/stop_refresh")
    res = {"scheduled_at": sched["scheduled_at"], "due_at": sched["due_at"],
           "downtime_s": round(restart_at - kill_at, 1),
           "fired": [{"requested_delay_s": round(f["due_at"] - f["scheduled_at"]),
                      "fired_vs_due_s": round(f["fired_at"] - f["due_at"], 2)} for f in forced],
           "refresh_ticks_before_kill": sum(1 for x in refresh if x < kill_at),
           "refresh_ticks_after_restart": sum(1 for x in refresh if x > restart_at),
           "max_gap_between_refresh_ticks_s": round(max(b - a for a, b in zip(refresh, refresh[1:])), 1)}
    save(name, res)


if __name__ == "__main__":
    for arg in sys.argv[1:]:
        if arg == "A":
            exp_baseline()
        elif arg == "B":
            exp_crash()
        elif arg == "C":
            exp_dynamic()
        elif arg == "D":
            exp_timers()
        elif arg == "D2":   # run with EMS_RETRY_MAX_INTERVAL_S=1 and re-registered deployments
            exp_timers(key="d0002", name="D2_durable_timers_capped_retry")
        elif arg.startswith("E:"):
            n = int(arg.split(":")[1])
            exp_baseline(name=f"E_scale_{n}", prefix=f"e{n}_", h=n)
