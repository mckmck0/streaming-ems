"""Load generator + verifier for the Restate EMS spike.

- ingest(): every household streams its measurements in order (sequential per key,
  concurrent across keys) through the HTTP ingress, using one-way sends with an
  idempotency key "<household>:<t>", retrying on any error -> safe across crashes.
- wait_done(): polls Restate's SQL introspection until every entity has processed
  all intervals.
- verify(): compares every strategy's state bit-for-bit with the offline reference
  simulation (model.reference) -> detects lost, duplicated or reordered events.
"""
import asyncio
import json
import time

import httpx

from ems_spike import model

INGRESS = "http://127.0.0.1:8080"
ADMIN = "http://127.0.0.1:9070"


def households(prefix: str, n: int) -> list:
    return [f"{prefix}{i:04d}" for i in range(n)]


def sql(query: str) -> list:
    for attempt in range(60):
        try:
            r = httpx.post(f"{ADMIN}/query", json={"query": query},
                           headers={"accept": "application/json"}, timeout=30)
            r.raise_for_status()
            return r.json()["rows"]
        except Exception:                      # server may be restarting
            time.sleep(1)
    raise RuntimeError("SQL endpoint unavailable")


def call(path: str, body=None, timeout=30):
    for attempt in range(60):
        try:
            kw = {"json": body} if body is not None else {}
            r = httpx.post(f"{INGRESS}{path}", timeout=timeout, **kw)
            r.raise_for_status()
            return r.json() if r.content else None
        except Exception:
            time.sleep(1)
    raise RuntimeError(f"call failed: {path}")


async def _stream(client, sem, h, t_from, t_to, stats):
    async with sem:
        for t in range(t_from, t_to + 1):
            m = model.measurement(h, t)
            delay = 0.05
            while True:                        # retry until accepted (idempotent)
                try:
                    m["sent_at"] = time.time()
                    r = await client.post(f"/Household/{h}/on_measurement/send", json=m,
                                          headers={"idempotency-key": f"{h}:{t}"})
                    if r.status_code in (200, 202):
                        stats["sent"] += 1
                        break
                    stats["http_errors"] += 1
                except Exception:
                    stats["retries"] += 1
                await asyncio.sleep(delay)
                delay = min(delay * 2, 2.0)


async def ingest(hs, t_from, t_to, concurrency=50, stats=None):
    stats = stats if stats is not None else {"sent": 0, "retries": 0, "http_errors": 0}
    sem = asyncio.Semaphore(concurrency)
    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
    async with httpx.AsyncClient(base_url=INGRESS, timeout=10, limits=limits) as client:
        await asyncio.gather(*[_stream(client, sem, h, t_from, t_to, stats) for h in hs])
    return stats


def states(prefix: str) -> dict:
    rows = sql(f"select service_name, service_key, key, value_utf8 from state "
               f"where service_key like '{prefix}%' and key in ('st', 's', 'acc')")
    return {(r["service_name"], r["service_key"]): json.loads(r["value_utf8"]) for r in rows}


def wait_done(prefix, hs, strategies, last_t, timeout_s=1800, poll_s=1.0):
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        st = states(prefix)
        done = all(st.get((s, h), {}).get("last_t") == last_t for s in strategies for h in hs) and \
            all(st.get((e, h), {}).get("n") == last_t + 1 for e in ("Ems", "Meter") for h in hs)
        if done:
            return time.time() - t0, st
        time.sleep(poll_s)
    raise TimeoutError("processing did not finish in time")


def verify(st, hs, strategies, last_t):
    report = {"checked": 0, "mismatches": 0, "events_total": 0, "out_of_order": 0,
              "late_joiners": 0, "meter_vs_selfconsumption_mismatch": 0, "examples": []}
    for h in hs:
        for s in strategies:
            got = st[(s, h)]
            ref = model.reference(h, s, got["first_t"], last_t)
            report["checked"] += 1
            report["events_total"] += got["n"]
            report["out_of_order"] += got["out_of_order"]
            report["late_joiners"] += got["first_t"] > 0
            keys = ("soc", "cost_pln", "import_kwh", "export_kwh", "n", "last_t")
            if any(got[k] != ref[k] for k in keys):          # exact, bit-for-bit
                report["mismatches"] += 1
                if len(report["examples"]) < 3:
                    report["examples"].append({"h": h, "s": s, "got": {k: got[k] for k in keys},
                                               "ref": {k: ref[k] for k in keys}})
        if "SelfConsumption" in strategies and st[("SelfConsumption", h)]["first_t"] == 0:
            # live loop (Ems -> Battery -> Meter, 4 hops) must bill exactly like the in-state strategy
            if st[("Meter", h)]["cost_pln"] != st[("SelfConsumption", h)]["cost_pln"]:
                report["meter_vs_selfconsumption_mismatch"] += 1
    ems = [st[("Ems", h)] for h in hs]
    n = sum(e["n"] for e in ems)
    report["ems_decisions"] = n
    report["ems_stale_decisions_pct"] = round(100 * sum(e["stale"] for e in ems) / max(n, 1), 1)
    report["ems_mean_lag_ms"] = round(1000 * sum(e["lag_sum_s"] for e in ems) / max(n, 1), 1)
    report["ems_max_lag_ms"] = round(1000 * max(e["lag_max_s"] for e in ems), 1)
    return report


def costs_table(st, hs, strategies):
    """Counterfactual comparison: mean daily cost per strategy (PLN/household)."""
    out = {}
    for s in strategies:
        vals = [st[(s, h)]["cost_pln"] for h in hs if st[(s, h)]["first_t"] == 0]
        if vals:
            out[s] = round(sum(vals) / len(vals), 3)
    return out
