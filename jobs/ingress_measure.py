"""Ingress measurements for the thesis (spec 010, AC-010-17 … -21; spec 011, AC-011-46): `make
measure [MODE=skew|backlog]`.

Each run takes a fresh run_id and topic prefix, closes the stack's ingestion, starts its own SDK
service and, where needed, its own ingress containers (src/ems/experiments/stack.py), and writes
experiments/results/ingress-<mode>-<UTC>/results.json with its config, seed, data window, hardware,
image references, package versions and git commit (P14).

- skew (default): an unpaced backfill with every EMS-managed household configured. Per managed
  household the largest lead of each of its streams over each other one, prices included, in
  quarter-hours of data time (``HouseholdState.leads``), their median, 95th percentile (nearest
  rank) and maximum across households; per receiving service the messages that arrived early
  (``reordered``) or stale (V5); records handled per second; the exactly-once check of the ingress
  logs against the offline fold; the peak memory of every container and whether any was OOM-killed.
- backlog: a backfill published while the SDK service is stopped. Consumer lag per ingress group and
  the run's invocations by status, sampled every second, until the service is back and every record
  is handled.
"""

from __future__ import annotations

import json
import math
import statistics
import time
import uuid
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any

import typer
from ruamel.yaml import YAML

REPO = Path(__file__).resolve().parents[1]
START = "2026-10-20T00:00:00Z"

app = typer.Typer(add_completion=False)


def nearest_rank(values: Sequence[float], q: float) -> float:
    """The nearest-rank percentile: the smallest value with at least ``q`` of the values at or below
    it.
    """
    if not values:
        raise ValueError("no values")
    ordered = sorted(values)
    return ordered[max(math.ceil(q * len(ordered)), 1) - 1]


def summarize_leads(leads_by_key: Mapping[str, Mapping[str, int]]) -> dict[str, dict[str, Any]]:
    """Per stream pair: how many keys have it, and the median, p95 and maximum of their largest
    leads.
    """
    by_pair: dict[str, list[int]] = defaultdict(list)
    for leads in leads_by_key.values():
        for pair, lead in leads.items():
            by_pair[pair].append(lead)
    return {
        pair: {
            "keys": len(values),
            "median": statistics.median(values),
            "p95": nearest_rank(values, 0.95),
            "max": max(values),
        }
        for pair, values in sorted(by_pair.items())
    }


def reorder_counts(state: Mapping[str, Any]) -> tuple[int, int]:
    """``(reordered, stale)`` summed over the inboxes in one object's stored state."""
    reordered = stale = 0
    for value in state.values():
        if isinstance(value, Mapping) and {"expected", "held", "reordered", "stale"} <= set(value):
            reordered += int(value["reordered"])
            stale += int(value["stale"])
    return reordered, stale


def _ts(value: Any) -> float:
    """A sys_invocation timestamp (RFC 3339 text or epoch milliseconds) in epoch seconds."""
    if isinstance(value, int | float):
        return float(value) / 1000
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()


def _generator_seed() -> int:
    return int(YAML(typ="safe").load((REPO / "config" / "generator.yaml").read_text())["seed"])


def _write(out: Path, mode: str, config: dict[str, Any], results: dict[str, Any]) -> Path:
    from ems.experiments.stack import manifest

    folder = out / f"ingress-{mode}-{datetime.now(UTC):%Y%m%dT%H%M%SZ}"
    folder.mkdir(parents=True, exist_ok=True)
    doc = {"kind": f"ingress-{mode}", "manifest": manifest(config), "results": results}
    (folder / "results.json").write_text(json.dumps(doc, indent=2, default=str) + "\n")
    return folder


def _env(tag: str, out: Path) -> Any:
    from ems.experiments.stack import StackEnv, remove_test_projects

    remove_test_projects()
    prefix = f"{tag}{uuid.uuid4().hex[:8]}."
    logs = out / "logs" / prefix.strip(".")
    return StackEnv(prefix, log_dir=logs, data_dir=logs / "data")


def run_skew(out: Path, locations: int, days: float, faults: bool) -> dict[str, Any]:
    from ems.experiments.runs import managed_params
    from ems.experiments.stack import (
        MemorySampler,
        offline_logs,
        offline_run,
        oom_killed,
        run_ends,
    )

    env = _env("ms", out)
    run_id = f"m{uuid.uuid4().hex[:10]}"
    start = datetime.fromisoformat(START.replace("Z", "+00:00"))
    params = managed_params(run_id=run_id, start=start, days=days, locations=locations)
    sampler = MemorySampler()
    env.open()
    sampler.start()
    try:
        env.restate.configure(params)
        started = time.monotonic()
        records = env.publish(run_id, locations=locations, days=days, start=START, faults=faults)
        published = time.monotonic() - started
        ends = run_ends(params, offline_run(params, records))
        env.restate.wait_run(run_id, len(records), ends, timeout=3600)
        wall = time.monotonic() - started
        span = env.restate.query(
            "SELECT min(created_at) AS first, max(completed_at) AS last FROM sys_invocation "
            f"WHERE starts_with(target_service_key, '{run_id}:')"
        )[0]
        logs = env.restate.ingress_logs(run_id)
        exact = logs == offline_logs(records)
        states = {
            service: {key: json.loads(v["state"]) for key, v in by_key.items() if "state" in v}
            for service in sorted(ends.keys() | {"Household"})
            for by_key in [env.restate.states(run_id, service)]
        }
    finally:
        sampler.stop()
        oom = oom_killed(sampler.peaks)  # before close removes the test project's containers
        env.close()
    server_s = _ts(span["last"]) - _ts(span["first"])
    leads = {key: state["leads"]["max"] for key, state in states["Household"].items()}
    reorders = {
        service: [reorder_counts(state) for state in by_key.values()]
        for service, by_key in states.items()
    }
    return {
        "run_id": run_id,
        "records": len(records),
        "managed_households": len(params),
        "publish_wall_s": round(published, 2),
        "handled_wall_s": round(wall, 2),
        "server_span_s": round(server_s, 3),
        "records_per_s": round(len(records) / server_s, 1) if server_s > 0 else None,
        "exactly_once": exact,
        "rejected": sum(log["rejected"] for log in logs.values()),
        "regressions": sum(
            t["regressions"] for log in logs.values() for t in log["topics"].values()
        ),
        "leads": summarize_leads(leads),
        "leads_by_key": leads,
        "reordered": {svc: sum(r for r, _ in counts) for svc, counts in sorted(reorders.items())},
        "stale": {svc: sum(st for _, st in counts) for svc, counts in sorted(reorders.items())},
        "memory": {
            name: {
                "peak_bytes": peak,
                "limit_bytes": sampler.limits.get(name),
                "oom_killed": oom.get(name),
            }
            for name, peak in sorted(sampler.peaks.items())
        },
    }


def run_backlog(out: Path, locations: int, days: float, stable_s: float = 30.0) -> dict[str, Any]:
    from ems.experiments.stack import INGRESS_SERVICES, group_progress

    env = _env("bl", out)
    run_id = f"b{uuid.uuid4().hex[:10]}"
    env.open()
    samples: list[dict[str, Any]] = []
    restarted_at = None
    try:
        env.sdk.stop()
        started = time.monotonic()
        records = env.publish(run_id, locations=locations, days=days, start=START)
        expected = len(records)
        while True:
            lag = {}
            for svc in INGRESS_SERVICES:
                committed, end = group_progress(
                    env.settings, env.ingress.group(svc), env.ingress.service_topics(svc)
                )
                lag[svc] = end - committed
            by_status = {
                row["status"]: int(row["n"])
                for row in env.restate.query(
                    "SELECT status, count(*) AS n FROM sys_invocation "
                    f"WHERE starts_with(target_service_key, '{run_id}:') "
                    "GROUP BY status"
                )
            }
            handled = env.restate.handled(run_id)
            now = round(time.monotonic() - started, 1)
            samples.append({"t_s": now, "lag": lag, "invocations": by_status, "handled": handled})
            if restarted_at is None:
                recent = [s for s in samples if s["t_s"] >= now - stable_s]
                steady = len(recent) > 1 and all(
                    (s["lag"], s["invocations"]) == (recent[-1]["lag"], recent[-1]["invocations"])
                    for s in recent
                )
                if steady and samples[0]["t_s"] <= now - stable_s:
                    env.sdk.start()
                    restarted_at = now
            elif handled >= expected:
                break
            if now > 3600:
                raise TimeoutError("backlog did not drain within an hour")
            time.sleep(1.0)
    finally:
        env.close()
    drained_at = samples[-1]["t_s"]
    return {
        "run_id": run_id,
        "records": expected,
        "sdk_restarted_at_s": restarted_at,
        "drained_at_s": drained_at,
        "drain_after_restart_s": round(drained_at - restarted_at, 1)
        if restarted_at is not None
        else None,
        "samples": samples,
    }


@app.command()
def main(
    mode: Annotated[str, typer.Argument(help="skew | backlog")] = "skew",
    locations: Annotated[int | None, typer.Option(help="Households (skew 100, backlog 20)")] = None,
    days: Annotated[float | None, typer.Option(help="Simulated days (skew 2, backlog 1)")] = None,
    faults: Annotated[bool, typer.Option(help="skew: publish with fault injection")] = False,
    out: Annotated[Path, typer.Option(help="Results root")] = Path("experiments/results"),
) -> None:
    out = out.resolve()
    config: dict[str, Any] = {"mode": mode, "generator_config": "config/generator.yaml"}
    if mode == "skew":
        n, d = locations or 100, days or 2.0
        config |= {
            "locations": n,
            "days": d,
            "start": START,
            "faults": faults,
            "seed": _generator_seed(),
            "late_slots": 8,
            "strategies": ["NoBattery", "SelfConsumption"],
            "live": "SelfConsumption",
        }
        results = run_skew(out, n, d, faults)
    elif mode == "backlog":
        n, d = locations or 20, days or 1.0
        config |= {"locations": n, "days": d, "start": START, "seed": _generator_seed()}
        results = run_backlog(out, n, d)
    else:
        typer.echo(f"unknown mode {mode!r} (skew | backlog)", err=True)
        raise typer.Exit(2)
    folder = _write(out, mode, config, results)
    summary = {
        k: v for k, v in results.items() if k not in ("leads_by_key", "skipped_by_key", "samples")
    }
    typer.echo(json.dumps(summary, indent=2, default=str))
    typer.echo(f"wrote {folder / 'results.json'}")


if __name__ == "__main__":
    app()
