"""Benchmark: one simulated day for N sites (arrays and full events), and producer throughput.

uv run python jobs/benchmark.py --sites 1000 --sites 10000 --produce-sites 1000

Writes ``experiments/results/benchmark-<UTC timestamp>/results.json`` with config, versions,
hardware and git commit (principle 14). Producing uses a fresh topic prefix on the sim profile and
deletes it afterwards.
"""

from __future__ import annotations

import asyncio
import json
import os
import platform
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated

import typer

from ems.contracts.envelope import to_json_bytes
from ems.contracts.time import quarter_index
from ems.ingest.catalog.build import build_catalog
from ems.ingest.generator import GeneratorConfig, SyntheticModel, emissions
from ems.ingest.runner import _git_commit, _versions

app = typer.Typer(add_completion=False, help=__doc__)
START = datetime(2026, 6, 15, tzinfo=UTC)
CONFIG = Path("config")


def bench_generation(n_sites: int, hours: int) -> dict[str, float | int]:
    sites = build_catalog(n_sites, catalog_seed=7, global_seed=4242, site_controlled_bess_share=0.3)
    model = SyntheticModel(sites, 4242, CONFIG)
    t0 = quarter_index(START)
    a = time.perf_counter()
    model.chunk(t0, hours * 4, list(range(t0, t0 + hours * 4, 12)))
    arrays_s = time.perf_counter() - a
    cfg = GeneratorConfig(streams={"price": {"mode": "synthetic"}})
    model = SyntheticModel(sites, 4242, CONFIG)
    a = time.perf_counter()
    events = 0
    payload_bytes = 0
    stamp = datetime.now(UTC)
    for _at, _stream, build in emissions(model, cfg, START, START + timedelta(hours=hours)):
        payload_bytes += len(to_json_bytes(build(stamp)))
        events += 1
    events_s = time.perf_counter() - a
    return {
        "sites": n_sites,
        "hours": hours,
        "arrays_s": round(arrays_s, 3),
        "events": events,
        "events_build_serialize_s": round(events_s, 2),
        "events_per_s": round(events / events_s, 1),
        "payload_mb": round(payload_bytes / 1e6, 1),
    }


def bench_produce(n_sites: int, hours: int, bootstrap: str) -> dict[str, float | int]:
    from ems.contracts.topics import load_plan
    from ems.ingest.kafka.admin import KafkaTopicAdmin, ensure
    from ems.ingest.runner import run
    from ems.settings import EmsSettings

    prefix = f"bench{uuid.uuid4().hex[:8]}."
    plan = load_plan(CONFIG, "sim", prefix)
    ensure(KafkaTopicAdmin(bootstrap), plan)
    settings = EmsSettings(kafka_bootstrap=bootstrap, topics_profile="sim", topic_prefix=prefix)
    cfg = GeneratorConfig(synthetic_locations=n_sites, streams={"price": {"mode": "synthetic"}})
    try:
        a = time.perf_counter()
        result = asyncio.run(
            run(
                settings,
                cfg,
                mode="backfill",
                start=START,
                end=START + timedelta(hours=hours),
                run_id=None,
            )
        )
        wall = time.perf_counter() - a
    finally:
        KafkaTopicAdmin(bootstrap).delete(t.name for t in plan.topics)
    total = sum(result.counts.values())
    return {
        "sites": n_sites,
        "hours": hours,
        "events": total,
        "wall_s": round(wall, 2),
        "events_per_s": round(total / wall, 1),
        "delivered": result.stats.delivered,
        "failures": result.stats.failures,
    }


@app.command()
def main(
    sites: Annotated[
        list[int] | None, typer.Option("--sites", help="Fleet sizes (repeatable)")
    ] = None,
    hours: Annotated[int, typer.Option(help="Simulated hours")] = 24,
    produce_sites: Annotated[int, typer.Option(help="Sites for the Kafka run (0 = skip)")] = 1000,
    bootstrap: Annotated[str, typer.Option(help="Kafka bootstrap")] = "localhost:9092",
    out: Annotated[Path, typer.Option(help="Results root")] = Path("experiments/results"),
) -> None:
    fleet = sites or [1000, 10000]
    results = {
        "kind": "ems-gen-benchmark",
        "started_at": datetime.now(UTC).isoformat(),
        "window": {"start": START.isoformat(), "hours": hours},
        "generation": [bench_generation(n, hours) for n in fleet],
        "produce": bench_produce(produce_sites, hours, bootstrap) if produce_sites else None,
        "config": {"catalog_seed": 7, "seed": 4242, "site_controlled_bess_share": 0.3},
        "versions": _versions(),
        "git_commit": _git_commit(Path.cwd()),
        "hardware": {
            "machine": platform.machine(),
            "system": platform.system(),
            "release": platform.release(),
            "cpus": os.cpu_count(),
            "processor": platform.processor(),
        },
    }
    folder = out / f"benchmark-{datetime.now(UTC):%Y%m%dT%H%M%SZ}"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    typer.echo(
        json.dumps({"generation": results["generation"], "produce": results["produce"]}, indent=2)
    )
    typer.echo(f"wrote {folder / 'results.json'}")


if __name__ == "__main__":
    app()
