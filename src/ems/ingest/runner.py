"""Run the generator against Kafka: preflight, pacing per clock mode, manifest (principle 14)."""

from __future__ import annotations

import asyncio
import json
import os
import platform
import subprocess
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from importlib import metadata
from pathlib import Path

import structlog

from ems.contracts.time import floor_quarter
from ems.contracts.topics import STREAM_TOPICS, load_plan
from ems.ingest.generator import (
    GeneratorConfig,
    SyntheticModel,
    emissions,
    load_sites,
    record_time_bounds,
)
from ems.ingest.kafka.preflight import problems
from ems.ingest.kafka.sink import KafkaSink, Sink, SinkStats
from ems.settings import EmsSettings

log = structlog.get_logger(__name__)
Clock = Callable[[], datetime]


def wall_clock() -> datetime:
    return datetime.now(UTC)


class PreflightError(RuntimeError):
    pass


@dataclass
class RunResult:
    counts: Counter[str]
    stats: SinkStats
    manifest: Path | None
    elapsed_s: float

    @property
    def exit_code(self) -> int:
        return self.stats.exit_code()


def _git_commit(repo: Path) -> str:
    try:
        out = subprocess.run(
            ["git", "-C", str(repo), "rev-parse", "HEAD"], capture_output=True, text=True, timeout=5
        )
        dirty = subprocess.run(
            ["git", "-C", str(repo), "status", "--porcelain"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return (
            out.stdout.strip() + ("-dirty" if dirty.stdout.strip() else "")
            if out.returncode == 0
            else "unknown"
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown"


def _versions() -> dict[str, str]:
    out = {"python": platform.python_version()}
    for pkg in ("ems", "numpy", "pandas", "pvlib", "demandlib", "confluent-kafka", "pydantic"):
        try:
            out[pkg] = metadata.version(pkg)
        except metadata.PackageNotFoundError:
            out[pkg] = "absent"
    try:
        import confluent_kafka

        out["librdkafka"] = confluent_kafka.libversion()[0]
    except ImportError:
        pass
    return out


def write_manifest(
    path: Path,
    *,
    cfg: GeneratorConfig,
    settings: EmsSettings,
    mode: str,
    start: datetime,
    end: datetime | None,
    run_id: str | None,
    counts: Counter[str],
    stats: SinkStats | None,
    sites: int,
) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    manifest = {
        "kind": "ems-gen-run",
        "run_id": run_id,
        "mode": mode,
        "window": {"start": start.isoformat(), "end": end.isoformat() if end else None},
        "seed": cfg.seed,
        "config": cfg.as_dict(),
        "settings": {
            "topics_profile": settings.topics_profile,
            "topic_prefix": settings.topic_prefix,
            "kafka_bootstrap": settings.kafka_bootstrap,
        },
        "sites": sites,
        "counts": dict(counts),
        "sink": (
            {
                "produced": stats.produced,
                "delivered": stats.delivered,
                "dlq": stats.dlq,
                "failed": dict(stats.failed),
                "remaining": stats.remaining,
            }
            if stats
            else None
        ),
        "versions": _versions(),
        "git_commit": _git_commit(Path.cwd()),
        "hardware": {
            "machine": platform.machine(),
            "system": platform.system(),
            "release": platform.release(),
            "cpus": os.cpu_count(),
        },
        "written_at": wall_clock().isoformat(),
    }
    file = path / "manifest.json"
    file.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return file


def preflight(
    settings: EmsSettings,
    model: SyntheticModel,
    cfg: GeneratorConfig,
    start: datetime,
    end: datetime | None,
    now: datetime,
) -> None:
    from ems.ingest.kafka.admin import KafkaTopicAdmin

    plan = load_plan(settings.config_dir, settings.topics_profile, settings.topic_prefix)
    names = [
        plan.name_for(STREAM_TOPICS[s])
        for s in STREAM_TOPICS
        if plan.name_for(STREAM_TOPICS[s]) in plan.by_name()
    ]
    actual = KafkaTopicAdmin(settings.kafka_bootstrap).actual(names)
    missing = sorted(set(names) - set(actual))
    if missing:
        raise PreflightError(f"topics missing: {missing}; run `ems-gen topics ensure` first")
    lo, hi = record_time_bounds(model, cfg, start, end or now)
    found = problems({n: a.configs for n, a in actual.items()}, min_ts=lo, max_ts=hi, now=now)
    if found:
        raise PreflightError("; ".join(found))


async def run(
    settings: EmsSettings,
    cfg: GeneratorConfig,
    *,
    mode: str,
    start: datetime | None,
    end: datetime | None,
    run_id: str | None,
    speed: float | None = None,
    clock: Clock = wall_clock,
    sink: Sink | None = None,
    check_topics: bool = True,
    max_events: int | None = None,
) -> RunResult:
    now = clock()
    if mode == "realtime":
        start = floor_quarter(now) if start is None else start
        speed = 1.0
    elif start is None:
        raise ValueError("backfill and accelerated runs need a start")
    if mode == "backfill" and end is None:
        raise ValueError("backfill needs an end (--end or --days)")
    speed = speed if speed is not None else cfg.speed
    sites = load_sites(settings.config_dir, cfg)
    model = SyntheticModel(sites, cfg.seed, settings.config_dir)
    if check_topics:
        preflight(settings, model, cfg, start, end, now)
    sink = (
        sink
        if sink is not None
        else KafkaSink(
            settings.kafka_bootstrap,
            topic_prefix=settings.topic_prefix,
            delivery_timeout_ms=settings.delivery_timeout_ms,
        )
    )
    wall0 = time.monotonic()
    counts: Counter[str] = Counter()
    log.info(
        "run_start",
        mode=mode,
        start=start.isoformat(),
        end=end.isoformat() if end else None,
        sites=len(sites),
    )
    stats: SinkStats | None = None
    manifest: Path | None = None
    try:
        for emit_at, stream, build in emissions(model, cfg, start, end, run_id):
            if mode != "backfill":
                due = wall0 + (emit_at - start).total_seconds() / speed
                delay = due - time.monotonic()
                while delay > 0:  # sleep in slices so delivery callbacks keep being served
                    await asyncio.sleep(min(delay, 0.5))
                    sink.poll(0)
                    delay = due - time.monotonic()
            sink.publish(build(clock()))
            counts[stream] += 1
            if max_events is not None and sum(counts.values()) >= max_events:
                break
            if sum(counts.values()) % 1000 == 0:
                await asyncio.sleep(0)
    finally:
        # Also on Ctrl+C / SIGTERM: flush, then record what was produced (principle 14).
        stats = sink.close()
        elapsed = time.monotonic() - wall0
        run_dir = settings.data_dir / "runs" / (run_id or f"{mode}-{start:%Y%m%dT%H%M%SZ}")
        manifest = write_manifest(
            run_dir,
            cfg=cfg,
            settings=settings,
            mode=mode,
            start=start,
            end=end,
            run_id=run_id,
            counts=counts,
            stats=stats,
            sites=len(sites),
        )
        total = sum(counts.values())
        log.info(
            "run_done",
            events=total,
            events_per_s=round(total / elapsed, 1) if elapsed > 0 else None,
            elapsed_s=round(elapsed, 2),
            manifest=str(manifest),
            exit_code=stats.exit_code(),
        )
    return RunResult(counts=counts, stats=stats, manifest=manifest, elapsed_s=elapsed)


def parse_window(
    start: str | None, end: str | None, days: float | None
) -> tuple[datetime | None, datetime | None]:
    def p(v: str) -> datetime:
        dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            raise ValueError(f"timestamp needs a zone: {v}")
        return dt.astimezone(UTC)

    s = p(start) if start else None
    e = p(end) if end else None
    if days is not None:
        if s is None:
            raise ValueError("--days needs --start")
        e = s + timedelta(days=days)
    return s, e
