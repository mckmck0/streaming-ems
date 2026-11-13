"""Run the generator against Kafka: preflight, pacing per clock mode, manifest (principle 14)."""

from __future__ import annotations

import asyncio
import dataclasses
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
from typing import Any

import structlog

from ems.contracts.time import floor_quarter
from ems.contracts.topics import STREAM_TOPICS, load_plan
from ems.ingest.faults import FaultConfig, apply_faults
from ems.ingest.generator import (
    GeneratorConfig,
    SyntheticModel,
    emissions,
    load_generator_config,
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


def preflight_topics(settings: EmsSettings, lo: datetime, hi: datetime, now: datetime) -> None:
    """Refuse unless every target topic exists and accepts and keeps records timed ``lo`` …
    ``hi`` (K1). Used by ``run`` and ``replay``.
    """
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
    found = problems({n: a.configs for n, a in actual.items()}, min_ts=lo, max_ts=hi, now=now)
    if found:
        raise PreflightError("; ".join(found))


def preflight(
    settings: EmsSettings,
    model: SyntheticModel,
    cfg: GeneratorConfig,
    start: datetime,
    end: datetime | None,
    now: datetime,
) -> None:
    lo, hi = record_time_bounds(model, cfg, start, end or now)
    preflight_topics(settings, lo, hi, now)


async def paced_publish(
    model: SyntheticModel,
    cfg: GeneratorConfig,
    start: datetime,
    end: datetime | None,
    run_id: str | None,
    sink: Sink,
    *,
    mode: str,
    speed: float,
    clock: Clock,
    counts: Counter[str],
    wall0: float,
    max_events: int | None = None,
) -> None:
    """Publish the synthetic emissions; in non-backfill modes, pace them against the wall clock."""
    source = emissions(model, cfg, start, end, run_id)
    faults = FaultConfig.from_dict(cfg.faults)
    if faults.enabled:
        source = apply_faults(source, faults, cfg.seed)
    for emit_at, stream, build in source:
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
        await paced_publish(
            model,
            cfg,
            start,
            end,
            run_id,
            sink,
            mode=mode,
            speed=speed,
            clock=clock,
            counts=counts,
            wall0=wall0,
            max_events=max_events,
        )
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


async def run_hybrid(
    settings: EmsSettings,
    cfg: GeneratorConfig,
    *,
    run_id: str | None = None,
    clock: Clock = wall_clock,
    sink: Sink | None = None,
    recorder: Any = None,
) -> RunResult:
    """Realtime run: live sources for live locations and prices, synthetic data for
    every other site.
    """
    from ems.ingest.sources.live_runner import LiveRunner, mask_synthetic
    from ems.ingest.sources.router import route

    sites = load_sites(settings.config_dir, cfg)
    routing = route(cfg, sites)
    start = floor_quarter(clock())
    sink = (
        sink
        if sink is not None
        else KafkaSink(
            settings.kafka_bootstrap,
            topic_prefix=settings.topic_prefix,
            delivery_timeout_ms=settings.delivery_timeout_ms,
        )
    )
    synth_cfg = mask_synthetic(cfg, routing.price == "live")
    live = LiveRunner(
        settings,
        cfg,
        sink,
        routing.live_sites,
        price_live=routing.price == "live",
        clock=clock,
        run_id=run_id,
        recorder=recorder,
    )
    counts: Counter[str] = Counter()
    wall0 = time.monotonic()
    log.info(
        "hybrid_start",
        live=[s.location_id for s in routing.live_sites],
        synthetic=len(routing.synthetic_sites),
    )
    stats: SinkStats | None = None
    try:
        tasks = [live.run_forever()]
        if routing.synthetic_sites:
            model = SyntheticModel(list(routing.synthetic_sites), cfg.seed, settings.config_dir)
            tasks.append(
                paced_publish(
                    model,
                    synth_cfg,
                    start,
                    None,
                    run_id,
                    sink,
                    mode="realtime",
                    speed=1.0,
                    clock=clock,
                    counts=counts,
                    wall0=wall0,
                )
            )
        await asyncio.gather(*tasks)
    finally:
        stats = sink.close()
        counts.update(live.state.counts)
        manifest = write_manifest(
            settings.data_dir / "runs" / (run_id or f"hybrid-{start:%Y%m%dT%H%M%SZ}"),
            cfg=cfg,
            settings=settings,
            mode="hybrid",
            start=start,
            end=None,
            run_id=run_id,
            counts=counts,
            stats=stats,
            sites=len(sites),
        )
    return RunResult(
        counts=counts, stats=stats, manifest=manifest, elapsed_s=time.monotonic() - wall0
    )


async def record(
    settings: EmsSettings,
    cfg: GeneratorConfig,
    *,
    name: str,
    out_dir: Path | None = None,
    once: bool = True,
    duration_s: float | None = None,
    clock: Clock = wall_clock,
) -> Path:
    """Record live sources (no Kafka): raw responses, canonical events and a manifest."""
    from ems.ingest.sources.live_runner import LiveRunner
    from ems.ingest.sources.recording import Recorder, RecordingSink
    from ems.ingest.sources.router import route

    secrets = [
        s.get_secret_value()
        for s in (settings.open_meteo_api_key, settings.entsoe_token)
        if s is not None
    ]
    root = out_dir or settings.data_dir / "recordings" / name
    recorder = Recorder(root, secrets=secrets)
    sites = load_sites(settings.config_dir, cfg)
    live_cfg = dataclasses.replace(
        cfg,
        streams={**cfg.streams, "price": {"mode": "live"}, "weather_forecast": {"mode": "live"}},
    )
    routing = route(live_cfg, sites)
    runner = LiveRunner(
        settings,
        live_cfg,
        RecordingSink(recorder),
        routing.live_sites,
        price_live=True,
        clock=clock,
        recorder=recorder.raw,
    )
    started = clock()
    try:
        if once:
            await runner.once()
        else:
            await asyncio.wait_for(runner.run_forever(), timeout=duration_s)
    except TimeoutError:
        pass
    finally:
        manifest = recorder.close(
            {
                "kind": "ems-gen-recording",
                "name": name,
                "started": started.isoformat(),
                "ended": clock().isoformat(),
                "live_sites": [s.location_id for s in routing.live_sites],
                "config": cfg.as_dict(),
                "versions": _versions(),
                "git_commit": _git_commit(Path.cwd()),
            }
        )
    log.info("recorded", root=str(root), manifest=str(manifest))
    return root


class ReplayRefused(ValueError):
    """The recording must not be replayed as asked. CLI exit 2."""


@dataclass(frozen=True)
class _History:
    """What a history recording (spec 008) contributes to a replay."""

    cfg: GeneratorConfig
    info: dict[str, Any]
    start: datetime
    end: datetime
    meter_delay: timedelta


def _history(
    settings: EmsSettings, root: Path, *, speed: float | None, background: bool
) -> _History | None:
    """Checks of a history recording; ``None`` for a spec 002 recording."""
    import hashlib

    from ems.ingest.history.build import input_digests, raw_set_digest, read_build_info, window

    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    if manifest.get("kind") != "ems-gen-history":
        if background:
            raise ReplayRefused("--background needs a history recording (ems-gen history build)")
        return None
    if speed:
        raise ReplayRefused(
            "--speed paces by ingested_at, which is a fetch time in a history recording; replay "
            "it without --speed (the consumer orders by availability)"
        )
    try:
        info = read_build_info(root)
    except ValueError as exc:
        raise ReplayRefused(str(exc)) from exc
    events_sha = hashlib.sha256((root / "events.jsonl").read_bytes()).hexdigest()
    if events_sha != info["events_sha256"]:
        raise ReplayRefused(f"{root.name}/events.jsonl no longer matches build.json; rebuild it")
    if raw_set_digest(root) != info["raw_set_digest"]:
        raise ReplayRefused(f"{root.name}: the raw data changed after the build; rebuild it")
    if background:
        now = input_digests(settings, root)
        changed = sorted(k for k, v in now.items() if info["digests"].get(k) != v)
        if changed:
            raise ReplayRefused(
                f"--background generates the fleet now, but {', '.join(changed)} changed since "
                "the build; rebuild under a new --run-id"
            )
    cfg = dataclasses.replace(
        load_generator_config(root / "generator.yaml"), seed=int(info["seed"])
    )
    start, end = window(manifest)
    return _History(cfg, info, start, end, timedelta(seconds=cfg.meter_delay_s))


async def replay(
    settings: EmsSettings,
    name: str,
    *,
    root: Path | None = None,
    speed: float | None = None,
    clock: Clock = wall_clock,
    sink: Sink | None = None,
    check_topics: bool = True,
    background: bool = False,
) -> RunResult:
    """Publish a recording: events keep their ids; ``source_system`` becomes ``replay:<orig>``.

    The topic preflight runs first (K1). A history recording (spec 008) must still match its
    ``build.json``; with ``background`` every other catalog site is generated synthetically over
    its window, merged in emission order, with synthetic prices off.
    """
    import heapq

    from ems.ingest.kafka.sink import record_timestamp
    from ems.ingest.sources.live_runner import mask_synthetic
    from ems.ingest.sources.recording import as_replay, read_events

    root = root or settings.data_dir / "recordings" / name
    hist = _history(settings, root, speed=speed, background=background)
    events = list(read_events(root))
    model: SyntheticModel | None = None
    synth_cfg: GeneratorConfig | None = None
    bg_sites = 0
    if background:
        assert hist is not None
        real = set(hist.cfg.live_locations)
        sites = [s for s in load_sites(settings.config_dir, hist.cfg) if s.location_id not in real]
        bg_sites = len(sites)
        synth_cfg = mask_synthetic(hist.cfg, price_live=True)
        model = SyntheticModel(sites, hist.cfg.seed, settings.config_dir, hist.cfg.site_chunk)
    if check_topics:
        stamps = [record_timestamp(e) for e in events]
        lo, hi = min(stamps, default=clock()), max(stamps, default=clock())
        if model is not None and synth_cfg is not None and hist is not None:
            blo, bhi = record_time_bounds(model, synth_cfg, hist.start, hist.end)
            lo, hi = min(lo, blo), max(hi, bhi)
        preflight_topics(settings, lo, hi, clock())
    sink = (
        sink
        if sink is not None
        else KafkaSink(
            settings.kafka_bootstrap,
            topic_prefix=settings.topic_prefix,
            delivery_timeout_ms=settings.delivery_timeout_ms,
        )
    )
    counts: Counter[str] = Counter()
    wall0 = time.monotonic()
    first: datetime | None = None
    try:
        if hist is not None:
            from ems.ingest.history.build import emission_key

            def key(e: Any) -> Any:
                return emission_key(e, hist.start, hist.meter_delay)

            stream: Any = ((key(e), 0, e) for e in events)
            if model is not None and synth_cfg is not None:
                run_id = str(hist.info["run_id"])
                generated = (
                    build(clock())
                    for _at, _s, build in emissions(model, synth_cfg, hist.start, hist.end, run_id)
                )
                stream = heapq.merge(stream, ((key(e), 1, e) for e in generated))
            for _key, origin, event in stream:
                sink.publish(as_replay(event, clock()) if origin == 0 else event)
                counts[event.stream.value] += 1
                if sum(counts.values()) % 1000 == 0:
                    await asyncio.sleep(0)
        else:
            for event in events:
                if speed:
                    first = first or event.ingested_at
                    due = wall0 + (event.ingested_at - first).total_seconds() / speed
                    while (delay := due - time.monotonic()) > 0:
                        await asyncio.sleep(min(delay, 0.5))
                        sink.poll(0)
                sink.publish(as_replay(event, clock()))
                counts[event.stream.value] += 1
    finally:
        stats = sink.close()
    manifest = None
    if hist is not None:
        manifest = write_manifest(
            settings.data_dir / "runs" / f"replay-{hist.info['run_id']}-{clock():%Y%m%dT%H%M%SZ}",
            cfg=hist.cfg,
            settings=settings,
            mode="replay",
            start=hist.start,
            end=hist.end,
            run_id=str(hist.info["run_id"]),
            counts=counts,
            stats=stats,
            sites=len(hist.cfg.live_locations) + bg_sites,
        )
    return RunResult(
        counts=counts, stats=stats, manifest=manifest, elapsed_s=time.monotonic() - wall0
    )
