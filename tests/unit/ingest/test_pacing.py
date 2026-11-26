"""AC-012-21, -22, -29: pacing a publication (``runner.pace``), the publisher's lateness behind its
schedule (``runner.Lateness``), history ``replay --speed``, and ``pacing`` in the run manifests.

No Kafka and no wall clock. ``FakeTime`` is a simulated monotonic and Unix clock that only moves
when the code under test sleeps through it or a ``TimedSink`` publishes (each publish costs
``PUBLISH_COST_S`` of fake time, so lateness builds up within a batch of events due together).
It reaches the runner through three seams of ``ems.ingest.runner``, all looked up at call time:

- ``runner.time``: ``monotonic`` (``wall0``) and ``time`` (``paced_from_unix``), taken together;
- ``runner.asyncio``: ``sleep`` (only so that the pre-``pace`` code cannot sleep for real);
- ``runner.pace``: wrapped to inject ``monotonic=`` and ``sleep=`` (its defaults are bound at
  import) and to note every ``due`` it is given.

History recordings come from ``history_recording.py`` (fetch through respx, then build); a spec
002 recording from ``record`` against respx, with its ``ingested_at`` spread out afterwards so that
pacing by ``ingested_at`` has something to wait for. The names that arrive with T25 (``pace``,
``Lateness``) are imported inside the tests.
"""

from __future__ import annotations

import asyncio
import json
import math
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from itertools import pairwise
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest
import respx
import typer

from ems.contracts.envelope import to_json_bytes
from ems.ingest import cli, runner
from ems.ingest.generator import SyntheticModel, emissions, load_generator_config, load_sites
from ems.ingest.kafka.sink import SinkStats
from ems.ingest.runner import RunResult, record, replay, run
from ems.ingest.sources.recording import as_replay, read_events
from ems.settings import EmsSettings
from tests.unit.ingest.history_recording import (
    CONFIG,
    POZNAN,
    RADOM,
    Recording,
    build,
    copy_recording,
    make_recording,
    window,
)
from tests.unit.ingest.test_recording_hybrid import NOW, live_cfg, mock_sources
from tests.unit.ingest.test_recording_hybrid import settings as live_settings

T0 = 5_000.0  # the fake monotonic clock (s) when a run or replay starts: its wall0
UNIX_T0 = datetime(2026, 11, 25, 9, tzinfo=UTC).timestamp()  # the fake Unix clock at T0 (s)
SPEED = 96.0  # one day of data in 15 minutes
RUN_SPEED = 3600.0  # one hour of data per second
PUBLISH_COST_S = 0.0007  # fake time per publish; k x 0.7 ms never lands on a bucket edge
EPS_S = 1e-6  # float slack on fake times of about 5000 s; far below any bucket width
EDGES_S = (
    0.001,
    0.002,
    0.005,
    0.01,
    0.02,
    0.05,
    0.1,
    0.2,
    0.5,
    1.0,
    2.0,
    5.0,
    10.0,
    20.0,
    60.0,
    math.inf,
)

REPLAY_NOW = datetime(2026, 11, 13, 12, tzinfo=UTC)  # injected: the replay's "now"
BG_SEED, BG_RUN_ID = 99, "pace-run"
RUN_START = datetime(2026, 10, 20, tzinfo=UTC)
RUN_END = RUN_START + timedelta(hours=6)
FIXED = datetime(2026, 10, 22, 12, tzinfo=UTC)  # the runner's event clock (ingested_at)
INGEST_OFFSETS_S = (0.0, 600.0, 1500.0)  # spec 002 events, ingested 0, 10 and 25 min apart
SPEC002_SPEED = 60.0


def _pace() -> Callable[..., Any]:
    from ems.ingest.runner import pace

    return pace


def _lateness() -> Any:
    from ems.ingest.runner import Lateness

    return Lateness()


# ── fakes ────────────────────────────────────────────────────────────────────────────────────────


def _public(module: ModuleType) -> dict[str, Any]:
    return {k: getattr(module, k) for k in dir(module) if not k.startswith("_")}


@dataclass
class FakeTime:
    """Simulated monotonic and Unix clocks; ``oversleep_s`` makes every real sleep run long."""

    t: float = T0
    oversleep_s: float = 0.0
    log: list[tuple[str, float]] = field(default_factory=list)  # ("sleep", s) and ("poll", timeout)
    dues: list[float] = field(default_factory=list)  # every ``due`` given to ``runner.pace``

    def monotonic(self) -> float:
        return self.t

    def unix(self) -> float:
        return UNIX_T0 + (self.t - T0)

    async def sleep(self, seconds: float) -> None:
        self.log.append(("sleep", seconds))
        if seconds > 0:
            self.t += seconds + self.oversleep_s

    @property
    def sleeps(self) -> list[float]:
        """The sleeps that wait (an ``asyncio.sleep(0)`` only yields)."""
        return [s for kind, s in self.log if kind == "sleep" and s > 0]

    def install(self, mp: pytest.MonkeyPatch) -> FakeTime:
        clocks = {"monotonic": self.monotonic, "time": self.unix}
        mp.setattr(runner, "time", SimpleNamespace(**(_public(time) | clocks)))
        mp.setattr(runner, "asyncio", SimpleNamespace(**(_public(asyncio) | {"sleep": self.sleep})))
        real = getattr(runner, "pace", None)
        if real is not None:  # before T25 there is no pace to wrap

            async def pace(due: float, sink: Any, **kwargs: Any) -> float:
                self.dues.append(due)
                kwargs.setdefault("monotonic", self.monotonic)
                kwargs.setdefault("sleep", self.sleep)
                return await real(due, sink, **kwargs)

            mp.setattr(runner, "pace", pace)
        return self


class TimedSink:
    """In-memory sink (the runner's Sink protocol) noting the fake time of every publish."""

    def __init__(self, clock: FakeTime, cost_s: float = 0.0) -> None:
        self.clock = clock
        self.cost_s = cost_s
        self.events: list[Any] = []
        self.times: list[float] = []
        self.stats = SinkStats()

    def publish(self, event: Any) -> bool:
        self.times.append(self.clock.t)
        self.events.append(event)
        self.clock.t += self.cost_s
        return True

    def poll(self, timeout: float = 0) -> None:
        self.clock.log.append(("poll", timeout))

    def close(self, timeout_s: float = 30.0) -> SinkStats:
        return self.stats


@dataclass
class Published:
    result: RunResult
    sink: TimedSink
    clock: FakeTime


@contextmanager
def _fake_time() -> Iterator[FakeTime]:
    with pytest.MonkeyPatch.context() as mp:
        yield FakeTime().install(mp)


def _manifest(path: Path | None) -> dict[str, Any]:
    assert path is not None, "the run wrote no manifest"

    def reject(constant: str) -> float:
        raise AssertionError(f"the manifest holds {constant}")

    return json.loads(path.read_text(), parse_constant=reject)


def _schedule(due: list[float], cost_s: float) -> list[float]:
    """When each event goes out if it waits for its due time and nothing else delays it: a
    publish takes ``cost_s``, so an event due with others goes out after them.
    """
    out: list[float] = []
    prev = -math.inf
    for d in due:
        prev = max(d, prev + cost_s)
        out.append(prev)
    return out


def _edge(x: float) -> float:
    return next(e for e in EDGES_S if x <= e)


def _p99_bounds(values: list[float]) -> tuple[float, float]:
    """The 99th percentile lies between these two order statistics under the usual definitions
    (linear interpolation at the low end, nearest rank at the high end).
    """
    s = sorted(values)
    n = len(s)
    return s[math.floor(0.99 * (n - 1))], s[math.ceil(0.99 * n) - 1]


# ── pace ─────────────────────────────────────────────────────────────────────────────────────────


async def test_ac_012_22_pace_sleeps_in_half_second_slices_until_due() -> None:
    clock = FakeTime()
    sink = TimedSink(clock)
    due = T0 + 1.7
    late = await _pace()(due, sink, monotonic=clock.monotonic, sleep=clock.sleep)
    assert clock.t >= due
    assert late == pytest.approx(0.0, abs=EPS_S)
    assert clock.sleeps and all(0.0 < s <= 0.5 for s in clock.sleeps)
    assert math.fsum(clock.sleeps) == pytest.approx(1.7, abs=EPS_S)
    assert sink.events == []  # pace waits; it never publishes


async def test_ac_012_22_pace_polls_the_sink_between_slices() -> None:
    clock = FakeTime()
    await _pace()(T0 + 2.2, TimedSink(clock), monotonic=clock.monotonic, sleep=clock.sleep)
    kinds = [kind for kind, s in clock.log if not (kind == "sleep" and s == 0)]
    assert kinds.count("sleep") >= 5  # 2.2 s in slices of at most 0.5 s
    assert not [pair for pair in pairwise(kinds) if pair == ("sleep", "sleep")]
    assert {timeout for kind, timeout in clock.log if kind == "poll"} == {0}


async def test_ac_012_22_pace_past_due_returns_at_once_with_its_lateness() -> None:
    clock = FakeTime()
    late = await _pace()(T0 - 2.25, TimedSink(clock), monotonic=clock.monotonic, sleep=clock.sleep)
    assert late == 2.25
    assert (clock.t, clock.sleeps) == (T0, [])


async def test_ac_012_22_pace_exactly_at_due_returns_zero_without_sleeping() -> None:
    clock = FakeTime()
    late = await _pace()(T0, TimedSink(clock), monotonic=clock.monotonic, sleep=clock.sleep)
    assert late == 0.0
    assert clock.sleeps == []


async def test_ac_012_29_pace_returns_how_late_it_woke() -> None:
    """A busy event loop wakes each sleep 0.3 s late: 0.5 s, then 0.2 s, lands 0.3 s past due."""
    clock = FakeTime(oversleep_s=0.3)
    late = await _pace()(T0 + 1.0, TimedSink(clock), monotonic=clock.monotonic, sleep=clock.sleep)
    assert all(0.0 < s <= 0.5 for s in clock.sleeps)
    assert late == pytest.approx(0.3, abs=EPS_S)
    assert late == pytest.approx(clock.t - (T0 + 1.0), abs=EPS_S)


# ── Lateness ─────────────────────────────────────────────────────────────────────────────────────


def test_ac_012_29_lateness_when_empty_is_all_zero() -> None:
    assert _lateness().as_dict() == {"count": 0, "max_s": 0.0, "p99_s": 0.0}


@pytest.mark.parametrize(
    ("seconds", "edge_s"),
    [
        (0.0, 0.001),
        (0.0004, 0.001),
        (0.0015, 0.002),
        (0.003, 0.005),
        (0.007, 0.01),
        (0.015, 0.02),
        (0.03, 0.05),
        (0.07, 0.1),
        (0.15, 0.2),
        (0.3, 0.5),
        (0.7, 1.0),
        (1.5, 2.0),
        (3.0, 5.0),
        (7.0, 10.0),
        (15.0, 20.0),
        (45.0, 60.0),
    ],
)
def test_ac_012_29_lateness_p99_is_the_upper_edge_of_its_bucket(
    seconds: float, edge_s: float
) -> None:
    lateness = _lateness()
    for _ in range(10):
        lateness.add(seconds)
    assert lateness.as_dict() == {"count": 10, "max_s": seconds, "p99_s": edge_s}


def test_ac_012_29_lateness_beyond_the_last_edge_reports_the_exact_max() -> None:
    lateness = _lateness()
    for seconds in (61.5, 75.25, 0.0004):
        lateness.add(seconds)
    out = lateness.as_dict()
    assert out == {"count": 3, "max_s": 75.25, "p99_s": 75.25}
    json.dumps(out, allow_nan=False)  # no infinity reaches a manifest


@pytest.mark.parametrize(
    ("slow", "p99_s"),
    [
        (20, 5.0),  # 2 % of 1000 are 3 s late: the 99th percentile is among them
        (5, 0.001),  # 0.5 %: the 99th percentile is still on time
    ],
)
def test_ac_012_29_lateness_p99_follows_the_tail(slow: int, p99_s: float) -> None:
    lateness = _lateness()
    for _ in range(1000 - slow):
        lateness.add(0.0004)
    for _ in range(slow):
        lateness.add(3.0)
    assert lateness.as_dict() == {"count": 1000, "max_s": 3.0, "p99_s": p99_s}


def test_ac_012_29_lateness_keeps_the_exact_max() -> None:
    lateness = _lateness()
    for seconds in (0.3, 7.25, 0.01, 0.0):
        lateness.add(seconds)
    out = lateness.as_dict()
    assert (out["count"], out["max_s"]) == (4, 7.25)


# ── history replay --speed (AC-012-22) ───────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def history(tmp_path_factory: pytest.TempPathFactory) -> Recording:
    """A one-day history recording of two real-data sites, with a fleet of 18 for --background."""
    rec = make_recording(
        tmp_path_factory.mktemp("history"),
        date(2026, 5, 15),
        date(2026, 5, 16),
        name="pace-test",
        base_config=CONFIG / "generator.history.yaml",
        locations=(POZNAN, RADOM),
        synthetic_locations=20,
    )
    build(rec, seed=BG_SEED, run_id=BG_RUN_ID)
    return rec


def _replay_history(rec: Recording, *, speed: float | None, background: bool) -> Published:
    with _fake_time() as clock:
        sink = TimedSink(clock, PUBLISH_COST_S)
        result = asyncio.run(
            replay(
                rec.settings,
                rec.name,
                sink=sink,
                speed=speed,
                clock=lambda: REPLAY_NOW,
                check_topics=False,
                background=background,
            )
        )
    return Published(result, sink, clock)


def _history_due(rec: Recording, events: list[Any], speed: float) -> list[float]:
    """``wall0 + (emission time - window start) / speed`` for every published event."""
    from ems.ingest.history.build import emission_key

    start, _end = window(rec.root)
    cfg = load_generator_config(rec.root / "generator.yaml")
    delay = timedelta(seconds=cfg.meter_delay_s)
    return [T0 + (emission_key(e, start, delay)[0] - start).total_seconds() / speed for e in events]


@pytest.mark.parametrize("background", [False, True], ids=["recording", "with-background"])
def test_ac_012_22_history_replay_with_speed_publishes_the_same_events_in_the_same_order(
    history: Recording, tmp_path: Path, background: bool
) -> None:
    rec = copy_recording(history, tmp_path)
    unpaced = _replay_history(rec, speed=None, background=background)
    paced = _replay_history(rec, speed=SPEED, background=background)
    assert len(paced.sink.events) == len(unpaced.sink.events) > 0
    assert paced.sink.events == unpaced.sink.events
    assert paced.result.counts == unpaced.result.counts


@pytest.mark.parametrize("background", [False, True], ids=["recording", "with-background"])
def test_ac_012_22_history_replay_publishes_no_event_before_its_due_time(
    history: Recording, tmp_path: Path, background: bool
) -> None:
    rec = copy_recording(history, tmp_path)
    paced = _replay_history(rec, speed=SPEED, background=background)
    due = _history_due(rec, paced.sink.events, SPEED)
    early = [(t, d) for t, d in zip(paced.sink.times, due, strict=True) if t < d - EPS_S]
    assert early == []
    # … and as soon as it may: the fake clock moves only in pace's sleeps and in publishing.
    expected = _schedule(due, PUBLISH_COST_S)
    behind = max(abs(t - e) for t, e in zip(paced.sink.times, expected, strict=True))
    assert behind <= EPS_S
    assert due[-1] - T0 > 0.9 * 86_400 / SPEED  # the window really was spread out


def test_ac_012_22_history_replay_paces_every_event_through_pace(
    history: Recording, tmp_path: Path
) -> None:
    rec = copy_recording(history, tmp_path)
    paced = _replay_history(rec, speed=SPEED, background=True)
    due = _history_due(rec, paced.sink.events, SPEED)
    assert paced.clock.dues == pytest.approx(due, abs=EPS_S)


def test_ac_012_22_history_replay_manifest_records_its_pacing(
    history: Recording, tmp_path: Path
) -> None:
    rec = copy_recording(history, tmp_path)
    paced = _replay_history(rec, speed=SPEED, background=False)
    manifest = _manifest(paced.result.manifest)
    assert "pacing" in manifest
    pacing = manifest["pacing"]
    assert pacing["paced_from_unix"] == UNIX_T0  # time.time() taken with wall0
    assert pacing["speed"] == SPEED
    due = _history_due(rec, paced.sink.events, SPEED)
    observed = [max(0.0, t - d) for t, d in zip(paced.sink.times, due, strict=True)]
    lateness = pacing["lateness"]
    assert lateness["count"] == len(paced.sink.events)
    assert max(observed) > 0  # events due together queue behind each other
    assert lateness["max_s"] == pytest.approx(max(observed), abs=EPS_S)
    lo, hi = _p99_bounds(observed)
    assert _edge(lo) <= lateness["p99_s"] <= _edge(hi)


def test_ac_012_22_unpaced_history_replay_records_no_pacing(
    history: Recording, tmp_path: Path
) -> None:
    rec = copy_recording(history, tmp_path)
    unpaced = _replay_history(rec, speed=None, background=False)
    manifest = _manifest(unpaced.result.manifest)
    assert "pacing" in manifest
    assert manifest["pacing"] is None
    assert unpaced.clock.sleeps == []


def test_ac_012_22_cli_speed_help_no_longer_excludes_history() -> None:
    command: Any = typer.main.get_command(cli.app)
    (speed,) = [p for p in command.commands["replay"].params if p.name == "speed"]
    assert speed.help
    assert "not for history" not in speed.help


# ── run manifests (AC-012-21) ────────────────────────────────────────────────────────────────────


def _run(tmp_path: Path, mode: str, speed: float | None) -> Published:
    settings = EmsSettings(config_dir=CONFIG, data_dir=tmp_path)
    cfg = load_generator_config(CONFIG / "generator.yaml")
    with _fake_time() as clock:
        sink = TimedSink(clock, PUBLISH_COST_S)
        result = asyncio.run(
            run(
                settings,
                cfg,
                mode=mode,
                start=RUN_START,
                end=RUN_END,
                run_id="pace-1",
                speed=speed,
                sink=sink,
                check_topics=False,
                clock=lambda: FIXED,
            )
        )
    return Published(result, sink, clock)


def test_ac_012_21_accelerated_run_publishes_on_its_schedule(tmp_path: Path) -> None:
    published = _run(tmp_path, "accelerated", RUN_SPEED)
    cfg = load_generator_config(CONFIG / "generator.yaml")
    model = SyntheticModel(load_sites(CONFIG, cfg), cfg.seed, CONFIG)
    emit_at = [at for at, _s, _b in emissions(model, cfg, RUN_START, RUN_END, "pace-1")]
    assert len(emit_at) == len(published.sink.events) > 0
    due = [T0 + (at - RUN_START).total_seconds() / RUN_SPEED for at in emit_at]
    early = [(t, d) for t, d in zip(published.sink.times, due, strict=True) if t < d - EPS_S]
    assert early == []
    assert published.sink.times[-1] >= T0 + 6 * 3600 / RUN_SPEED - EPS_S


def test_ac_012_21_accelerated_run_manifest_records_its_pacing(tmp_path: Path) -> None:
    published = _run(tmp_path, "accelerated", RUN_SPEED)
    manifest = _manifest(published.result.manifest)
    assert "pacing" in manifest
    pacing = manifest["pacing"]
    assert pacing["paced_from_unix"] == UNIX_T0  # time.time() taken with wall0
    assert pacing["speed"] == RUN_SPEED
    cfg = load_generator_config(CONFIG / "generator.yaml")
    model = SyntheticModel(load_sites(CONFIG, cfg), cfg.seed, CONFIG)
    emit_at = [at for at, _s, _b in emissions(model, cfg, RUN_START, RUN_END, "pace-1")]
    due = [T0 + (at - RUN_START).total_seconds() / RUN_SPEED for at in emit_at]
    observed = [max(0.0, t - d) for t, d in zip(published.sink.times, due, strict=True)]
    lateness = pacing["lateness"]
    assert lateness["count"] == len(published.sink.events) == sum(published.result.counts.values())
    assert lateness["max_s"] == pytest.approx(max(observed), abs=EPS_S)
    assert lateness["p99_s"] in EDGES_S[:-1] or lateness["p99_s"] == lateness["max_s"]
    assert lateness["p99_s"] <= _edge(lateness["max_s"])


def test_ac_012_21_backfill_manifest_records_no_pacing(tmp_path: Path) -> None:
    published = _run(tmp_path, "backfill", None)
    manifest = _manifest(published.result.manifest)
    assert "pacing" in manifest
    assert manifest["pacing"] is None
    assert published.clock.sleeps == []  # backfill does not wait


# ── spec 002 replay: unchanged, paced by ingested_at (AC-012-22) ─────────────────────────────────


@pytest.fixture
def spec002(tmp_path: Path) -> tuple[EmsSettings, str]:
    """A spec 002 recording (live sources recorded once), ingested 0, 10 and 25 minutes apart."""
    s = live_settings(tmp_path)
    with respx.mock:
        mock_sources()
        asyncio.run(record(s, live_cfg(), name="t", clock=lambda: NOW))
    root = s.data_dir / "recordings" / "t"
    events = list(read_events(root))
    assert len(events) == len(INGEST_OFFSETS_S)
    spread = [
        e.model_copy(update={"ingested_at": NOW + timedelta(seconds=dt)})
        for e, dt in zip(events, INGEST_OFFSETS_S, strict=True)
    ]
    (root / "events.jsonl").write_bytes(b"".join(to_json_bytes(e) + b"\n" for e in spread))
    return s, "t"


def _replay_spec002(s: EmsSettings, name: str) -> Published:
    with _fake_time() as clock:
        sink = TimedSink(clock, PUBLISH_COST_S)
        result = asyncio.run(
            replay(
                s,
                name,
                sink=sink,
                speed=SPEC002_SPEED,
                clock=lambda: NOW + timedelta(days=1),
                check_topics=False,
            )
        )
    return Published(result, sink, clock)


def test_ac_012_22_spec_002_replay_still_paces_by_ingested_at(
    spec002: tuple[EmsSettings, str],
) -> None:
    s, name = spec002
    recorded = list(read_events(s.data_dir / "recordings" / name))
    published = _replay_spec002(s, name)
    assert published.sink.events == [as_replay(e, NOW + timedelta(days=1)) for e in recorded]
    due = [T0 + dt / SPEC002_SPEED for dt in INGEST_OFFSETS_S]
    expected = _schedule(due, PUBLISH_COST_S)
    assert published.sink.times == pytest.approx(expected, abs=EPS_S)


def test_ac_012_22_spec_002_replay_paces_through_pace(spec002: tuple[EmsSettings, str]) -> None:
    s, name = spec002
    published = _replay_spec002(s, name)
    due = [T0 + dt / SPEC002_SPEED for dt in INGEST_OFFSETS_S]
    assert published.clock.dues == pytest.approx(due, abs=EPS_S)


# ── preflight of a realtime run (AC-012-36: sys-live) ────────────────────────────────────────────


@pytest.mark.parametrize(("mode", "capped"), [("realtime", True), ("accelerated", False)])
def test_ac_012_36_realtime_preflight_checks_only_records_due_by_now(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, mode: str, capped: bool
) -> None:
    # A realtime run publishes each record at its own timestamp, so a window ending hours ahead
    # never puts a record ahead of the wall clock: the live profile's after.max must not refuse it.
    seen: list[tuple[datetime, datetime, datetime]] = []
    monkeypatch.setattr(
        runner, "preflight_topics", lambda _s, lo, hi, now: seen.append((lo, hi, now))
    )
    settings = EmsSettings(config_dir=CONFIG, data_dir=tmp_path)
    cfg = load_generator_config(CONFIG / "generator.yaml")
    model = SyntheticModel(load_sites(CONFIG, cfg), cfg.seed, CONFIG)
    now = RUN_START + timedelta(minutes=10)
    end = RUN_START + timedelta(hours=2)
    runner.preflight(settings, model, cfg, RUN_START, end, now, realtime=mode == "realtime")
    [(lo, hi, at)] = seen
    assert at == now and lo <= RUN_START
    assert (hi == now) if capped else (hi >= end)
