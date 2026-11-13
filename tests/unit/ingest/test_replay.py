"""AC-008-17, -18: ``ems-gen replay`` behind the topic preflight, history checks, ``--background``.

No Kafka: ``KafkaTopicAdmin`` is stubbed to report the ``live`` profile's topic configs (as the
broker would after ``make topics PROFILE=live``) and events go to an in-memory sink. History
recordings come from ``history_recording.py`` (fetch through respx, then build); the names that
arrive with T19 (``ReplayRefused``, ``check_topics``, ``background``) are used inside the tests.
"""

from __future__ import annotations

import asyncio
import json
import re
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import respx
from typer.testing import CliRunner

from ems.contracts.ids import location_seed, origin_source
from ems.contracts.topics import load_plan
from ems.ingest import cli, runner
from ems.ingest.generator import load_generator_config, load_sites
from ems.ingest.kafka.admin import ActualTopic, KafkaTopicAdmin
from ems.ingest.runner import PreflightError, record, replay
from tests.unit.ingest.history_recording import (
    CONFIG,
    MAPPINGS,
    POZNAN,
    RADOM,
    Recording,
    build,
    build_info,
    copy_config,
    copy_recording,
    make_recording,
    read_built,
    window,
)
from tests.unit.ingest.test_recording_hybrid import NOW, Collect, live_cfg, mock_sources
from tests.unit.ingest.test_recording_hybrid import settings as live_settings

REPLAY_NOW = datetime(2026, 11, 13, 12, tzinfo=UTC)  # injected: the replay's "now"
BG_SEED, BG_RUN_ID = 99, "bg-run"  # build options, so replay must read them from build.json


def _refused() -> type[Exception]:
    from ems.ingest.runner import ReplayRefused

    return ReplayRefused


# ── stubs ────────────────────────────────────────────────────────────────────────────────────────


class LiveTopics:
    """``KafkaTopicAdmin`` against a broker whose topics were created with the live profile."""

    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def install(self, monkeypatch: pytest.MonkeyPatch) -> LiveTopics:
        stub = self

        def init(self: Any, bootstrap: str, timeout_s: float = 30.0) -> None:
            self.bootstrap = bootstrap  # no AdminClient: nothing may connect

        def actual(self: Any, names: Any) -> dict[str, ActualTopic]:
            wanted = list(names)
            stub.calls.append(wanted)
            out = {}
            for name in wanted:
                prefix = name[: name.index("ems.")] if "ems." in name else ""
                desired = load_plan(CONFIG, "live", prefix).by_name().get(name)
                if desired is not None:
                    out[name] = ActualTopic(
                        name, desired.partitions, desired.replication_factor, dict(desired.configs)
                    )
            return out

        monkeypatch.setattr(KafkaTopicAdmin, "__init__", init)
        monkeypatch.setattr(KafkaTopicAdmin, "actual", actual)
        return self


@pytest.fixture
def live_topics(monkeypatch: pytest.MonkeyPatch) -> LiveTopics:
    return LiveTopics().install(monkeypatch)


# ── recordings ───────────────────────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def history(tmp_path_factory: pytest.TempPathFactory) -> Recording:
    """A one-day history recording with ``generator.history.yaml``'s sites and fleet, built with
    a seed and a run_id that differ from the defaults.
    """
    rec = make_recording(
        tmp_path_factory.mktemp("history"),
        date(2026, 5, 15),
        date(2026, 5, 16),
        name="spring-test",
        base_config=CONFIG / "generator.history.yaml",
        locations=(POZNAN, RADOM),
        synthetic_locations=20,
    )
    build(rec, seed=BG_SEED, run_id=BG_RUN_ID)
    return rec


@pytest.fixture
def spec002(tmp_path: Path) -> tuple[Any, str]:
    """A spec 002 recording (live sources recorded once, 2026-10-20)."""
    s = live_settings(tmp_path)
    with respx.mock:
        mock_sources()
        asyncio.run(record(s, live_cfg(), name="t", clock=lambda: NOW))
    return s, "t"


# ── AC-008-17: the topic preflight ───────────────────────────────────────────────────────────────


def test_ac_008_17_history_replay_into_live_topics_is_refused(
    history: Recording, live_topics: LiveTopics
) -> None:
    s = history.settings.model_copy(update={"topics_profile": "live"})
    sink = Collect()
    with pytest.raises(PreflightError, match=r"(?i)retention|before\.max|sim profile"):
        asyncio.run(replay(s, history.name, sink=sink, clock=lambda: REPLAY_NOW))
    assert sink.events == []  # refused before any publish
    assert live_topics.calls  # the broker's topic configs were consulted


def test_ac_008_17_spec_002_recording_is_preflighted_too(
    spec002: tuple[Any, str], live_topics: LiveTopics
) -> None:
    """Thirty days after recording, a forecast is older than the live retention (7 days)."""
    s, name = spec002
    sink = Collect()
    later = NOW + timedelta(days=30)
    with pytest.raises(PreflightError):
        asyncio.run(replay(s, name, sink=sink, clock=lambda: later))
    assert sink.events == []
    assert live_topics.calls


def test_ac_008_17_cli_replay_refused_by_preflight_exits_1(
    spec002: tuple[Any, str], live_topics: LiveTopics, monkeypatch: pytest.MonkeyPatch
) -> None:
    s, name = spec002
    sink = Collect()
    monkeypatch.setattr(runner, "KafkaSink", lambda *_a, **_k: sink)  # never a real producer
    monkeypatch.setenv("EMS_DATA_DIR", str(s.data_dir))
    monkeypatch.setenv("EMS_CONFIG_DIR", str(CONFIG))
    monkeypatch.setenv("EMS_MAPPINGS_DIR", str(MAPPINGS))
    monkeypatch.setenv("EMS_TOPICS_PROFILE", "live")
    monkeypatch.setenv("EMS_TOPIC_PREFIX", "")
    result = CliRunner().invoke(cli.app, ["replay", "--name", name])
    assert result.exit_code == 1, result.output
    assert isinstance(result.exception, SystemExit)  # a refusal, not a crash
    assert re.search(r"(?i)preflight", result.output)
    assert sink.events == []


def test_ac_008_17_replay_keeps_ids_and_marks_replay(history: Recording) -> None:
    recorded = read_built(history.root)
    sink = Collect()
    asyncio.run(
        replay(
            history.settings,
            history.name,
            sink=sink,
            clock=lambda: REPLAY_NOW,
            check_topics=False,
        )
    )
    assert [e.event_id for e in sink.events] == [e.event_id for e in recorded]
    pairs = zip(sink.events, recorded, strict=True)
    assert all(e.source_system == f"replay:{r.source_system}" for e, r in pairs)


# ── history checks ───────────────────────────────────────────────────────────────────────────────


def test_ac_008_17_changed_events_file_is_refused(history: Recording, tmp_path: Path) -> None:
    rec = copy_recording(history, tmp_path)
    path = rec.root / "events.jsonl"
    data = bytearray(path.read_bytes())
    i = data.index(b'"price":') + len(b'"price":')
    while not chr(data[i]).isdigit():
        i += 1
    data[i] = ord(str((int(chr(data[i])) + 1) % 10))  # one digit of one price, still valid JSON
    path.write_bytes(bytes(data))
    sink = Collect()
    with pytest.raises(_refused()):
        asyncio.run(
            replay(rec.settings, rec.name, sink=sink, clock=lambda: REPLAY_NOW, check_topics=False)
        )
    assert sink.events == []


def test_ac_008_17_speed_is_refused_for_history_recordings(history: Recording) -> None:
    """``--speed`` paces by ``ingested_at``, which is a fetch time in a history recording."""
    sink = Collect()
    with pytest.raises(_refused()):
        asyncio.run(
            replay(
                history.settings,
                history.name,
                sink=sink,
                speed=96.0,
                clock=lambda: REPLAY_NOW,
                check_topics=False,
            )
        )
    assert sink.events == []


def test_ac_008_18_background_needs_a_history_recording(spec002: tuple[Any, str]) -> None:
    s, name = spec002
    sink = Collect()
    with pytest.raises(_refused()):
        asyncio.run(
            replay(
                s,
                name,
                sink=sink,
                clock=lambda: NOW + timedelta(days=1),
                check_topics=False,
                background=True,
            )
        )
    assert sink.events == []


# ── AC-008-18: --background ──────────────────────────────────────────────────────────────────────


def test_ac_008_18_background_fleet_around_the_real_data_sites(
    history: Recording, tmp_path: Path
) -> None:
    from ems.ingest.history.build import emission_key

    rec = copy_recording(history, tmp_path)
    recorded = read_built(rec.root)
    info = build_info(rec.root)
    assert (info["seed"], info["run_id"]) == (BG_SEED, BG_RUN_ID)

    sink = Collect()
    result = asyncio.run(
        replay(
            rec.settings,
            rec.name,
            sink=sink,
            clock=lambda: REPLAY_NOW,
            check_topics=False,
            background=True,
        )
    )
    published = sink.events
    cfg = load_generator_config(rec.root / "generator.yaml")
    real = set(cfg.live_locations)
    fleet = {s.location_id for s in load_sites(CONFIG, cfg)} - real
    assert real == {POZNAN, RADOM} and len(fleet) == 18

    background = [e for e in published if origin_source(e.source_system) == "synthetic"]
    from_recording = [e for e in published if origin_source(e.source_system) != "synthetic"]
    # The recording's events, all of them and only them …
    assert sorted(str(e.event_id) for e in from_recording) == sorted(
        str(e.event_id) for e in recorded
    )
    # … and a fleet of every other catalog site, never a real-data site.
    assert {e.location_id for e in background} == fleet
    assert not [e for e in background if e.location_id in real]
    # One price source: the recording's PSE days.
    prices = [e for e in published if e.stream.value == "price"]
    assert prices and {origin_source(e.source_system) for e in prices} == {"pse-rce"}
    assert len(prices) == len([e for e in recorded if e.stream.value == "price"])
    # Seed and run_id from build.json (site seeds derive from the global seed).
    assert {e.run_id for e in background} == {BG_RUN_ID}
    assert all(e.seed == location_seed(BG_SEED, e.location_id) for e in background)
    assert sum(result.counts.values()) == len(published)

    start, _end = window(rec.root)
    meter_delay = timedelta(seconds=cfg.meter_delay_s)
    keys = [emission_key(e, start, meter_delay) for e in published]
    assert keys == sorted(keys)  # merged in emission order

    manifests = [
        json.loads(p.read_text()) for p in (rec.settings.data_dir / "runs").glob("*/manifest.json")
    ]
    (run,) = [m for m in manifests if m.get("mode") == "replay"]
    assert (run["run_id"], run["seed"]) == (BG_RUN_ID, BG_SEED)
    assert run["window"]["start"].startswith("2026-05-15")


@pytest.mark.parametrize("changed", ["load.yaml", "prices.yaml"])
def test_ac_008_18_background_with_a_changed_calibration_is_refused(
    history: Recording, tmp_path: Path, changed: str
) -> None:
    config_dir = copy_config(tmp_path)
    path = config_dir / "calibration" / changed
    path.write_text(path.read_text() + "# changed by a test\n")
    rec = copy_recording(history, tmp_path, config_dir=config_dir)
    sink = Collect()
    with pytest.raises(_refused()):
        asyncio.run(
            replay(
                rec.settings,
                rec.name,
                sink=sink,
                clock=lambda: REPLAY_NOW,
                check_topics=False,
                background=True,
            )
        )
    assert sink.events == []
    assert not (rec.settings.data_dir / "runs").exists()
