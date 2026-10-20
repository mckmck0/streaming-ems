"""AC-001-17/-25/-27/-28: emission schedule, counts, reproducibility, run manifest."""

from __future__ import annotations

import asyncio
import json
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from ems.contracts.envelope import PriceEvent, to_json_bytes, validate_strict
from ems.ingest.generator import SyntheticModel, emissions, load_generator_config, load_sites
from ems.ingest.runner import run
from ems.settings import EmsSettings

REPO = Path(__file__).resolve().parents[3]
START = datetime(2026, 10, 20, tzinfo=UTC)
FIXED = datetime(2026, 10, 22, 12, tzinfo=UTC)


def _model() -> tuple[Any, Any]:
    cfg = load_generator_config(REPO / "config" / "generator.yaml")
    return SyntheticModel(load_sites(REPO / "config", cfg), cfg.seed, REPO / "config"), cfg


def _run(
    start: datetime, days: float, run_id: str | None = None
) -> list[tuple[datetime, str, Any]]:
    model, cfg = _model()
    return [
        (at, stream, build(FIXED))
        for at, stream, build in emissions(model, cfg, start, start + timedelta(days=days), run_id)
    ]


def test_ac_001_27_two_day_backfill_counts_and_order() -> None:
    events = _run(START, 2, "r1")
    counts = Counter(stream for _, stream, _ in events)
    assert counts == {"production": 192, "consumption": 192, "weather_forecast": 16, "price": 3}
    times = [at for at, _, _ in events]
    assert times == sorted(times)
    days = [e.payload.delivery_day.isoformat() for _, s, e in events if s == "price"]
    assert days == ["2026-10-20", "2026-10-21", "2026-10-22"]
    for stream in ("production", "consumption"):
        starts = [e.interval_start for _, s, e in events if s == stream]
        assert starts == sorted(starts) and len(set(starts)) == 192
    for _, _, ev in events:
        validate_strict(json.loads(to_json_bytes(ev)))


def test_meters_are_emitted_at_interval_end_and_forecasts_after_availability() -> None:
    for at, stream, ev in _run(START, 0.5):
        if stream in ("production", "consumption"):
            assert at == ev.interval_end
        elif stream == "weather_forecast":
            assert at == ev.issued_at + timedelta(minutes=90)
            assert len(ev.payload.steps) == 192 and ev.issued_at.hour % 3 == 0


def test_ac_001_17_largest_record_stays_below_900_kb() -> None:
    sizes = [len(to_json_bytes(ev)) for _, _, ev in _run(START, 0.5)]
    assert max(sizes) < 900_000


def test_ac_001_25_price_event_on_fall_back_day_has_100_slots() -> None:
    events = _run(datetime(2026, 10, 24, tzinfo=UTC), 1)
    prices = [e for _, s, e in events if s == "price"]
    by_day = {p.payload.delivery_day.isoformat(): p for p in prices if isinstance(p, PriceEvent)}
    assert len(by_day["2026-10-25"].payload.slots) == 100


def test_same_seed_reproduces_content_and_run_id_changes_only_identity() -> None:
    a = _run(START, 0.25, "run-a")
    b = _run(START, 0.25, "run-a")
    c = _run(START, 0.25, "run-b")
    assert [e.model_dump() for *_, e in a] == [e.model_dump() for *_, e in b]
    for (_, _, x), (_, _, y) in zip(a, c, strict=True):
        assert x.event_id != y.event_id
        assert x.payload == y.payload


class NullSink:
    def __init__(self) -> None:
        from ems.ingest.kafka.sink import SinkStats

        self.events: list[Any] = []
        self.stats = SinkStats()

    def publish(self, event: Any) -> bool:
        self.events.append(event)
        self.stats.produced += 1
        self.stats.delivered += 1
        return True

    def poll(self, timeout: float = 0) -> None:
        return None

    def close(self, timeout_s: float = 30.0) -> Any:
        return self.stats


def test_ac_001_28_run_writes_a_manifest(tmp_path: Path) -> None:
    settings = EmsSettings(config_dir=REPO / "config", data_dir=tmp_path)
    cfg = load_generator_config(REPO / "config" / "generator.yaml")
    sink = NullSink()
    result = asyncio.run(
        run(
            settings,
            cfg,
            mode="backfill",
            start=START,
            end=START + timedelta(hours=6),
            run_id="m1",
            sink=sink,
            check_topics=False,
            clock=lambda: FIXED,
        )
    )
    assert result.exit_code == 0 and result.manifest is not None
    manifest = json.loads(result.manifest.read_text())
    for key in (
        "config",
        "seed",
        "window",
        "run_id",
        "versions",
        "git_commit",
        "hardware",
        "counts",
    ):
        assert key in manifest
    assert manifest["run_id"] == "m1" and manifest["seed"] == cfg.seed
    assert manifest["counts"]["production"] == 24
    assert all(ev.run_id == "m1" for ev in sink.events)


def test_backfill_needs_an_end() -> None:
    settings = EmsSettings(config_dir=REPO / "config")
    cfg = load_generator_config(REPO / "config" / "generator.yaml")
    with pytest.raises(ValueError, match="needs an end"):
        asyncio.run(
            run(
                settings,
                cfg,
                mode="backfill",
                start=START,
                end=None,
                run_id=None,
                check_topics=False,
            )
        )
