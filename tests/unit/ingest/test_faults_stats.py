"""AC-003-05/-06/-07: deterministic faults with the right semantics; stats detect them."""

from __future__ import annotations

import dataclasses
import itertools
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from ems.contracts.envelope import Aggregation, Quality, to_json_bytes
from ems.contracts.topics import expected_partition, route
from ems.ingest.catalog.build import build_catalog
from ems.ingest.faults import FaultConfig, apply_faults
from ems.ingest.generator import GeneratorConfig, SyntheticModel, emissions
from ems.ingest.tools.reader import Record
from ems.ingest.tools.stats import topic_stats

REPO = Path(__file__).resolve().parents[3]
START = datetime(2026, 10, 20, tzinfo=UTC)
FIXED = datetime(2026, 10, 21, tzinfo=UTC)
FAULTS = FaultConfig(
    enabled=True,
    missing_rate=0.03,
    duplicate_rate=0.03,
    late_rate=0.03,
    late_max=timedelta(hours=2),
    stuck_value_rate=0.03,
    register_reset_rate=0.02,
    estimated_rate=0.02,
    substituted_rate=0.01,
)


def _cfg() -> GeneratorConfig:
    return GeneratorConfig(
        streams={"weather_forecast": {"mode": "off"}, "price": {"mode": "off"}}, emit_registers=True
    )


def _clean(n_sites: int = 20, hours: int = 24) -> list[tuple[datetime, str, Any]]:
    sites = build_catalog(n_sites, catalog_seed=7, global_seed=4242)
    model = SyntheticModel(sites, 4242, REPO / "config")
    return list(emissions(model, _cfg(), START, START + timedelta(hours=hours)))


def _faulty(seed: int = 4242) -> list[tuple[datetime, str, Any]]:
    return [(at, s, b(FIXED)) for at, s, b in apply_faults(iter(_clean()), FAULTS, seed)]


def test_disabled_faults_change_nothing() -> None:
    clean = _clean(3, 2)
    out = list(apply_faults(iter(clean), FaultConfig(), 1))
    assert [(a, s) for a, s, _ in out] == [(a, s) for a, s, _ in clean]


def test_ac_003_05_faults_are_deterministic_per_seed() -> None:
    a = [(at, e.model_dump()) for at, _, e in _faulty()]
    b = [(at, e.model_dump()) for at, _, e in _faulty()]
    c = [(at, e.model_dump()) for at, _, e in _faulty(seed=7)]
    assert a == b and a != c


def test_ac_003_06_duplicates_are_identical_and_late_events_keep_time_and_id() -> None:
    clean = {str(b(FIXED).event_id): (at, b(FIXED)) for at, _, b in _clean()}
    faulty = _faulty()
    ids = Counter(str(e.event_id) for _, _, e in faulty)
    dups = [i for i, n in ids.items() if n > 1]
    assert dups
    for i in dups:
        copies = [e for _, _, e in faulty if str(e.event_id) == i]
        assert all(c == copies[0] for c in copies)
    missing = set(clean) - set(ids)
    assert missing  # some events were dropped
    late = [(at, e) for at, _, e in faulty if at > clean[str(e.event_id)][0]]
    assert late
    for at, e in late:
        assert e.interval_start == clean[str(e.event_id)][1].interval_start
        assert at - clean[str(e.event_id)][0] <= timedelta(hours=2)
    times = [at for at, _, _ in faulty]
    assert times == sorted(times)  # still emission-time ordered


def test_stuck_register_and_quality_faults_touch_meters_only() -> None:
    faulty = _faulty()
    qualities = Counter(m.quality for _, _, e in faulty for m in e.payload.measurements)
    assert qualities[Quality.ESTIMATED] and qualities[Quality.SUBSTITUTED]
    registers: dict[str, list[float]] = {}
    for _, s, e in faulty:
        if s == "consumption":
            for m in e.payload.measurements:
                if m.aggregation is Aggregation.REGISTER:
                    registers.setdefault(str(e.location_id), []).append(m.value)
    assert registers  # unmanaged sites carry cumulative registers
    resets = sum(any(b < a for a, b in itertools.pairwise(v)) for v in registers.values())
    assert resets >= 1  # at least one register counter restarted


def _records(events: list[Any]) -> list[Record]:
    out = []
    for i, ev in enumerate(events):
        topic, key = route(ev)
        out.append(Record(topic, expected_partition(key, 3), i, key, to_json_bytes(ev), {}, 0))
    return out


def test_ac_003_07_stats_detect_gaps_duplicates_and_late_events() -> None:
    faulty = [e for _, s, e in _faulty() if s == "consumption"]
    stats = topic_stats("ems.consumption.v1", _records(faulty))
    clean_ids = [str(b(FIXED).event_id) for _, s, b in _clean() if s == "consumption"]
    n = len(clean_ids)
    assert stats.duplicates > 0 and stats.late > 0 and stats.gaps() > 0
    assert abs(stats.duplicates - 0.03 * n) <= max(3, 0.5 * 0.03 * n)
    assert stats.expected() <= n
    clean_stats = topic_stats(
        "ems.consumption.v1", _records([b(FIXED) for _, s, b in _clean() if s == "consumption"])
    )
    assert (clean_stats.duplicates, clean_stats.late, clean_stats.gaps()) == (0, 0, 0)


def test_fault_config_parses_iso_durations() -> None:
    cfg = FaultConfig.from_dict({"enabled": True, "late_max": "PT1H30M", "missing_rate": 0.1})
    assert cfg.late_max == timedelta(minutes=90) and cfg.missing_rate == 0.1
    assert dataclasses.replace(cfg, enabled=False).enabled is False
