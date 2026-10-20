"""Integration test 4 (AC-003-07): with faults on, stats report duplicates, late events
and gaps that
match the configured rates within tolerance.
"""

from __future__ import annotations

import asyncio
import dataclasses
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from ems.ingest.generator import load_generator_config
from ems.ingest.runner import run
from ems.ingest.tools.reader import read_topic
from ems.ingest.tools.stats import topic_stats
from ems.settings import EmsSettings

REPO = Path(__file__).resolve().parents[2]
START = datetime(2026, 10, 20, tzinfo=UTC)
RATES = {"missing_rate": 0.02, "duplicate_rate": 0.02, "late_rate": 0.02}


def within(observed: int, rate: float, n: int) -> bool:
    expected = rate * n
    return abs(observed - expected) <= max(3.0, 0.5 * expected)


def test_ac_003_07_fault_rates_reported_by_stats(
    bootstrap: str, run_tag: str, topics_factory: Any, tmp_path: Path
) -> None:
    prefix = f"{run_tag}."
    plan = topics_factory("sim", prefix)
    settings = EmsSettings(
        kafka_bootstrap=bootstrap,
        topics_profile="sim",
        topic_prefix=prefix,
        config_dir=REPO / "config",
        data_dir=tmp_path,
    )
    cfg = load_generator_config(REPO / "config" / "generator.yaml")
    cfg = dataclasses.replace(
        cfg,
        synthetic_locations=100,
        streams={**cfg.streams, "weather_forecast": {"mode": "off"}, "price": {"mode": "off"}},
        faults={"enabled": True, "late_max": "PT2H", **RATES},
    )
    result = asyncio.run(
        run(
            settings,
            cfg,
            mode="backfill",
            start=START,
            end=START + timedelta(days=1),
            run_id=run_tag,
        )
    )
    assert result.exit_code == 0
    stats = topic_stats(
        plan.name_for("ems.consumption.v1"),
        read_topic(bootstrap, plan.name_for("ems.consumption.v1")),
    )
    n = 100 * 96  # clean consumption events: one per site and interval
    assert within(stats.duplicates, RATES["duplicate_rate"], n), stats.duplicates
    assert within(stats.late, RATES["late_rate"], n), stats.late
    # gaps count interior misses only (a missing first or last interval shortens the key's range)
    assert stats.gaps() <= RATES["missing_rate"] * n * 1.5 + 3 and stats.gaps() > 0
    assert len(stats.keys) == 100 and all(len(k.partitions) == 1 for k in stats.keys.values())
