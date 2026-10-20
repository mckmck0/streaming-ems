"""AC-001-07/-27: two-day backfill for PL-LOC-000001 through Kafka, read back and validated."""

from __future__ import annotations

import asyncio
import json
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from ems.contracts.envelope import parse_event
from ems.contracts.topics import expected_partition
from ems.ingest.generator import load_generator_config
from ems.ingest.runner import PreflightError, run
from ems.ingest.tools.reader import read_topic
from ems.ingest.tools.validate import validate_records
from ems.settings import EmsSettings

REPO = Path(__file__).resolve().parents[2]
START = datetime(2026, 10, 20, tzinfo=UTC)


def test_ac_001_27_two_day_backfill(
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
    result = asyncio.run(
        run(
            settings,
            cfg,
            mode="backfill",
            start=START,
            end=START + timedelta(days=2),
            run_id=run_tag,
        )
    )
    assert result.exit_code == 0, result.stats
    assert result.counts == {
        "production": 192,
        "consumption": 192,
        "weather_forecast": 16,
        "price": 3,
    }

    by_name = plan.by_name()
    seen: Counter[str] = Counter()
    for base, want in (
        ("ems.production.v1", 192),
        ("ems.consumption.v1", 192),
        ("ems.weather.forecast.v1", 16),
        ("ems.price.v1", 3),
    ):
        name = plan.name_for(base)
        records = list(read_topic(bootstrap, name))
        assert len(records) == want, name
        partitions = {r.partition for r in records}
        assert len(partitions) == 1  # one key per topic, so one partition
        key = records[0].key
        assert key is not None and partitions == {expected_partition(key, by_name[name].partitions)}
        report = validate_records(name, records, by_name[name].partitions, prefix)
        assert report.clean and report.valid == want, report.lines()
        events = [parse_event(r.value or b"") for r in records]
        starts = [e.interval_start for e in events]
        assert starts == sorted(starts)  # monotonic per key (faults off)
        assert all(e.run_id == run_tag for e in events)
        seen[base] = len(records)
    assert seen["ems.price.v1"] == 3
    manifest = json.loads((tmp_path / "runs" / run_tag / "manifest.json").read_text())
    assert manifest["counts"]["production"] == 192 and manifest["sink"]["delivered"] == 403


def test_ac_001_07_preflight_refuses_backfill_into_live_topics(
    bootstrap: str, run_tag: str, topics_factory: Any, tmp_path: Path
) -> None:
    prefix = f"{run_tag}."
    topics_factory("live", prefix)
    settings = EmsSettings(
        kafka_bootstrap=bootstrap,
        topics_profile="live",
        topic_prefix=prefix,
        config_dir=REPO / "config",
        data_dir=tmp_path,
    )
    cfg = load_generator_config(REPO / "config" / "generator.yaml")
    old = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(days=30)
    with pytest.raises(PreflightError, match=r"retention\.ms"):
        asyncio.run(
            run(
                settings,
                cfg,
                mode="backfill",
                start=old,
                end=old + timedelta(days=1),
                run_id=run_tag,
            )
        )
