"""AC-002-11: a recording replayed into Kafka keeps its event ids and validates."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import respx

from ems.contracts.envelope import parse_event
from ems.ingest.runner import record, replay
from ems.ingest.sources.recording import read_events
from ems.ingest.tools.reader import read_topic
from ems.ingest.tools.validate import validate_records
from ems.settings import EmsSettings
from tests.unit.ingest.test_recording_hybrid import NOW, live_cfg, mock_sources

REPO = Path(__file__).resolve().parents[2]


def test_ac_002_11_replay_into_kafka(
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
    with respx.mock:
        mock_sources()
        root = asyncio.run(record(settings, live_cfg(), name="it", clock=lambda: NOW))
    recorded = {e.event_id for e in read_events(root)}
    result = asyncio.run(replay(settings, "it"))
    assert result.exit_code == 0 and sum(result.counts.values()) == len(recorded) == 3

    seen = set()
    by_name = plan.by_name()
    for base in ("ems.price.v1", "ems.weather.forecast.v1"):
        name = plan.name_for(base)
        records = list(read_topic(bootstrap, name))
        report = validate_records(name, records, by_name[name].partitions, prefix)
        assert report.clean, report.lines()
        for rec in records:
            event = parse_event(rec.value or b"")
            assert event.source_system.startswith("replay:")
            seen.add(event.event_id)
    assert seen == recorded
