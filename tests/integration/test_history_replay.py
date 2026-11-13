"""AC-008-17, -19: a history recording replayed into real Kafka (``make up``).

The recording is fetched through respx with synthetic bodies and built offline
(``tests/unit/ingest/history_recording.py``); topics are isolated per run by a prefix.
"""

from __future__ import annotations

import asyncio
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from ems.contracts.envelope import parse_event
from ems.ingest.runner import PreflightError, replay
from ems.ingest.tools.reader import read_topic
from ems.ingest.tools.validate import validate_records
from tests.unit.ingest.history_recording import (
    POZNAN,
    RADOM,
    Recording,
    build,
    make_recording,
    read_built,
)


def _recording(tmp_path: Path) -> Recording:
    rec = make_recording(
        tmp_path,
        date(2026, 5, 15),
        date(2026, 5, 16),
        name="it-history",
        locations=(POZNAN, RADOM),
        synthetic_locations=4,
    )
    build(rec)
    return rec


def _settings(rec: Recording, bootstrap: str, profile: str, prefix: str) -> Any:
    return rec.settings.model_copy(
        update={"kafka_bootstrap": bootstrap, "topics_profile": profile, "topic_prefix": prefix}
    )


def test_ac_008_17_replay_into_live_topics_is_refused(
    bootstrap: str, run_tag: str, topics_factory: Any, tmp_path: Path
) -> None:
    prefix = f"{run_tag}."
    plan = topics_factory("live", prefix)
    rec = _recording(tmp_path)
    with pytest.raises(PreflightError):
        asyncio.run(replay(_settings(rec, bootstrap, "live", prefix), rec.name))
    for topic in plan.topics:
        assert list(read_topic(bootstrap, topic.name)) == [], topic.name  # nothing published


def test_ac_008_19_fixture_recording_replayed_into_sim_topics(
    bootstrap: str, run_tag: str, topics_factory: Any, tmp_path: Path
) -> None:
    prefix = f"{run_tag}."
    plan = topics_factory("sim", prefix)
    rec = _recording(tmp_path)
    recorded = {e.event_id for e in read_built(rec.root)}

    result = asyncio.run(replay(_settings(rec, bootstrap, "sim", prefix), rec.name))
    assert result.exit_code == 0
    assert sum(result.counts.values()) == len(recorded)

    seen = []
    for topic in plan.topics:
        records = list(read_topic(bootstrap, topic.name))
        report = validate_records(topic.name, records, topic.partitions, prefix)
        assert report.clean, report.lines()  # what `make validate` checks
        for rec_ in records:
            event = parse_event(rec_.value or b"")
            assert event.source_system.startswith("replay:")
            seen.append(event.event_id)
    assert len(seen) == len(set(seen))
    assert set(seen) == recorded  # exactly the recording's event ids
