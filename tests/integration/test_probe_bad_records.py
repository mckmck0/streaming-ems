"""AC-010-10: a record that is not JSON or breaks the v1 contract is counted and does not block its
key.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

from confluent_kafka import Producer

from ems.contracts.envelope import AssetType, Direction, Stream, to_json_bytes
from ems.contracts.examples import energy, measurement_event
from ems.contracts.time import QUARTER
from ems.experiments import stack
from ems.ingest.kafka.config import base_config

START = datetime(2026, 10, 20, 10, 0, tzinfo=UTC)


def _valid(run_id: str, slot: int) -> bytes:
    start = START + slot * QUARTER
    event = measurement_event(
        stream=Stream.PRODUCTION,
        location_id="PL-LOC-999001",
        interval_start=start,
        measurements=[energy("pv-1", AssetType.PV, Direction.GENERATION, 0.25)],
        ingested_at=start + QUARTER,
        run_id=run_id,
    )
    return to_json_bytes(event)


def test_ac_010_10_bad_records_are_counted_and_later_records_follow(
    stack_env: stack.StackEnv, run_id: str, bootstrap: str
) -> None:
    topic = stack_env.prefix + "ems.production.v1"
    key = f"{run_id}:PL-LOC-999001"
    contract_breaking = json.loads(_valid(run_id, 0))
    contract_breaking["payload"] = {"measurements": "not a list"}
    values = [
        _valid(run_id, 0),
        b"not json {",
        json.dumps(contract_breaking).encode(),
        _valid(run_id, 1),
    ]
    producer = Producer(base_config(bootstrap, "ems-tests"))
    for value in values:
        producer.produce(topic, key=key.encode(), value=value)
    assert producer.flush(30) == 0

    stack_env.restate.wait_handled(run_id, len(values), timeout=180)
    log = stack_env.restate.probe_logs(run_id)[key]
    assert log["rejected"] == 2
    entry = log["topics"][topic]
    assert (entry["n"], entry["regressions"], entry["partition_changes"]) == (4, 0, 0)
    assert all(inv["status"] == "completed" for inv in stack_env.restate.invocations(run_id))
    records = stack.read_run(stack_env.settings, stack_env.prefix, run_id)
    assert stack.offline_logs(records) == {key: log}
