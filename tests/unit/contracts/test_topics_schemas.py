"""AC-001-06/-11/-15/-17: topic plans, routing, partitioning, schema export and compatibility."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import confluent_kafka
import pytest
from hypothesis import given
from hypothesis import strategies as st

from ems.contracts import schema_export
from ems.contracts.envelope import to_json_bytes
from ems.contracts.examples import golden_examples
from ems.contracts.topics import (
    DLQ_TOPIC,
    TopicConfigError,
    build_plan,
    expected_partition,
    load_plan,
    load_topics_file,
    route,
)


def test_ac_001_06_profiles(repo: Path) -> None:
    live = load_plan(repo / "config", "live").by_name()
    sim = load_plan(repo / "config", "sim").by_name()
    prod_live = live["ems.production.v1"]
    assert prod_live.partitions == 3 and prod_live.replication_factor == 1
    assert prod_live.configs["message.timestamp.after.max.ms"] == "3600000"
    assert prod_live.configs["message.timestamp.before.max.ms"] == prod_live.configs["retention.ms"]
    assert sim["ems.production.v1"].configs["retention.ms"] == "-1"
    assert (
        sim["ems.production.v1"].configs["message.timestamp.after.max.ms"] == "9223372036854775807"
    )
    assert live[DLQ_TOPIC].configs["message.timestamp.type"] == "LogAppendTime"
    assert "ems.weather.actual.v1" not in live  # disabled
    assert load_plan(repo / "config", "scale").by_name()["ems.production.v1"].partitions == 48
    assert load_plan(repo / "config", "cluster").by_name()["ems.price.v1"].replication_factor == 3


def test_prefix_applies_to_every_topic(repo: Path) -> None:
    plan = load_plan(repo / "config", "sim", prefix="it-abc.")
    assert all(t.name.startswith("it-abc.") for t in plan.topics)


def test_removed_or_unknown_configs_are_rejected(repo: Path) -> None:
    data = load_topics_file(repo / "config" / "topics.yaml")
    bad = copy.deepcopy(data)
    bad["topics"][0]["configs"]["message.timestamp.difference.max.ms"] = "1"
    with pytest.raises(TopicConfigError, match="unknown or removed"):
        build_plan(bad, "live")
    with pytest.raises(TopicConfigError, match="unknown profile"):
        build_plan(data, "prod")


def test_ac_001_11_routing_and_keys() -> None:
    ex = golden_examples()
    assert route(ex["production.json"]) == ("ems.production.v1", "PL-LOC-000001")
    assert route(ex["price.json"]) == ("ems.price.v1", "PL:RCE")
    scoped = ex["production.json"].model_copy(update={"run_id": "r7"})
    assert route(scoped, prefix="it.") == ("it.ems.production.v1", "r7:PL-LOC-000001")


@given(st.text(max_size=40), st.sampled_from([1, 3, 48]))
def test_ac_001_11_partition_matches_librdkafka(key: str, n: int) -> None:
    assert expected_partition(key, n) == confluent_kafka.murmur2(key.encode(), n)


def test_ac_001_15_exported_schemas_are_current(repo: Path) -> None:
    assert schema_export.stale(repo / "schemas") == [], "run: uv run ems-gen schemas export"
    for name, text in schema_export.render_examples().items():
        assert (repo / "schemas" / "examples" / name).read_text() == text


def _additive(schema: dict[str, Any]) -> dict[str, Any]:
    new = copy.deepcopy(schema)
    new["properties"]["added_in_v1_1"] = {"type": "string"}
    return new


def test_ac_001_15_additive_change_is_full_compatible() -> None:
    for schema in schema_export.build_schemas().values():
        assert schema_export.incompatibilities(schema, _additive(schema)) == []
        assert schema_export.incompatibilities(schema, schema) == []


def test_ac_001_15_breaking_changes_are_detected() -> None:
    schema = schema_export.build_schemas()["ems.measurement.v1.json"]
    removed = copy.deepcopy(schema)
    del removed["properties"]["seed"]
    assert schema_export.incompatibilities(schema, removed)
    required = copy.deepcopy(schema)
    required["properties"]["x"] = {"type": "string"}
    required["required"] = [*required.get("required", []), "x"]
    assert schema_export.incompatibilities(schema, required)
    closed = copy.deepcopy(schema)
    closed["additionalProperties"] = False
    assert schema_export.incompatibilities(schema, closed)


def test_exported_schemas_validate_golden_examples() -> None:
    import jsonschema

    schemas = schema_export.build_schemas()
    for name, event in golden_examples().items():
        sname = f"{event.schema_name}.v1.json"
        jsonschema.Draft202012Validator(schemas[sname]).validate(json.loads(to_json_bytes(event)))
        assert name


def test_ac_001_17_records_stay_below_900_kb() -> None:
    for event in golden_examples().values():
        assert len(to_json_bytes(event)) < 900_000
