"""AC-001-01…-04, -06: the compose stack, topic ensure, auto-create off, persistence, time rules."""

from __future__ import annotations

import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition
from confluent_kafka.admin import AdminClient

from ems.contracts.topics import load_plan
from ems.ingest.kafka.admin import ActionKind, KafkaTopicAdmin, ensure
from ems.ingest.kafka.config import base_config
from tests.integration.conftest import REPO, compose


def _deliver(producer: Producer, topic: str, value: bytes, timestamp_ms: int | None = None) -> Any:
    result: dict[str, Any] = {}

    def cb(err: KafkaError | None, msg: Any) -> None:
        result["err"] = err
        result["msg"] = msg

    kwargs: dict[str, Any] = {"value": value, "key": b"PL-LOC-000001", "on_delivery": cb}
    if timestamp_ms is not None:
        kwargs["timestamp"] = timestamp_ms
    producer.produce(topic, **kwargs)
    producer.flush(20)
    return result


def test_ac_001_01_services_healthy_and_bound_to_localhost(bootstrap: str) -> None:
    ps = compose("ps", "--format", "{{.Service}} {{.Health}}").stdout
    health = dict(line.split(" ", 1) for line in ps.strip().splitlines() if " " in line)
    assert health.get("kafka") == "healthy"
    assert health.get("kafka-ui") == "healthy"
    for service, port in (("kafka", "9092"), ("kafka-ui", "8080")):
        mapped = compose("port", service, port).stdout.strip()
        assert mapped.startswith("127.0.0.1:"), mapped


def test_ac_001_02_ensure_creates_then_is_a_noop(
    bootstrap: str, run_tag: str, topics_factory: Any
) -> None:
    prefix = f"{run_tag}."
    plan = load_plan(REPO / "config", "live", prefix)
    first = ensure(KafkaTopicAdmin(bootstrap), plan)
    try:
        assert {a.kind for a in first.actions} == {ActionKind.CREATE}
        actual = KafkaTopicAdmin(bootstrap).actual(t.name for t in plan.topics)
        for want in plan.topics:
            assert actual[want.name].partitions == want.partitions
            assert actual[want.name].configs["retention.ms"] == want.configs["retention.ms"]
        second = ensure(KafkaTopicAdmin(bootstrap), plan)
        assert not second.changed and not second.failed
        assert {a.kind for a in second.actions} == {ActionKind.OK}
    finally:
        KafkaTopicAdmin(bootstrap).delete(t.name for t in plan.topics)


def test_ac_001_03_undefined_topic_write_fails_and_topic_stays_absent(bootstrap: str) -> None:
    missing = f"ems.it.missing.{uuid.uuid4().hex}"
    conf = base_config(bootstrap, "it-missing") | {
        "allow.auto.create.topics": True,  # the client asks; the broker must refuse
        "topic.metadata.propagation.max.ms": 1000,
        "message.timeout.ms": 10000,
    }
    result = _deliver(Producer(conf), missing, b"{}")
    err = result.get("err")
    assert err is not None, "write to an undefined topic must fail"
    assert err.code() in (KafkaError.UNKNOWN_TOPIC_OR_PART, KafkaError._UNKNOWN_TOPIC), err
    topics = AdminClient(base_config(bootstrap)).list_topics(timeout=10).topics
    assert missing not in topics


def test_ac_001_06_live_rejects_future_records_sim_accepts(
    bootstrap: str, run_tag: str, topics_factory: Any
) -> None:
    future_ms = int((datetime.now(UTC) + timedelta(hours=2)).timestamp() * 1000)
    live = topics_factory("live", f"{run_tag}.live.")
    sim = topics_factory("sim", f"{run_tag}.sim.")
    producer = Producer(base_config(bootstrap) | {"message.timeout.ms": 15000})
    rejected = _deliver(producer, live.name_for("ems.production.v1"), b"{}", future_ms)
    assert rejected["err"] is not None and rejected["err"].code() == KafkaError.INVALID_TIMESTAMP
    accepted = _deliver(producer, sim.name_for("ems.production.v1"), b"{}", future_ms)
    assert accepted["err"] is None
    old_ms = int(datetime(2025, 1, 1, tzinfo=UTC).timestamp() * 1000)
    assert _deliver(producer, sim.name_for("ems.production.v1"), b"{}", old_ms)["err"] is None
    stale = _deliver(producer, live.name_for("ems.production.v1"), b"{}", old_ms)
    assert stale["err"] is not None  # older than retention: rejected loudly, not silently deleted


def test_ac_001_04_restart_keeps_topics_and_records(
    bootstrap: str, run_tag: str, topics_factory: Any
) -> None:
    plan = topics_factory("sim", f"{run_tag}.persist.")
    topic = plan.name_for("ems.price.v1")
    result = _deliver(Producer(base_config(bootstrap)), topic, b'{"persist": true}')
    assert result["err"] is None
    offset = result["msg"].offset()
    compose("restart", "kafka")
    deadline = time.monotonic() + 120
    while True:
        try:
            AdminClient(base_config(bootstrap)).list_topics(timeout=5)
            break
        except Exception:
            if time.monotonic() > deadline:
                raise
            time.sleep(2)
    consumer = Consumer(
        base_config(bootstrap) | {"group.id": f"{run_tag}-persist", "enable.auto.commit": False}
    )
    consumer.assign([TopicPartition(topic, 0, offset)])
    msg = None
    deadline = time.monotonic() + 60
    while msg is None and time.monotonic() < deadline:
        msg = consumer.poll(2)
    consumer.close()
    assert msg is not None and msg.value() == b'{"persist": true}'
