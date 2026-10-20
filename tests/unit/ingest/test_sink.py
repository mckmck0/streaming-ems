"""AC-001-07/-12/-13/-14/-16: sink behaviour, preflight and validation, with fakes (no broker)."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

from ems.contracts.envelope import to_json_bytes
from ems.contracts.examples import golden_examples
from ems.contracts.topics import expected_partition
from ems.ingest.kafka.preflight import problems
from ems.ingest.kafka.sink import KafkaSink, record_timestamp
from ems.ingest.tools.reader import Record
from ems.ingest.tools.validate import validate_records


class FakeError:
    def __init__(self, name: str, fatal: bool = False) -> None:
        self._name = name
        self._fatal = fatal

    def name(self) -> str:
        return self._name

    def fatal(self) -> bool:
        return self._fatal

    def __str__(self) -> str:
        return self._name


class FakeMsg:
    def __init__(self, topic: str) -> None:
        self._topic = topic

    def topic(self) -> str:
        return self._topic


class FakeProducer:
    def __init__(self, conf: dict[str, Any], fail_with: str | None = None, stuck: int = 0) -> None:
        self.conf = conf
        self.sent: list[dict[str, Any]] = []
        self.pending: list[tuple[Any, str]] = []
        self.fail_with = fail_with
        self.stuck = stuck
        self.purged = False

    def produce(self, topic: str, **kwargs: Any) -> None:
        self.sent.append({"topic": topic, **kwargs})
        self.pending.append((kwargs["on_delivery"], topic))

    def poll(self, timeout: float = 0) -> int:
        n = len(self.pending)
        for cb, topic in self.pending:
            cb(FakeError(self.fail_with) if self.fail_with else None, FakeMsg(topic))
        self.pending = []
        return n

    def flush(self, timeout: float = 0) -> int:
        self.poll()
        return self.stuck

    def purge(self, *args: Any, **kwargs: Any) -> None:
        self.purged = True


def _sink(**kw: Any) -> tuple[KafkaSink, FakeProducer]:
    holder: dict[str, FakeProducer] = {}

    def factory(conf: dict[str, Any]) -> FakeProducer:
        holder["p"] = FakeProducer(conf, **kw)
        return holder["p"]

    sink = KafkaSink("localhost:9092", topic_prefix="t.", producer_factory=factory)
    return sink, holder["p"]


def test_producer_config_is_java_compatible_and_safe() -> None:
    _, p = _sink()
    c = p.conf
    assert c["partitioner"] == "murmur2_random"
    assert c["acks"] == "all" and c["enable.idempotence"] is True
    assert c["compression.type"] == "zstd" and c["allow.auto.create.topics"] is False
    assert c["delivery.timeout.ms"] == 120_000 and c["broker.address.family"] == "v4"


def test_ac_001_12_headers_key_and_event_time() -> None:
    sink, p = _sink()
    ev = golden_examples()["production.json"]
    assert sink.publish(ev)
    sent = p.sent[0]
    assert sent["topic"] == "t.ems.production.v1" and sent["key"] == b"PL-LOC-000001"
    headers = dict(sent["headers"])
    assert headers == {
        "schema": "ems.measurement",
        "schema_version": "1.0.0",
        "source_system": "synthetic",
        "content-type": "application/json",
        "synthetic": "true",
    }
    assert sent["timestamp"] == int(ev.interval_end.timestamp() * 1000)
    price = golden_examples()["price.json"]
    assert record_timestamp(price) == price.issued_at
    assert sink.close().exit_code() == 0


def test_ac_001_13_invalid_event_goes_to_dlq() -> None:
    sink, p = _sink()
    ev = golden_examples()["production.json"].model_copy(update={"t": 1})  # breaks the t rule
    assert not sink.publish(ev)
    assert p.sent[0]["topic"] == "t.ems.gen.dlq.v1"
    assert "t must be" in dict(p.sent[0]["headers"])["error"]
    stats = sink.close()
    assert stats.dlq == 1 and stats.exit_code() == 0


def test_ac_001_14_delivery_failures_and_stuck_records_exit_nonzero() -> None:
    sink, _ = _sink(fail_with="_MSG_TIMED_OUT")
    sink.publish(golden_examples()["production.json"])
    stats = sink.close()
    assert stats.failed["_MSG_TIMED_OUT"] == 1 and stats.exit_code() == 3
    sink2, p2 = _sink(stuck=2)
    stats2 = sink2.close()
    assert p2.purged and stats2.remaining == 2 and stats2.exit_code() == 3


def test_fatal_client_error_exits_nonzero() -> None:
    sink, p = _sink()
    p.conf["error_cb"](FakeError("_FATAL", fatal=True))
    assert sink.close().exit_code() == 3


NOW = datetime(2026, 10, 20, 12, tzinfo=UTC)
LIVE = {
    "ems.production.v1": {
        "message.timestamp.type": "CreateTime",
        "message.timestamp.after.max.ms": "3600000",
        "message.timestamp.before.max.ms": "604800000",
        "retention.ms": "604800000",
    }
}
SIM = {
    "ems.production.v1": {
        "message.timestamp.type": "CreateTime",
        "message.timestamp.after.max.ms": "9223372036854775807",
        "message.timestamp.before.max.ms": "9223372036854775807",
        "retention.ms": "-1",
    }
}


def test_ac_001_07_preflight_refuses_future_and_expiring_windows() -> None:
    ok = problems(LIVE, min_ts=NOW - timedelta(hours=1), max_ts=NOW, now=NOW)
    assert ok == []
    future = problems(LIVE, min_ts=NOW, max_ts=NOW + timedelta(days=2), now=NOW)
    assert any("after.max.ms" in p for p in future)
    old = problems(LIVE, min_ts=NOW - timedelta(days=30), max_ts=NOW, now=NOW)
    assert any("retention.ms" in p for p in old) and any("before.max.ms" in p for p in old)
    sim = problems(SIM, min_ts=NOW - timedelta(days=400), max_ts=NOW + timedelta(days=2), now=NOW)
    assert sim == []


def _record(ev: Any, partition: int, offset: int, key: str | None = None) -> Record:
    return Record(
        topic="ems.production.v1",
        partition=partition,
        offset=offset,
        key=key if key is not None else "PL-LOC-000001",
        value=to_json_bytes(ev),
        headers={},
        timestamp_ms=0,
    )


def test_ac_001_16_validate_reports_clean_and_dirty_topics() -> None:
    ev = golden_examples()["production.json"]
    good_p = expected_partition("PL-LOC-000001", 3)
    clean = validate_records("ems.production.v1", [_record(ev, good_p, 0)], 3)
    assert clean.clean and clean.valid == 1
    broken = json.loads(to_json_bytes(ev))
    broken["t"] = 5
    bad_rec = Record(
        "ems.production.v1", good_p, 1, "PL-LOC-000001", json.dumps(broken).encode(), {}, 0
    )
    dirty = validate_records(
        "ems.production.v1",
        [_record(ev, good_p, 0), _record(ev, (good_p + 1) % 3, 2), bad_rec],
        3,
    )
    assert dirty.schema_errors == 1 and dirty.partition_errors >= 1 and dirty.duplicates == 1
    assert not dirty.clean
