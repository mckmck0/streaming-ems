"""AC-004-09: the `cluster` profile (RF 3, min ISR 2) survives one broker down, refuses
writes with two.

Self-contained: starts the profile's controller and three brokers, stops brokers one at a time and
removes the containers at the end (the single-node stack from `make up` is not touched).
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from typing import Any

import pytest
from confluent_kafka import KafkaError, Producer

from ems.contracts.topics import load_plan
from ems.ingest.kafka.admin import KafkaTopicAdmin, ensure
from ems.ingest.kafka.config import base_config
from tests.integration.conftest import REPO, compose

BOOTSTRAP = "127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094"
BROKERS = ("kafka-1", "kafka-2", "kafka-3")
SERVICES = ("kafka-ctl", *BROKERS)


@pytest.fixture(scope="module")
def cluster() -> Iterator[str]:
    compose("--profile", "cluster", "up", "-d", "--wait", *SERVICES, timeout=300)
    try:
        yield BOOTSTRAP
    finally:
        compose("--profile", "cluster", "rm", "-f", "-s", *SERVICES, timeout=300)


def _write(topic: str) -> KafkaError | None:
    """One acks=all write without retries, so the broker's own error comes back."""
    conf: dict[str, Any] = {
        **base_config(BOOTSTRAP, "ems-it-cluster"),
        "acks": "all",
        "enable.idempotence": False,
        "retries": 0,
        "message.timeout.ms": 15000,
    }
    producer = Producer(conf)
    result: dict[str, KafkaError | None] = {}

    def cb(err: KafkaError | None, _msg: Any) -> None:
        result["err"] = err

    producer.produce(topic, key=b"PL-LOC-000001", value=b"{}", on_delivery=cb)
    producer.flush(30)
    assert "err" in result, "no delivery report"
    return result["err"]


def _write_until(topic: str, accept: Any, deadline_s: float = 60.0) -> KafkaError | None:
    """Retry while the cluster settles (leader moves, ISR shrinks) until ``accept(err)``."""
    end = time.monotonic() + deadline_s
    err = _write(topic)
    while not accept(err) and time.monotonic() < end:
        time.sleep(1.0)
        err = _write(topic)
    return err


def test_ac_004_09_rf3_min_isr2(cluster: str, run_tag: str) -> None:
    plan = load_plan(REPO / "config", "cluster", f"{run_tag}.")
    admin = KafkaTopicAdmin(cluster)
    report = ensure(admin, plan, apply_config=True)
    assert not report.failed, report.lines()
    topic = next(t for t in plan.topics if t.name.endswith("ems.price.v1"))
    assert topic.replication_factor == 3
    assert topic.configs["min.insync.replicas"] == "2"

    assert _write_until(topic.name, lambda e: e is None) is None

    compose("--profile", "cluster", "stop", "kafka-3", timeout=120)
    assert _write_until(topic.name, lambda e: e is None) is None  # one down: still writable

    compose("--profile", "cluster", "stop", "kafka-2", timeout=120)
    err = _write_until(
        topic.name, lambda e: e is not None and e.code() == KafkaError.NOT_ENOUGH_REPLICAS
    )
    assert err is not None and err.code() == KafkaError.NOT_ENOUGH_REPLICAS, err
