"""Publish contract events to Kafka: validate, route, set headers and event-time timestamps.

One synchronous confluent-kafka Producer per process; callers call ``poll`` regularly (the
generator does it after every produce). Delivery callbacks only count; the exit code comes
from the counters.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

import structlog

from ems.contracts.envelope import (
    ContractError,
    EnvelopeBase,
    MeasurementEvent,
    to_json_bytes,
    validate_strict,
)
from ems.contracts.topics import DLQ_TOPIC, route
from ems.ingest.kafka.config import ClientConf, base_config

log = structlog.get_logger(__name__)

Headers = list[tuple[str, str | bytes | None]]


class ProducerLike(Protocol):
    def produce(self, *args: Any, **kwargs: Any) -> Any: ...
    def poll(self, *args: Any, **kwargs: Any) -> Any: ...
    def flush(self, *args: Any, **kwargs: Any) -> Any: ...
    def purge(self, *args: Any, **kwargs: Any) -> Any: ...


def producer_config(bootstrap: str, delivery_timeout_ms: int) -> ClientConf:
    return base_config(bootstrap) | {
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "zstd",
        "linger.ms": 20,
        # Java-compatible key hashing; librdkafka's default (consistent_random, CRC32) is not.
        "partitioner": "murmur2_random",
        "delivery.timeout.ms": delivery_timeout_ms,
    }


def record_timestamp(event: EnvelopeBase) -> datetime:
    """Event time: interval end for measurements, issue time for forecasts and prices."""
    if isinstance(event, MeasurementEvent) or event.issued_at is None:
        return event.interval_end
    return event.issued_at


def event_headers(event: EnvelopeBase) -> Headers:
    return [
        ("schema", event.schema_name),
        ("schema_version", event.schema_version),
        ("source_system", event.source_system),
        ("content-type", "application/json"),
        ("synthetic", "true" if event.synthetic else "false"),
    ]


@dataclass
class SinkStats:
    produced: int = 0
    delivered: int = 0
    dlq: int = 0
    failed: Counter[str] = field(default_factory=Counter)
    fatal: str | None = None
    remaining: int = 0

    @property
    def failures(self) -> int:
        return sum(self.failed.values()) + self.remaining

    def exit_code(self) -> int:
        """0 when every record was delivered; 3 on any delivery failure or a fatal client error."""
        return 3 if self.fatal or self.failures else 0


class KafkaSink:
    def __init__(
        self,
        bootstrap: str,
        *,
        topic_prefix: str = "",
        delivery_timeout_ms: int = 120_000,
        validate: bool = True,
        producer_factory: Callable[[dict[str, Any]], ProducerLike] | None = None,
    ) -> None:
        conf: dict[str, Any] = {
            **producer_config(bootstrap, delivery_timeout_ms),
            "error_cb": self._on_error,
        }
        if producer_factory is None:
            from confluent_kafka import Producer

            self._producer: ProducerLike = Producer(conf)
        else:
            self._producer = producer_factory(conf)
        self._prefix = topic_prefix
        self._validate = validate
        self.stats = SinkStats()

    # ── callbacks (served from poll/flush on the caller's thread) ─────────────────────────────────

    def _on_error(self, err: Any) -> None:
        try:
            if err.fatal():
                self.stats.fatal = str(err)
                log.error("kafka_fatal_error", error=str(err))
            else:
                log.warning("kafka_error", error=str(err))
        except Exception:  # never let a callback raise out of poll()
            log.exception("error_cb_failed")

    def _on_delivery(self, err: Any, msg: Any) -> None:
        try:
            if err is None:
                self.stats.delivered += 1
            else:
                self.stats.failed[err.name()] += 1
                log.error("delivery_failed", error=str(err), topic=msg.topic())
        except Exception:
            log.exception("delivery_cb_failed")

    # ── publishing ──────────────────────────────────────────────────────────────────────────────

    def _produce(self, topic: str, **kwargs: Any) -> None:
        while True:
            try:
                self._producer.produce(topic, on_delivery=self._on_delivery, **kwargs)
                break
            except BufferError:  # local queue full: serve callbacks, then retry
                self._producer.poll(0.1)
        self.stats.produced += 1
        self._producer.poll(0)

    def publish(self, event: EnvelopeBase) -> bool:
        """Publish ``event`` (invalid events go to the DLQ); True when it reached its topic."""
        value = to_json_bytes(event)
        topic, key = route(event, self._prefix)
        if self._validate:
            try:
                validate_strict(json.loads(value))
            except ContractError as exc:
                self.publish_invalid(value, key, str(exc))
                return False
        self._produce(
            topic,
            key=key.encode(),
            value=value,
            headers=event_headers(event),
            timestamp=int(record_timestamp(event).timestamp() * 1000),
        )
        return True

    def publish_invalid(self, value: bytes, key: str | None, error: str) -> None:
        self.stats.dlq += 1
        log.warning("dlq", key=key, error=error)
        self._produce(
            self._prefix + DLQ_TOPIC,
            key=key.encode() if key else None,
            value=value,
            headers=[("error", error[:1000]), ("content-type", "application/json")],
        )

    def poll(self, timeout: float = 0) -> None:
        self._producer.poll(timeout)

    def close(self, timeout_s: float = 30.0) -> SinkStats:
        remaining = self._producer.flush(timeout_s)
        if remaining > 0:
            self._producer.purge()
            self._producer.poll(0)
        self.stats.remaining = max(remaining, 0)
        log.info(
            "sink_closed",
            produced=self.stats.produced,
            delivered=self.stats.delivered,
            dlq=self.stats.dlq,
            failed=dict(self.stats.failed),
            remaining=self.stats.remaining,
        )
        return self.stats
