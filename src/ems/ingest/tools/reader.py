"""Read a topic up to its current high watermark, without a consumer group."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from dataclasses import dataclass

from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

from ems.ingest.kafka.config import base_config


@dataclass(frozen=True)
class Record:
    topic: str
    partition: int
    offset: int
    key: str | None
    value: bytes | None
    headers: dict[str, str]
    timestamp_ms: int


def read_topic(
    bootstrap: str,
    topic: str,
    *,
    from_beginning: bool = True,
    start_offsets: dict[int, int] | None = None,
    max_records: int | None = None,
    timeout_s: float = 30.0,
) -> Iterator[Record]:
    """Yield the records present now (up to each partition's high watermark), then stop."""
    consumer = Consumer(
        base_config(bootstrap, "ems-gen-reader")
        | {"group.id": f"ems-gen-reader-{uuid.uuid4().hex}", "enable.auto.commit": False}
    )
    try:
        meta = consumer.list_topics(timeout=timeout_s)
        if topic not in meta.topics:
            raise SystemExit(f"topic {topic} does not exist")
        partitions = sorted(meta.topics[topic].partitions)
        assignment: list[TopicPartition] = []
        ends: dict[int, int] = {}
        for p in partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=timeout_s)
            if start_offsets is not None:
                start = start_offsets.get(p, low)
            else:
                start = OFFSET_BEGINNING if from_beginning else high
            if (start if start >= 0 else low) < high:
                assignment.append(TopicPartition(topic, p, start))
                ends[p] = high
        if not assignment:
            return
        consumer.assign(assignment)
        done: set[int] = set()
        count = 0
        while len(done) < len(ends):
            msg = consumer.poll(timeout_s)
            if msg is None:
                raise TimeoutError(f"no record from {topic} within {timeout_s}s")
            if msg.error():
                raise RuntimeError(str(msg.error()))
            p = msg.partition()
            off = msg.offset()
            assert p is not None and off is not None
            if off >= ends[p] - 1:
                done.add(p)
            raw_headers = msg.headers() or []
            key = msg.key()
            ts = msg.timestamp()[1]
            yield Record(
                topic=topic,
                partition=p,
                offset=off,
                key=key.decode() if isinstance(key, bytes) else key,
                value=msg.value() if isinstance(msg.value(), bytes) else None,
                headers={
                    k: (v.decode() if isinstance(v, bytes) else str(v)) for k, v in raw_headers
                },
                timestamp_ms=ts,
            )
            count += 1
            if max_records is not None and count >= max_records:
                return
    finally:
        consumer.close()


def end_offsets(bootstrap: str, topic: str, timeout_s: float = 30.0) -> dict[int, int]:
    """Current high watermark per partition (use as start offsets to read only new records)."""
    consumer = Consumer(base_config(bootstrap, "ems-gen-reader") | {"group.id": "ems-gen-offsets"})
    try:
        meta = consumer.list_topics(timeout=timeout_s)
        return {
            p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=timeout_s)[1]
            for p in meta.topics[topic].partitions
        }
    finally:
        consumer.close()
