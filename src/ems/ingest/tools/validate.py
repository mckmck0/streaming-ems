"""Validate records on a topic: schema, routing rule, partition stability, ordering."""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime

from ems.contracts.envelope import ContractError, validate_strict
from ems.contracts.topics import DLQ_TOPIC, expected_partition, route
from ems.ingest.tools.reader import Record

MAX_EXAMPLES = 5


@dataclass
class TopicReport:
    topic: str
    records: int = 0
    valid: int = 0
    schema_errors: int = 0
    key_errors: int = 0
    partition_errors: int = 0
    order_errors: int = 0
    duplicates: int = 0
    keys: Counter[str] = field(default_factory=Counter)
    examples: list[str] = field(default_factory=list)

    @property
    def clean(self) -> bool:
        return not (
            self.schema_errors or self.key_errors or self.partition_errors or self.order_errors
        )

    def note(self, text: str) -> None:
        if len(self.examples) < MAX_EXAMPLES:
            self.examples.append(text)

    def lines(self) -> list[str]:
        out = [
            f"{self.topic}: records={self.records} valid={self.valid} keys={len(self.keys)} "
            f"schema_errors={self.schema_errors} key_errors={self.key_errors} "
            f"partition_errors={self.partition_errors} order_errors={self.order_errors} "
            f"duplicate_event_ids={self.duplicates}"
        ]
        out += [f"    {e}" for e in self.examples]
        return out


def validate_records(
    topic: str, records: Iterable[Record], partitions: int, prefix: str = ""
) -> TopicReport:
    report = TopicReport(topic)
    partition_of: dict[str, int] = {}
    last_start: dict[tuple[str, str], datetime] = {}
    seen_ids: set[str] = set()
    dlq = topic == prefix + DLQ_TOPIC
    for rec in records:
        report.records += 1
        key = rec.key or ""
        report.keys[key] += 1
        if key in partition_of and partition_of[key] != rec.partition:
            report.partition_errors += 1
            report.note(f"key {key} moved from partition {partition_of[key]} to {rec.partition}")
        partition_of.setdefault(key, rec.partition)
        if dlq:
            continue
        if rec.key is None:
            report.key_errors += 1
            report.note(f"offset {rec.offset}: null key")
        elif rec.partition != expected_partition(rec.key, partitions):
            report.partition_errors += 1
            report.note(f"offset {rec.offset}: key {key} on partition {rec.partition}")
        try:
            event = validate_strict(json.loads(rec.value or b"null"))
        except (ContractError, ValueError) as exc:
            report.schema_errors += 1
            report.note(f"offset {rec.offset}: {str(exc)[:200]}")
            continue
        report.valid += 1
        want_topic, want_key = route(event, prefix)
        if (want_topic, want_key) != (topic, rec.key):
            report.key_errors += 1
            report.note(f"offset {rec.offset}: routing rule says {want_topic} / {want_key}")
        eid = str(event.event_id)
        if eid in seen_ids:
            report.duplicates += 1
        seen_ids.add(eid)
        order_key = (key, event.stream.value)
        prev = last_start.get(order_key)
        if prev is not None and event.interval_start < prev:
            report.order_errors += 1
            report.note(f"offset {rec.offset}: interval_start went back for key {key}")
        last_start[order_key] = event.interval_start
    return report
