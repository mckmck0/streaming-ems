"""Per-record check of the Kafka-fed Virtual Objects (spec 010, plan §4). Pure: no Restate, clock or
I/O.

Two functions keep the two parts of a probe's state for one key:

- ``log_apply``: per topic, the record count, the last offset, a SHA-256 chain over every
  (partition, offset) seen, the number of regressions (offsets that did not rise) and of partition
  changes; plus the key's count of rejected records. The result does not depend on how the key's
  topics interleave, so the state in Restate equals, with ``==``, the fold the offline side computes
  over the records read back from Kafka.
- ``lead_apply``: how far each input stream of the key runs ahead of the others in data time (the
  newest ``available_at`` handled per stream, in quarter-hours). It depends on the interleaving and
  is only measured.

Both return new dictionaries of plain JSON types, never mutate their input and never raise on the
content of a record: a Kafka-fed handler that raised would be retried and block its key. ``apply``
does both with one parse of the record. Stage 3 keeps ``log_apply`` in the real Kafka-fed handlers.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping
from typing import Any

from ems.contracts.availability import available_at
from ems.contracts.envelope import EnvelopeBase, parse_event
from ems.contracts.time import quarter_index

Meta = tuple[str, int, int]
"""A record's Kafka coordinates: (topic, partition, offset)."""

_TOPIC, _PARTITION, _OFFSET = "kafka.topic", "kafka.partition", "kafka.offset"


def kafka_meta(headers: Mapping[str, str]) -> Meta | None:
    """The Kafka coordinates the integration attaches as headers, or None if one is missing."""
    lower = {str(k).lower(): v for k, v in headers.items()}
    try:
        return (str(lower[_TOPIC]), int(lower[_PARTITION]), int(lower[_OFFSET]))
    except (KeyError, TypeError, ValueError):
        return None


def new_log() -> dict[str, Any]:
    return {"topics": {}, "rejected": 0}


def new_leads() -> dict[str, Any]:
    return {"newest": {}, "max": {}, "skipped": 0}


def _parse(raw: bytes) -> EnvelopeBase | None:
    try:
        return parse_event(raw)
    except Exception:  # any failure counts: invalid JSON, a contract violation, a date out of range
        return None


def _chain(previous: str, partition: int, offset: int) -> str:
    return hashlib.sha256(f"{previous}:{partition}:{offset}".encode()).hexdigest()


def _log_step(log: Mapping[str, Any], meta: Meta | None, parsed: bool) -> dict[str, Any]:
    topics: dict[str, Any] = dict(log["topics"])
    rejected = int(log["rejected"])
    if meta is None:
        return {"topics": topics, "rejected": rejected + 1}
    topic, partition, offset = meta
    previous = topics.get(topic)
    if previous is None:
        entry = {
            "partition": partition,
            "n": 1,
            "last_offset": offset,
            "digest": _chain("", partition, offset),
            "regressions": 0,
            "partition_changes": 0,
        }
    else:
        entry = dict(previous)
        if partition != entry["partition"]:
            entry["partition_changes"] += 1
            entry["partition"] = partition
        elif offset <= entry["last_offset"]:
            entry["regressions"] += 1
        entry["last_offset"] = offset
        entry["n"] += 1
        entry["digest"] = _chain(entry["digest"], partition, offset)
    topics[topic] = entry
    return {"topics": topics, "rejected": rejected + (0 if parsed else 1)}


def _lead_step(leads: Mapping[str, Any], event: EnvelopeBase | None) -> dict[str, Any]:
    newest: dict[str, int] = dict(leads["newest"])
    best: dict[str, int] = dict(leads["max"])
    skipped = int(leads["skipped"])
    try:
        if event is None:
            raise ValueError("record did not parse")
        stream = event.stream.value
        slot = quarter_index(available_at(event))
    except Exception:  # unparsed record, a forecast model without a delay, a date out of range
        return {"newest": newest, "max": best, "skipped": skipped + 1}
    newest[stream] = max(newest.get(stream, slot), slot)
    for other, other_slot in newest.items():
        if other != stream:
            lead = newest[stream] - other_slot
            key = f"{stream}>{other}"
            best[key] = max(best.get(key, lead), lead)
    return {"newest": newest, "max": best, "skipped": skipped}


def log_apply(log: Mapping[str, Any], meta: Meta | None, raw: bytes) -> dict[str, Any]:
    """The log after one record: compared with ``==`` between Restate and the offline fold."""
    parsed = True if meta is None else _parse(raw) is not None
    return _log_step(log, meta, parsed)


def lead_apply(leads: Mapping[str, Any], raw: bytes) -> dict[str, Any]:
    """The stream leads after one record: measured, never compared."""
    return _lead_step(leads, _parse(raw))


def apply(
    log: Mapping[str, Any], leads: Mapping[str, Any], meta: Meta | None, raw: bytes
) -> tuple[dict[str, Any], dict[str, Any]]:
    """``(log_apply(log, meta, raw), lead_apply(leads, raw))`` with one parse of the record."""
    event = _parse(raw)
    return _log_step(log, meta, event is not None), _lead_step(leads, event)


def fold_log(records: Iterable[tuple[Meta | None, bytes]]) -> dict[str, Any]:
    """The log of one key over its records in arrival order (the offline side of the comparison)."""
    log = new_log()
    for meta, raw in records:
        log = log_apply(log, meta, raw)
    return log
