"""Per-topic statistics: counts per key and partition, event-time range, gaps,
duplicates, late events.

A gap is a missing 15-minute interval between a key's first and last interval (meter topics); a
duplicate is a repeated event id; a late event arrived after a newer interval of the same key (order
of arrival).
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from ems.ingest.tools.reader import Record

QUARTER = timedelta(minutes=15)


@dataclass
class KeyStats:
    count: int = 0
    partitions: set[int] = field(default_factory=set)
    first: datetime | None = None
    last: datetime | None = None
    starts: set[datetime] = field(default_factory=set)
    duplicates: int = 0
    late: int = 0


@dataclass
class TopicStats:
    topic: str
    records: int = 0
    keys: dict[str, KeyStats] = field(default_factory=lambda: defaultdict(KeyStats))
    partitions: Counter[int] = field(default_factory=Counter)

    @property
    def duplicates(self) -> int:
        return sum(k.duplicates for k in self.keys.values())

    @property
    def late(self) -> int:
        return sum(k.late for k in self.keys.values())

    def gaps(self, step: timedelta = QUARTER) -> int:
        total = 0
        for k in self.keys.values():
            if k.first is None or k.last is None:
                continue
            expected = int((k.last - k.first) / step) + 1
            total += expected - len(k.starts)
        return total

    def expected(self, step: timedelta = QUARTER) -> int:
        return sum(
            int((k.last - k.first) / step) + 1 for k in self.keys.values() if k.first and k.last
        )

    def lines(self, meter: bool) -> list[str]:
        unique = sum(len(k.starts) for k in self.keys.values())
        out = [
            f"{self.topic}: records={self.records} unique_intervals={unique} keys={len(self.keys)} "
            f"partitions={dict(sorted(self.partitions.items()))} "
            f"duplicates={self.duplicates} late={self.late}"
            + (f" gaps={self.gaps()} expected={self.expected()}" if meter else "")
        ]
        for key, k in sorted(self.keys.items())[:5]:
            out.append(
                f"    {key}: n={k.count} partitions={sorted(k.partitions)} "
                f"from={k.first.isoformat() if k.first else None} "
                f"to={k.last.isoformat() if k.last else None}"
            )
        if len(self.keys) > 5:
            out.append(f"    ... {len(self.keys) - 5} more keys")
        return out


def topic_stats(topic: str, records: Iterable[Record]) -> TopicStats:
    stats = TopicStats(topic)
    seen: dict[str, set[str]] = defaultdict(set)
    newest: dict[str, datetime] = {}
    for rec in records:
        stats.records += 1
        stats.partitions[rec.partition] += 1
        key = rec.key or ""
        k = stats.keys[key]
        k.count += 1
        k.partitions.add(rec.partition)
        try:
            doc = json.loads(rec.value or b"{}")
            start = datetime.fromisoformat(str(doc["interval_start"]).replace("Z", "+00:00"))
            eid = str(doc["event_id"])
        except (ValueError, KeyError):
            continue
        if eid in seen[key]:
            k.duplicates += 1
            continue
        seen[key].add(eid)
        if key in newest and start < newest[key]:
            k.late += 1
        newest[key] = max(newest.get(key, start), start)
        k.starts.add(start)
        k.first = start if k.first is None else min(k.first, start)
        k.last = start if k.last is None else max(k.last, start)
    return stats
