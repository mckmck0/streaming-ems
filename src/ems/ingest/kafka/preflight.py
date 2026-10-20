"""Refuse a run whose record timestamps the target topics would reject or delete by age.

Kafka 4.x rejects CreateTime records more than ``message.timestamp.after.max.ms`` (default 1 h)
in the future, and time-based retention deletes segments whose newest record is older than
``retention.ms`` within minutes. Both would break a backfill or an accelerated run, silently
or halfway through.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta

KAFKA_DEFAULT_AFTER_MAX_MS = 3_600_000  # Kafka 4.0+ (KIP-1030)
LONG_MAX = 9_223_372_036_854_775_807
EXPIRY_MARGIN = timedelta(hours=1)


def problems(
    topic_configs: Mapping[str, Mapping[str, str]],
    *,
    min_ts: datetime,
    max_ts: datetime,
    now: datetime,
) -> list[str]:
    """Human-readable reasons the run must not start (empty list = OK)."""
    out: list[str] = []
    for topic, cfg in sorted(topic_configs.items()):
        if cfg.get("message.timestamp.type", "CreateTime") != "CreateTime":
            continue
        after_ms = int(cfg.get("message.timestamp.after.max.ms", KAFKA_DEFAULT_AFTER_MAX_MS))
        if after_ms < LONG_MAX and max_ts > now + timedelta(milliseconds=after_ms):
            out.append(
                f"{topic}: records up to {max_ts.isoformat()} are more than "
                f"message.timestamp.after.max.ms={after_ms} ahead of now and would be rejected; "
                "use the sim profile (make topics PROFILE=sim)"
            )
        before_ms = int(cfg.get("message.timestamp.before.max.ms", LONG_MAX))
        if before_ms < LONG_MAX and min_ts < now - timedelta(milliseconds=before_ms):
            out.append(
                f"{topic}: records from {min_ts.isoformat()} are older than "
                f"message.timestamp.before.max.ms={before_ms} and would be rejected; "
                "use the sim profile"
            )
        retention_ms = int(cfg.get("retention.ms", "604800000"))
        if (
            retention_ms >= 0
            and min_ts < now - timedelta(milliseconds=retention_ms) + EXPIRY_MARGIN
        ):
            out.append(
                f"{topic}: records from {min_ts.isoformat()} would expire under "
                f"retention.ms={retention_ms} within minutes of being written; use the sim profile"
            )
    return out
