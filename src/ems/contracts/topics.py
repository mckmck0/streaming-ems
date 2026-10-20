"""Topic definitions and routing rules, shared by the generator and the ingestion side.

``load_plan`` reads ``config/topics.yaml``; ``route`` maps an event to ``(topic, key)``;
``murmur2`` and ``expected_partition`` reproduce the Java client's default partitioner without
importing a Kafka client.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from ruamel.yaml import YAML

from ems.contracts.envelope import MARKET_KEYS, EnvelopeBase, PriceEvent, Stream

TopicClass = Literal["location", "global"]

STREAM_TOPICS: dict[Stream, str] = {
    Stream.WEATHER_FORECAST: "ems.weather.forecast.v1",
    Stream.WEATHER_ACTUAL: "ems.weather.actual.v1",
    Stream.PRODUCTION: "ems.production.v1",
    Stream.CONSUMPTION: "ems.consumption.v1",
    Stream.PRICE: "ems.price.v1",
}
DLQ_TOPIC = "ems.gen.dlq.v1"

# Topic configs ensure may set. Anything else is a typo or a config Kafka 4.x removed
# (message.timestamp.difference.max.ms was removed in 4.0).
ALLOWED_CONFIGS = frozenset(
    {
        "cleanup.policy",
        "compression.type",
        "message.timestamp.type",
        "message.timestamp.after.max.ms",
        "message.timestamp.before.max.ms",
        "min.insync.replicas",
        "retention.ms",
        "retention.bytes",
        "segment.ms",
        "segment.bytes",
        "max.message.bytes",
    }
)


class TopicConfigError(ValueError):
    """The topic definition file is invalid."""


@dataclass(frozen=True)
class DesiredTopic:
    name: str
    topic_class: TopicClass
    key: str
    partitions: int
    replication_factor: int
    configs: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class TopicPlan:
    profile: str
    prefix: str
    topics: tuple[DesiredTopic, ...]

    def by_name(self) -> dict[str, DesiredTopic]:
        return {t.name: t for t in self.topics}

    def name_for(self, base: str) -> str:
        return self.prefix + base


def _str(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _configs(raw: Any, where: str) -> dict[str, str]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise TopicConfigError(f"{where}: configs must be a mapping")
    out = {str(k): _str(v) for k, v in raw.items()}
    unknown = sorted(set(out) - ALLOWED_CONFIGS)
    if unknown:
        raise TopicConfigError(f"{where}: unknown or removed topic configs {unknown}")
    return out


def load_topics_file(path: Path) -> dict[str, Any]:
    yaml = YAML(typ="safe")
    data = yaml.load(path.read_text())
    if not isinstance(data, dict) or "topics" not in data or "profiles" not in data:
        raise TopicConfigError(f"{path}: needs 'defaults', 'profiles' and 'topics'")
    return data


def build_plan(data: dict[str, Any], profile: str, prefix: str = "") -> TopicPlan:
    """Resolve defaults, profile and topic overrides into the desired state of enabled topics."""
    profiles = data["profiles"]
    if profile not in profiles:
        raise TopicConfigError(f"unknown profile {profile!r}; choose one of {sorted(profiles)}")
    defaults = data.get("defaults") or {}
    prof = profiles[profile] or {}
    rf = int(prof.get("replication_factor", defaults.get("replication_factor", 1)))
    partitions = prof.get("partitions") or {}
    base_cfg = _configs(defaults.get("configs"), "defaults")
    prof_cfg = _configs(prof.get("configs"), f"profile {profile}")
    tie_before_to_retention = bool(prof.get("before_max_equals_retention", False))
    seen: set[str] = set()
    topics: list[DesiredTopic] = []
    for raw in data["topics"]:
        name = str(raw["name"])
        if name in seen:
            raise TopicConfigError(f"duplicate topic {name}")
        seen.add(name)
        if not raw.get("enabled", True):
            continue
        cls = raw.get("class")
        if cls not in ("location", "global"):
            raise TopicConfigError(f"{name}: class must be 'location' or 'global'")
        if cls not in partitions:
            raise TopicConfigError(f"profile {profile}: no partition count for class {cls}")
        # Precedence: defaults < topic < profile, except the DLQ keeps its own timestamp type.
        cfg = {**base_cfg, **_configs(raw.get("configs"), name), **prof_cfg}
        topic_cfg = _configs(raw.get("configs"), name)
        if "message.timestamp.type" in topic_cfg:
            cfg["message.timestamp.type"] = topic_cfg["message.timestamp.type"]
        if tie_before_to_retention and "retention.ms" in cfg and cfg["retention.ms"] != "-1":
            cfg["message.timestamp.before.max.ms"] = cfg["retention.ms"]
        if cfg.get("message.timestamp.type") == "LogAppendTime":
            cfg.pop("message.timestamp.after.max.ms", None)
            cfg.pop("message.timestamp.before.max.ms", None)
        topics.append(
            DesiredTopic(
                name=prefix + name,
                topic_class=cls,
                key=str(raw.get("key", "")),
                partitions=int(partitions[cls]),
                replication_factor=rf,
                configs=dict(sorted(cfg.items())),
            )
        )
    return TopicPlan(profile=profile, prefix=prefix, topics=tuple(topics))


def load_plan(config_dir: Path, profile: str, prefix: str = "") -> TopicPlan:
    return build_plan(load_topics_file(config_dir / "topics.yaml"), profile, prefix)


def market_key(event: PriceEvent) -> str:
    return MARKET_KEYS[event.payload.market]


def record_key(event: EnvelopeBase) -> str:
    """Kafka key: location id or market key, prefixed with ``run_id:`` for run-scoped events."""
    subject = event.subject
    if not subject:
        raise ValueError("events on location and price topics need a non-empty key")
    return f"{event.run_id}:{subject}" if event.run_id else subject


def route(event: EnvelopeBase, prefix: str = "") -> tuple[str, str]:
    return prefix + STREAM_TOPICS[event.stream], record_key(event)


def murmur2(data: bytes) -> int:
    """Kafka's murmur2 (``org.apache.kafka.common.utils.Utils.murmur2``), as a signed 32-bit int."""
    m = 0x5BD1E995
    r = 24
    length = len(data)
    h = (0x9747B28C ^ length) & 0xFFFFFFFF
    n4 = length // 4
    for i in range(n4):
        k = int.from_bytes(data[4 * i : 4 * i + 4], "little")
        k = (k * m) & 0xFFFFFFFF
        k ^= k >> r
        k = (k * m) & 0xFFFFFFFF
        h = (h * m) & 0xFFFFFFFF
        h ^= k
    tail = data[4 * n4 :]
    rest = len(tail)
    if rest >= 3:
        h ^= tail[2] << 16
    if rest >= 2:
        h ^= tail[1] << 8
    if rest >= 1:
        h ^= tail[0]
        h = (h * m) & 0xFFFFFFFF
    h ^= h >> 13
    h = (h * m) & 0xFFFFFFFF
    h ^= h >> 15
    return h - (1 << 32) if h & 0x80000000 else h


def expected_partition(key: str, partitions: int) -> int:
    """Partition the Java default partitioner (librdkafka ``murmur2_random``) picks."""
    return (murmur2(key.encode()) & 0x7FFFFFFF) % partitions
