"""Idempotent topic management: compare desired topics with the broker and act.

``plan_actions`` is pure (unit-tested with fake data); ``KafkaTopicAdmin`` reads and applies through
confluent-kafka's AdminClient. Topics are never auto-created by the broker or the clients.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, cast

from ems.contracts.topics import DesiredTopic, TopicPlan


class ActionKind(StrEnum):
    CREATE = "create"
    OK = "ok"
    INCREASE_PARTITIONS = "increase_partitions"
    FAIL_FEWER_PARTITIONS = "fail_fewer_partitions"
    WARN_MORE_PARTITIONS = "warn_more_partitions"
    DRIFT = "drift"
    APPLY_CONFIG = "apply_config"
    FAIL_REPLICATION = "fail_replication"
    WARN_REPLICATION_DRIFT = "warn_replication_drift"


FAILURES = {ActionKind.FAIL_FEWER_PARTITIONS, ActionKind.FAIL_REPLICATION}
CHANGES = {ActionKind.CREATE, ActionKind.INCREASE_PARTITIONS, ActionKind.APPLY_CONFIG}


@dataclass(frozen=True)
class ActualTopic:
    name: str
    partitions: int
    replication_factor: int
    configs: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class Action:
    kind: ActionKind
    topic: str
    detail: str = ""
    diff: Mapping[str, tuple[str | None, str]] = field(default_factory=dict)


@dataclass
class EnsureReport:
    actions: list[Action] = field(default_factory=list)

    @property
    def failed(self) -> bool:
        return any(a.kind in FAILURES for a in self.actions)

    @property
    def changed(self) -> bool:
        return any(a.kind in CHANGES for a in self.actions)

    def lines(self) -> list[str]:
        out = []
        for a in self.actions:
            line = f"{a.kind.value:24} {a.topic}"
            if a.detail:
                line += f"  {a.detail}"
            out.append(line)
            for key, (old, new) in sorted(a.diff.items()):
                out.append(f"{'':24}   {key}: {old!r} -> {new!r}")
        if not self.changed and not self.failed:
            out.append("no change")
        return out


def config_diff(desired: DesiredTopic, actual: ActualTopic) -> dict[str, tuple[str | None, str]]:
    """Desired keys whose broker value differs (keys not in the desired config are ignored)."""
    return {
        k: (actual.configs.get(k), v)
        for k, v in desired.configs.items()
        if actual.configs.get(k) != v
    }


def plan_actions(
    desired: Iterable[DesiredTopic],
    actual: Mapping[str, ActualTopic],
    brokers: int,
    *,
    allow_partition_increase: bool = False,
    apply_config: bool = False,
) -> EnsureReport:
    report = EnsureReport()
    for want in desired:
        if want.replication_factor > brokers:
            report.actions.append(
                Action(
                    ActionKind.FAIL_REPLICATION,
                    want.name,
                    f"replication factor {want.replication_factor} > {brokers} broker(s); "
                    "use a profile matching the cluster",
                )
            )
            continue
        have = actual.get(want.name)
        if have is None:
            report.actions.append(
                Action(
                    ActionKind.CREATE,
                    want.name,
                    f"partitions={want.partitions} rf={want.replication_factor}",
                )
            )
            continue
        problem = False
        if have.partitions < want.partitions:
            if allow_partition_increase:
                report.actions.append(
                    Action(
                        ActionKind.INCREASE_PARTITIONS,
                        want.name,
                        f"{have.partitions} -> {want.partitions}; WARNING: per-key ordering breaks "
                        "across this change (keys move to other partitions)",
                    )
                )
            else:
                report.actions.append(
                    Action(
                        ActionKind.FAIL_FEWER_PARTITIONS,
                        want.name,
                        f"has {have.partitions} partitions, configured {want.partitions}; "
                        "rerun with --allow-partition-increase to grow it "
                        "(breaks per-key ordering)",
                    )
                )
                problem = True
        elif have.partitions > want.partitions:
            report.actions.append(
                Action(
                    ActionKind.WARN_MORE_PARTITIONS,
                    want.name,
                    f"has {have.partitions} partitions, configured {want.partitions}",
                )
            )
        if have.replication_factor != want.replication_factor:
            report.actions.append(
                Action(
                    ActionKind.WARN_REPLICATION_DRIFT,
                    want.name,
                    f"rf {have.replication_factor}, configured {want.replication_factor} "
                    "(cannot be changed by ensure)",
                )
            )
        diff = config_diff(want, have)
        if diff:
            kind = ActionKind.APPLY_CONFIG if apply_config else ActionKind.DRIFT
            detail = "" if apply_config else "rerun with --apply-config to apply"
            report.actions.append(Action(kind, want.name, detail, diff))
        if not problem and not diff and have.partitions >= want.partitions:
            report.actions.append(Action(ActionKind.OK, want.name))
    return report


class KafkaTopicAdmin:
    """Thin wrapper over AdminClient: read the current state, apply a plan's actions."""

    def __init__(self, bootstrap: str, timeout_s: float = 30.0) -> None:
        from confluent_kafka.admin import AdminClient

        from ems.ingest.kafka.config import base_config

        self._admin = AdminClient(base_config(bootstrap, "ems-gen-admin"))
        self._timeout = timeout_s

    def broker_count(self) -> int:
        result = self._admin.describe_cluster(request_timeout=self._timeout).result(
            timeout=self._timeout
        )
        return len(result.nodes)

    def actual(self, names: Iterable[str]) -> dict[str, ActualTopic]:
        from confluent_kafka.admin import ConfigResource, ResourceType

        wanted = set(names)
        meta = self._admin.list_topics(timeout=self._timeout)  # all topics: never auto-creates
        present = {n: t for n, t in meta.topics.items() if n in wanted}
        out: dict[str, ActualTopic] = {}
        if not present:
            return out
        resources = [ConfigResource(ResourceType.TOPIC, n) for n in present]
        futures = self._admin.describe_configs(resources, request_timeout=self._timeout)
        for res, fut in futures.items():
            entries = cast(dict[str, Any], fut.result(timeout=self._timeout))
            name = res.name
            topic = present[name]
            parts = topic.partitions
            rf = len(next(iter(parts.values())).replicas) if parts else 0
            out[name] = ActualTopic(
                name=name,
                partitions=len(parts),
                replication_factor=rf,
                configs={k: e.value for k, e in entries.items() if e.value is not None},
            )
        return out

    def apply(self, plan: TopicPlan, report: EnsureReport) -> None:
        from confluent_kafka import KafkaError, KafkaException
        from confluent_kafka.admin import (
            AlterConfigOpType,
            ConfigEntry,
            ConfigResource,
            NewPartitions,
            NewTopic,
            ResourceType,
        )

        desired = plan.by_name()
        creates = [desired[a.topic] for a in report.actions if a.kind is ActionKind.CREATE]
        if creates:
            futures = self._admin.create_topics(
                [
                    NewTopic(
                        t.name,
                        num_partitions=t.partitions,
                        replication_factor=t.replication_factor,
                        config=dict(t.configs),
                    )
                    for t in creates
                ],
                operation_timeout=self._timeout,
                request_timeout=self._timeout,
            )
            for fut in futures.values():
                try:
                    fut.result(timeout=self._timeout)
                except KafkaException as exc:  # a concurrent ensure created it first: fine
                    if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                        raise
        grows = [
            desired[a.topic] for a in report.actions if a.kind is ActionKind.INCREASE_PARTITIONS
        ]
        if grows:
            futures = self._admin.create_partitions(
                [NewPartitions(t.name, t.partitions) for t in grows],
                operation_timeout=self._timeout,
                request_timeout=self._timeout,
            )
            for fut in futures.values():
                fut.result(timeout=self._timeout)
        alters = [a for a in report.actions if a.kind is ActionKind.APPLY_CONFIG]
        if alters:
            resources = [
                ConfigResource(
                    ResourceType.TOPIC,
                    a.topic,
                    incremental_configs=[
                        ConfigEntry(k, new, incremental_operation=AlterConfigOpType.SET)
                        for k, (_old, new) in sorted(a.diff.items())
                    ],
                )
                for a in alters
            ]
            futures = self._admin.incremental_alter_configs(
                resources, request_timeout=self._timeout
            )
            for fut in futures.values():
                fut.result(timeout=self._timeout)

    def delete(self, names: Iterable[str]) -> list[str]:
        existing = set(self._admin.list_topics(timeout=self._timeout).topics)
        targets = [n for n in names if n in existing]
        if targets:
            futures = self._admin.delete_topics(targets, operation_timeout=self._timeout)
            for fut in futures.values():
                fut.result(timeout=self._timeout)
        return targets


def ensure(
    admin: KafkaTopicAdmin,
    plan: TopicPlan,
    *,
    allow_partition_increase: bool = False,
    apply_config: bool = False,
) -> EnsureReport:
    names = [t.name for t in plan.topics]
    report = plan_actions(
        plan.topics,
        admin.actual(names),
        admin.broker_count(),
        allow_partition_increase=allow_partition_increase,
        apply_config=apply_config,
    )
    if not report.failed:
        admin.apply(plan, report)
    return report


LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1", "kafka"}


def is_local_bootstrap(bootstrap: str) -> bool:
    """True when every bootstrap host is the local machine or the compose broker."""
    hosts = []
    for server in bootstrap.split(","):
        host = server.strip().rsplit(":", 1)[0].strip("[]")
        hosts.append(host)
    return bool(hosts) and all(h in LOCAL_HOSTS or h.startswith("kafka-") for h in hosts)
