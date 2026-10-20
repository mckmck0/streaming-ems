"""AC-001-05: topic-ensure decisions, with fake broker state."""

from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest

from ems.contracts.topics import DesiredTopic, load_plan
from ems.ingest.kafka.admin import (
    ActionKind,
    ActualTopic,
    EnsureReport,
    is_local_bootstrap,
    plan_actions,
)

WANT = DesiredTopic(
    name="ems.production.v1",
    topic_class="location",
    key="location_id",
    partitions=3,
    replication_factor=1,
    configs={"retention.ms": "604800000", "cleanup.policy": "delete"},
)


def _have(
    partitions: int = 3, rf: int = 1, configs: dict[str, str] | None = None
) -> dict[str, ActualTopic]:
    cfg = {"retention.ms": "604800000", "cleanup.policy": "delete", "segment.ms": "604800000"}
    cfg.update(configs or {})
    return {WANT.name: ActualTopic(WANT.name, partitions, rf, cfg)}


def kinds(report: EnsureReport) -> list[ActionKind]:
    return [a.kind for a in report.actions]


def test_missing_topic_is_created() -> None:
    report = plan_actions([WANT], {}, brokers=1)
    assert kinds(report) == [ActionKind.CREATE] and report.changed and not report.failed


def test_matching_topic_is_ok_and_unchanged() -> None:
    report = plan_actions([WANT], _have(), brokers=1)
    assert kinds(report) == [ActionKind.OK]
    assert not report.changed and report.lines()[-1] == "no change"


def test_fewer_partitions_fail_unless_increase_allowed() -> None:
    report = plan_actions([WANT], _have(partitions=1), brokers=1)
    assert kinds(report) == [ActionKind.FAIL_FEWER_PARTITIONS] and report.failed
    allowed = plan_actions([WANT], _have(partitions=1), brokers=1, allow_partition_increase=True)
    assert kinds(allowed) == [ActionKind.INCREASE_PARTITIONS]
    assert "ordering" in allowed.actions[0].detail


def test_more_partitions_warn() -> None:
    report = plan_actions([WANT], _have(partitions=6), brokers=1)
    assert ActionKind.WARN_MORE_PARTITIONS in kinds(report) and not report.failed


def test_config_drift_is_reported_then_applied_on_request() -> None:
    drifted = _have(configs={"retention.ms": "-1"})
    report = plan_actions([WANT], drifted, brokers=1)
    assert kinds(report) == [ActionKind.DRIFT] and not report.changed
    assert report.actions[0].diff == {"retention.ms": ("-1", "604800000")}
    applied = plan_actions([WANT], drifted, brokers=1, apply_config=True)
    assert kinds(applied) == [ActionKind.APPLY_CONFIG] and applied.changed


def test_replication_factor_above_broker_count_fails() -> None:
    want = dataclasses.replace(WANT, replication_factor=3)
    report = plan_actions([want], {}, brokers=1)
    assert kinds(report) == [ActionKind.FAIL_REPLICATION] and report.failed


def test_replication_drift_is_only_reported() -> None:
    report = plan_actions([WANT], _have(rf=3), brokers=3)
    assert ActionKind.WARN_REPLICATION_DRIFT in kinds(report) and not report.failed


def test_second_run_on_ensured_state_is_noop(repo: Path) -> None:
    plan = load_plan(repo / "config", "sim")
    actual = {
        t.name: ActualTopic(t.name, t.partitions, t.replication_factor, dict(t.configs))
        for t in plan.topics
    }
    report = plan_actions(plan.topics, actual, brokers=1)
    assert set(kinds(report)) == {ActionKind.OK} and not report.changed


@pytest.mark.parametrize(
    ("bootstrap", "local"),
    [
        ("localhost:9092", True),
        ("kafka:29092", True),
        ("127.0.0.1:9092,kafka-2:29092", True),
        ("broker.prod.example.com:9092", False),
        ("localhost:9092,prod:9092", False),
    ],
)
def test_delete_only_against_local_brokers(bootstrap: str, local: bool) -> None:
    assert is_local_bootstrap(bootstrap) is local
