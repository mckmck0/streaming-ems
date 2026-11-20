"""Integration fixtures: the compose stack from `make up`, isolated per run by topic prefix.

The Restate fixtures (spec 010) live in tests/stack_fixtures.py, shared with the e2e and fault
tiers.
"""

from __future__ import annotations

import subprocess
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest

from ems.contracts.topics import TopicPlan, load_plan
from ems.ingest.kafka.admin import KafkaTopicAdmin, ensure
from tests.stack_fixtures import bootstrap, restate_session, run_id, stack_env

__all__ = ["bootstrap", "restate_session", "run_id", "stack_env"]

REPO = Path(__file__).resolve().parents[2]
COMPOSE = ["docker", "compose", "-f", str(REPO / "infra" / "docker-compose.yml")]


@pytest.fixture
def run_tag() -> str:
    # Unique per test run so data left by earlier runs never affects results (tests rule).
    return f"it{uuid.uuid4().hex[:10]}"


def _ensure(bootstrap: str, profile: str, prefix: str) -> TopicPlan:
    plan = load_plan(REPO / "config", profile, prefix)
    report = ensure(KafkaTopicAdmin(bootstrap), plan, apply_config=True)
    assert not report.failed, report.lines()
    return plan


@pytest.fixture
def topics_factory(bootstrap: str) -> Iterator[object]:
    created: list[TopicPlan] = []

    def make(profile: str, prefix: str) -> TopicPlan:
        plan = _ensure(bootstrap, profile, prefix)
        created.append(plan)
        return plan

    yield make
    admin = KafkaTopicAdmin(bootstrap)
    for plan in created:
        admin.delete(t.name for t in plan.topics)


def compose(*args: str, timeout: float = 180) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [*COMPOSE, *args], check=True, capture_output=True, text=True, timeout=timeout
    )
