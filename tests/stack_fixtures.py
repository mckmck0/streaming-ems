"""Fixtures shared by the tiers that run against the compose stack (integration, e2e, fault; spec
010).

Each tier's conftest imports what it needs from here. The Restate fixtures wrap the harness in
`ems.experiments.stack`: every module gets its own topic prefix, SDK service and ingress containers,
and every test its own run_id, so no test reads data an earlier run left behind and none writes to
the stack's own topics.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from confluent_kafka.admin import AdminClient

from ems.experiments import stack
from ems.ingest.kafka.config import base_config
from ems.settings import EmsSettings

REPO = Path(__file__).resolve().parents[1]
LOGS = REPO / "logs" / "tests"


@pytest.fixture(scope="session")
def bootstrap() -> str:
    s = EmsSettings()
    try:
        AdminClient(base_config(s.kafka_bootstrap)).list_topics(timeout=10)
    except Exception as exc:  # broker missing: these tests need real services
        pytest.fail(f"Kafka not reachable at {s.kafka_bootstrap} ({exc}); run `make up` first")
    return s.kafka_bootstrap


@pytest.fixture(scope="session", autouse=True)
def restate_session() -> Iterator[None]:
    """AC-010-26: stop before the first test if the SDK port is taken; clear stale test projects."""
    if stack.port_in_use(stack.SDK_PORT):
        pytest.exit(
            f"port {stack.SDK_PORT} is in use (is `make dev` running?): the Restate tiers start "
            "their own SDK service there. Stop it and run again.",
            returncode=3,
        )
    stack.remove_test_projects()
    yield


def new_prefix(tag: str = "it") -> str:
    return f"{tag}{uuid.uuid4().hex[:10]}."


@pytest.fixture(scope="module")
def stack_env(bootstrap: str) -> Iterator[stack.StackEnv]:
    """A module's environment: own topics, SDK service and ingress containers; ingestion of the
    stack closed.
    """
    env = stack.StackEnv(new_prefix(), log_dir=LOGS)
    try:
        env.open()
        yield env
    finally:
        env.close()


@pytest.fixture
def run_id() -> str:
    return f"r{uuid.uuid4().hex[:10]}"
