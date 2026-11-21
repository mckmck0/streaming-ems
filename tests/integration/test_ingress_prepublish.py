"""AC-010-11 on the actors (AC-011-47): while no ingress container runs nothing is consumed; once
they start, each record is handled once.

No household is configured: `Household`, `Weather` and `Tariff` only log and count the records.
"""

from __future__ import annotations

import time
from collections.abc import Iterator

import pytest

from ems.experiments import stack
from tests.stack_fixtures import LOGS, new_prefix


@pytest.fixture(scope="module")
def closed_env(bootstrap: str) -> Iterator[stack.StackEnv]:
    """Topics, SDK service and registration, but no ingress containers yet."""
    env = stack.StackEnv(new_prefix("ip"), log_dir=LOGS)
    try:
        env.open(start_ingress=False)
        yield env
    finally:
        env.close()


def test_ac_010_11_records_wait_in_kafka_until_the_ingress_starts(
    closed_env: stack.StackEnv, run_id: str
) -> None:
    records = closed_env.publish(run_id, locations=2, days=0.5)
    time.sleep(5)
    assert closed_env.restate.handled(run_id) == 0
    for service in stack.INGRESS_SERVICES:
        committed, end = stack.group_progress(
            closed_env.settings,
            closed_env.ingress.group(service),
            closed_env.ingress.service_topics(service),
        )
        assert committed == 0 and end > 0, service

    closed_env.ingress.up()
    closed_env.restate.wait_handled(run_id, len(records), timeout=300)
    time.sleep(2)  # anything delivered twice would show up now
    assert closed_env.restate.handled(run_id) == len(records)
    assert closed_env.restate.ingress_logs(run_id) == stack.offline_logs(records)
