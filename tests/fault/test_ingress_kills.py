"""AC-010-13 … -16: kill -9 of the Restate server, the SDK service or an ingress container mid-run
loses nothing and doubles nothing; the ingress containers outlast a Restate outage longer than
their reconnect budget, and one that exits on its own comes back by its restart policy.

Each kill test publishes a backfill while the module's ingress containers are stopped, starts them,
waits for its trigger (30 % of the run handled or committed) and kills. It then proves, from data
that survives the kill, that the kill landed mid-run: the record counters of the Kafka-fed actors
for the SDK service, the meters group's committed offsets in Kafka for the server and the ingress
container. A test without that proof fails; enlarge the backfill then.

Since spec 011 (AC-011-47) the records reach `Household`, `Weather` and `Tariff`; no household is
configured, so the actors only keep their ingress logs and counters.
"""

from __future__ import annotations

import time

from ems.experiments import stack
from tests.stack_fixtures import new_prefix

# About 41,000 records: an unkilled run lasts about a minute at the rate the e2e tier showed on the
# author's machine (spec 010, plan decisions).
HOUSEHOLDS, DAYS = 120, 2
TRIGGER = 0.3
METERS = "ingress-meters"
OUTAGE_S = 30  # two reconnect attempts take about 1.5 s


def _publish_paused(env: stack.StackEnv, run_id: str) -> tuple[list[stack.Record], int, int]:
    """Publish while nothing consumes; returns the records and the meters group's (committed, end)
    before.
    """
    env.ingress.pause_consumption()
    try:
        records = env.publish(run_id, locations=HOUSEHOLDS, days=DAYS)
        committed, end = env.meters_progress()
    finally:
        env.ingress.resume_consumption()
    return records, committed, end


def _share(env: stack.StackEnv, base: int, end: int) -> float:
    committed, _ = env.meters_progress()
    return (committed - base) / (end - base)


def _exactly_once(env: stack.StackEnv, run_id: str, records: list[stack.Record]) -> None:
    env.restate.wait_handled(run_id, len(records), timeout=1800)
    logs = env.restate.ingress_logs(run_id)
    assert logs == stack.offline_logs(records)
    entries = [entry for log in logs.values() for entry in log["topics"].values()]
    assert sum(e["regressions"] for e in entries) == 0
    assert sum(e["n"] for e in entries) == len(records)


def test_ac_010_13_restate_server_kill(stack_env: stack.StackEnv, run_id: str) -> None:
    records, base, end = _publish_paused(stack_env, run_id)
    stack.wait_for(lambda: _share(stack_env, base, end) >= TRIGGER, 600, "trigger not reached")
    assert stack.kill_restate() == 137
    committed, _ = stack_env.meters_progress()
    assert committed < end, "the kill landed after ingestion had finished"
    stack.start_restate(stack_env.restate)  # same configuration, healthy without a manual step
    _exactly_once(stack_env, run_id, records)


def test_ac_010_14_sdk_service_kill(stack_env: stack.StackEnv, run_id: str) -> None:
    records, _, _ = _publish_paused(stack_env, run_id)
    expected = len(records)
    stack.wait_for(
        lambda: stack_env.restate.handled(run_id) >= TRIGGER * expected, 600, "trigger not reached"
    )
    stack_env.sdk.kill9()  # every process of the group; returns once nothing listens on 9080
    assert stack_env.restate.handled(run_id) < expected, "the kill landed after the last record"
    stack_env.sdk.start()
    _exactly_once(stack_env, run_id, records)


def test_ac_010_15_ingress_container_kill(stack_env: stack.StackEnv, run_id: str) -> None:
    records, base, end = _publish_paused(stack_env, run_id)
    stack.wait_for(lambda: _share(stack_env, base, end) >= TRIGGER, 600, "trigger not reached")
    assert stack_env.ingress.kill9(METERS) == 137
    committed, _ = stack_env.meters_progress()
    assert committed < end, "the kill landed after ingestion had finished"
    stack_env.ingress.start(METERS)
    _exactly_once(stack_env, run_id, records)


def test_ac_010_16_ingress_outlasts_a_restate_outage_and_restarts_when_it_exits(
    stack_env: stack.StackEnv, run_id: str
) -> None:
    settings = stack_env.settings
    prefix = new_prefix("ig")
    plan = stack.ensure_topics(settings, prefix)
    ingress = stack.IngressSet(prefix, retry_max_attempts=2)
    ingress.up()
    try:
        # Restate down for far longer than two reconnect attempts: the containers keep reconnecting.
        stack.stop_restate()
        stack.publish_backfill(
            settings, prefix, run_id, locations=5, days=1, data_dir=stack_env.data_dir
        )
        records = stack.read_run(settings, prefix, run_id)
        time.sleep(OUTAGE_S)
        for service in stack.INGRESS_SERVICES:
            cid = ingress.container(service)
            assert stack.container_state(cid)["Running"], service
            assert stack.restart_count(cid) == 0, service
        stack.start_restate(stack_env.restate)
        _exactly_once(stack_env, run_id, records)

        # A container that exits on its own (SIGTERM to the JVM from inside) is restarted.
        cid = ingress.container(METERS)
        stack.docker("exec", cid, "bash", "-c", "kill -TERM 1", check=False)
        stack.wait_for(lambda: stack.restart_count(cid) >= 1, 120, f"{METERS} not restarted", 1.0)
        stack.wait_for(lambda: stack.container_state(cid)["Running"], 120, f"{METERS} not running")
        later = f"{run_id}b"
        stack.publish_backfill(
            settings, prefix, later, locations=5, days=1, data_dir=stack_env.data_dir
        )
        _exactly_once(stack_env, later, stack.read_run(settings, prefix, later))
    finally:
        if not stack_env.restate.healthy():
            stack.start_restate(stack_env.restate)
        ingress.down()
        stack.delete_topics(settings, plan, [ingress.group(s) for s in stack.INGRESS_SERVICES])
