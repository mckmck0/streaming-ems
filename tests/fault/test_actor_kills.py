"""AC-011-43: kill -9 of every process of the SDK service during a configured backfill, after the
first step and before the last; once it is started again the actors end equal to the offline side.

30 households over 2 days from 2026-10-20 (about 9 managed sites, 10,900 records; plan, "Fault-tier
run"), configured before anything is published. The backfill is published while the module's
ingress containers are stopped, so the kill lands while records are still flowing. The SDK service
is killed once 30 % of the records are handled. From data that survives the kill the test proves
the kill landed inside the run: fewer records handled than published, and a managed strategy whose
next step lies strictly between `first_step` and its offline end. Restate retries the interrupted
invocations against their journals (A11: their bytes must not differ), so the wait is lenient
about recorded failed attempts, never about failed or paused invocations.
"""

from __future__ import annotations

import json

from ems.experiments import stack
from tests.stack_fixtures import assert_run, configure_run, dump_on_failure, finish_run

HOUSEHOLDS, DAYS = 30, 2
START = "2026-10-20T00:00:00Z"
TRIGGER = 0.3


def test_ac_011_43_sdk_service_kill_during_a_configured_run(
    stack_env: stack.StackEnv, run_id: str
) -> None:
    params = configure_run(
        stack_env, run_id, start=START, days=DAYS, locations=HOUSEHOLDS, late_slots=8
    )
    stack_env.ingress.pause_consumption()
    try:
        records = stack_env.publish(run_id, locations=HOUSEHOLDS, days=DAYS, start=START)
    finally:
        stack_env.ingress.resume_consumption()
    published = len(records)

    with dump_on_failure(run_id, params, records):
        stack.wait_for(
            lambda: stack_env.restate.handled(run_id) >= TRIGGER * published,
            600,
            "trigger not reached",
        )
        stack_env.sdk.kill9()  # every process of the group; returns once nothing listens on 9080
        try:
            # The kill landed inside the run (state read while nothing can change it): records still
            # to handle, and a managed strategy mid-way to the end the offline side computes.
            assert stack_env.restate.handled(run_id) < published, (
                "the kill came after the last record"
            )
            ends = stack.run_ends(params, stack.offline_run(params, records))
            strategies = sorted({name for p in params.values() for name in p.strategies})
            midway = [
                (service, key, step)
                for service in strategies
                for key, values in stack_env.restate.states(run_id, service).items()
                if key in params and "state" in values
                for step in [stack.next_step(service, json.loads(values["state"]))]
                if step is not None and params[key].first_step < step < ends[service][key]
            ]
            assert midway, "no managed strategy stood between its first step and its end"
        finally:
            stack_env.sdk.start()
    run = finish_run(stack_env, run_id, params, records, strict=False, timeout=1800)
    assert_run(stack_env, run, strict=False)
