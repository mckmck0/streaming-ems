"""AC-010-08, AC-010-26: each topic reaches its Probe handler under the record key; the stack's
topics stay untouched.

One household for one day through the module's own ingress containers (tests/stack_fixtures.py).
"""

from __future__ import annotations

from ems.experiments import stack
from ems.ingest.tools.reader import end_offsets

LOCATION = "PL-LOC-000001"
MARKET = "PL:RCE"


def test_ac_010_08_each_topic_reaches_its_handler_under_the_record_key(
    stack_env: stack.StackEnv, run_id: str, bootstrap: str
) -> None:
    before = {t: end_offsets(bootstrap, t) for t in stack.PROBE_TOPICS}
    records = stack_env.publish(run_id, locations=1, days=1)
    stack_env.restate.wait_handled(run_id, len(records), timeout=300)

    prefix = stack_env.prefix
    logs = stack_env.restate.probe_logs(run_id)
    location, market = f"{run_id}:{LOCATION}", f"{run_id}:{MARKET}"
    assert set(logs) == {location, market}
    assert set(logs[location]["topics"]) == {
        prefix + "ems.production.v1",
        prefix + "ems.consumption.v1",
        prefix + "ems.weather.forecast.v1",
    }
    assert set(logs[market]["topics"]) == {prefix + "ems.price.v1"}

    handlers: dict[str, set[str]] = {}
    for inv in stack_env.restate.invocations(run_id):
        assert inv["status"] == "completed", inv
        handlers.setdefault(inv["target_service_key"], set()).add(inv["target_handler_name"])
    assert handlers == {location: {"on_measurement", "on_forecast"}, market: {"on_price"}}
    assert logs == stack.offline_logs(records)

    # AC-010-26: the run wrote only to its prefixed topics.
    assert {t: end_offsets(bootstrap, t) for t in stack.PROBE_TOPICS} == before
