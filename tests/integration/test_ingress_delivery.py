"""AC-010-08, AC-010-26 on the actors (AC-011-25, -47): each topic reaches its target handler under
the record key; the stack's topics stay untouched.

One household for one day through the module's own ingress containers (tests/stack_fixtures.py),
with no household configured: `Household` and `Weather` keep only their ingress log and counter.
Since spec 011 the meters go to `Household.on_measurement`, the forecasts to `Weather.on_forecast`
and the prices to `Tariff.on_price`.
"""

from __future__ import annotations

from ems.experiments import stack
from ems.ingest.tools.reader import end_offsets

LOCATION = "PL-LOC-000001"
MARKET = "PL:RCE"


def test_ac_010_08_each_topic_reaches_its_handler_under_the_record_key(
    stack_env: stack.StackEnv, run_id: str, bootstrap: str
) -> None:
    before = {t: end_offsets(bootstrap, t) for t in stack.TOPICS}
    records = stack_env.publish(run_id, locations=1, days=1)
    stack_env.restate.wait_handled(run_id, len(records), timeout=300)

    prefix = stack_env.prefix
    logs = stack_env.restate.ingress_logs(run_id)
    location, market = f"{run_id}:{LOCATION}", f"{run_id}:{MARKET}"
    assert set(logs) == {("Household", location), ("Weather", location), ("Tariff", market)}
    assert set(logs[("Household", location)]["topics"]) == {
        prefix + "ems.production.v1",
        prefix + "ems.consumption.v1",
    }
    assert set(logs[("Weather", location)]["topics"]) == {prefix + "ems.weather.forecast.v1"}
    assert set(logs[("Tariff", market)]["topics"]) == {prefix + "ems.price.v1"}

    handlers: dict[tuple[str, str], set[str]] = {}
    for inv in stack_env.restate.invocations(run_id):
        assert inv["status"] == "completed", inv
        target = (inv["target_service_name"], inv["target_service_key"])
        handlers.setdefault(target, set()).add(inv["target_handler_name"])
    assert handlers == {
        ("Household", location): {"on_measurement"},
        ("Weather", location): {"on_forecast"},
        ("Tariff", market): {"on_price"},
    }
    assert logs == stack.offline_logs(records)

    # Nothing configured: a location's objects hold only the log and the record counter.
    for service in ("Household", "Weather"):
        assert set(stack_env.restate.states(run_id, service)[location]) == {"log", "n"}, service

    # AC-010-26: the run wrote only to its prefixed topics.
    assert {t: end_offsets(bootstrap, t) for t in stack.TOPICS} == before
