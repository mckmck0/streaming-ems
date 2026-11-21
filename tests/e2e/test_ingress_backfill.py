"""AC-010-09 on the actors (AC-011-47): ten households over one day through Kafka and Restate give,
for every Kafka-fed object and topic, the ingress log the offline fold gives for the records read
back from Kafka, compared with `==`; nothing lost, doubled or out of order (0 regressions), nothing
rejected. No household is configured.
"""

from __future__ import annotations

from collections import Counter

from ems.experiments import stack

HOUSEHOLDS, DAYS = 10, 1


def test_ac_010_09_every_key_sees_its_records_in_partition_order_exactly_once(
    stack_env: stack.StackEnv, run_id: str
) -> None:
    records = stack_env.publish(run_id, locations=HOUSEHOLDS, days=DAYS)
    stack_env.restate.wait_handled(run_id, len(records), timeout=600)

    logs = stack_env.restate.ingress_logs(run_id)
    assert logs == stack.offline_logs(records)
    # Every household's meters and forecasts, and the price market.
    assert Counter(service for service, _ in logs) == {
        "Household": HOUSEHOLDS,
        "Weather": HOUSEHOLDS,
        "Tariff": 1,
    }
    assert sum(log["rejected"] for log in logs.values()) == 0
    entries = [entry for log in logs.values() for entry in log["topics"].values()]
    assert sum(e["regressions"] + e["partition_changes"] for e in entries) == 0
    assert sum(e["n"] for e in entries) == len(records)
