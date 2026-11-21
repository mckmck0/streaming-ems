"""AC-011-38 … -42: ten households through Kafka and the actors equal the offline side, bit for bit.

The 10-household catalog has three EMS-managed sites (PL-LOC-000001 and -000004 with PV, -000002
without); the other seven locations are unmanaged. Each test configures the managed households,
publishes a backfill into the module's own topics, folds the records read back from Kafka offline,
waits until every managed strategy, `Ems`, `Battery` and `Meter` expects the step after the last
one the offline side released, and then compares (``tests.stack_fixtures.assert_run``):
- each managed household's `join` and each strategy's `result` equal the offline side's;
- `Battery`'s SoC equals SelfConsumption's virtual SoC, and `Meter`'s (accumulator, kept flows,
  closed months) equal SelfConsumption's (AC-011-36);
- every Kafka-fed object's ingress log equals the offline fold of the records;
- every unmanaged `Household` and `Weather` holds only `log` and `n`, under 1 kB in total;
- every managed object received 0 records before its `configure`, and no invocation failed.

Five runs: the clean 30 h from 2026-10-31 (October closes inside it); one day from 2026-10-20 with
the meter topics first and the price topic last, and the reverse; and two runs of that day with the
fault fixture, at `L` = 8 and `L` = 2, each first checked on the offline side for the faults it
must contain. Fault patterns depend on the event ids, which contain the run's fresh run_id, so each
fault run asserts that its records hold what the criterion needs before it compares.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence

from ems.actors.meter import MeterState
from ems.actors.strategy import StrategyState
from ems.assembler.offline import Offline
from ems.experiments import stack
from tests.stack_fixtures import FAULTS, actor_run, decoded, offline_counts

HOUSEHOLDS = 10
CLEAN_START, CLEAN_DAYS = "2026-10-31T00:00:00Z", 1.25  # 30 h: 120 steps, across October's end
DAY_START, DAY = "2026-10-20T00:00:00Z", 1.0
STEPS_IN_30_H, STEPS_IN_A_DAY = 120, 96
METERS_FIRST = (
    ("ingress-meters", "Household"),
    ("ingress-forecasts", "Weather"),
    ("ingress-prices", "Tariff"),
)
PRICES_FIRST = tuple(reversed(METERS_FIRST))


def _every_step_released(steps: int) -> Callable[[dict[str, Offline]], None]:
    def check(offline: dict[str, Offline]) -> None:
        for key, o in offline.items():
            assert len(o.steps) == steps, key
            assert [s.t for s in o.steps] == list(range(o.steps[0].t, o.steps[0].t + steps)), key

    return check


def test_ac_011_38_clean_run_across_the_end_of_october(
    stack_env: stack.StackEnv, run_id: str
) -> None:
    run = actor_run(
        stack_env,
        run_id,
        start=CLEAN_START,
        days=CLEAN_DAYS,
        locations=HOUSEHOLDS,
        late_slots=8,
        check_offline=_every_step_released(STEPS_IN_30_H),
    )
    assert len(run.params) == 3
    for key, p in run.params.items():
        assert run.offline[key].join.next == p.end_step == p.first_step + STEPS_IN_30_H, key
        for name in p.strategies:
            assert "2026-10" in [m.month for m in run.offline[key].strategies[name].months], name
    # October's settlement closed in every strategy object and in Meter (equal to the offline side
    # by assert_run; read here from the actors directly).
    meters = decoded(stack_env, run_id, "Meter", MeterState)
    for key, p in run.params.items():
        assert "2026-10" in [m.month for m in meters[key].months], key
        for name in p.strategies:
            state = decoded(stack_env, run_id, name, StrategyState)[key]
            assert "2026-10" in [m.month for m in state.result.months], (key, name)


def _gated(
    env: stack.StackEnv, run_id: str, order: Sequence[tuple[str, str]]
) -> Callable[[], list[stack.Record]]:
    """Publish while nothing consumes, then start the ingress containers one after the other,
    each once the one before has had all its records handled.
    """

    def publish() -> list[stack.Record]:
        env.ingress.pause_consumption()
        try:
            records = env.publish(run_id, locations=HOUSEHOLDS, days=DAY, start=DAY_START)
            for container, service in order:
                expected = sum(stack.service_of(r.meta[0]) == service for r in records)
                assert expected > 0, service
                env.ingress.start(container)
                env.restate.wait_handled(run_id, expected, timeout=600, services=(service,))
        finally:
            env.ingress.resume_consumption()
        return records

    return publish


def test_ac_011_39_meter_topics_first_and_price_topic_last(
    stack_env: stack.StackEnv, run_id: str
) -> None:
    actor_run(
        stack_env,
        run_id,
        start=DAY_START,
        days=DAY,
        locations=HOUSEHOLDS,
        publish=_gated(stack_env, run_id, METERS_FIRST),
        check_offline=_every_step_released(STEPS_IN_A_DAY),
    )


def test_ac_011_39_price_topic_first_and_meter_topics_last(
    stack_env: stack.StackEnv, run_id: str
) -> None:
    actor_run(
        stack_env,
        run_id,
        start=DAY_START,
        days=DAY,
        locations=HOUSEHOLDS,
        publish=_gated(stack_env, run_id, PRICES_FIRST),
        check_offline=_every_step_released(STEPS_IN_A_DAY),
    )


def _faults_at_8(offline: Mapping[str, Offline]) -> None:
    counts = offline_counts(offline)
    assert counts["estimated"] > 0, counts
    assert counts["duplicate"] > 0, counts
    assert counts["corrections"] == 0, counts  # a reading late by at most 2 h is never too late


def test_ac_011_40_faults_at_l_8(stack_env: stack.StackEnv, run_id: str) -> None:
    actor_run(
        stack_env,
        run_id,
        start=DAY_START,
        days=DAY,
        locations=HOUSEHOLDS,
        late_slots=8,
        faults=FAULTS,
        check_offline=_faults_at_8,
    )


def _faults_at_2(offline: Mapping[str, Offline]) -> None:
    counts = offline_counts(offline)
    assert counts["corrections"] > 0, counts
    assert counts["too_old"] > 0, counts


def test_ac_011_41_faults_at_l_2(stack_env: stack.StackEnv, run_id: str) -> None:
    actor_run(
        stack_env,
        run_id,
        start=DAY_START,
        days=DAY,
        locations=HOUSEHOLDS,
        late_slots=2,
        faults=FAULTS,
        check_offline=_faults_at_2,
    )
