"""AC-012-34, -23 (T42): strategies added and removed with `set_strategies` during a paced replay of
the 10-household catalog, compared bit for bit with the offline twin given the recorded schedules.

The three managed households (PL-LOC-000001 and -000004 with PV, -000002 without) start one day
from 2026-10-20 with ``[SelfConsumption]``. Before anything is published, as the runner does
(AC-012-17):
- every household's `NoBattery` object is configured with ``strategy_params(params, change)``, and
  the test waits until each stores exactly those parameters;
- then each household gets, through the HTTP ingress with the runner's Idempotency-Key, the change
  "from 06:00, NoBattery and SelfConsumption" (``t1``) and the change "from 18:00, SelfConsumption"
  (``t2``), both accepted.

The day is published paced (`ems-gen run --mode accelerated --speed 960`, about 90 s of wall time)
while the ingress containers consume, so the records reach the actors as a live stream would, only
faster (AC-012-23). Once the publisher is done, the records are read back and the join folded
offline; the schedules are read from the households' state; `wait_run` waits for the ends
`run_ends(params, offline, schedules)` gives (NoBattery stops at ``t2``). Then:
- one more change, a removal at an hour already released, is refused ``released`` and counted;
- each household's join, and each strategy's result over its own window, equal
  ``offline.fold(..., schedule)``; `NoBattery` stores ``strategy_params`` and stands at ``t2``;
- `Battery` and `Meter` equal SelfConsumption's shadow (AC-011-36);
- the ingress logs equal the offline fold, unmanaged sites hold only their log and counter, and no
  invocation failed.
"""

from __future__ import annotations

from typing import Any

from ems.actors.battery import BatteryState
from ems.actors.household import HouseholdState
from ems.actors.messages import StrategyChange
from ems.actors.meter import MeterState
from ems.actors.serde import serde_for
from ems.actors.strategy import StrategyState
from ems.assembler.offline import Offline, fold
from ems.assembler.params import RunParams
from ems.assembler.schedule import Change, Refusal, Schedule, strategy_params
from ems.contracts.envelope import Stream
from ems.experiments import stack
from ems.experiments.runs import managed_params, pse_table
from tests.stack_fixtures import (
    LOGS,
    ActorRun,
    _assert_logs_and_unmanaged,
    decoded,
    dump_on_failure,
    utc,
)

HOUSEHOLDS = 10
START, END = "2026-10-20T00:00:00Z", "2026-10-21T00:00:00Z"
SPEED = 960.0  # one day of data in 90 s of wall time
LIVE = "SelfConsumption"
BOTH = ("NoBattery", "SelfConsumption")
T1_HOUR, T2_HOUR, LATE_HOUR = 6, 18, 1  # hours after first_step
TIMEOUT_S = 900.0


def _key(key: str, change: StrategyChange) -> str:
    return f"{key}:set_strategies:{change.t}:{'+'.join(sorted(change.strategies))}"


def _send_change(env: stack.StackEnv, key: str, change: StrategyChange) -> None:
    body = serde_for(StrategyChange).serialize(change)
    env.restate.send("Household", key, "set_strategies", body, idempotency_key=_key(key, change))


def _households(env: stack.StackEnv, run_id: str) -> dict[str, HouseholdState]:
    return decoded(env, run_id, "Household", HouseholdState)


def _stored_params(env: stack.StackEnv, run_id: str, service: str) -> dict[str, Any]:
    serde = serde_for(RunParams)
    return {
        key: serde.deserialize(values["params"].encode())
        for key, values in env.restate.states(run_id, service).items()
        if "params" in values
    }


def _twin(p: RunParams, records: list[stack.Record], schedule: Schedule) -> Offline:
    own = stack.records_by_stream(records, p.household_key)
    market = stack.records_by_stream(records, p.market_key)
    return fold(
        p,
        own.get(Stream.PRODUCTION.value, []),
        own.get(Stream.CONSUMPTION.value, []),
        own.get(Stream.WEATHER_FORECAST.value, []),
        market.get(Stream.PRICE.value, []),
        schedule=schedule,
    )


def test_ac_012_34_strategy_added_and_removed_during_a_paced_replay(
    stack_env: stack.StackEnv, run_id: str
) -> None:
    env = stack_env
    params = managed_params(
        run_id=run_id,
        start=utc(START),
        days=1.0,
        locations=HOUSEHOLDS,
        rcem=pse_table(),
        strategies=(LIVE,),
        live=LIVE,
    )
    assert len(params) == 3, sorted(params)
    env.restate.configure(params)
    first = next(iter(params.values())).first_step
    t1, t2 = first + 4 * T1_HOUR, first + 4 * T2_HOUR
    add, remove = StrategyChange(t1, BOTH), StrategyChange(t2, (LIVE,))

    # The added objects first, with the parameters the twin gives them; the changes only once each
    # stores exactly those (AC-012-17).
    added = {k: strategy_params(p, Change(t1, BOTH)) for k, p in params.items()}
    env.restate.configure(added, services=("NoBattery",))
    stack.wait_for(
        lambda: _stored_params(env, run_id, "NoBattery") == added,
        120,
        "NoBattery not configured with the parameters sent",
        poll=0.5,
    )
    for key in params:
        _send_change(env, key, add)
        _send_change(env, key, remove)
    expected = Schedule(changes=(Change(t1, BOTH), Change(t2, (LIVE,))))
    stack.wait_for(
        lambda: (
            {k: h.schedule for k, h in _households(env, run_id).items() if k in params}
            == dict.fromkeys(params, expected)
        ),
        120,
        "the changes were not both accepted",
        poll=0.5,
    )

    publisher = stack.Publisher(
        env.settings,
        env.prefix,
        run_id,
        mode="accelerated",
        start=START,
        end=END,
        locations=HOUSEHOLDS,
        data_dir=env.data_dir,
        log_path=LOGS / run_id / "publisher.log",
        speed=SPEED,
    ).start()
    try:
        assert publisher.wait(timeout=TIMEOUT_S) == 0, f"ems-gen failed: {publisher.log_path}"
    finally:
        publisher.kill()
    pacing = publisher.manifest().get("pacing") or {}
    assert pacing.get("speed") == SPEED and pacing.get("paced_from_unix"), pacing
    records = stack.read_run(env.settings, env.prefix, run_id)

    with dump_on_failure(run_id, params, records):
        join_only = stack.offline_run(params, records)
        schedules = {k: h.schedule for k, h in _households(env, run_id).items() if k in params}
        assert schedules == dict.fromkeys(params, expected)
        ends = stack.run_ends(params, join_only, schedules)
        for key in params:
            assert join_only[key].join.next == params[key].end_step, key  # the whole day released
            assert ends["NoBattery"][key] == t2, ends["NoBattery"]
        env.restate.wait_run(run_id, len(records), ends, timeout=TIMEOUT_S)

        # The late change: a removal at an hour already released.
        late = StrategyChange(first + 4 * LATE_HOUR, (LIVE,))
        for key in params:
            _send_change(env, key, late)
        refusal = Refusal(late.t, (LIVE,), "released")
        stack.wait_for(
            lambda: all(
                h.schedule.refused == (refusal,)
                for k, h in _households(env, run_id).items()
                if k in params
            ),
            120,
            "the late change was not refused",
            poll=0.5,
        )

        _assert_against_twin(env, run_id, params, records, expected, added)
        run = ActorRun(run_id, dict(params), records, join_only)
        _assert_logs_and_unmanaged(env, run)
        assert env.restate.failures(run_id) == []


def _assert_against_twin(
    env: stack.StackEnv,
    run_id: str,
    params: dict[str, RunParams],
    records: list[stack.Record],
    schedule: Schedule,
    added: dict[str, RunParams],
) -> None:
    households = _households(env, run_id)
    batteries = decoded(env, run_id, "Battery", BatteryState)
    meters = decoded(env, run_id, "Meter", MeterState)
    strategies: dict[str, dict[str, Any]] = {
        name: decoded(env, run_id, name, StrategyState) for name in ("NoBattery", LIVE)
    }
    for key, p in params.items():
        twin = _twin(p, records, schedule)
        h = households[key]
        assert h.join == twin.join, key
        assert h.schedule.changes == schedule.changes, key
        assert set(twin.strategies) == {"NoBattery", LIVE}, sorted(twin.strategies)
        for name, result in twin.strategies.items():
            assert strategies[name][key].result == result, (key, name)
        # NoBattery got the steps of [t1, t2) and nothing after its removal.
        assert strategies["NoBattery"][key].steps.expected == schedule.changes[1].t, key
        # AC-011-36: the real battery and meter equal the live strategy's virtual ones.
        live = twin.strategies[LIVE]
        assert batteries[key].soc_kwh == live.state.soc_kwh, key
        m = meters[key]
        assert (m.acc, m.recent, m.months) == (live.state.acc, live.recent, live.months), key
        # Stored parameters: the run's for the configured objects, strategy_params for NoBattery.
        for service in (*stack.CONFIGURED, LIVE):
            assert _stored_params(env, run_id, service)[key] == p, (service, key)
        assert _stored_params(env, run_id, "NoBattery")[key] == added[key], key
