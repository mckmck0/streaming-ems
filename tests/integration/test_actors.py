"""AC-011-25 … -29, -31: the actors on the real stack, one managed site at a time.

Every test has its own run_id, so its objects are new. Calls the tests make themselves go through
the HTTP ingress as one-way sends with an idempotency key, so Restate keeps each completed
invocation in `sys_invocation` and the test can read how it ended: a call that fails without a retry
is `completed` with `completion_result = 'failure'`.

The one-site run: PL-LOC-000001 (PV, heat pump, G12 Enea) of the generator's catalog, 2026-10-20
from 00:00Z for 6 h (24 steps), configured before anything is published.
"""

from __future__ import annotations

import dataclasses
import json
from datetime import UTC, datetime
from typing import Any

from confluent_kafka import Producer

from ems.actors.battery import BatteryState
from ems.actors.ems import EmsState
from ems.actors.household import HouseholdState
from ems.actors.messages import PriceUpdate
from ems.actors.meter import MeterState
from ems.actors.serde import serde_for
from ems.actors.strategy import StrategyState
from ems.actors.tariff import TariffState
from ems.actors.weather import WeatherState
from ems.assembler.join import Step
from ems.assembler.params import RunParams
from ems.contracts.envelope import AssetType, Direction, Stream, to_json_bytes
from ems.contracts.examples import energy, measurement_event
from ems.contracts.time import QUARTER
from ems.experiments import stack
from ems.experiments.runs import managed_params
from ems.ingest.kafka.config import base_config
from tests.stack_fixtures import decoded

START = "2026-10-20T00:00:00Z"
START_UTC = datetime(2026, 10, 20, tzinfo=UTC)
DAYS = 0.25  # 6 h: 24 steps
LOCATION = "PL-LOC-000001"
PARAMS_SERDE = serde_for(RunParams)
TIMEOUT_S = 120.0


def _site(run_id: str, **kwargs: Any) -> tuple[str, RunParams]:
    params = managed_params(run_id=run_id, start=START_UTC, days=DAYS, locations=1, **kwargs)
    assert list(params) == [f"{run_id}:{LOCATION}"]
    return next(iter(params.items()))


def _services(p: RunParams) -> tuple[str, ...]:
    return (*stack.CONFIGURED, *p.strategies)


def _completed(
    env: stack.StackEnv, run_id: str, service: str, key: str, handler: str, count: int
) -> list[dict[str, Any]]:
    """The completed invocations of one handler of one object, once there are ``count`` of them."""

    def rows() -> list[dict[str, Any]]:
        return [
            r
            for r in env.restate.invocations(run_id)
            if (r["target_service_name"], r["target_service_key"], r["target_handler_name"])
            == (service, key, handler)
        ]

    stack.wait_for(
        lambda: sum(r["status"] == "completed" for r in rows()) >= count,
        TIMEOUT_S,
        f"{service}/{key}/{handler}: {count} completed invocations",
        poll=0.25,
    )
    found = rows()
    assert all(r["status"] == "completed" for r in found), found
    return found


def _stored_params(env: stack.StackEnv, run_id: str, service: str, key: str) -> RunParams | None:
    text = env.restate.states(run_id, service).get(key, {}).get("params")
    return None if text is None else PARAMS_SERDE.deserialize(text.encode())


# ── configure (AC-011-27) ─────────────────────────────────────────────────────────────────────────


def test_ac_011_27_configure_twice_with_equal_parameters_changes_nothing(
    stack_env: stack.StackEnv, run_id: str
) -> None:
    key, p = _site(run_id)
    restate = stack_env.restate
    restate.configure({key: p})
    restate.configure({key: p})  # the same idempotency keys: answered from the first call
    for service in _services(p):  # a second call that reaches each handler, with equal parameters
        restate.send(
            service,
            key,
            "configure",
            PARAMS_SERDE.serialize(p),
            idempotency_key=f"{key}:{service}:configure:again",
        )

    for service in _services(p):
        rows = _completed(stack_env, run_id, service, key, "configure", 2)
        assert [r["completion_result"] for r in rows] == ["success", "success"], service
        assert _stored_params(stack_env, run_id, service, key) == p, service

    household = restate.states(run_id, "Household")[key]
    assert set(household) == {"params", "n_at_configure", "state"}  # no record has arrived
    assert json.loads(household["n_at_configure"]) == 0
    assert json.loads(restate.states(run_id, "Weather")[key]["n_at_configure"]) == 0

    # The household subscribed to its market once: one subscriber.
    stack.wait_for(
        lambda: p.market_key in decoded(stack_env, run_id, "Tariff", TariffState),
        TIMEOUT_S,
        "Tariff has no state",
    )
    tariff = decoded(stack_env, run_id, "Tariff", TariffState)[p.market_key]
    assert tariff.subscribers == (key,)
    assert restate.failures(run_id) == []


def test_ac_011_27_configure_with_other_parameters_fails_without_retry_and_changes_nothing(
    stack_env: stack.StackEnv, run_id: str
) -> None:
    key, p = _site(run_id)
    other = dataclasses.replace(p, late_slots=p.late_slots // 2)
    assert other != p
    stack_env.restate.configure({key: p})
    for service in _services(p):
        stack_env.restate.send(
            service,
            key,
            "configure",
            PARAMS_SERDE.serialize(other),
            idempotency_key=f"{key}:{service}:configure:other",
        )

    for service in _services(p):
        rows = _completed(stack_env, run_id, service, key, "configure", 2)
        assert sorted(r["completion_result"] for r in rows) == ["failure", "success"], service
        assert _stored_params(stack_env, run_id, service, key) == p, service


def test_ac_011_27_parameters_for_another_key_fail_without_retry(
    stack_env: stack.StackEnv, run_id: str
) -> None:
    key, p = _site(run_id)
    wrong = f"{run_id}:PL-LOC-000004"
    for service in _services(p):
        stack_env.restate.send(
            service,
            wrong,
            "configure",
            PARAMS_SERDE.serialize(p),
            idempotency_key=f"{wrong}:{service}:configure",
        )

    for service in _services(p):
        rows = _completed(stack_env, run_id, service, wrong, "configure", 1)
        assert [r["completion_result"] for r in rows] == ["failure"], service
        assert _stored_params(stack_env, run_id, service, wrong) is None, service
        assert "state" not in stack_env.restate.states(run_id, service).get(wrong, {}), service
    assert _stored_params(stack_env, run_id, "Household", key) is None


# ── an internal message at an unconfigured object (AC-011-29, -28) ───────────────────────────────


def _measurement(run_id: str, interval_start: datetime) -> bytes:
    event = measurement_event(
        stream=Stream.CONSUMPTION,
        location_id=LOCATION,
        interval_start=interval_start,
        measurements=[energy("load-1", AssetType.LOAD, Direction.CONSUMPTION, 0.25)],
        ingested_at=interval_start + QUARTER,
        run_id=run_id,
    )
    return to_json_bytes(event)


def test_ac_011_29_an_internal_message_at_an_unconfigured_object_fails_once_and_the_key_goes_on(
    stack_env: stack.StackEnv, run_id: str, bootstrap: str
) -> None:
    key, p = _site(run_id)
    restate = stack_env.restate
    step = Step(
        t=p.first_step,
        pv_kwh=0.0,
        load_kwh=0.25,
        rce_pln_per_kwh=0.4,
        pv_estimated=False,
        load_estimated=False,
        price_replaced=False,
        forecast_issue_s=None,
        corrections=(),
    )
    restate.send(
        "Ems", key, "on_step", serde_for(Step).serialize(step), idempotency_key=f"{key}:Ems:step"
    )
    update = PriceUpdate(t=0, seq=1, snapshot=False, curves=())
    restate.send(
        "Household",
        key,
        "on_prices",
        serde_for(PriceUpdate).serialize(update),
        idempotency_key=f"{key}:Household:prices",
    )
    for service, handler in (("Ems", "on_step"), ("Household", "on_prices")):
        rows = _completed(stack_env, run_id, service, key, handler, 1)
        assert [r["completion_result"] for r in rows] == ["failure"], service

    # A later record of the same household is handled: the key is not blocked.
    producer = Producer(base_config(bootstrap, "ems-tests"))
    producer.produce(
        stack_env.prefix + "ems.consumption.v1",
        key=key.encode(),
        value=_measurement(run_id, START_UTC),
    )
    assert producer.flush(30) == 0
    restate.wait_handled(run_id, 1, timeout=TIMEOUT_S, services=("Household",))
    rows = _completed(stack_env, run_id, "Household", key, "on_measurement", 1)
    assert [r["completion_result"] for r in rows] == ["success"]
    # AC-011-28: unconfigured, it keeps the log and the counter only, and sent nothing.
    assert set(restate.states(run_id, "Household")[key]) == {"log", "n"}
    assert restate.states(run_id, "Tariff") == {}

    # A later invocation of the Ems key runs too.
    restate.send(
        "Ems", key, "configure", PARAMS_SERDE.serialize(p), idempotency_key=f"{key}:Ems:configure"
    )
    rows = _completed(stack_env, run_id, "Ems", key, "configure", 1)
    assert [r["completion_result"] for r in rows] == ["success"]
    assert _stored_params(stack_env, run_id, "Ems", key) == p


# ── one managed site through every edge (AC-011-25, -26, -31; AC-011-36 on the stack) ────────────


def test_ac_011_25_one_managed_site_through_every_edge_equals_the_offline_side(
    stack_env: stack.StackEnv, run_id: str
) -> None:
    key, p = _site(run_id)
    restate = stack_env.restate
    restate.configure({key: p})
    records = stack_env.publish(run_id, locations=1, days=DAYS, start=START)
    offline = stack.offline_run({key: p}, records)[key]
    assert offline.join.next == p.end_step  # every step of the 6 h is released: not vacuous
    restate.wait_run(run_id, len(records), stack.run_ends({key: p}, {key: offline}), timeout=600)

    # AC-011-25, -26: each topic reached its target and every log equals the offline fold.
    logs = restate.ingress_logs(run_id)
    assert set(logs) == {("Household", key), ("Weather", key), ("Tariff", p.market_key)}
    assert logs == stack.offline_logs(records)

    household = decoded(stack_env, run_id, "Household", HouseholdState)[key]
    assert household.join == offline.join
    strategies = {
        name: decoded(stack_env, run_id, name, StrategyState)[key] for name in p.strategies
    }
    for name, state in strategies.items():
        assert state.result == offline.strategies[name], name
    live = offline.strategies[p.live]
    battery = decoded(stack_env, run_id, "Battery", BatteryState)[key]
    meter = decoded(stack_env, run_id, "Meter", MeterState)[key]
    assert battery.soc_kwh == live.state.soc_kwh == strategies[p.live].result.state.soc_kwh
    assert (meter.acc, meter.recent, meter.months) == (live.state.acc, live.recent, live.months)
    ems = decoded(stack_env, run_id, "Ems", EmsState)[key]
    assert (ems.sent, ems.soc_kwh) == (p.end_step - 1, battery.soc_kwh)

    # AC-011-31: the household holds every curve the Tariff numbered, from its snapshot on.
    tariff = decoded(stack_env, run_id, "Tariff", TariffState)[p.market_key]
    assert tariff.subscribers == (key,)
    assert household.prices.expected == tariff.stream.seq + 1
    assert household.join.price_covered == tariff.stream.covered
    assert tariff.curves  # the run published at least the first day's curve

    # The shared handlers answer with the stored state.
    shared = [
        ("Household", "status", key, HouseholdState),
        *((name, "summary", key, StrategyState) for name in p.strategies),
        ("Battery", "state", key, BatteryState),
        ("Meter", "bill", key, MeterState),
        ("Ems", "status", key, EmsState),
        ("Tariff", "status", p.market_key, TariffState),
        ("Weather", "status", key, WeatherState),
    ]
    for service, handler, k, tp in shared:
        answer = restate.call(service, k, handler)
        stored = restate.states(run_id, service)[k]["state"]
        assert answer == json.loads(stored), (service, handler)
        assert (
            serde_for(tp).deserialize(json.dumps(answer).encode())
            == decoded(stack_env, run_id, service, tp)[k]
        ), (service, handler)

    assert restate.failures(run_id) == []
