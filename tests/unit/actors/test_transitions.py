"""The actors' pure transitions (spec 011, T27): AC-011-26, -27, -30, -31, -34, -35.

Inputs are real: the run parameters of the managed sites of the 10-household catalog and the records
of a generated 6 h window (``tests/network.py``). Expected values come from the domain and assembler
functions the criteria name (``join``, ``price_apply``, ``forecast_apply``, ``stepper.step``,
``reference.prices/decide/apply``), never from the transition under test.

Not here: the ``configure`` checks (equal, different, another key), a message at an unconfigured
object and the ``Tariff`` default live in ``shell`` and need a Restate server (integration tier,
T35).
"""

from __future__ import annotations

import dataclasses
import itertools
from collections.abc import Callable
from datetime import UTC, date, datetime
from typing import Any, Literal

import pytest

from ems.actors import battery, ems, household, meter, strategy, tariff, weather
from ems.actors.battery import BatteryState
from ems.actors.ems import EmsState
from ems.actors.household import HouseholdState
from ems.actors.messages import Ack, Apply, Flows, ForecastUpdate, PriceUpdate, Subscribe
from ems.actors.meter import MeterState
from ems.actors.reorder import Inbox
from ems.actors.shell import Send
from ems.actors.strategy import StrategyState
from ems.actors.tariff import TariffState
from ems.actors.weather import WeatherState
from ems.assembler import join
from ems.assembler.join import Step
from ems.assembler.mapping import curve, parse, run
from ems.assembler.offline import StrategyResult
from ems.assembler.params import RunParams
from ems.assembler.sparse_streams import (
    Curve,
    ForecastStream,
    PriceStream,
    forecast_apply,
    price_apply,
)
from ems.contracts.envelope import Market, PriceEvent, Resolution
from ems.contracts.examples import golden_examples, price_event
from ems.contracts.time import quarter_index
from ems.domain.actions import POLICIES, Action, ActionKind
from ems.domain.reference import QuarterIn, apply, decide, initial_state, prices
from ems.domain.settlement import Correction, SettleAcc
from ems.domain.stepper import step as reference_step
from tests.network import (
    CONSUMPTION,
    FAULT_RUN_ID,
    FORECAST,
    HOURS,
    PRICE,
    PRODUCTION,
    RUN_ID,
    START,
    generate,
    managed_params,
)

PV, NO_PV = f"{RUN_ID}:PL-LOC-000001", f"{RUN_ID}:PL-LOC-000002"
INGESTED = datetime(2026, 11, 5, tzinfo=UTC)


def _params(key: str = PV, late_slots: int = 8) -> RunParams:
    return managed_params(START, HOURS, late_slots)[key]


def _price_events() -> list[Any]:
    gen = generate(START, HOURS, 8, False)
    return [parse(raw) for raw in gen.of(PRICE, gen.market_key)]


def _accepted_curves() -> tuple[PriceStream, list[Curve]]:
    stream, out = PriceStream(), []
    for event in _price_events():
        c = curve(event)
        assert c is not None
        stream, accepted = price_apply(stream, c)
        assert accepted is not None
        out.append(accepted)
    return stream, out


def _fold_tariff(
    events: list[Any], state: TariffState | None = None
) -> tuple[TariffState, list[Send]]:
    state, sends = state or TariffState(), []
    for event in events:
        state, out = tariff.on_record(state, event)
        sends += out
    return state, sends


# ── start states (plan, Start states; AC-011-27: "the initial state") ───────────────────────────


@pytest.mark.parametrize("key", [PV, NO_PV])
def test_ac_011_27_household_starts_its_join_and_subscribes_once(key: str) -> None:
    p = _params(key)
    state, sends = household.start(p)
    j = state.join
    assert j == join.start(p)
    assert (j.next, j.price_covered, j.forecast_covered, j.curves, j.runs) == (
        p.first_step,
        0,
        0,
        (),
        (),
    )
    sides = [j.consumption] + ([j.production] if p.has_pv else [])
    assert (j.production is None) == (not p.has_pv)
    for side in sides:
        assert side is not None
        assert (side.stream.closed, side.stream.last_kwh, side.ready, side.late) == (
            p.first_step,
            0.0,
            (),
            (),
        )
    assert state.prices == Inbox() and state.prices.expected is None
    assert state.forecasts == Inbox(expected=1)
    assert sends == (
        Send("Tariff", "subscribe", p.market_key, Subscribe(p.first_step, p.household_key)),
    )
    assert sends[0].message.schema_version == "1.0.0"


def test_ac_011_27_weather_starts_empty_and_sends_nothing() -> None:
    state, sends = weather.start(_params())
    assert state == WeatherState(ForecastStream(covered=0, newest_issue_s=None, seq=0), 0)
    assert sends == ()


@pytest.mark.parametrize("name", sorted(POLICIES))
def test_ac_011_27_strategy_starts_on_its_virtual_battery(name: str) -> None:
    p = _params()
    state, sends = strategy.start(p)
    assert state == StrategyState(
        StrategyResult(initial_state(p.model, p.first_step, p.initial_soc_kwh), (), ()),
        Inbox(expected=p.first_step),
    )
    assert state.result.state.soc_kwh == p.initial_soc_kwh == p.model.battery.capacity_kwh / 2
    assert sends == ()


def test_ac_011_27_ems_starts_before_the_first_step() -> None:
    p = _params()
    state, sends = ems.start(p)
    assert state == EmsState(
        soc_kwh=p.initial_soc_kwh,
        last_pv_kwh=0.0,
        last_load_kwh=0.0,
        sent=p.first_step - 1,
        steps=Inbox(expected=p.first_step),
        acks=Inbox(expected=p.first_step),
        waiting=(),
    )
    assert sends == ()


def test_ac_011_27_battery_starts_at_half_capacity() -> None:
    p = _params()
    state, sends = battery.start(p)
    assert state == BatteryState(p.initial_soc_kwh, Inbox(expected=p.first_step), 0)
    assert sends == ()


def test_ac_011_27_meter_starts_an_empty_ledger() -> None:
    p = _params()
    state, sends = meter.start(p)
    assert state == MeterState(
        SettleAcc(p.first_step, (), (), ()), (), (), Inbox(expected=p.first_step)
    )
    assert sends == ()


def test_ac_011_27_tariff_starts_from_its_default() -> None:
    state = TariffState()
    assert state.stream == PriceStream() and (state.stream.covered, state.stream.seq) == (0, 0)
    assert (state.curves, state.subscribers, state.unusable) == ((), (), 0)


# ── Tariff (AC-011-31, -26) ─────────────────────────────────────────────────────────────────────


def test_ac_011_31_a_new_subscriber_gets_one_snapshot_of_every_accepted_curve() -> None:
    stream, curves = _accepted_curves()
    assert len(curves) == 2  # the days 2026-10-31 and 2026-11-01 (local)
    state, sends = _fold_tariff(_price_events())
    assert sends == []  # nobody subscribed yet
    assert (state.stream, state.curves) == (stream, tuple(curves))

    new, out = tariff.subscribe(state, Subscribe(_params().first_step, PV))
    snapshot = PriceUpdate(t=stream.covered, seq=stream.seq, snapshot=True, curves=tuple(curves))
    assert out == (Send("Household", "on_prices", PV, snapshot),)
    assert (snapshot.seq, snapshot.t) == (2, stream.covered) and snapshot.t > 0
    assert new.subscribers == (PV,)
    assert (new.stream, new.curves) == (state.stream, state.curves)


def test_ac_011_31_a_known_subscriber_gets_nothing() -> None:
    state, _ = _fold_tariff(_price_events())
    once, _ = tariff.subscribe(state, Subscribe(1, PV))
    twice, out = tariff.subscribe(once, Subscribe(1, PV))
    assert out == ()
    assert twice == once


def test_ac_011_31_each_accepted_curve_goes_once_to_every_subscriber() -> None:
    first, second = _price_events()
    _, (c1, c2) = _accepted_curves()
    state, out_a = tariff.subscribe(TariffState(), Subscribe(1, PV))
    state, out_b = tariff.subscribe(state, Subscribe(1, NO_PV))
    for out, key in ((out_a, PV), (out_b, NO_PV)):
        assert out == (Send("Household", "on_prices", key, PriceUpdate(0, 0, True, ())),)

    state, out = tariff.on_record(state, first)
    update = PriceUpdate(state.stream.covered, 1, False, (c1,))
    assert out == (
        Send("Household", "on_prices", PV, update),
        Send("Household", "on_prices", NO_PV, update),
    )

    again, out = tariff.on_record(state, first)  # the same event published twice
    assert out == ()
    assert again.stream.duplicate == 1 and again.curves == state.curves

    late = f"{RUN_ID}:PL-LOC-000004"  # subscribes between the two curves
    state, out = tariff.subscribe(again, Subscribe(1, late))
    assert out == (
        Send("Household", "on_prices", late, PriceUpdate(state.stream.covered, 1, True, (c1,))),
    )
    state, out = tariff.on_record(state, second)
    update = PriceUpdate(state.stream.covered, 2, False, (c2,))
    assert out == tuple(Send("Household", "on_prices", k, update) for k in (PV, NO_PV, late))


def _price(
    market: Market = Market.PL_RCE,
    currency: Literal["PLN", "EUR"] = "PLN",
    resolution: Resolution = Resolution.PT15M,
) -> PriceEvent:
    """A valid v1 curve of 2026-11-01, published the day before."""
    slots = 24 if resolution is Resolution.PT1H else 96
    return price_event(
        market=market,
        delivery_day=date(2026, 11, 1),
        prices=[300.0 + i for i in range(slots)],
        published_at=datetime(2026, 10, 31, 12, 30, tzinfo=UTC),
        ingested_at=INGESTED,
        resolution=resolution,
        run_id=RUN_ID,
        currency=currency,
    )


UNUSABLE_PRICES = [
    pytest.param(lambda: _price(currency="EUR"), id="EUR curve"),
    pytest.param(lambda: _price(resolution=Resolution.PT1H), id="PT1H curve"),
    pytest.param(lambda: _price(market=Market.PL_SDAC), id="another market"),
    pytest.param(lambda: golden_examples()["production.json"], id="a measurement"),
    pytest.param(lambda: None, id="unparseable"),
]


@pytest.mark.parametrize("make", UNUSABLE_PRICES)
def test_ac_011_26_tariff_counts_an_unusable_record_and_sends_nothing(
    make: Callable[[], Any],
) -> None:
    assert curve(_price()) is not None  # the base event is usable
    state, _ = tariff.subscribe(TariffState(), Subscribe(1, PV))
    after, out = tariff.on_record(state, make())
    assert out == ()
    assert after.unusable == 1
    assert (after.stream, after.curves, after.subscribers) == (
        state.stream,
        state.curves,
        state.subscribers,
    )


# ── Weather (AC-011-15, -26) ────────────────────────────────────────────────────────────────────


def _forecast_events(key: str = PV) -> list[Any]:
    return [parse(raw) for raw in generate(START, HOURS, 8, False).of(FORECAST, key)]


def test_ac_011_14_weather_sends_an_accepted_run_to_its_household() -> None:
    p = _params()
    event = _forecast_events()[0]
    r = run(event)
    assert r is not None
    stream, accepted = forecast_apply(ForecastStream(), r)
    assert accepted is not None and accepted.seq == 1

    state, out = weather.on_record(p, weather.start(p)[0], event)
    assert state.stream == stream
    assert out == (
        Send("Household", "on_forecast", PV, ForecastUpdate(stream.covered, 1, accepted)),
    )

    again, out = weather.on_record(p, state, event)  # the same run again: stale
    assert out == ()
    assert again.stream.stale == 1 and again.stream.seq == 1


def test_ac_011_15_a_run_of_an_unknown_model_is_counted_and_sends_nothing() -> None:
    p = _params()
    state, _ = weather.on_record(p, weather.start(p)[0], _forecast_events()[0])
    golden = golden_examples()["weather_forecast.json"]
    unknown = golden.model_copy(
        update={"payload": golden.payload.model_copy(update={"forecast_model": "gfs_seamless"})}
    )
    assert parse(unknown.model_dump_json(by_alias=True).encode()) is not None  # valid on the wire
    after, out = weather.on_record(p, state, unknown)
    assert out == ()
    assert after.unusable == 1
    assert after.stream == state.stream  # the watermark is unchanged


def test_ac_011_26_weather_counts_an_unparseable_record() -> None:
    p = _params()
    state, out = weather.on_record(p, weather.start(p)[0], None)
    assert (state.unusable, out) == (1, ())


# ── Household (AC-011-34 routing, -31 snapshot, -30 order, -26) ─────────────────────────────────


def _snapshot(key: str) -> PriceUpdate:
    state, _ = _fold_tariff(_price_events())
    _, (send,) = tariff.subscribe(state, Subscribe(1, key))
    return send.message


def _forecast_updates(key: str) -> list[ForecastUpdate]:
    p = _params(key)
    state, out = weather.start(p)[0], []
    for event in _forecast_events(key):
        state, sends = weather.on_record(p, state, event)
        out += [s.message for s in sends]
    return out


def _drive_household(p: RunParams) -> tuple[HouseholdState, list[Send]]:
    """Snapshot, forecast updates, then the meter records in partition order."""
    gen = generate(START, HOURS, 8, False)
    state, sends = household.start(p)[0], []
    state, out = household.on_prices(p, state, _snapshot(p.household_key))
    sends += out
    for update in _forecast_updates(p.household_key):
        state, out = household.on_forecast(p, state, update)
        sends += out
    for topic in (PRODUCTION, CONSUMPTION):
        for raw in gen.of(topic, p.household_key):
            state, out = household.on_record(p, state, parse(raw))
            sends += out
    return state, sends


@pytest.mark.parametrize("key", [PV, NO_PV])
@pytest.mark.parametrize("strategies", [("NoBattery", "SelfConsumption"), ("SelfConsumption",)])
def test_ac_011_34_household_sends_every_released_step_to_each_strategy_and_ems(
    key: str, strategies: tuple[str, ...]
) -> None:
    p = dataclasses.replace(_params(key), strategies=strategies)
    offline = generate(START, HOURS, 8, False).offline(key)
    assert len(offline.steps) == 4 * HOURS

    state, sends = _drive_household(p)
    targets = (*strategies, "Ems")
    assert sends == [Send(target, "on_step", key, s) for s in offline.steps for target in targets]
    assert state.join == offline.join


def test_ac_011_31_a_second_snapshot_is_dropped() -> None:
    p = _params()
    state, _ = household.on_prices(p, household.start(p)[0], _snapshot(PV))
    assert state.prices.expected == 3
    again, out = household.on_prices(p, state, _snapshot(PV))
    assert out == ()
    assert again.join == state.join and again.prices == state.prices
    assert again.prices.stale == state.prices.stale + 1


def test_ac_011_31_updates_before_the_snapshot_wait_and_the_covered_ones_are_dropped() -> None:
    """A household subscribes after curve 1: its snapshot holds curve 1 (seq 1). Update 1 (sent to
    an earlier subscriber's copy of the stream) and update 2 reach it before the snapshot."""
    p = _params()
    first, second = _price_events()
    state, _ = _fold_tariff([first])
    _, (snap,) = tariff.subscribe(state, Subscribe(1, PV))
    _, (u1,) = tariff.on_record(tariff.subscribe(TariffState(), Subscribe(1, PV))[0], first)
    _, (u2,) = tariff.on_record(tariff.subscribe(state, Subscribe(1, PV))[0], second)
    assert (snap.message.seq, u1.message.seq, u2.message.seq) == (1, 1, 2)

    in_order, _ = household.on_prices(p, household.start(p)[0], snap.message)
    in_order, _ = household.on_prices(p, in_order, u2.message)

    shuffled = household.start(p)[0]
    for update in (u2.message, u1.message, snap.message):
        shuffled, _ = household.on_prices(p, shuffled, update)
    assert shuffled == in_order
    assert shuffled.join.curves == in_order.join.curves and len(in_order.join.curves) == 2
    assert shuffled.prices.expected == 3 and shuffled.prices.held == ()
    assert (shuffled.prices.reordered, shuffled.prices.stale) == (2, 1)


def test_ac_011_30_forecast_updates_apply_in_seq_order() -> None:
    p = _params()
    updates = _forecast_updates(PV)
    assert [u.seq for u in updates] == [1, 2]
    in_order = household.start(p)[0]
    for u in updates:
        in_order, _ = household.on_forecast(p, in_order, u)
    shuffled, out = household.on_forecast(p, household.start(p)[0], updates[1])
    assert out == () and shuffled.forecasts.held == ((2, updates[1]),)
    shuffled, _ = household.on_forecast(p, shuffled, updates[0])
    shuffled, _ = household.on_forecast(p, shuffled, updates[0])  # a repeat: stale
    assert shuffled == in_order
    assert (shuffled.forecasts.reordered, shuffled.forecasts.stale) == (1, 1)


@pytest.mark.parametrize(
    "event",
    [
        pytest.param(None, id="unparseable"),
        pytest.param(golden_examples()["price.json"], id="a price event"),
    ],
)
def test_ac_011_26_household_counts_a_record_it_cannot_map(event: Any) -> None:
    p = _params()
    start = household.start(p)[0]
    state, out = household.on_record(p, start, event)
    assert out == ()
    assert state.join == dataclasses.replace(start.join, unusable=1)


# ── strategies (AC-011-34, -30) ─────────────────────────────────────────────────────────────────


def _reference(p: RunParams, name: str, steps: tuple[Step, ...]) -> StrategyResult:
    """``stepper.step`` with the strategy's policy, the step's corrections and lag 2L."""
    state = initial_state(p.model, p.first_step, p.initial_soc_kwh)
    recent: tuple[Any, ...] = ()
    months: tuple[Any, ...] = ()
    for s in steps:
        q = QuarterIn(s.t, s.pv_kwh, s.load_kwh, s.rce_pln_per_kwh)
        out = reference_step(
            p.model, POLICIES[name], p.rcem, state, q, recent, s.corrections, 2 * p.late_slots
        )
        state, recent = out.state, out.recent
        months = months if out.month is None else (*months, out.month)
    return StrategyResult(state, recent, months)


@pytest.fixture(scope="module")
def fault_run() -> tuple[RunParams, tuple[Step, ...]]:
    """The managed household of the fault run at L = 2 whose steps carry the most corrections."""
    gen = generate(START, HOURS, 2, True, FAULT_RUN_ID)
    runs = [(gen.params[key], gen.offline(key).steps) for key in sorted(gen.params)]
    p, steps = max(runs, key=lambda r: sum(len(s.corrections) for s in r[1]))
    assert any(s.corrections for s in steps)
    return p, steps


@pytest.mark.parametrize("name", sorted(POLICIES))
def test_ac_011_34_a_strategy_steps_its_policy_with_corrections_and_lag(
    name: str, fault_run: tuple[RunParams, tuple[Step, ...]]
) -> None:
    p, steps = fault_run
    state = strategy.start(p)[0]
    for s in steps:
        state, out = strategy.on_step(name, p, state, s)
        assert out == ()
    assert state.result == _reference(p, name, steps)
    assert state.steps == Inbox(expected=steps[-1].t + 1)
    assert len(state.result.months) == 1  # October closed


@pytest.mark.parametrize("name", sorted(POLICIES))
def test_ac_011_30_a_strategy_applies_steps_in_t_order(
    name: str, fault_run: tuple[RunParams, tuple[Step, ...]]
) -> None:
    p, steps = fault_run
    state = strategy.start(p)[0]
    for s in reversed(steps):
        state, _ = strategy.on_step(name, p, state, s)
    state, _ = strategy.on_step(name, p, state, steps[0])  # a repeat: stale
    assert state.result == _reference(p, name, steps)
    assert (state.steps.reordered, state.steps.stale) == (len(steps) - 1, 1)


# ── Ems (AC-011-35) ─────────────────────────────────────────────────────────────────────────────


def _steps(p: RunParams, n: int) -> list[Step]:
    """Distinct flows per step, so an observation names the step it came from."""
    return [
        Step(
            p.first_step + i,
            0.1 * (i + 1),
            0.3 + 0.01 * i,
            0.4 + 0.1 * i,
            False,
            False,
            False,
            None,
            (),
        )
        for i in range(n)
    ]


def test_ac_011_35_ems_decides_the_first_step_at_once() -> None:
    p = _params()
    s = _steps(p, 1)[0]
    state, out = ems.on_step(p, ems.start(p)[0], s)
    pr = prices(p.model, p.rcem, s.t, s.rce_pln_per_kwh)
    action = decide(POLICIES[p.live], pr, s.t, p.initial_soc_kwh, 0.0, 0.0)
    assert out == (
        Send(
            "Battery",
            "apply",
            PV,
            Apply(s.t, action, s.pv_kwh, s.load_kwh, s.rce_pln_per_kwh, s.corrections),
        ),
    )
    assert (state.sent, state.last_pv_kwh, state.last_load_kwh) == (s.t, s.pv_kwh, s.load_kwh)


def test_ac_011_35_ems_never_decides_a_step_before_the_ack_of_the_previous_one() -> None:
    """Exhaustive over 3 steps: steps arrive in any order, interleaved in any way with the acks
    of the commands sent so far (an ack exists only once its command was sent). Each step is
    commanded once, in order, only after the ack of the step before, from that ack's SoC and the
    flows of the step before."""
    p = _params()
    n = 3
    steps = _steps(p, n)
    soc = [2.0 + 0.5 * i for i in range(n)]  # the SoC each ack reports
    orders = 0

    def explore(
        state: EmsState, delivered: frozenset[tuple[str, int]], commanded: tuple[int, ...]
    ) -> None:
        nonlocal orders
        options = [("step", i) for i in range(n) if ("step", i) not in delivered]
        options += [("ack", i) for i in commanded if ("ack", i) not in delivered]
        if not options:
            orders += 1
            assert commanded == tuple(range(n))
            assert state.sent == steps[-1].t and state.waiting == ()
            assert state.steps.expected == state.acks.expected == steps[-1].t + 1
            return
        for kind, i in options:
            if kind == "step":
                new, out = ems.on_step(p, state, steps[i])
            else:
                new, out = ems.on_ack(p, state, Ack(steps[i].t, soc[i]))
            assert len(out) <= 1
            done = delivered | {(kind, i)}
            if out:
                (send,) = out
                assert (send.service, send.handler, send.key) == ("Battery", "apply", PV)
                t = send.message.t
                k = t - p.first_step
                assert k == len(commanded)  # in order, once
                assert all(("ack", j) in done for j in range(k))  # after every earlier ack
                # decided from the previous ack's SoC and the previous step's flows
                assert new.soc_kwh == (soc[k - 1] if k else p.initial_soc_kwh)
                previous = steps[k - 1] if k else None
                last = (previous.pv_kwh, previous.load_kwh) if previous else (0.0, 0.0)
                expected_action = decide(
                    POLICIES[p.live],
                    prices(p.model, p.rcem, t, steps[k].rce_pln_per_kwh),
                    t,
                    new.soc_kwh,
                    *last,
                )
                assert send.message == Apply(
                    t,
                    expected_action,
                    steps[k].pv_kwh,
                    steps[k].load_kwh,
                    steps[k].rce_pln_per_kwh,
                    (),
                )
                explore(new, done, (*commanded, k))
            else:
                explore(new, done, commanded)

    explore(ems.start(p)[0], frozenset(), ())
    # Every order is explored: the orders of s0 < a0 < a1 < a2, s1 < a1, s2 < a2 (a2 last; three
    # orders of s0, a0, s1 before a1; five places for s2).
    assert orders == 15


# ── Battery (AC-011-35, P8) ─────────────────────────────────────────────────────────────────────


def _command(p: RunParams, action: Action, t: int | None = None) -> Apply:
    correction = (Correction("consumption", p.first_step - 3, 0.2),)
    return Apply(p.first_step if t is None else t, action, 0.5, 0.2, 0.45, correction)


CLAMPS = [
    pytest.param(
        lambda b: (b.capacity_kwh / 2, Action(ActionKind.SETPOINT, p_kw=10 * b.p_max_kw)),
        id="charge beyond p_max",
    ),
    pytest.param(
        lambda b: (b.capacity_kwh / 2, Action(ActionKind.SETPOINT, p_kw=-10 * b.p_max_kw)),
        id="discharge beyond p_max",
    ),
    pytest.param(
        lambda b: (b.soc_max_kwh, Action(ActionKind.CHARGE_FROM_GRID, level=1.0)),
        id="charge when full",
    ),
    pytest.param(
        lambda b: (b.soc_min_kwh, Action(ActionKind.EXPORT, level=1.0)), id="discharge when empty"
    ),
    pytest.param(
        lambda b: (b.soc_max_kwh - 0.01, Action(ActionKind.SETPOINT, p_kw=b.p_max_kw)),
        id="charge into the bound",
    ),
]


@pytest.mark.parametrize("case", CLAMPS)
def test_ac_011_35_battery_clamps_a_command_counts_it_and_sends_ack_then_flows(
    case: Callable[[Any], tuple[float, Action]],
) -> None:
    p = _params()
    b = p.model.battery
    soc0, action = case(b)
    state = BatteryState(soc0, Inbox(expected=p.first_step), clamped=3)
    command = _command(p, action)
    new, out = battery.on_apply(p, state, command)

    p_cmd, p_kw, soc = apply(b, soc0, action, command.pv_kwh, command.load_kwh)
    assert p_kw != p_cmd  # the case needs a clamp
    assert abs(p_kw) <= b.p_max_kw and b.soc_min_kwh <= soc <= b.soc_max_kwh
    assert new == BatteryState(soc, Inbox(expected=p.first_step + 1), clamped=4)
    assert out == (
        Send("Ems", "on_ack", PV, Ack(command.t, soc)),
        Send(
            "Meter",
            "on_flows",
            PV,
            Flows(
                command.t,
                command.pv_kwh,
                command.load_kwh,
                p_kw,
                command.rce_pln_per_kwh,
                command.corrections,
            ),
        ),
    )


@pytest.mark.parametrize(
    "action",
    [
        Action(ActionKind.HOLD),
        Action(ActionKind.SELF_CONSUME),
        Action(ActionKind.SETPOINT, p_kw=1.0),
    ],
    ids=lambda a: a.kind.value,
)
def test_ac_011_35_battery_does_not_count_a_feasible_command(action: Action) -> None:
    p = _params()
    new, out = battery.on_apply(p, battery.start(p)[0], _command(p, action))
    _, p_kw, soc = apply(p.model.battery, p.initial_soc_kwh, action, 0.5, 0.2)
    assert new.clamped == 0 and new.soc_kwh == soc
    assert [type(s.message) for s in out] == [Ack, Flows] and out[1].message.p_kw == p_kw


def test_ac_011_30_battery_applies_commands_in_t_order() -> None:
    p = _params()
    action = Action(ActionKind.SETPOINT, p_kw=3.0)
    c0, c1 = _command(p, action), _command(p, action, p.first_step + 1)
    in_order, out0 = battery.on_apply(p, battery.start(p)[0], c0)
    in_order, out1 = battery.on_apply(p, in_order, c1)

    held, out = battery.on_apply(p, battery.start(p)[0], c1)
    assert out == () and held.commands.held == ((c1.t, c1),)
    shuffled, out = battery.on_apply(p, held, c0)
    assert out == (*out0, *out1)
    assert shuffled == in_order


# ── Meter (AC-011-35) ───────────────────────────────────────────────────────────────────────────

MONTH_END = datetime(2026, 10, 31, 22, tzinfo=UTC)  # October ends at 23:00Z


@pytest.fixture(scope="module")
def ledger_case() -> tuple[RunParams, list[Flows], StrategyResult, StrategyResult]:
    """3 h across the end of October at L = 1 (lag 2): SelfConsumption flows with one correction
    of each stream; October's last hour is corrected before the month closes."""
    p = managed_params(MONTH_END, 3, 1)[PV]
    first = quarter_index(MONTH_END)
    corrections = {
        first + 4: (Correction("consumption", first + 2, 1.7),),  # 22:30Z, carried by 23:00Z
        first + 7: (Correction("production", first + 5, 0.9),),  # 23:15Z, carried by 23:45Z
    }
    flows: list[Flows] = []
    reference: dict[bool, StrategyResult] = {}
    for with_corrections in (True, False):
        state = initial_state(p.model, first, p.initial_soc_kwh)
        recent: tuple[Any, ...] = ()
        months: tuple[Any, ...] = ()
        for i in range(12):
            t = first + i
            q = QuarterIn(t, 0.05 * (i % 5), 0.2 + 0.03 * i, 0.3 + 0.02 * i)
            fixes = corrections.get(t, ()) if with_corrections else ()
            out = reference_step(
                p.model, POLICIES["SelfConsumption"], p.rcem, state, q, recent, fixes, p.lag
            )
            if with_corrections:
                flows.append(
                    Flows(t, q.pv_kwh, q.load_kwh, out.quarter.p_kw, q.rce_pln_per_kwh, fixes)
                )
            state, recent = out.state, out.recent
            months = months if out.month is None else (*months, out.month)
        reference[with_corrections] = StrategyResult(state, recent, months)
    return p, flows, reference[True], reference[False]


def test_ac_011_35_meter_settles_with_the_corrections_a_flows_carries(
    ledger_case: tuple[RunParams, list[Flows], StrategyResult, StrategyResult],
) -> None:
    p, flows, corrected, uncorrected = ledger_case
    state = meter.start(p)[0]
    for f in flows:
        state, out = meter.on_flows(p, state, f)
        assert out == ()
    assert (state.acc, state.recent, state.months) == (
        corrected.state.acc,
        corrected.recent,
        corrected.months,
    )
    assert [m.month for m in state.months] == ["2026-10"]
    assert state.months != uncorrected.months  # the correction reached October's bill


def test_ac_011_30_meter_applies_flows_in_t_order(
    ledger_case: tuple[RunParams, list[Flows], StrategyResult, StrategyResult],
) -> None:
    p, flows, corrected, _ = ledger_case
    state = meter.start(p)[0]
    for f in itertools.chain(reversed(flows), flows[:2]):
        state, _ = meter.on_flows(p, state, f)
    assert (state.acc, state.recent, state.months) == (
        corrected.state.acc,
        corrected.recent,
        corrected.months,
    )
    assert (state.flows.reordered, state.flows.stale) == (len(flows) - 1, 2)
