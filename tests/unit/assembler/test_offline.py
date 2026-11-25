"""AC-011-23: the offline fold over the records of a clean generated day equals ``reference.run``.

The day is 2026-10-20 (inside October) for the 10-household catalog at ``L`` = 8, with NoBattery and
SelfConsumption, for each managed site (PL-LOC-000002 has no PV and no production records). The
records are the bytes the Kafka sink publishes, per key in emission (partition) order. The reference
inputs are built without the assembler: per slot, the ``math.fsum`` of the slot's wanted meter
entries and the price of the slot in the curve of its local delivery day.

AC-012-14 … -16 (spec 012), in the last section, on the same day for PL-LOC-000001:
- ``step_strategy`` drops a correction for a slot before the strategy's own first step and counts it
  in ``dropped_corrections`` (an added strategy has no flow there to correct);
- ``fold(..., schedule)`` steps each strategy only over the released steps of its window
  ``[start, stop)``: an added one from ``t`` with ``strategy_params`` (fresh battery at the run's
  initial SoC), a removed one up to ``t``; ``Offline.strategies`` holds every strategy that ran;
- ``trace`` gives each strategy's ``StepOut`` per step it ran, equal to the stepper run by hand.
The schedule module is imported inside these tests, so the spec 011 tests above keep running on
their own.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from datetime import date

import pytest

from ems.assembler import offline as off
from ems.assembler.join import Step
from ems.assembler.offline import Offline, fold, start_strategy, step_strategy
from ems.assembler.params import RunParams
from ems.contracts.envelope import (
    Aggregation,
    AssetType,
    Direction,
    MeasurementEvent,
    PriceEvent,
    Quality,
    Quantity,
)
from ems.domain import reference, stepper
from ems.domain.actions import POLICIES
from ems.domain.reference import QuarterIn
from ems.domain.settlement import Correction, Flow, grid_kwh
from ems.domain.stepper import StepOut
from ems.domain.units import pln_per_mwh_to_pln_per_kwh
from tests.unit.assembler.cases import (
    NO_PV_SITE,
    PV_SITE,
    PV_SITE_2,
    STRATEGIES,
    Generated,
    generated,
    local_day,
    run_params,
    step_at,
)

FIRST = step_at("2026-10-20T00:00")
END = FIRST + 96
LOAD_TYPES = {AssetType.LOAD, AssetType.HEAT_PUMP, AssetType.EV_CHARGER}


def _energy(event: MeasurementEvent, types: set[AssetType], direction: Direction) -> float:
    return math.fsum(
        m.value
        for m in event.payload.measurements
        if m.asset_type in types
        and m.direction is direction
        and m.quantity is Quantity.ACTIVE_ENERGY
        and m.aggregation is Aggregation.INTERVAL_SUM
        and m.unit == "kWh"
        and m.quality is not Quality.MISSING
    )


def plain_inputs(params: RunParams, day: Generated) -> tuple[QuarterIn, ...]:
    """One ``QuarterIn`` per slot of the window by direct lookup, no assembler involved."""
    loc = params.location_id
    pv: dict[int, float] = {}
    load: dict[int, float] = {}
    for e in day.of("production", loc):
        assert isinstance(e, MeasurementEvent)
        pv[e.t] = _energy(e, {AssetType.PV}, Direction.GENERATION)
    for e in day.of("consumption", loc):
        assert isinstance(e, MeasurementEvent)
        load[e.t] = _energy(e, LOAD_TYPES, Direction.CONSUMPTION)
    curves: dict[date, PriceEvent] = {}
    for e in day.of("price"):
        assert isinstance(e, PriceEvent)
        curves[e.payload.delivery_day] = e
    inputs = []
    for q in range(params.first_step, params.end_step):
        curve = curves[local_day(q)]
        price = pln_per_mwh_to_pln_per_kwh(curve.payload.slots[q - curve.t].price)
        inputs.append(QuarterIn(q, pv[q] if params.has_pv else 0.0, load[q], price))
    return tuple(inputs)


@pytest.fixture(scope="module", params=[PV_SITE, NO_PV_SITE, PV_SITE_2])
def case(request: pytest.FixtureRequest) -> tuple[RunParams, Offline, tuple[QuarterIn, ...]]:
    params = run_params(request.param, FIRST, END, 8, "r1", STRATEGIES, "SelfConsumption")
    day = generated()
    loc = params.location_id
    offline = fold(
        params,
        day.raw("production", loc),
        day.raw("consumption", loc),
        day.raw("weather_forecast", loc),
        day.raw("price"),
    )
    return params, offline, plain_inputs(params, day)


def test_ac_011_23_every_step_of_the_clean_day_is_released(
    case: tuple[RunParams, Offline, tuple[QuarterIn, ...]],
) -> None:
    params, offline, inputs = case
    assert params.has_pv == (params.location_id != NO_PV_SITE)
    assert [s.t for s in offline.steps] == list(range(FIRST, END))
    assert offline.join.next == END
    assert [QuarterIn(s.t, s.pv_kwh, s.load_kwh, s.rce_pln_per_kwh) for s in offline.steps] == list(
        inputs
    )
    assert not any(s.pv_estimated or s.load_estimated or s.price_replaced for s in offline.steps)
    assert all(s.corrections == () for s in offline.steps)
    # the first run is issued at 00:00 and usable at 02:00
    assert [s.forecast_issue_s is None for s in offline.steps] == [True] * 8 + [False] * 88
    j = offline.join
    assert (j.unusable, j.unneeded, j.price_replaced) == (0, 0, 0)
    assert (j.consumption.stream.estimated, j.consumption.stream.duplicate) == (0, 0)


@pytest.mark.parametrize("name", STRATEGIES)
def test_ac_011_23_strategy_state_and_quarters_equal_reference_run(
    case: tuple[RunParams, Offline, tuple[QuarterIn, ...]], name: str
) -> None:
    params, offline, inputs = case
    assert set(offline.strategies) == set(STRATEGIES)
    policy = POLICIES[name]
    start = reference.initial_state(params.model, FIRST, params.initial_soc_kwh)
    ref = reference.run(params.model, policy, params.rcem, start, inputs)
    got = offline.strategies[name]
    assert got.state == ref.state
    assert got.months == ref.months == ()  # one day inside October: no month closes
    # the quarter results of the released steps, stepped as the fold steps them
    state, recent, quarters = start, (), []
    for s in offline.steps:
        q = QuarterIn(s.t, s.pv_kwh, s.load_kwh, s.rce_pln_per_kwh)
        out = stepper.step(
            params.model, policy, params.rcem, state, q, recent, s.corrections, params.lag
        )
        state, recent = out.state, out.recent
        quarters.append(out.quarter)
    assert tuple(quarters) == ref.quarters
    assert (state, recent) == (got.state, got.recent)


# ── spec 012: strategy changes and the steps table ────────────────────────────────────────────────

NB, SC = "NoBattery", "SelfConsumption"
BOTH = (NB, SC)
ADD_AT, DROP_AT = FIRST + 16, FIRST + 48  # 2026-10-20T04:00Z and 12:00Z


def _records(location: str) -> tuple[tuple[bytes, ...], ...]:
    day = generated()
    return (
        day.raw("production", location),
        day.raw("consumption", location),
        day.raw("weather_forecast", location),
        day.raw("price"),
    )


def _step(t: int, corrections: tuple[Correction, ...] = ()) -> Step:
    return Step(t, 0.0, 0.3, 0.5, False, False, False, None, corrections)


def _by_hand(params: RunParams, name: str, steps: Sequence[Step]) -> list[StepOut]:
    """The stepper over ``steps`` from ``params.first_step``, leaving out the corrections for slots
    before it."""
    state = reference.initial_state(params.model, params.first_step, params.initial_soc_kwh)
    recent: tuple[Flow, ...] = ()
    outs: list[StepOut] = []
    for s in steps:
        q = QuarterIn(s.t, s.pv_kwh, s.load_kwh, s.rce_pln_per_kwh)
        kept = tuple(c for c in s.corrections if c.t >= params.first_step)
        out = stepper.step(
            params.model, POLICIES[name], params.rcem, state, q, recent, kept, params.lag
        )
        state, recent = out.state, out.recent
        outs.append(out)
    return outs


def test_ac_012_14_step_strategy_drops_and_counts_corrections_before_its_first_step() -> None:
    from ems.assembler.schedule import Change, strategy_params

    p = run_params(PV_SITE, FIRST, END, 8, "r1", (SC,), SC)
    q = strategy_params(p, Change(ADD_AT, BOTH))
    steps = (
        _step(ADD_AT, (Correction("consumption", ADD_AT - 3, 0.9),)),
        _step(
            ADD_AT + 1,
            (Correction("production", ADD_AT - 1, 0.2), Correction("consumption", ADD_AT, 0.7)),
        ),
    )
    r = start_strategy(q)
    for s in steps:
        r = step_strategy(q, NB, r, s)
    assert r.dropped_corrections == 2
    outs = _by_hand(q, NB, steps)
    assert (r.state, r.recent, r.months) == (outs[-1].state, outs[-1].recent, ())
    # the correction for ADD_AT itself is applied: its quarter carries the corrected load
    assert r.state.acc.hour[0].grid_kwh == grid_kwh(0.0, 0.7, outs[0].quarter.p_kw)


def test_ac_012_14_a_strategy_from_the_runs_first_step_drops_nothing_and_nothing_is_clamped(
    case: tuple[RunParams, Offline, tuple[QuarterIn, ...]],
) -> None:
    _, result, _ = case
    assert [r.dropped_corrections for r in result.strategies.values()] == [0, 0]
    j = result.join
    assert (j.clamped_price, j.clamped_forecast, j.after_end_late) == (0, 0, 0)


@pytest.fixture(scope="module")
def added() -> tuple[RunParams, Offline, Offline]:
    """SelfConsumption all day; NoBattery added at 04:00 and removed at 12:00."""
    from ems.assembler.schedule import Change, Schedule

    p = run_params(PV_SITE, FIRST, END, 8, "r1", (SC,), SC)
    schedule = Schedule(changes=(Change(ADD_AT, BOTH), Change(DROP_AT, (SC,))))
    records = _records(PV_SITE)
    prod, cons, fc, pr = records
    return p, fold(p, prod, cons, fc, pr), fold(p, prod, cons, fc, pr, schedule=schedule)


def test_ac_012_16_fold_runs_an_added_strategy_over_the_steps_of_its_window(
    added: tuple[RunParams, Offline, Offline],
) -> None:
    from ems.assembler.schedule import Change, strategy_params

    p, plain, changed = added
    assert (changed.join, changed.steps) == (plain.join, plain.steps)
    assert set(changed.strategies) == {NB, SC}
    assert changed.strategies[SC] == plain.strategies[SC]  # in force all day
    nb = changed.strategies[NB]
    assert nb.state.acc.t == DROP_AT and nb.months == () and nb.dropped_corrections == 0
    # a fresh battery at the run's initial SoC, over [04:00, 12:00): the batch reference
    inputs = plain_inputs(p, generated())[ADD_AT - FIRST : DROP_AT - FIRST]
    start = reference.initial_state(p.model, ADD_AT, p.initial_soc_kwh)
    assert nb.state == reference.run(p.model, POLICIES[NB], p.rcem, start, inputs).state
    # and step_strategy with strategy_params over the same released steps
    q = strategy_params(p, Change(ADD_AT, BOTH))
    r = start_strategy(q)
    for s in plain.steps[ADD_AT - FIRST : DROP_AT - FIRST]:
        r = step_strategy(q, NB, r, s)
    assert nb == r


def test_ac_012_15_16_fold_runs_a_removed_strategy_over_the_steps_before_t() -> None:
    from ems.assembler.schedule import Change, Schedule

    p = run_params(PV_SITE, FIRST, END, 8, "r1", BOTH, SC)
    records = _records(PV_SITE)
    prod, cons, fc, pr = records
    plain = fold(p, prod, cons, fc, pr)
    changed = fold(p, prod, cons, fc, pr, schedule=Schedule(changes=(Change(DROP_AT, (SC,)),)))
    assert set(changed.strategies) == {NB, SC}
    assert changed.strategies[SC] == plain.strategies[SC]
    nb = changed.strategies[NB]
    inputs = plain_inputs(p, generated())[: DROP_AT - FIRST]
    start = reference.initial_state(p.model, FIRST, p.initial_soc_kwh)
    assert nb.state == reference.run(p.model, POLICIES[NB], p.rcem, start, inputs).state
    assert nb.state.acc.t == DROP_AT


def test_ac_012_16_trace_without_changes_is_every_step_of_every_strategy(
    case: tuple[RunParams, Offline, tuple[QuarterIn, ...]],
) -> None:
    params, result, inputs = case
    traced = off.trace(params, result.steps)
    assert set(traced) == set(STRATEGIES)
    for name in STRATEGIES:
        assert list(traced[name]) == _by_hand(params, name, result.steps)
        start = reference.initial_state(params.model, FIRST, params.initial_soc_kwh)
        ref = reference.run(params.model, POLICIES[name], params.rcem, start, inputs)
        assert tuple(o.quarter for o in traced[name]) == ref.quarters
        assert (traced[name][-1].state, traced[name][-1].recent) == (
            result.strategies[name].state,
            result.strategies[name].recent,
        )


def test_ac_012_16_trace_follows_the_schedule(added: tuple[RunParams, Offline, Offline]) -> None:
    from ems.assembler.schedule import Change, Schedule, strategy_params

    p, plain, changed = added
    schedule = Schedule(changes=(Change(ADD_AT, BOTH), Change(DROP_AT, (SC,))))
    traced = off.trace(p, plain.steps, schedule)
    assert set(traced) == {NB, SC}
    assert list(traced[SC]) == _by_hand(p, SC, plain.steps)
    q = strategy_params(p, Change(ADD_AT, BOTH))
    assert list(traced[NB]) == _by_hand(q, NB, plain.steps[ADD_AT - FIRST : DROP_AT - FIRST])
    assert [o.quarter.t for o in traced[NB]] == list(range(ADD_AT, DROP_AT))
    for name in (NB, SC):
        assert (traced[name][-1].state, traced[name][-1].recent) == (
            changed.strategies[name].state,
            changed.strategies[name].recent,
        )


def test_ac_012_14_trace_drops_corrections_before_an_added_strategys_first_step() -> None:
    """Hand-built steps: the correction for slot ``ADD_AT - 2`` reaches SelfConsumption, which ran
    that slot, and is dropped for NoBattery, added at ``ADD_AT``; ``trace`` ends where
    ``fold``'s strategy step ends."""
    from ems.assembler.schedule import Change, Schedule, strategy_params

    p = run_params(PV_SITE, FIRST, END, 8, "r1", (SC,), SC)
    schedule = Schedule(changes=(Change(ADD_AT, BOTH),))
    steps = (
        *(_step(t) for t in range(FIRST, ADD_AT)),
        _step(ADD_AT, (Correction("consumption", ADD_AT - 2, 0.9),)),
        _step(ADD_AT + 1, (Correction("consumption", ADD_AT, 0.7),)),
    )
    traced = off.trace(p, steps, schedule)
    assert list(traced[SC]) == _by_hand(p, SC, steps)
    q = strategy_params(p, Change(ADD_AT, BOTH))
    assert list(traced[NB]) == _by_hand(q, NB, steps[ADD_AT - FIRST :])
    r = start_strategy(q)
    for s in steps[ADD_AT - FIRST :]:
        r = step_strategy(q, NB, r, s)
    assert r.dropped_corrections == 1
    assert (traced[NB][-1].state, traced[NB][-1].recent) == (r.state, r.recent)


# ── spec 012: which record made each step releasable (release lag, AC-012-29) ───────────────────


def _merged(location_id: str) -> list[tuple[str, bytes]]:
    """One household's records and the market's prices, in emission (publication) order."""
    from ems.contracts.envelope import to_json_bytes

    return [
        (s, to_json_bytes(e))
        for s, e in generated().events
        if s == "price" or e.location_id == location_id
    ]


def test_ac_012_29_released_by_releases_the_same_steps_as_fold() -> None:
    p = run_params()
    events = _merged(PV_SITE)
    by = off.released_by(p, events)
    g = generated()
    folded = fold(
        p,
        g.raw("production", PV_SITE),
        g.raw("consumption", PV_SITE),
        g.raw("weather_forecast", PV_SITE),
        g.raw("price"),
    )
    assert list(by) == [s.t for s in folded.steps]


def test_ac_012_29_a_step_is_released_by_the_last_input_it_waited_for() -> None:
    from ems.assembler.mapping import parse, reading

    p = run_params()
    events = _merged(PV_SITE)
    by = off.released_by(p, events)
    own: dict[int, list[int]] = {}
    first_forecast = next(i for i, (s, _) in enumerate(events) if s == "weather_forecast")
    for i, (s, raw) in enumerate(events):
        if s in ("production", "consumption"):
            r = reading(parse(raw))
            assert r is not None
            own.setdefault(r.t, []).append(i)
    for t, i in by.items():
        assert i >= max(own[t]), (t, i)  # never before its own readings
        if i != max(own[t]):  # then by a price or forecast it waited for, published later
            assert events[i][0] in ("price", "weather_forecast"), (t, events[i][0])
    # The day's first steps wait for its first forecast run (none is published before the window).
    assert [by[t] for t in range(FIRST, FIRST + 7)] == [first_forecast] * 7
    # 13:00 local waits for the next day's curve: the price watermark stops there until it comes.
    assert events[by[step_at("2026-10-20T11:00")]][0] == "price"
