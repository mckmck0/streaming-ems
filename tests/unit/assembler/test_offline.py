"""AC-011-23: the offline fold over the records of a clean generated day equals ``reference.run``.

The day is 2026-10-20 (inside October) for the 10-household catalog at ``L`` = 8, with NoBattery and
SelfConsumption, for each managed site (PL-LOC-000002 has no PV and no production records). The
records are the bytes the Kafka sink publishes, per key in emission (partition) order. The reference
inputs are built without the assembler: per slot, the ``math.fsum`` of the slot's wanted meter
entries and the price of the slot in the curve of its local delivery day.
"""

from __future__ import annotations

import math
from datetime import date

import pytest

from ems.assembler.offline import Offline, fold
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
