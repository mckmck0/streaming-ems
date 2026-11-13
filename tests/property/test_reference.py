"""AC-009-13, -18, -19: chunk independence with carried state, and energy conservation."""

from __future__ import annotations

import math
from datetime import UTC, date, datetime
from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

from ems.contracts.time import quarter_index
from ems.domain.actions import NoBattery, SelfConsumption
from ems.domain.netbilling import Cohort, PriceBasis
from ems.domain.reference import QuarterIn, RefResult, SiteModel, initial_state, run
from ems.domain.settlement import Tranche
from ems.domain.units import DT_H
from ems.ingest.catalog.build import load_overrides
from ems.tariffs import load_book, load_rcem, real_rcem, site_model

CONFIG = Path(__file__).resolve().parents[2] / "config"
T0 = quarter_index(datetime(2026, 9, 30, 16, tzinfo=UTC))  # 2026-09-30 18:00 CEST
N = 48  # twelve hours; the civil month (and the DSO version) changes at quarter 24
MONTH_END = 24
SITE = load_overrides(CONFIG / "sites.yaml", 4242)["PL-LOC-000001"]
BOOK = load_book(CONFIG / "tariffs")
RCE_MODEL = site_model(SITE, BOOK, (T0, T0 + N))
SWITCH_MODEL = SiteModel(  # RCEm in September, RCE from October: the switch is inside the window
    RCE_MODEL.location_id,
    RCE_MODEL.tariff,
    Cohort(date(2023, 5, 1), "2026-10"),
    RCE_MODEL.fixed_inputs,
    RCE_MODEL.battery,
    RCE_MODEL.rules,
)
RCEM = real_rcem(load_rcem(CONFIG / "tariffs" / "rcem.yaml"), ["2026-08", "2026-09", "2026-10"])
OPENING = (
    Tranche("2025-09", "2025-10", 50.0, 30.0, 9.0, PriceBasis.RCEM),  # expires at September's close
    Tranche("2026-08", "2026-09", 20.0, 20.0, 5.0, PriceBasis.RCE),
)

energy = st.floats(min_value=0.0, max_value=2.5, allow_nan=False)
price = st.floats(min_value=-0.2, max_value=1.5, allow_nan=False)


@st.composite
def inputs(draw: st.DrawFn) -> tuple[QuarterIn, ...]:
    pv = draw(st.lists(energy, min_size=N, max_size=N))
    load = draw(st.lists(energy, min_size=N, max_size=N))
    rce = draw(st.lists(price, min_size=N, max_size=N))
    return tuple(QuarterIn(T0 + i, pv[i], load[i], rce[i]) for i in range(N))


def _model(switch: bool) -> SiteModel:
    return SWITCH_MODEL if switch else RCE_MODEL


@settings(max_examples=25)
@given(
    qs=inputs(),
    border_h=st.integers(min_value=0, max_value=N // 4),
    switch=st.booleans(),
    self_consume=st.booleans(),
)
def test_chunks_with_carried_state_equal_one_run(
    qs: tuple[QuarterIn, ...], border_h: int, switch: bool, self_consume: bool
) -> None:
    model = _model(switch)
    policy = SelfConsumption() if self_consume else NoBattery()
    s0 = initial_state(model, T0, 5.0, OPENING)
    whole = run(model, policy, RCEM, s0, qs)
    for b in sorted({4 * border_h, MONTH_END}):
        first = run(model, policy, RCEM, s0, qs[:b])
        second = run(model, policy, RCEM, first.state, qs[b:])
        joined = RefResult(
            first.quarters + second.quarters,
            first.rows + second.rows,
            first.months + second.months,
            second.state,
        )
        assert joined.quarters == whole.quarters
        assert joined.rows == whole.rows
        assert joined.months == whole.months
        assert joined.state == whole.state
    assert [m.month for m in whole.months] == ["2026-09"]
    assert whole.months[0].basis is (PriceBasis.RCEM if switch else PriceBasis.RCE)


@settings(max_examples=25)
@given(qs=inputs(), switch=st.booleans())
def test_energy_is_conserved(qs: tuple[QuarterIn, ...], switch: bool) -> None:
    model = _model(switch)
    res = run(model, SelfConsumption(), RCEM, initial_state(model, T0, 5.0, ()), qs)
    soc = 5.0
    eta = model.battery.eta
    for q_in, q in zip(qs, res.quarters, strict=True):
        assert math.isclose(q_in.load_kwh, q_in.pv_kwh + q.grid_kwh - q.p_kw * DT_H, abs_tol=1e-9)
        expected = soc + (q.p_kw * eta * DT_H if q.p_kw >= 0 else q.p_kw / eta * DT_H)
        assert math.isclose(q.soc_kwh, expected, abs_tol=1e-9)
        soc = q.soc_kwh
    for i, row in enumerate(res.rows):
        eb = math.fsum(q.grid_kwh for q in res.quarters[4 * i : 4 * i + 4])
        assert math.isclose(row.import_kwh - row.export_kwh, eb, abs_tol=1e-12)
