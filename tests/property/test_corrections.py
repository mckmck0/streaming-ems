"""AC-011-03 … -05: random late readings within the allowed lateness (spec 011, plan §2; ADR 0018).

The window is 24 h of the spec 009 window across the civil end of October (2026-10-31T06:00Z …
2026-11-01T06:00Z; October ends at 23:00Z), with its opening tranches, stepped with SelfConsumption
for both price bases; it is built once per process (``tests/unit/domain/ref_window.py``).

Each drawn correction replaces the PV or the load of a slot ``t`` and is attached to a step ``s``
with ``t + L + 1 <= s <= t + 2L`` (the bound the meter stream guarantees), for ``L`` in 1…8 at lag
``2L``. A correction attached to a step past the window is never applied, as on both sides.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import cache

import pytest
from hypothesis import given
from hypothesis import strategies as st

from ems.contracts.time import quarter_index
from ems.domain.actions import SelfConsumption
from ems.domain.calendar import civil_month
from ems.domain.netbilling import PriceBasis, RcemTable, basis_for
from ems.domain.reference import QuarterIn, SiteModel, initial_state
from ems.domain.settlement import (
    Correction,
    Flow,
    HourRow,
    MonthSettlement,
    SettleIn,
    Tranche,
    grid_kwh,
    hour_rows,
    settle_month,
)
from ems.domain.stepper import StepOut, step
from ems.domain.tariff import fixed_charges
from tests.unit.domain.ref_window import OPENING, SOC0_KWH, spec009_window

T0 = quarter_index(datetime(2026, 10, 31, 6, tzinfo=UTC))
N = 96
T_END = T0 + N
OCT_LAST = quarter_index(datetime(2026, 10, 31, 23, tzinfo=UTC)) - 1  # civil October's last slot
STREAMS = ("production", "consumption")
MAX_CORRECTIONS = 40

late_slots = st.integers(min_value=1, max_value=8)
reading_kwh = st.floats(min_value=0.0, max_value=5.0, allow_nan=False, allow_infinity=False)


@dataclass(frozen=True)
class Base:
    model: SiteModel
    rcem: RcemTable
    opening: tuple[Tranche, ...]
    inputs: tuple[QuarterIn, ...]
    plain: tuple[StepOut, ...]  # lag 0, no correction


def _run(
    base: Base, lag: int, attached: dict[int, list[Correction]] | None = None
) -> list[StepOut]:
    state = initial_state(base.model, T0, SOC0_KWH, base.opening)
    recent: tuple[Flow, ...] = ()
    outs: list[StepOut] = []
    for q in base.inputs:
        corrections = (attached or {}).get(q.t, ())
        out = step(base.model, SelfConsumption(), base.rcem, state, q, recent, corrections, lag)
        outs.append(out)
        state, recent = out.state, out.recent
    return outs


@cache
def _base(basis: PriceBasis) -> Base:
    w = spec009_window()
    inputs = tuple(q for q in w.inputs if T0 <= q.t < T_END)
    assert len(inputs) == N
    assert civil_month(OCT_LAST) == "2026-10" and civil_month(OCT_LAST + 1) == "2026-11"
    partial = Base(w.model(basis), w.proxies, OPENING[basis], inputs, ())
    return Base(partial.model, partial.rcem, partial.opening, inputs, tuple(_run(partial, 0)))


@pytest.fixture(scope="module", params=[PriceBasis.RCE, PriceBasis.RCEM], ids=["rce", "rcem"])
def base(request: pytest.FixtureRequest) -> Base:
    return _base(request.param)


@st.composite
def late_readings(draw: st.DrawFn) -> tuple[int, list[tuple[int, str, float, int]]]:
    """``L`` and (slot offset, stream, corrected kWh, delay to the step it is attached to)."""
    late = draw(late_slots)
    items = draw(
        st.lists(
            st.tuples(
                st.integers(min_value=0, max_value=N - 1),
                st.sampled_from(STREAMS),
                reading_kwh,
                st.integers(min_value=late + 1, max_value=2 * late),
            ),
            max_size=MAX_CORRECTIONS,
            unique_by=lambda x: (x[0], x[1]),  # a slot's reading arrives once per stream
        )
    )
    return late, items


def _months(outs: Sequence[StepOut]) -> list[tuple[int, MonthSettlement]]:
    return [(o.quarter.t, o.month) for o in outs if o.month is not None]


@given(case=late_readings())
def test_ac_011_03_to_05_rows_and_months_follow_the_final_flows(
    base: Base, case: tuple[int, list[tuple[int, str, float, int]]]
) -> None:
    late, items = case
    lag = 2 * late
    pv = {q.t: q.pv_kwh for q in base.inputs}
    load = {q.t: q.load_kwh for q in base.inputs}
    attached: dict[int, list[Correction]] = {}
    # Production's before consumption's within a step (AC-011-18), then by slot.
    for i, stream, kwh, delay in sorted(
        items, key=lambda x: (x[0] + x[3], STREAMS.index(x[1]), x[0])
    ):
        t = T0 + i
        if t + delay >= T_END:
            continue  # attached to a step never released: never applied
        attached.setdefault(t + delay, []).append(Correction(stream, t, kwh))
        (pv if stream == "production" else load)[t] = kwh
    final_in = [
        SettleIn(
            o.quarter.t,
            grid_kwh(pv[o.quarter.t], load[o.quarter.t], o.quarter.p_kw),
            o.quarter.buy,
            o.quarter.export_price_pln_per_kwh,
        )
        for o in base.plain
    ]
    final_row: dict[int, HourRow] = {r.h: r for r in hour_rows(final_in)}

    outs = _run(base, lag, attached)  # ``correct`` never raises inside the allowed lateness
    for plain, out in zip(base.plain, outs, strict=True):
        assert out.quarter == plain.quarter  # no decision, power or SoC changes
        acc = out.state.acc
        assert len(out.recent) <= 2 * late + 3  # AC-011-05
        assert len({r.month for r in acc.month_rows}) <= 2
        for row in acc.month_rows:
            if 4 * row.h + 3 + lag < acc.t:  # no correction can reach this hour any more
                assert row == final_row[row.h]

    ((t_close, october),) = _months(outs)
    assert t_close == OCT_LAST + lag
    model = base.model
    assert october == settle_month(
        "2026-10",
        [r for r in final_row.values() if r.month == "2026-10"],
        fixed_charges(model.tariff, "2026-10", model.fixed_inputs),
        basis_for(model.cohort, "2026-10"),
        model.rules,
        base.opening,
    )
    end = outs[-1].state.acc
    assert end.hour == ()
    assert end.month_rows == tuple(r for r in final_row.values() if r.month == "2026-11")


@given(late=late_slots)
def test_ac_011_04_a_lag_only_delays_the_close(base: Base, late: int) -> None:
    lag = 2 * late
    outs = _run(base, lag)
    for plain, out in zip(base.plain, outs, strict=True):
        assert out.quarter == plain.quarter and out.row == plain.row
        in_lag = OCT_LAST <= out.quarter.t < OCT_LAST + lag
        assert (out.state == plain.state) is (not in_lag), out.quarter.t
        assert len(out.recent) <= lag + 3
    ((t_plain, oct_plain),) = _months(base.plain)
    ((t_lag, oct_lag),) = _months(outs)
    assert (t_plain, t_lag) == (OCT_LAST, OCT_LAST + lag)
    assert oct_lag == oct_plain
