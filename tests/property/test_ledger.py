"""AC-009-11: the hourly ledger is additive and chunk-independent on hour borders."""

from __future__ import annotations

import math
from datetime import UTC, datetime

import pytest
from hypothesis import given
from hypothesis import strategies as st

from ems.contracts.time import quarter_index
from ems.domain.netbilling import NetBillingRules, PriceBasis
from ems.domain.settlement import SettleIn, hour_rows, settle_month
from ems.domain.tariff import BuyPrice, FixedCharges

T0 = quarter_index(datetime(2026, 9, 30, 18, tzinfo=UTC))  # spans the civil month end
prices = st.floats(min_value=-0.5, max_value=2.0, allow_nan=False)
flows = st.floats(min_value=-5.0, max_value=5.0, allow_nan=False)


@st.composite
def quarters(draw: st.DrawFn) -> list[SettleIn]:
    hours = draw(st.integers(min_value=1, max_value=12))
    out = []
    for i in range(4 * hours):
        buy = BuyPrice(draw(prices), draw(prices), "z", "op", "sel", "q")
        out.append(SettleIn(T0 + i, draw(flows), buy, draw(prices)))
    return out


@given(qs=quarters(), data=st.data())
def test_hour_aligned_splits_give_equal_rows(qs: list[SettleIn], data: st.DataObject) -> None:
    k = 4 * data.draw(st.integers(min_value=0, max_value=len(qs) // 4))
    assert hour_rows(qs) == hour_rows(qs[:k]) + hour_rows(qs[k:])


@given(qs=quarters(), off=st.integers(min_value=1, max_value=3))
def test_non_hour_border_is_rejected(qs: list[SettleIn], off: int) -> None:
    with pytest.raises(ValueError, match="hour"):
        hour_rows(qs[off:])
    with pytest.raises(ValueError, match="hour"):
        hour_rows(qs[: len(qs) - off])


@given(qs=quarters())
def test_rows_balance_and_totals_are_order_independent(qs: list[SettleIn]) -> None:
    rows = hour_rows(qs)
    for i, row in enumerate(rows):
        eb = math.fsum(q.grid_kwh for q in qs[4 * i : 4 * i + 4])
        assert row.import_kwh >= 0.0 and row.export_kwh >= 0.0
        assert row.import_kwh == 0.0 or row.export_kwh == 0.0
        assert row.import_kwh - row.export_kwh == eb
    values = [r.energy_cost_pln for r in rows] + [r.export_value_pln for r in rows]
    assert math.fsum(values) == math.fsum(reversed(values))


@given(qs=quarters(), data=st.data())
def test_month_totals_do_not_depend_on_row_order(qs: list[SettleIn], data: st.DataObject) -> None:
    rows = [r for r in hour_rows(qs) if r.month == "2026-09"]
    shuffled = data.draw(st.permutations(rows))
    rules = NetBillingRules(1.23, 0.20, 0.30, 12)
    fixed = FixedCharges(10.0, 4.0, 20.0, "op")
    a = settle_month("2026-09", rows, fixed, PriceBasis.RCE, rules, ())
    b = settle_month("2026-09", shuffled, fixed, PriceBasis.RCE, rules, ())
    assert a == b
