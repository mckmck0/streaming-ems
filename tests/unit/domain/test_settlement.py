"""AC-009-10, -12: hourly ledger rows and the monthly billing settlement with the deposit."""

from __future__ import annotations

import math
from datetime import UTC, datetime

import pytest

from ems.contracts.time import quarter_index
from ems.domain.netbilling import NetBillingRules, PriceBasis
from ems.domain.settlement import (
    HourRow,
    SettleIn,
    Tranche,
    hour_rows,
    settle_hour,
    settle_month,
)
from ems.domain.tariff import BuyPrice, FixedCharges

RULES = NetBillingRules(1.23, 0.20, 0.30, 12)
BUY = BuyPrice(0.8, 0.4, "day", "op", "sel", "q")
FIXED = FixedCharges(13.0, 4.0, 20.0, "op")
APRIL = quarter_index(datetime(2026, 4, 10, 8, tzinfo=UTC))


def _row(month: str, *, energy: float = 0.0, dist: float = 0.0, value: float = 0.0) -> HourRow:
    return HourRow(
        h=0,
        month=month,
        import_kwh=1.0 if energy else 0.0,
        export_kwh=1.0 if value else 0.0,
        energy_cost_pln=energy,
        dist_cost_pln=dist,
        export_value_pln=value,
    )


def _tranche(created: str, assigned: str, remaining: float, cap: float = 0.0) -> Tranche:
    return Tranche(created, assigned, remaining, remaining, cap, PriceBasis.RCE)


def test_ac_009_10_row_fields() -> None:
    quarters = [SettleIn(APRIL + i, g, BUY, 0.3) for i, g in enumerate([0.5, 0.25, -0.1, 0.35])]
    row = settle_hour(quarters)
    assert row.h == APRIL // 4 and row.month == "2026-04"
    assert row.import_kwh == pytest.approx(1.0) and row.export_kwh == 0.0
    assert row.energy_cost_pln == math.fsum([row.import_kwh / 4 * 0.8] * 4)
    assert row.dist_cost_pln == math.fsum([row.import_kwh / 4 * 0.4] * 4)
    assert row.export_value_pln == 0.0


def test_ac_009_10_rows_need_whole_consecutive_hours() -> None:
    q = [SettleIn(APRIL + i, 0.1, BUY, 0.3) for i in range(4)]
    with pytest.raises(ValueError):
        settle_hour(q[:3])
    with pytest.raises(ValueError):
        settle_hour([SettleIn(APRIL + 1 + i, 0.1, BUY, 0.3) for i in range(4)])
    with pytest.raises(ValueError):
        settle_hour([q[0], q[2], q[1], q[3]])
    with pytest.raises(ValueError, match="hour"):
        hour_rows([SettleIn(APRIL + 2 + i, 0.1, BUY, 0.3) for i in range(8)])
    with pytest.raises(ValueError, match="hour"):
        hour_rows(q + q[:2])


def test_ac_009_12_offset_is_energy_only_and_carries_over() -> None:
    rows = [_row("2026-04", energy=30.0, dist=20.0)]
    s = settle_month(
        "2026-04", rows, FIXED, PriceBasis.RCE, RULES, (_tranche("2026-02", "2026-03", 100.0),)
    )
    assert s.offset_pln == 30.0
    assert s.closing[0].remaining_pln == 70.0
    assert s.payable_pln == math.fsum(
        [20.0, 13.0, 4.0, 20.0]
    )  # distribution and fixed paid in full


def test_ac_009_12_oldest_assignment_first() -> None:
    older = _tranche("2026-01", "2026-02", 40.0)
    newer = _tranche("2026-03", "2026-04", 40.0)
    s = settle_month(
        "2026-04", [_row("2026-04", energy=50.0)], FIXED, PriceBasis.RCE, RULES, (newer, older)
    )
    assert dict(s.offsets) == {"2026-02": 40.0, "2026-04": 10.0}
    assert [t.assigned for t in s.closing] == ["2026-04"]
    assert s.closing[0].remaining_pln == 30.0


def test_ac_009_12_expiry_twelve_months_after_assignment_with_capped_refund() -> None:
    old = Tranche("2025-04", "2025-05", 123.0, 100.0, 30.0, PriceBasis.RCE)  # last usable 2026-04
    s = settle_month(
        "2026-04", [_row("2026-04", energy=10.0)], FIXED, PriceBasis.RCE, RULES, (old,)
    )
    assert s.offset_pln == 10.0
    assert s.expired_pln == 90.0
    assert s.refund_pln == 30.0 and s.extinguished_pln == 60.0
    assert s.closing == ()
    small = Tranche("2025-04", "2025-05", 123.0, 12.0, 30.0, PriceBasis.RCE)
    s2 = settle_month("2026-04", [_row("2026-04")], FIXED, PriceBasis.RCE, RULES, (small,))
    assert (s2.refund_pln, s2.extinguished_pln) == (12.0, 0.0)
    # One month earlier it is still usable and not expired.
    s3 = settle_month("2026-03", [_row("2026-03")], FIXED, PriceBasis.RCE, RULES, (old,))
    assert s3.expired_pln == 0.0 and s3.closing == (old,)


@pytest.mark.parametrize(("basis", "pct"), [(PriceBasis.RCE, 0.30), (PriceBasis.RCEM, 0.20)])
def test_ac_009_12_created_tranche_and_cap_before_the_factor(basis: PriceBasis, pct: float) -> None:
    s = settle_month("2026-04", [_row("2026-04", value=50.0)], FIXED, basis, RULES, ())
    assert s.created is not None
    assert s.created.created == "2026-04" and s.created.assigned == "2026-05"
    assert s.created.amount_pln == 1.23 * 50.0 == s.created.remaining_pln
    assert s.created.cap_pln == pct * 50.0
    assert s.created.basis is basis


def test_ac_009_12_month_m_tranche_is_first_usable_in_m_plus_one_after_older_ones() -> None:
    april = settle_month(
        "2026-04", [_row("2026-04", energy=40.0, value=50.0)], FIXED, PriceBasis.RCE, RULES, ()
    )
    assert april.offset_pln == 0.0  # its own export never offsets April
    older = _tranche("2026-03", "2026-04", 10.0)
    may = settle_month(
        "2026-05",
        [_row("2026-05", energy=40.0)],
        FIXED,
        PriceBasis.RCE,
        RULES,
        (*april.closing, older),
    )
    assert may.offsets == (("2026-04", 10.0), ("2026-05", 30.0))


def test_ac_009_12_a_month_without_export_creates_no_tranche() -> None:
    s = settle_month("2026-04", [_row("2026-04", energy=5.0)], FIXED, PriceBasis.RCE, RULES, ())
    assert s.created is None and s.closing == ()


def test_ac_009_12_rcem_correction_changes_the_tranche_not_the_payable() -> None:
    first = [_row("2026-02", energy=40.0, dist=10.0, value=33.901)]
    corrected = [_row("2026-02", energy=40.0, dist=10.0, value=33.139)]
    a = settle_month("2026-02", first, FIXED, PriceBasis.RCEM, RULES, ())
    b = settle_month("2026-02", corrected, FIXED, PriceBasis.RCEM, RULES, ())
    assert a.payable_pln == b.payable_pln
    assert a.created is not None and b.created is not None
    assert a.created.amount_pln != b.created.amount_pln


def test_ac_009_12_rejects_expired_opening_and_foreign_rows() -> None:
    expired = _tranche("2025-02", "2025-03", 5.0)  # last usable 2026-02
    with pytest.raises(ValueError, match="expired"):
        settle_month("2026-04", [_row("2026-04")], FIXED, PriceBasis.RCE, RULES, (expired,))
    with pytest.raises(ValueError, match="2026-05"):
        settle_month("2026-04", [_row("2026-05")], FIXED, PriceBasis.RCE, RULES, ())


def test_ac_009_12_totals() -> None:
    rows = [_row("2026-04", energy=1.0, dist=0.5), _row("2026-04", value=2.0)]
    s = settle_month("2026-04", rows, FIXED, PriceBasis.RCE, RULES, ())
    assert (s.energy_cost_pln, s.dist_cost_pln, s.export_value_pln) == (1.0, 0.5, 2.0)
    assert (s.import_kwh, s.export_kwh) == (1.0, 1.0)
    assert s.fixed == FIXED and s.basis is PriceBasis.RCE
