"""AC-012-27: the cost columns of a strategy window, from its closed months and its settlement
accumulator (``ems.domain.settlement.costs``, spec 012 plan §5).

- ``bill_pln`` = Σ ``payable_pln`` - Σ ``refund_pln`` over the closed months; None while none
  closed. ``payable_pln`` already nets the deposit's offset; a refund is what remains of an
  expiring tranche, up to its cap.
- ``deposit_closing_pln`` = Σ ``remaining_pln`` of the tranches held (opening tranches included);
  None while no month closed.
- ``ledger_net_cost_pln`` = Σ (energy + distribution - ``rules.deposit_factor`` · export value)
  over the closed months and every hour row still held (two months during the lag); None with
  neither. The open hour (fewer than four quarter-hours) has no row and counts for nothing.
- Every sum is a ``math.fsum``.

Most values are multiples of 1/8 PLN with a deposit factor of 1.25, so every product and sum is
exact in binary floating point and any order of summation gives the same float; the statutory 1.23
comes in where the factor itself is the point.
"""

from __future__ import annotations

import dataclasses
import math

import pytest

from ems.domain.netbilling import NetBillingRules, PriceBasis
from ems.domain.settlement import (
    Costs,
    HourRow,
    MonthSettlement,
    SettleAcc,
    SettleIn,
    Tranche,
    costs,
    settle_month,
)
from ems.domain.tariff import BuyPrice, FixedCharges

EXACT = NetBillingRules(1.25, 0.20, 0.30, 12)  # a factor exact in binary
STATUTORY = NetBillingRules(1.23, 0.20, 0.30, 12)
FIXED = FixedCharges(13.0, 4.0, 20.0, "op")  # 37.0 a month
BUY = BuyPrice(0.8, 0.4, "day", "op", "sel", "q")
APRIL_H = 2026 * 1000  # hour indices only need to be ordered here


def row(
    month: str, h: int, *, energy: float = 0.0, dist: float = 0.0, value: float = 0.0
) -> HourRow:
    return HourRow(
        h=h,
        month=month,
        import_kwh=1.0 if energy else 0.0,  # costs reads only the PLN columns
        export_kwh=1.0 if value else 0.0,
        energy_cost_pln=energy,
        dist_cost_pln=dist,
        export_value_pln=value,
    )


def tranche(created: str, assigned: str, remaining: float, *, amount: float = 0.0) -> Tranche:
    return Tranche(
        created, assigned, max(amount, remaining), remaining, 0.25 * remaining, PriceBasis.RCE
    )


def acc(
    rows: tuple[HourRow, ...] = (),
    tranches: tuple[Tranche, ...] = (),
    hour: tuple[SettleIn, ...] = (),
) -> SettleAcc:
    return SettleAcc(t=0, hour=hour, month_rows=rows, tranches=tranches)


def month(
    name: str,
    rows: list[HourRow],
    opening: tuple[Tranche, ...] = (),
    rules: NetBillingRules = EXACT,
) -> MonthSettlement:
    """A closed month, settled by the domain from hand-built rows and opening tranches."""
    return settle_month(name, rows, FIXED, PriceBasis.RCE, rules, opening)


def ledger_term(rules: NetBillingRules, energy: float, dist: float, value: float) -> float:
    return energy + dist - rules.deposit_factor * value


def test_ac_012_27_costs_fields_are_pinned_and_frozen() -> None:
    assert [f.name for f in dataclasses.fields(Costs)] == [
        "bill_pln",
        "deposit_closing_pln",
        "ledger_net_cost_pln",
    ]
    assert Costs.__dataclass_params__.frozen


def test_ac_012_27_a_window_that_never_started_has_no_costs() -> None:
    """Neither a closed month nor a row: all three are None, not 0."""
    assert costs(EXACT, (), acc()) == Costs(None, None, None)


def test_ac_012_27_no_closed_month_yet_leaves_bill_and_deposit_null() -> None:
    rows = (
        row("2026-04", APRIL_H, energy=2.0, dist=1.0, value=0.5),
        row("2026-04", APRIL_H + 1, value=3.0),
    )
    got = costs(EXACT, (), acc(rows))
    expected = ledger_term(EXACT, 2.0, 1.0, 0.5) + ledger_term(EXACT, 0.0, 0.0, 3.0)
    assert got == Costs(None, None, expected)
    assert expected == -1.375  # 3.0 - 0.625 - 3.75


def test_ac_012_27_opening_tranches_alone_are_no_deposit_yet() -> None:
    """Tranches carried into the window count once a month has closed, not before."""
    opening = (tranche("2026-02", "2026-03", 100.0),)
    rows = (row("2026-04", APRIL_H, energy=1.0),)
    assert costs(EXACT, (), acc(rows, opening)) == Costs(None, None, 1.0)


def test_ac_012_27_a_refund_month() -> None:
    """An opening tranche expires in April: 10 offsets the energy, 90 expires and 30 (its cap) is
    refunded. Bill = payable (37 fixed: energy fully offset) - refund."""
    old = Tranche("2025-04", "2025-05", 123.0, 100.0, 30.0, PriceBasis.RCE)  # last usable 2026-04
    april = month("2026-04", [row("2026-04", APRIL_H, energy=10.0)], (old,))
    assert (april.offset_pln, april.refund_pln, april.payable_pln) == (10.0, 30.0, 37.0)
    assert april.closing == ()
    got = costs(EXACT, (april,), acc(tranches=april.closing))
    assert got == Costs(bill_pln=7.0, deposit_closing_pln=0.0, ledger_net_cost_pln=10.0)


def test_ac_012_27_an_export_month() -> None:
    """April exports 50 PLN of value: its tranche (1.25 · 50) is the deposit, the ledger nets the
    export at the factor, and the bill is the energy, distribution and fixed charges."""
    rows = [
        row("2026-04", APRIL_H, energy=40.0, dist=8.0),
        row("2026-04", APRIL_H + 1, value=50.0),
    ]
    april = month("2026-04", rows)
    assert april.created is not None and april.created.remaining_pln == 62.5
    assert (april.refund_pln, april.payable_pln) == (0.0, 85.0)
    got = costs(EXACT, (april,), acc(tranches=april.closing))
    assert got == Costs(bill_pln=85.0, deposit_closing_pln=62.5, ledger_net_cost_pln=-14.5)


def test_ac_012_27_a_month_without_export() -> None:
    april = month("2026-04", [row("2026-04", APRIL_H, energy=8.0, dist=2.0)])
    assert april.created is None and april.closing == ()
    got = costs(EXACT, (april,), acc(tranches=april.closing))
    assert got == Costs(bill_pln=47.0, deposit_closing_pln=0.0, ledger_net_cost_pln=10.0)


def test_ac_012_27_two_months_held_during_the_lag() -> None:
    """March closed; April has ended but waits for its lag, May is open: both months' rows are
    held, and both count in the ledger, not in the bill or the deposit."""
    march = month("2026-03", [row("2026-03", APRIL_H - 10, energy=4.0, value=2.0)])
    assert march.created is not None  # 2.5 for April
    held = (
        row("2026-04", APRIL_H, energy=1.0, dist=0.5),
        row("2026-04", APRIL_H + 1, value=4.0),
        row("2026-05", APRIL_H + 2, energy=2.0, dist=1.0),
    )
    got = costs(EXACT, (march,), acc(held, march.closing))
    assert got.bill_pln == march.payable_pln - march.refund_pln == 41.0
    assert got.deposit_closing_pln == 2.5
    # March 4 - 2.5; April 1.5 - 5; May 3
    assert got.ledger_net_cost_pln == 1.5 + (1.5 - 5.0) + 3.0 == 1.0


def test_ac_012_27_several_closed_months_and_opening_tranches() -> None:
    """An opening tranche (from before the window) offsets March and April; the deposit is what
    the account holds after April: the rest of it plus March's and April's tranches."""
    opening = (tranche("2026-01", "2026-02", 20.0),)
    march = month("2026-03", [row("2026-03", APRIL_H - 10, energy=12.0, value=4.0)], opening)
    april = month("2026-04", [row("2026-04", APRIL_H, energy=6.0, value=2.0)], march.closing)
    assert march.offset_pln == 12.0 and april.offset_pln == 6.0
    got = costs(EXACT, (march, april), acc(tranches=april.closing))
    assert got.bill_pln == march.payable_pln + april.payable_pln == 74.0  # fixed charges only
    assert got.deposit_closing_pln == math.fsum(t.remaining_pln for t in april.closing) == 9.5
    assert got.ledger_net_cost_pln == (12.0 - 5.0) + (6.0 - 2.5) == 10.5


def test_ac_012_27_the_open_hour_is_left_out() -> None:
    """Three quarter-hours of an hour that has not ended have no row: they cost nothing yet."""
    open_hour = tuple(SettleIn(4 * APRIL_H + i, 2.0, BUY, 0.5) for i in range(3))
    assert costs(EXACT, (), acc(hour=open_hour)) == Costs(None, None, None)
    rows = (row("2026-04", APRIL_H - 1, energy=1.0, dist=0.25),)
    assert costs(EXACT, (), acc(rows, hour=open_hour)) == Costs(None, None, 1.25)


@pytest.mark.parametrize("rules", [STATUTORY, EXACT, NetBillingRules(1.5, 0.2, 0.3, 12)])
def test_ac_012_27_the_deposit_factor_comes_from_the_rules(rules: NetBillingRules) -> None:
    rows = (row("2026-04", APRIL_H, value=2.0),)
    assert costs(rules, (), acc(rows)).ledger_net_cost_pln == -(rules.deposit_factor * 2.0)
    april = month("2026-04", [row("2026-04", APRIL_H, value=2.0)], rules=rules)
    got = costs(rules, (april,), acc(tranches=april.closing))
    assert got.ledger_net_cost_pln == -(rules.deposit_factor * 2.0)


def test_ac_012_27_sums_are_fsum() -> None:
    """Ten values of 0.1 add to exactly 1.0 with ``math.fsum`` (a left-to-right float sum gives
    0.9999999999999999; Python 3.12+ ``sum`` compensates too, so it is not the reference here)."""
    assert math.fsum([0.1] * 10) == 1.0
    rows = tuple(row("2026-04", APRIL_H + i, energy=0.1) for i in range(10))
    tranches = tuple(tranche("2026-03", "2026-04", 0.1) for _ in range(10))
    months = tuple(
        dataclasses.replace(
            month("2026-03", [row("2026-03", APRIL_H - 10)]),
            payable_pln=0.1,
            energy_cost_pln=0.0,
            dist_cost_pln=0.0,
            export_value_pln=0.0,
        )
        for _ in range(10)
    )
    got = costs(EXACT, months, acc(rows, tranches))
    assert got == Costs(bill_pln=1.0, deposit_closing_pln=1.0, ledger_net_cost_pln=1.0)
