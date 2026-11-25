"""Two settlement layers and the price signal.

(a) The ledger: one row per hour, from the hourly vector-balanced net split evenly over the
    hour's quarter-hours (RES act art. 4 ust. 2b, art. 4b ust. 11). Every energy-dependent DSO fee
    uses the hourly import (art. 4 ust. 4c). Each row depends only on its own hour, so the ledger
    is additive.
(b) The billing settlement: one civil month at a time, a pure function of that month's rows, its
    fixed charges, the price basis and the deposit tranches carried in. It is not additive.

The incremental form is four pieces (spec 011, allowed lateness, ADR 0018):
- ``add_quarter`` adds one quarter-hour and writes its hour's row on the hour's fourth quarter;
- ``correct`` applies a late reading: it recomputes the quarter's grid energy from the corrected PV
  or load and the battery power already executed, then re-fires its hour (the row is recomputed);
- ``due_month`` and ``close_month`` settle a civil month once it has ended and a lag has passed, so
  that a correction inside the lateness window still reaches the month's bill.
``settle_quarter`` is their composition with no lag: it closes the hour and the month exactly where
the batch reference does.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass, replace

from ems.domain.calendar import hour_month, month_add, month_index
from ems.domain.netbilling import NetBillingRules, PriceBasis, floored
from ems.domain.tariff import BuyPrice, FixedCharges
from ems.domain.units import DT_H, QUARTERS_PER_HOUR

# ── (a) ledger ───────────────────────────────────────────────────────────────────────────────────


def grid_kwh(pv_kwh: float, load_kwh: float, p_kw: float) -> float:
    """Energy through the meter in one quarter-hour (> 0 import): load - PV + battery power · Δt."""
    return (load_kwh - pv_kwh) + p_kw * DT_H


@dataclass(frozen=True)
class SettleIn:
    """One quarter-hour as settlement sees it."""

    t: int
    grid_kwh: float  # > 0 import, < 0 export (before hourly balancing)
    buy: BuyPrice
    export_price_pln_per_kwh: float  # RCE of the quarter or the month's RCEm, before the floor


@dataclass(frozen=True)
class HourRow:
    h: int  # UTC hour index, t // 4
    month: str  # civil billing month
    import_kwh: float
    export_kwh: float
    energy_cost_pln: float  # gross, offsettable by the deposit
    dist_cost_pln: float  # gross, always paid
    export_value_pln: float  # before the 1.23 factor


def settle_hour(quarters: Sequence[SettleIn]) -> HourRow:
    if len(quarters) != QUARTERS_PER_HOUR or quarters[0].t % QUARTERS_PER_HOUR != 0:
        raise ValueError("an hour needs its four quarter-hours, starting on the hour")
    t0 = quarters[0].t
    if any(q.t != t0 + i for i, q in enumerate(quarters)):
        raise ValueError(f"quarter-hours of hour {t0 // QUARTERS_PER_HOUR} are not consecutive")
    eb = math.fsum(q.grid_kwh for q in quarters)
    import_kwh = eb if eb > 0.0 else 0.0
    export_kwh = -eb if eb < 0.0 else 0.0
    import_q = import_kwh / QUARTERS_PER_HOUR
    export_q = export_kwh / QUARTERS_PER_HOUR
    return HourRow(
        h=t0 // QUARTERS_PER_HOUR,
        month=hour_month(t0 // QUARTERS_PER_HOUR),
        import_kwh=import_kwh,
        export_kwh=export_kwh,
        energy_cost_pln=math.fsum(import_q * q.buy.energy_pln_per_kwh for q in quarters),
        dist_cost_pln=math.fsum(import_q * q.buy.dist_pln_per_kwh for q in quarters),
        export_value_pln=math.fsum(
            floored(q.export_price_pln_per_kwh) * export_q for q in quarters
        ),
    )


def hour_rows(quarters: Sequence[SettleIn]) -> tuple[HourRow, ...]:
    """Rows for a window; its borders must lie on UTC hours."""
    if len(quarters) % QUARTERS_PER_HOUR or (quarters and quarters[0].t % QUARTERS_PER_HOUR):
        raise ValueError("ledger window must start and end on a UTC hour")
    return tuple(
        settle_hour(quarters[i : i + QUARTERS_PER_HOUR])
        for i in range(0, len(quarters), QUARTERS_PER_HOUR)
    )


# ── (b) billing settlement ───────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Tranche:
    """One month's deposit: 1.23 x its export value, assigned in the next month."""

    created: str  # month of the feed-in
    assigned: str  # created + 1: first month it may offset
    amount_pln: float
    remaining_pln: float
    cap_pln: float  # refund cap: 20 % (RCEm) or 30 % (RCE) of the value before 1.23
    basis: PriceBasis

    def __post_init__(self) -> None:
        if self.assigned != month_add(self.created, 1):
            raise ValueError(f"tranche of {self.created} must be assigned in the next month")
        if not 0.0 <= self.remaining_pln <= self.amount_pln or self.cap_pln < 0.0:
            raise ValueError(f"tranche of {self.created}: need 0 <= remaining <= amount, cap >= 0")

    def last_month(self, rules: NetBillingRules) -> str:
        return month_add(self.assigned, rules.validity_months - 1)


def check_opening(tranches: Sequence[Tranche], month: str, rules: NetBillingRules) -> None:
    """Tranches carried into ``month``: assigned by then and not expired before it."""
    m = month_index(month)
    for tr in tranches:
        if month_index(tr.last_month(rules)) < m:
            raise ValueError(f"tranche assigned {tr.assigned} expired before {month}")
        if month_index(tr.assigned) > m:
            raise ValueError(f"tranche assigned {tr.assigned} is not usable yet in {month}")


@dataclass(frozen=True)
class MonthSettlement:
    month: str
    basis: PriceBasis
    import_kwh: float
    export_kwh: float
    energy_cost_pln: float
    dist_cost_pln: float
    export_value_pln: float
    fixed: FixedCharges
    offsets: tuple[tuple[str, float], ...]  # (tranche's assigned month, amount), in use order
    offset_pln: float
    expired_pln: float  # remaining value of the tranches whose validity ends this month
    refund_pln: float  # booked this month, paid by the end of the next
    extinguished_pln: float
    created: Tranche | None  # None for a month without export
    payable_pln: float
    closing: tuple[Tranche, ...]


def settle_month(
    month: str,
    rows: Sequence[HourRow],
    fixed: FixedCharges,
    basis: PriceBasis,
    rules: NetBillingRules,
    opening: Sequence[Tranche],
) -> MonthSettlement:
    """Steps, in this order: totals; offset (oldest assignment first); expiry with a capped refund;
    this month's tranche; payable = energy - offset + distribution + fixed charges.
    """
    for r in rows:
        if r.month != month:
            raise ValueError(f"row of hour {r.h} belongs to {r.month}, not {month}")
    m = month_index(month)
    check_opening(opening, month, rules)
    energy = math.fsum(r.energy_cost_pln for r in rows)
    dist = math.fsum(r.dist_cost_pln for r in rows)
    value = math.fsum(r.export_value_pln for r in rows)

    need = energy
    offsets: list[tuple[str, float]] = []
    kept: list[Tranche] = []
    expired: list[float] = []
    refunds: list[float] = []
    for tr in sorted(opening, key=lambda x: (month_index(x.assigned), month_index(x.created))):
        take = min(tr.remaining_pln, need) if need > 0.0 else 0.0
        if take > 0.0:
            need = need - take
            offsets.append((tr.assigned, take))
        remaining = tr.remaining_pln - take
        if month_index(tr.last_month(rules)) <= m:
            expired.append(remaining)
            refunds.append(min(remaining, tr.cap_pln))
        elif remaining > 0.0:  # a used-up tranche leaves the account
            kept.append(
                Tranche(tr.created, tr.assigned, tr.amount_pln, remaining, tr.cap_pln, tr.basis)
            )

    created = None
    if value > 0.0:
        created = Tranche(
            created=month,
            assigned=month_add(month, 1),
            amount_pln=rules.deposit_factor * value,
            remaining_pln=rules.deposit_factor * value,
            cap_pln=rules.refund_cap(basis) * value,
            basis=basis,
        )
        kept.append(created)
    offset = math.fsum(a for _, a in offsets)
    expired_pln = math.fsum(expired)
    refund = math.fsum(refunds)
    return MonthSettlement(
        month=month,
        basis=basis,
        import_kwh=math.fsum(r.import_kwh for r in rows),
        export_kwh=math.fsum(r.export_kwh for r in rows),
        energy_cost_pln=energy,
        dist_cost_pln=dist,
        export_value_pln=value,
        fixed=fixed,
        offsets=tuple(offsets),
        offset_pln=offset,
        expired_pln=expired_pln,
        refund_pln=refund,
        extinguished_pln=expired_pln - refund,
        created=created,
        payable_pln=math.fsum(
            (energy, -offset, dist, fixed.network_pln, fixed.subscription_pln, fixed.capacity_pln)
        ),
        closing=tuple(kept),
    )


# ── price signal ─────────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Signal:
    buy_pln_per_kwh: float  # energy + distribution, gross
    sell_pln_per_kwh: float  # 1.23 · max(P, 0): export valued as if the deposit is used


def price_signal(buy: BuyPrice, p_pln_per_kwh: float, rules: NetBillingRules) -> Signal:
    """Marginal prices for a policy. The gaps to the bill (hourly netting, the hour's mean price,
    deposit saturation, monthly billing) are documented in 12-domain-model.
    """
    return Signal(
        buy_pln_per_kwh=buy.energy_pln_per_kwh + buy.dist_pln_per_kwh,
        sell_pln_per_kwh=rules.deposit_factor * floored(p_pln_per_kwh),
    )


# ── incremental settlement ───────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SettleAcc:
    t: int  # the next quarter-hour expected
    hour: tuple[SettleIn, ...]  # the open hour (at most three quarters)
    # Rows not yet settled: the open month's, and an ended month's until its lag has passed.
    month_rows: tuple[HourRow, ...]
    tranches: tuple[Tranche, ...]


@dataclass(frozen=True)
class Correction:
    """A reading that arrived after its quarter-hour ran on an estimate."""

    stream: str  # "production" (replaces PV) or "consumption" (replaces load)
    t: int
    kwh: float


@dataclass(frozen=True)
class Flow:
    """A settled quarter-hour, kept while its hour can still be corrected."""

    pv_kwh: float
    load_kwh: float
    p_kw: float  # the battery power executed; a correction never changes it
    settle: SettleIn


def add_quarter(acc: SettleAcc, s: SettleIn) -> tuple[SettleAcc, HourRow | None]:
    """Add one quarter-hour; its hour's fourth quarter writes the hour's row."""
    if s.t != acc.t:
        raise ValueError(f"quarter-hour {s.t} out of order: expected {acc.t}")
    if s.t % QUARTERS_PER_HOUR < QUARTERS_PER_HOUR - 1:
        return SettleAcc(s.t + 1, (*acc.hour, s), acc.month_rows, acc.tranches), None
    row = settle_hour((*acc.hour, s))
    return SettleAcc(s.t + 1, (), (*acc.month_rows, row), acc.tranches), row


def correct(
    acc: SettleAcc, recent: tuple[Flow, ...], c: Correction
) -> tuple[SettleAcc, tuple[Flow, ...], HourRow | None]:
    """Apply a late reading to the kept flow of ``c.t`` and re-fire its hour.

    The quarter's grid energy is recomputed from the corrected PV or load and the battery power
    that was executed. A quarter of the open hour is replaced in place (no row yet); a quarter of a
    closed hour recomputes that hour's row, which is returned.
    """
    if c.stream not in ("production", "consumption"):
        raise ValueError(f"correction of an unknown stream {c.stream!r}")
    if c.t >= acc.t:
        raise ValueError(f"correction of quarter-hour {c.t}, which is not settled yet")
    found = [i for i, f in enumerate(recent) if f.settle.t == c.t]
    if not found:
        raise ValueError(f"correction of quarter-hour {c.t}, whose flow is no longer kept")
    i = found[0]
    old = recent[i]
    pv, load = (c.kwh, old.load_kwh) if c.stream == "production" else (old.pv_kwh, c.kwh)
    settle = replace(old.settle, grid_kwh=grid_kwh(pv, load, old.p_kw))
    recent = (*recent[:i], Flow(pv, load, old.p_kw, settle), *recent[i + 1 :])

    hour_start = QUARTERS_PER_HOUR * (acc.t // QUARTERS_PER_HOUR)
    if c.t >= hour_start:
        k = c.t - hour_start
        hour = (*acc.hour[:k], settle, *acc.hour[k + 1 :])
        return SettleAcc(acc.t, hour, acc.month_rows, acc.tranches), recent, None

    h = c.t // QUARTERS_PER_HOUR
    rows = acc.month_rows
    j = h - rows[0].h if rows else -1
    if not 0 <= j < len(rows) or rows[j].h != h:
        raise ValueError(f"correction of hour {h}, whose month is already settled")
    row = settle_hour([f.settle for f in recent if f.settle.t // QUARTERS_PER_HOUR == h])
    rows = (*rows[:j], row, *rows[j + 1 :])
    return SettleAcc(acc.t, acc.hour, rows, acc.tranches), recent, row


def due_month(acc: SettleAcc, lag: int) -> str | None:
    """The oldest unsettled month, once its last hour has a row and ``lag`` more quarter-hours
    have been added after it; else None.
    """
    rows = acc.month_rows
    if not rows:
        return None
    month = rows[0].month
    last = rows[0]
    for row in rows:
        if row.month != month:
            break
        last = row
    if hour_month(last.h + 1) == month:
        return None  # the month's last hour has no row yet
    last_quarter = QUARTERS_PER_HOUR * last.h + QUARTERS_PER_HOUR - 1
    return month if acc.t > last_quarter + lag else None


def close_month(
    acc: SettleAcc, fixed: FixedCharges, basis: PriceBasis, rules: NetBillingRules
) -> tuple[SettleAcc, MonthSettlement]:
    """Settle the oldest unsettled month with its own fixed charges and price basis."""
    rows = acc.month_rows
    month = rows[0].month
    n = 0
    while n < len(rows) and rows[n].month == month:
        n += 1
    settled = settle_month(month, rows[:n], fixed, basis, rules, acc.tranches)
    return SettleAcc(acc.t, acc.hour, rows[n:], settled.closing), settled


def settle_quarter(
    acc: SettleAcc,
    s: SettleIn,
    fixed: FixedCharges,
    basis: PriceBasis,
    rules: NetBillingRules,
) -> tuple[SettleAcc, HourRow | None, MonthSettlement | None]:
    """Add one quarter-hour with no lag. Its fourth quarter closes the hour; the last hour of a
    civil month closes the month with ``fixed`` and ``basis`` of that month.
    """
    acc, row = add_quarter(acc, s)
    month = None
    if due_month(acc, 0) is not None:
        acc, month = close_month(acc, fixed, basis, rules)
    return acc, row, month


# ── cost metrics of a run (spec 012; definitions in 11-experiments.md) ──────────────────────────


@dataclass(frozen=True)
class Costs:
    """The three cost numbers of a household and strategy, from what its state holds."""

    bill_pln: (
        float | None
    )  # sum of payable minus refunds over the closed months; None with none closed
    deposit_closing_pln: float | None  # the deposit held after the last closed month
    ledger_net_cost_pln: float | None  # sum of (energy + distribution - factor x export value)


def costs(rules: NetBillingRules, months: Sequence[MonthSettlement], acc: SettleAcc) -> Costs:
    """``bill_pln`` and ``deposit_closing_pln`` over the closed months (None while none closed);
    ``ledger_net_cost_pln`` over the closed months and every hour row still held, which during the
    lag can span two months (None when there is neither). The open hour has no row yet.
    """
    bill = deposit = None
    if months:
        bill = math.fsum(m.payable_pln for m in months) - math.fsum(m.refund_pln for m in months)
        deposit = math.fsum(tr.remaining_pln for tr in acc.tranches)
    terms = [
        x.energy_cost_pln + x.dist_cost_pln - rules.deposit_factor * x.export_value_pln
        for x in (*months, *acc.month_rows)
    ]
    ledger = math.fsum(terms) if terms else None
    return Costs(bill, deposit, ledger)
