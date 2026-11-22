"""Offline reference: a policy replayed over one site's inputs through the battery model and both
settlement layers (batch form). ``stepper.step`` does the same one quarter-hour at a time, as the
actors will; the two are compared bit for bit (AC-009-20).

Chunks start and end on UTC hours. Run [a, c) or [a, b) then [b, c) with the returned state: the
results are identical (AC-009-18).

One quarter-hour is four pieces, so that the actors run the reference's own code (spec 011):
``prices`` (what the quarter costs and pays), ``decide`` (the policy, in ``Ems``), ``apply`` (the
battery, in ``Battery``) and ``settlement.grid_kwh`` (the meter, in ``Meter``). ``quarter`` composes
them.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date

from ems.domain.actions import Action, BatteryState, Observation, Policy, resolve
from ems.domain.battery import BatteryParams, step_params
from ems.domain.calendar import civil_date, civil_month, hour_month
from ems.domain.netbilling import (
    Cohort,
    NetBillingRules,
    PriceBasis,
    Publication,
    RcemEntry,
    RcemTable,
    basis_for,
)
from ems.domain.settlement import (
    HourRow,
    MonthSettlement,
    SettleAcc,
    SettleIn,
    Signal,
    Tranche,
    check_opening,
    grid_kwh,
    hour_rows,
    price_signal,
    settle_month,
)
from ems.domain.tariff import BuyPrice, FixedInputs, SiteTariff, buy_price, fixed_charges
from ems.domain.units import DT_H, QUARTERS_PER_HOUR


@dataclass(frozen=True)
class SiteModel:
    location_id: str
    tariff: SiteTariff
    cohort: Cohort
    fixed_inputs: FixedInputs
    battery: BatteryParams
    rules: NetBillingRules


@dataclass(frozen=True)
class QuarterIn:
    t: int
    pv_kwh: float
    load_kwh: float  # uncontrolled: base load, heat pump, EV
    rce_pln_per_kwh: float


@dataclass(frozen=True)
class QuarterOut:
    t: int
    action: Action
    p_cmd_kw: float
    p_kw: float  # after clamping, > 0 charges
    soc_kwh: float  # at the end of q
    grid_kwh: float  # load - pv + p·Δt
    buy: BuyPrice
    basis: PriceBasis
    sell_signal_pln_per_kwh: float  # what the policy saw (lagged RCEm for an RCEm month)
    export_price_pln_per_kwh: float  # what the ledger uses (RCE_q or the month's own RCEm)


@dataclass(frozen=True)
class RefState:
    soc_kwh: float
    last_pv_kwh: float
    last_load_kwh: float
    acc: SettleAcc


@dataclass(frozen=True)
class RunMeta:
    """What a result rests on (P14)."""

    location_id: str
    cohort: Cohort
    versions: tuple[str, ...]  # operator, seller and quality-fee period ids, in order of first use
    rcem: tuple[tuple[str, str, date], ...]  # (month, source, publication date) used


@dataclass(frozen=True)
class RefResult:
    quarters: tuple[QuarterOut, ...]
    rows: tuple[HourRow, ...]
    months: tuple[MonthSettlement, ...]
    state: RefState
    meta: RunMeta | None = None


def initial_state(
    model: SiteModel, t0: int, soc_kwh: float, opening: Sequence[Tranche] = ()
) -> RefState:
    """The start of a run, checked once for both paths: an hour border, SoC within its bounds,
    and opening tranches usable in the first month.
    """
    if t0 % QUARTERS_PER_HOUR:
        raise ValueError(f"{model.location_id}: a run must start on a UTC hour")
    b = model.battery
    if not b.soc_min_kwh <= soc_kwh <= b.soc_max_kwh:
        raise ValueError(f"{model.location_id}: initial SoC {soc_kwh} kWh outside its bounds")
    try:
        check_opening(opening, civil_month(t0), model.rules)
    except ValueError as exc:
        raise ValueError(f"{model.location_id}: opening {exc}") from exc
    return RefState(soc_kwh, 0.0, 0.0, SettleAcc(t0, (), (), tuple(opening)))


def rcem_used(rcem: RcemTable, t: int) -> tuple[RcemEntry, Publication, RcemEntry]:
    """For an RCEm month: the entry and publication the signal uses (newest known at q) and the
    month's own entry the ledger uses (its latest publication).
    """
    signal_entry, signal_pub = rcem.signal(civil_date(t))
    return signal_entry, signal_pub, rcem.entry(civil_month(t))


@dataclass(frozen=True)
class Prices:
    """What one quarter-hour costs and pays a site."""

    buy: BuyPrice
    basis: PriceBasis
    signal: Signal  # what the policy sees (lagged RCEm for an RCEm month)
    export_price_pln_per_kwh: float  # what the ledger uses (RCE_q or the month's own RCEm)


def prices(model: SiteModel, rcem: RcemTable, t: int, rce_pln_per_kwh: float) -> Prices:
    buy = buy_price(model.tariff, t)
    basis = basis_for(model.cohort, civil_month(t))
    if basis is PriceBasis.RCE:
        signal_p = export_p = rce_pln_per_kwh
    else:
        _entry, pub, own = rcem_used(rcem, t)
        signal_p = pub.value_pln_per_kwh
        export_p = own.billing_value
    return Prices(buy, basis, price_signal(buy, signal_p, model.rules), export_p)


def decide(
    policy: Policy, pr: Prices, t: int, soc_kwh: float, last_pv_kwh: float, last_load_kwh: float
) -> Action:
    """The policy's action for ``t``, from what is known at its start."""
    obs = Observation(
        t, pr.signal.buy_pln_per_kwh, pr.signal.sell_pln_per_kwh, last_pv_kwh, last_load_kwh
    )
    return policy.decide(BatteryState(soc_kwh), obs)


def apply(
    battery: BatteryParams, soc_kwh: float, action: Action, pv_kwh: float, load_kwh: float
) -> tuple[float, float, float]:
    """``(p_cmd_kw, p_kw, soc_kwh)``: the command, the power the battery executes after clamping,
    and the SoC at the end of the quarter-hour.
    """
    cmd = resolve(action, pv_kwh, load_kwh, battery.p_max_kw)
    soc, p = step_params(soc_kwh, cmd, DT_H, battery)
    return cmd, p, soc


def quarter(
    model: SiteModel, policy: Policy, rcem: RcemTable, state: RefState, q: QuarterIn
) -> QuarterOut:
    """The physics of one quarter-hour, shared by ``run`` and ``stepper.step``."""
    pr = prices(model, rcem, q.t, q.rce_pln_per_kwh)
    action = decide(policy, pr, q.t, state.soc_kwh, state.last_pv_kwh, state.last_load_kwh)
    cmd, p, soc = apply(model.battery, state.soc_kwh, action, q.pv_kwh, q.load_kwh)
    grid = grid_kwh(q.pv_kwh, q.load_kwh, p)
    return QuarterOut(
        q.t,
        action,
        cmd,
        p,
        soc,
        grid,
        pr.buy,
        pr.basis,
        pr.signal.sell_pln_per_kwh,
        pr.export_price_pln_per_kwh,
    )


def settle_in(out: QuarterOut) -> SettleIn:
    return SettleIn(out.t, out.grid_kwh, out.buy, out.export_price_pln_per_kwh)


def run_meta(model: SiteModel, rcem: RcemTable, quarters: Sequence[QuarterOut]) -> RunMeta:
    versions: list[str] = []
    used: list[tuple[str, str, date]] = []
    for q in quarters:
        for v in (q.buy.operator_version, q.buy.seller_version, q.buy.quality_period):
            if v not in versions:
                versions.append(v)
        if q.basis is PriceBasis.RCEM:
            sig_entry, sig_pub, bill = rcem_used(rcem, q.t)
            for item in (
                (sig_entry.month, sig_entry.source, sig_pub.published),
                (bill.month, bill.source, bill.publications[-1].published),
            ):
                if item not in used:
                    used.append(item)
    return RunMeta(model.location_id, model.cohort, tuple(versions), tuple(sorted(used)))


def run(
    model: SiteModel,
    policy: Policy,
    rcem: RcemTable,
    state: RefState,
    inputs: Sequence[QuarterIn],
) -> RefResult:
    """Batch form: the quarter loop, then hour rows, then every month whose last hour is inside."""
    acc = state.acc
    if acc.hour:
        raise ValueError("a run must start on a UTC hour")
    if len(inputs) % QUARTERS_PER_HOUR:
        raise ValueError("a run must end on a UTC hour")
    quarters: list[QuarterOut] = []
    cur = state
    for i, q in enumerate(inputs):
        if q.t != acc.t + i:
            raise ValueError(f"quarter-hour {q.t} out of order: expected {acc.t + i}")
        out = quarter(model, policy, rcem, cur, q)
        quarters.append(out)
        cur = RefState(out.soc_kwh, q.pv_kwh, q.load_kwh, acc)
    rows = hour_rows([settle_in(o) for o in quarters])
    months: list[MonthSettlement] = []
    open_rows = list(acc.month_rows)
    tranches = acc.tranches
    for row in rows:
        open_rows.append(row)
        if hour_month(row.h + 1) != row.month:
            fixed = fixed_charges(model.tariff, row.month, model.fixed_inputs)
            basis = basis_for(model.cohort, row.month)
            settled = settle_month(row.month, open_rows, fixed, basis, model.rules, tranches)
            months.append(settled)
            tranches = settled.closing
            open_rows = []
    end = SettleAcc(acc.t + len(inputs), (), tuple(open_rows), tranches)
    final = RefState(cur.soc_kwh, cur.last_pv_kwh, cur.last_load_kwh, end)
    return RefResult(tuple(quarters), rows, tuple(months), final, run_meta(model, rcem, quarters))
