"""Incremental form of the reference: one quarter-hour with carried state, as a strategy actor runs
it on its virtual battery. The physics is ``reference.quarter``; settlement is ``settle_step``,
which a ``Meter`` also calls on its own (``Battery`` owns the real SoC).

Late readings (spec 011, ADR 0018): a step may carry corrections of earlier quarter-hours. They
change the ledger only, never a decision, the executed power or the SoC. The flows of the hours
still open to correction travel beside the ``RefState`` (``recent``), and a civil month is settled
``lag`` quarter-hours after it ends, so no correction inside the lateness window is lost. With
``lag`` 0 and no corrections the results are those of spec 009.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from ems.domain.actions import Policy
from ems.domain.netbilling import RcemTable, basis_for
from ems.domain.reference import QuarterIn, QuarterOut, RefState, SiteModel, quarter, settle_in
from ems.domain.settlement import (
    Correction,
    Flow,
    HourRow,
    MonthSettlement,
    SettleAcc,
    add_quarter,
    close_month,
    correct,
    due_month,
)
from ems.domain.tariff import fixed_charges
from ems.domain.units import QUARTERS_PER_HOUR


@dataclass(frozen=True)
class StepOut:
    state: RefState
    quarter: QuarterOut
    row: HourRow | None  # set on the hour's fourth quarter
    month: MonthSettlement | None  # set when a civil month is settled
    recent: tuple[Flow, ...] = ()  # the flows still open to correction


def settle_step(
    model: SiteModel,
    acc: SettleAcc,
    recent: tuple[Flow, ...],
    flow: Flow,
    corrections: Sequence[Correction] = (),
    lag: int = 0,
) -> tuple[SettleAcc, tuple[Flow, ...], HourRow | None, MonthSettlement | None]:
    """Settle one quarter-hour: its corrections first, then the quarter, then a month whose lag has
    passed (with that month's own fixed charges and price basis), then drop the flows that can no
    longer be corrected.
    """
    for c in corrections:
        acc, recent, _ = correct(acc, recent, c)
    acc, row = add_quarter(acc, flow.settle)
    recent = (*recent, flow)
    month = None
    due = due_month(acc, lag)
    if due is not None:
        fixed = fixed_charges(model.tariff, due, model.fixed_inputs)
        acc, month = close_month(acc, fixed, basis_for(model.cohort, due), model.rules)
    keep_from = QUARTERS_PER_HOUR * ((acc.t - lag) // QUARTERS_PER_HOUR)
    return acc, tuple(f for f in recent if f.settle.t >= keep_from), row, month


def step(
    model: SiteModel,
    policy: Policy,
    rcem: RcemTable,
    state: RefState,
    q: QuarterIn,
    recent: tuple[Flow, ...] = (),
    corrections: Sequence[Correction] = (),
    lag: int = 0,
) -> StepOut:
    if q.t != state.acc.t:
        raise ValueError(f"quarter-hour {q.t} out of order: expected {state.acc.t}")
    out = quarter(model, policy, rcem, state, q)
    flow = Flow(q.pv_kwh, q.load_kwh, out.p_kw, settle_in(out))
    acc, recent, row, month = settle_step(model, state.acc, recent, flow, corrections, lag)
    return StepOut(RefState(out.soc_kwh, q.pv_kwh, q.load_kwh, acc), out, row, month, recent)
