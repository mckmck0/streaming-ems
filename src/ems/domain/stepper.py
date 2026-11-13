"""Incremental form of the reference: one quarter-hour with carried state, as a ``Strategy_*`` actor
runs it on its virtual battery. The physics is ``reference.quarter``; settlement is
``settlement.settle_quarter``, which a ``Meter`` calls on its own (``Battery`` owns the real SoC).
"""

from __future__ import annotations

from dataclasses import dataclass

from ems.domain.actions import Policy
from ems.domain.calendar import civil_month
from ems.domain.netbilling import RcemTable, basis_for
from ems.domain.reference import QuarterIn, QuarterOut, RefState, SiteModel, quarter, settle_in
from ems.domain.settlement import HourRow, MonthSettlement, settle_quarter
from ems.domain.tariff import fixed_charges


@dataclass(frozen=True)
class StepOut:
    state: RefState
    quarter: QuarterOut
    row: HourRow | None  # set on the hour's fourth quarter
    month: MonthSettlement | None  # set on the last quarter of a civil month


def step(
    model: SiteModel, policy: Policy, rcem: RcemTable, state: RefState, q: QuarterIn
) -> StepOut:
    if q.t != state.acc.t:
        raise ValueError(f"quarter-hour {q.t} out of order: expected {state.acc.t}")
    out = quarter(model, policy, rcem, state, q)
    month = civil_month(q.t)
    acc, row, settled = settle_quarter(
        state.acc,
        settle_in(out),
        fixed_charges(model.tariff, month, model.fixed_inputs),
        basis_for(model.cohort, month),
        model.rules,
    )
    return StepOut(RefState(out.soc_kwh, q.pv_kwh, q.load_kwh, acc), out, row, settled)
