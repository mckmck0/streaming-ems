"""Meter: the real grid connection of a household (key: household).

It owns the grid energy of each step, the real ledger, the bills and the deposit. It derives a
step's grid energy from the flows ``Battery`` executed and settles it with the same function each
strategy uses (``stepper.settle_step``), corrections and month lag included, so with SelfConsumption
live its ledger equals that strategy's (the parity check).
"""

from __future__ import annotations

from dataclasses import dataclass

from restate import ObjectContext, ObjectSharedContext, VirtualObject

from ems.actors import shell
from ems.actors.messages import Flows
from ems.actors.reorder import Inbox, in_order
from ems.actors.serde import serde_for
from ems.actors.shell import Out
from ems.assembler.params import RunParams
from ems.domain.reference import initial_state, prices
from ems.domain.settlement import Flow, MonthSettlement, SettleAcc, SettleIn, grid_kwh
from ems.domain.stepper import settle_step


@dataclass(frozen=True)
class MeterState:
    acc: SettleAcc
    recent: tuple[Flow, ...]  # flows still open to correction
    months: tuple[MonthSettlement, ...]
    flows: Inbox[Flows]


def start(params: RunParams) -> Out[MeterState]:
    acc = initial_state(params.model, params.first_step, params.initial_soc_kwh).acc
    return MeterState(acc, (), (), Inbox(expected=params.first_step)), ()


def on_flows(params: RunParams, state: MeterState, flows: Flows) -> Out[MeterState]:
    inbox, ready = in_order(state.flows, flows.t, flows)
    acc, recent, months = state.acc, state.recent, state.months
    for f in ready:
        pr = prices(params.model, params.rcem, f.t, f.rce_pln_per_kwh)
        settle = SettleIn(
            f.t, grid_kwh(f.pv_kwh, f.load_kwh, f.p_kw), pr.buy, pr.export_price_pln_per_kwh
        )
        flow = Flow(f.pv_kwh, f.load_kwh, f.p_kw, settle)
        acc, recent, _, month = settle_step(
            params.model, acc, recent, flow, f.corrections, params.lag
        )
        if month is not None:
            months = (*months, month)
    return MeterState(acc, recent, months, inbox), ()


# ── Restate ──────────────────────────────────────────────────────────────────────────────────────

meter = VirtualObject("Meter")
STATE = serde_for(MeterState)


@meter.handler(input_serde=shell.PARAMS_SERDE)
async def configure(ctx: ObjectContext, params: RunParams) -> None:
    await shell.configure(ctx, params, start, STATE)


@meter.handler(name="on_flows", input_serde=serde_for(Flows))
async def on_flows_handler(ctx: ObjectContext, flows: Flows) -> None:
    await shell.handle(ctx, STATE, lambda p, s: on_flows(p, s, flows))


@meter.handler(kind="shared", output_serde=STATE)
async def bill(ctx: ObjectSharedContext) -> MeterState | None:
    return await ctx.get(shell.STATE, serde=STATE)
