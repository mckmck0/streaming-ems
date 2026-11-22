"""Ems: decides the real battery's action for each step with the live strategy (key: household).

It decides step ``t`` only once ``Battery`` has acknowledged ``t - 1``, from the SoC that
acknowledgement reports, so its view of the real battery is never stale. The decision is the
reference's own code (``reference.prices`` and ``reference.decide``), so with SelfConsumption live
the real battery follows that strategy's virtual one exactly (the parity check).
"""

from __future__ import annotations

from dataclasses import dataclass, replace

from restate import ObjectContext, ObjectSharedContext, VirtualObject

from ems.actors import shell
from ems.actors.messages import Ack, Apply
from ems.actors.reorder import Inbox, in_order
from ems.actors.serde import serde_for
from ems.actors.shell import Out, Send
from ems.assembler.join import Step
from ems.assembler.params import RunParams
from ems.domain.actions import POLICIES
from ems.domain.reference import decide, prices


@dataclass(frozen=True)
class EmsState:
    soc_kwh: float  # the real SoC from the latest acknowledgement
    last_pv_kwh: float  # the flows of the last decided step: the next observation
    last_load_kwh: float
    sent: int  # the last step commanded
    steps: Inbox[Step]
    acks: Inbox[Ack]  # ``acks.expected`` is the step whose acknowledgement comes next
    waiting: tuple[Step, ...] = ()  # released steps not yet decided


def start(params: RunParams) -> Out[EmsState]:
    first = params.first_step
    return EmsState(
        params.initial_soc_kwh, 0.0, 0.0, first - 1, Inbox(expected=first), Inbox(expected=first)
    ), ()


def on_step(params: RunParams, state: EmsState, step: Step) -> Out[EmsState]:
    inbox, ready = in_order(state.steps, step.t, step)
    return _decide(params, replace(state, steps=inbox, waiting=(*state.waiting, *ready)))


def on_ack(params: RunParams, state: EmsState, ack: Ack) -> Out[EmsState]:
    inbox, ready = in_order(state.acks, ack.t, ack)
    soc = ready[-1].soc_kwh if ready else state.soc_kwh
    return _decide(params, replace(state, acks=inbox, soc_kwh=soc))


def _decide(params: RunParams, state: EmsState) -> Out[EmsState]:
    """Command the next step once every earlier one is acknowledged (one command in flight)."""
    if not state.waiting:
        return state, ()
    s = state.waiting[0]
    if s.t != state.acks.expected or s.t <= state.sent:
        return state, ()
    pr = prices(params.model, params.rcem, s.t, s.rce_pln_per_kwh)
    action = decide(
        POLICIES[params.live], pr, s.t, state.soc_kwh, state.last_pv_kwh, state.last_load_kwh
    )
    command = Apply(s.t, action, s.pv_kwh, s.load_kwh, s.rce_pln_per_kwh, s.corrections)
    new = replace(
        state,
        sent=s.t,
        last_pv_kwh=s.pv_kwh,
        last_load_kwh=s.load_kwh,
        waiting=state.waiting[1:],
    )
    return new, (Send("Battery", "apply", params.household_key, command),)


# ── Restate ──────────────────────────────────────────────────────────────────────────────────────

ems = VirtualObject("Ems")
STATE = serde_for(EmsState)


@ems.handler(input_serde=shell.PARAMS_SERDE)
async def configure(ctx: ObjectContext, params: RunParams) -> None:
    await shell.configure(ctx, params, start, STATE)


@ems.handler(name="on_step", input_serde=serde_for(Step))
async def on_step_handler(ctx: ObjectContext, step: Step) -> None:
    await shell.handle(ctx, STATE, lambda p, s: on_step(p, s, step))


@ems.handler(name="on_ack", input_serde=serde_for(Ack))
async def on_ack_handler(ctx: ObjectContext, ack: Ack) -> None:
    await shell.handle(ctx, STATE, lambda p, s: on_ack(p, s, ack))


@ems.handler(kind="shared", output_serde=STATE)
async def status(ctx: ObjectSharedContext) -> EmsState | None:
    return await ctx.get(shell.STATE, serde=STATE)
