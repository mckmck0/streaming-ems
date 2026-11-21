"""One Virtual Object service per strategy, keyed by household; its name is the policy's
(``POLICIES``).

A strategy runs its policy on its own virtual battery and keeps its shadow ledger and bills: the
counterfactual answer to "what would this household pay with this strategy in charge?". It applies
the household's steps in order, with their corrections, through the same function the offline side
uses (``offline.step_strategy``).
"""

from __future__ import annotations

from dataclasses import dataclass

from restate import ObjectContext, ObjectSharedContext, VirtualObject

from ems.actors import shell
from ems.actors.reorder import Inbox, in_order
from ems.actors.serde import serde_for
from ems.actors.shell import Out
from ems.assembler.join import Step
from ems.assembler.offline import StrategyResult, start_strategy, step_strategy
from ems.assembler.params import RunParams


@dataclass(frozen=True)
class StrategyState:
    result: StrategyResult  # virtual SoC, ledger, flows open to correction, closed months
    steps: Inbox[Step]


def start(params: RunParams) -> Out[StrategyState]:
    return StrategyState(start_strategy(params), Inbox(expected=params.first_step)), ()


def on_step(name: str, params: RunParams, state: StrategyState, step: Step) -> Out[StrategyState]:
    inbox, ready = in_order(state.steps, step.t, step)
    result = state.result
    for s in ready:
        result = step_strategy(params, name, result, s)
    return StrategyState(result, inbox), ()


# ── Restate ──────────────────────────────────────────────────────────────────────────────────────

STATE = serde_for(StrategyState)


def strategy_object(name: str) -> VirtualObject:
    obj = VirtualObject(name)

    @obj.handler(input_serde=shell.PARAMS_SERDE)
    async def configure(ctx: ObjectContext, params: RunParams) -> None:
        await shell.configure(ctx, params, start, STATE)

    @obj.handler(name="on_step", input_serde=serde_for(Step))
    async def on_step_handler(ctx: ObjectContext, step: Step) -> None:
        await shell.handle(ctx, STATE, lambda p, s: on_step(name, p, s, step))

    @obj.handler(kind="shared", output_serde=STATE)
    async def summary(ctx: ObjectSharedContext) -> StrategyState | None:
        return await ctx.get(shell.STATE, serde=STATE)

    return obj
