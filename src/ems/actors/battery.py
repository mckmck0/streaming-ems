"""Battery: the real battery of a household (key: household; ADR 0014).

It owns the real SoC and the power executed at each step. It resolves the commanded action against
the step's flows and clamps it to its physical limits (P8) with the reference's own code
(``reference.apply``), then reports the new SoC to ``Ems`` and the step's flows to ``Meter``. Mode
actions resolve here because the step's measured flows arrive here (risk D6).
"""

from __future__ import annotations

from dataclasses import dataclass

from restate import ObjectContext, ObjectSharedContext, VirtualObject

from ems.actors import shell
from ems.actors.messages import Ack, Apply, Flows
from ems.actors.reorder import Inbox, in_order
from ems.actors.serde import serde_for
from ems.actors.shell import Out, Send
from ems.assembler.params import RunParams
from ems.domain.reference import apply


@dataclass(frozen=True)
class BatteryState:
    soc_kwh: float
    commands: Inbox[Apply]
    clamped: int = 0  # commands the battery could not execute in full


def start(params: RunParams) -> Out[BatteryState]:
    return BatteryState(params.initial_soc_kwh, Inbox(expected=params.first_step)), ()


def on_apply(params: RunParams, state: BatteryState, command: Apply) -> Out[BatteryState]:
    inbox, ready = in_order(state.commands, command.t, command)
    soc, clamped = state.soc_kwh, state.clamped
    sends: list[Send] = []
    key = params.household_key
    for c in ready:
        p_cmd, p, soc = apply(params.model.battery, soc, c.action, c.pv_kwh, c.load_kwh)
        clamped += int(p != p_cmd)
        sends.append(Send("Ems", "on_ack", key, Ack(c.t, soc)))
        flows = Flows(c.t, c.pv_kwh, c.load_kwh, p, c.rce_pln_per_kwh, c.corrections)
        sends.append(Send("Meter", "on_flows", key, flows))
    return BatteryState(soc, inbox, clamped), tuple(sends)


# ── Restate ──────────────────────────────────────────────────────────────────────────────────────

battery = VirtualObject("Battery")
STATE = serde_for(BatteryState)


@battery.handler(input_serde=shell.PARAMS_SERDE)
async def configure(ctx: ObjectContext, params: RunParams) -> None:
    await shell.configure(ctx, ("Battery", "configure"), params, start, STATE)


@battery.handler(name="apply", input_serde=serde_for(Apply))
async def apply_handler(ctx: ObjectContext, command: Apply) -> None:
    await shell.handle(
        ctx, ("Battery", "apply"), STATE, lambda p, s: on_apply(p, s, command), command.t
    )


@battery.handler(kind="shared", output_serde=STATE)
async def state(ctx: ObjectSharedContext) -> BatteryState | None:
    return await ctx.get(shell.STATE, serde=STATE)
