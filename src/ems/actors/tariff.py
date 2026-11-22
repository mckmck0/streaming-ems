"""Tariff: the price curves of one market (ADR 0012; key ``[run_id:]market_key``).

Fed by Kafka (``ems.price.v1``). It owns the curves, their visibility and the price watermark, and
pushes them to the households that subscribed: a snapshot of every accepted curve on subscribe, then
one update per accepted curve. One sequence number per market numbers both, so a household that
subscribes late still applies every curve once, in order. It keeps every curve of the run (A10).
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace

from restate import ObjectContext, ObjectSharedContext, VirtualObject
from restate.serde import BytesSerde

from ems.actors import shell
from ems.actors.messages import PriceUpdate, Subscribe
from ems.actors.serde import serde_for
from ems.actors.shell import Out, Send
from ems.assembler.mapping import Event, curve
from ems.assembler.sparse_streams import Curve, PriceStream, price_apply


@dataclass(frozen=True)
class TariffState:
    stream: PriceStream = field(default_factory=PriceStream)
    curves: tuple[Curve, ...] = ()
    subscribers: tuple[str, ...] = ()  # household keys
    unusable: int = 0  # records that are no usable RCE curve


def on_record(state: TariffState, event: Event | None) -> Out[TariffState]:
    c = curve(event)
    if c is None:
        return replace(state, unusable=state.unusable + 1), ()
    stream, accepted = price_apply(state.stream, c)
    if accepted is None:
        return replace(state, stream=stream), ()
    update = PriceUpdate(stream.covered, accepted.seq, False, (accepted,))
    sends = tuple(Send("Household", "on_prices", key, update) for key in state.subscribers)
    return replace(state, stream=stream, curves=(*state.curves, accepted)), sends


def subscribe(state: TariffState, msg: Subscribe) -> Out[TariffState]:
    """A new subscriber gets the snapshot; a known one gets nothing."""
    if msg.household_key in state.subscribers:
        return state, ()
    snapshot = PriceUpdate(state.stream.covered, state.stream.seq, True, state.curves)
    new = replace(state, subscribers=(*state.subscribers, msg.household_key))
    return new, (Send("Household", "on_prices", msg.household_key, snapshot),)


# ── Restate ──────────────────────────────────────────────────────────────────────────────────────

tariff = VirtualObject("Tariff")
STATE = serde_for(TariffState)


@tariff.handler(input_serde=BytesSerde(), accept="*/*")
async def on_price(ctx: ObjectContext, raw: bytes) -> None:
    """`ems.price.v1`, through `ingress-prices`."""
    await shell.ingest_market(ctx, raw, STATE, on_record, TariffState())


@tariff.handler(name="subscribe", input_serde=serde_for(Subscribe))
async def subscribe_handler(ctx: ObjectContext, msg: Subscribe) -> None:
    await shell.handle_market(ctx, STATE, lambda s: subscribe(s, msg), TariffState())


@tariff.handler(kind="shared", output_serde=STATE)
async def status(ctx: ObjectSharedContext) -> TariffState | None:
    return await ctx.get(shell.STATE, serde=STATE)
