"""The one shape of every handler: read own state, call a pure transition, write own state, send.

The transitions live in the actor files and import no Restate; this module is the only place that
touches the context. A transition returns its new state and the messages to send, as ``Send`` values
addressed by service name, so a household can route steps to the strategies named in its parameters
and no actor file imports another.

State keys of an object: ``params`` (its ``RunParams``), ``state`` (one dataclass), and on Kafka-fed
objects ``log`` (the ingress log, compared with the offline fold), ``n`` (records handled, written
last, so a progress query never counts a record whose effects are not written) and, once configured,
``n_at_configure`` (records that arrived before ``configure``).

``Tariff`` is keyed by market and never configured: its handlers start from a default state.

Spec 012: every entry point takes the ``(service, handler)`` names and, after its writes, marks the
commit in the timing log (``ems.actors.timing``, off unless configured), which touches no context.
``handle_timed`` is the one entry point that reads the journaled time, for the live deadline, and
``Send.delay_s`` makes a delayed one-way send.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from restate import ObjectContext, TerminalError

from ems.actors import ingress_check, timing
from ems.actors.serde import DataclassSerde, serde_for
from ems.assembler.mapping import Event, parse
from ems.assembler.params import RunParams

PARAMS, STATE, LOG, N, N_AT_CONFIGURE = "params", "state", "log", "n", "n_at_configure"
PARAMS_SERDE = serde_for(RunParams)


@dataclass(frozen=True)
class Send:
    """A one-way message to the handler of the object with this key."""

    service: str
    handler: str
    key: str
    message: Any  # a message dataclass
    delay_s: float | None = None  # a delayed send (the live deadline); None sends at once


type Out[S] = tuple[S, tuple[Send, ...]]
type Names = tuple[str, str]  # (service, handler), for the timing log


async def configure[S](
    ctx: ObjectContext,
    names: Names,
    params: RunParams,
    start: Callable[[RunParams], Out[S]],
    state_serde: DataclassSerde[S],
    *,
    kafka_fed: bool = False,
) -> None:
    """Store a run's parameters once (ADR 0019). An equal second call changes nothing; other
    parameters, or parameters for another key, fail the call without a retry. A Kafka-fed object
    also stores how many records it had received before (the harness checks that it is 0).
    """
    if params.household_key != ctx.key():
        raise TerminalError(f"{ctx.key()}: parameters are for {params.household_key}")
    stored = await ctx.get(PARAMS, serde=PARAMS_SERDE)
    if stored is not None:
        if stored != params:
            raise TerminalError(f"{ctx.key()}: already configured with other parameters")
        return
    ctx.set(PARAMS, params, serde=PARAMS_SERDE)
    if kafka_fed:
        ctx.set(N_AT_CONFIGURE, await ctx.get(N) or 0)
    sends = _commit(ctx, state_serde, start(params))
    _mark(ctx, names, params.first_step, sends)


async def handle[S](
    ctx: ObjectContext,
    names: Names,
    state_serde: DataclassSerde[S],
    transition: Callable[[RunParams, S], Out[S]],
    in_t: int | None = None,
) -> None:
    """A message from another actor (or the operator) to a configured object. Without parameters
    the call fails without a retry, and the key goes on with its next invocation.
    """
    params = await ctx.get(PARAMS, serde=PARAMS_SERDE)
    state = await ctx.get(STATE, serde=state_serde)
    if params is None or state is None:
        raise TerminalError(f"{ctx.key()}: not configured")
    sends = _commit(ctx, state_serde, transition(params, state))
    _mark(ctx, names, in_t, sends)


async def handle_timed[S](
    ctx: ObjectContext,
    names: Names,
    state_serde: DataclassSerde[S],
    transition: Callable[[RunParams, S, float], Out[S]],
    in_t: int | None = None,
) -> None:
    """``handle`` for the live deadline: the transition also gets the journaled wall time, so a
    retried invocation sees the same time and writes the same bytes (R3).
    """
    params = await ctx.get(PARAMS, serde=PARAMS_SERDE)
    state = await ctx.get(STATE, serde=state_serde)
    if params is None or state is None:
        raise TerminalError(f"{ctx.key()}: not configured")
    now = await ctx.time()
    sends = _commit(ctx, state_serde, transition(params, state, now))
    _mark(ctx, names, in_t, sends)


async def handle_market[S](
    ctx: ObjectContext,
    names: Names,
    state_serde: DataclassSerde[S],
    transition: Callable[[S], Out[S]],
    default: S,
    in_t: int | None = None,
) -> None:
    """A message to a market-keyed object (no parameters)."""
    state = await ctx.get(STATE, serde=state_serde) or default
    sends = _commit(ctx, state_serde, transition(state))
    _mark(ctx, names, in_t, sends)


async def ingest[S](
    ctx: ObjectContext,
    names: Names,
    raw: bytes,
    state_serde: DataclassSerde[S],
    transition: Callable[[RunParams, S, Event | None], Out[S]],
) -> None:
    """A Kafka record for a location. The ingress log is always kept; the transition runs only once
    the object is configured. Never raises on the content of a record.
    """
    event = await _log(ctx, raw)
    params = await ctx.get(PARAMS, serde=PARAMS_SERDE)
    state = await ctx.get(STATE, serde=state_serde)
    sends: tuple[Send, ...] | None = None
    if params is not None and state is not None:
        sends = _commit(ctx, state_serde, transition(params, state, event))
    await _count(ctx)
    if sends is not None:  # an unmanaged site is not marked
        _mark(ctx, names, None, sends)


async def ingest_market[S](
    ctx: ObjectContext,
    names: Names,
    raw: bytes,
    state_serde: DataclassSerde[S],
    transition: Callable[[S, Event | None], Out[S]],
    default: S,
) -> None:
    """A Kafka record for a market. Never raises on the content of a record."""
    event = await _log(ctx, raw)
    state = await ctx.get(STATE, serde=state_serde) or default
    sends = _commit(ctx, state_serde, transition(state, event))
    await _count(ctx)
    _mark(ctx, names, None, sends)


async def _log(ctx: ObjectContext, raw: bytes) -> Event | None:
    meta = ingress_check.kafka_meta(ctx.request().headers)
    event = parse(raw or b"")
    log = await ctx.get(LOG) or ingress_check.new_log()
    # As ``log_apply``: a record without its Kafka coordinates is counted as rejected anyway.
    ctx.set(LOG, ingress_check.log_step(log, meta, meta is None or event is not None))
    return event


async def _count(ctx: ObjectContext) -> None:
    ctx.set(N, (await ctx.get(N) or 0) + 1)


def _commit[S](ctx: ObjectContext, state_serde: DataclassSerde[S], out: Out[S]) -> tuple[Send, ...]:
    """Write the state, then send. Every message is encoded first, so a value the serde refuses
    (AC-012-40) fails the invocation before anything is stored or sent.
    """
    state, sends = out
    args = [serde_for(type(s.message)).serialize(s.message) for s in sends]
    ctx.set(STATE, state, serde=state_serde)
    for s, arg in zip(sends, args, strict=True):
        if s.delay_s is not None:
            delay = timedelta(seconds=s.delay_s)
            ctx.generic_send(s.service, s.handler, arg=arg, key=s.key, send_delay=delay)
        else:
            ctx.generic_send(s.service, s.handler, arg=arg, key=s.key)
    return sends


def _mark(ctx: ObjectContext, names: Names, in_t: int | None, sends: tuple[Send, ...]) -> None:
    timing.mark(names, ctx.key(), ctx.request().id, in_t, sends)
