"""Household: one site's join of its four input streams (ADR 0017; key ``[run_id:]location_id``).

Fed by Kafka (production and consumption), by ``Tariff`` (price curves) and by ``Weather`` (forecast
runs). It releases the steps the join allows and sends each one to every strategy of the run and to
``Ems``. A household that was never configured is a site the EMS does not manage: it keeps only the
ingress log and the record counter.

Spec 012 adds two things. A strategy schedule (``set_strategies``, ADR 0021): from an hour ``t`` the
household feeds exactly the set the operator named, each released step routed by its own ``t``; a
change that cannot take effect exactly at ``t`` is refused and recorded. And, in live mode, a
deadline chain (ADR 0020): a delayed self-send per quarter-hour that applies the wall clock
(``join.on_clock``), started once the price snapshot is held, carrying the step it was set for as
its generation (R6).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field, replace
from typing import Any

from restate import ObjectContext, ObjectSharedContext, VirtualObject
from restate.serde import BytesSerde

from ems.actors import ingress_check, shell
from ems.actors.messages import Deadline, ForecastUpdate, PriceUpdate, StrategyChange, Subscribe
from ems.actors.reorder import Inbox, begin, in_order
from ems.actors.serde import serde_for
from ems.actors.shell import Out, Send
from ems.assembler import join
from ems.assembler.join import Join, Step
from ems.assembler.mapping import Event, reading
from ems.assembler.params import RunParams
from ems.assembler.schedule import Schedule, accept, strategies_at

PRICE, FORECAST = "price", "weather_forecast"  # stream names of the leads, as on the wire
QUARTER_S = 900
MARGIN_S = 1.0  # a deadline fires this long after its moment (the VM and host clocks may differ)
MIN_DELAY_S = 1.0  # an early firing waits at least this long, so a clock lead cannot loop


@dataclass(frozen=True)
class Clock:
    """The live deadline chain: the step the pending deadline was set for, and counters."""

    expected: int | None = None  # None: no chain (sim, or not started yet, or stopped)
    stopped: bool = False
    firings: int = 0
    early: int = 0  # firings that came before their moment and rescheduled the same step
    stale: int = 0  # deadlines for another step than the expected one


@dataclass(frozen=True)
class HouseholdState:
    join: Join
    # Numbered by Tariff; the snapshot sets the start.
    prices: Inbox[PriceUpdate] = field(default_factory=Inbox)
    # Weather numbers its runs from 1.
    forecasts: Inbox[ForecastUpdate] = field(default_factory=lambda: Inbox(expected=1))
    # How far each stream runs ahead of the others (measured, never compared: arrival order).
    leads: dict[str, Any] = field(default_factory=ingress_check.new_leads, compare=False)
    schedule: Schedule = field(default_factory=Schedule)
    clock: Clock = field(default_factory=Clock)


def start(params: RunParams) -> Out[HouseholdState]:
    subscribe = Subscribe(params.first_step, params.household_key)
    return HouseholdState(join.start(params)), (
        Send("Tariff", "subscribe", params.market_key, subscribe),
    )


def on_record(params: RunParams, state: HouseholdState, event: Event | None) -> Out[HouseholdState]:
    r = reading(event)
    j, steps = join.on_reading(params, state.join, r)
    leads = state.leads if r is None else ingress_check.lead_step(state.leads, r.stream, r.t + 1)
    return replace(state, join=j, leads=leads), _route(params, state.schedule, steps)


def on_prices(params: RunParams, state: HouseholdState, update: PriceUpdate) -> Out[HouseholdState]:
    j, steps = state.join, ()
    clock, starts = state.clock, ()
    if update.snapshot and state.prices.expected is None:
        j, steps = join.on_curves(params, j, update.t, update.curves)
        inbox, ready = begin(state.prices, update.seq + 1)
        if params.mode == "live" and clock.expected is None and not clock.stopped:
            # The chain starts once the curves are held, so a first firing cannot raise the price
            # watermark past curves the household has not seen yet.
            clock = replace(clock, expected=params.first_step)
            first = Deadline(params.first_step)
            starts = (Send("Household", "on_deadline", params.household_key, first),)
    elif update.snapshot:  # cannot happen: Tariff sends one snapshot per subscriber
        inbox, ready = replace(state.prices, stale=state.prices.stale + 1), ()
    else:
        inbox, ready = in_order(state.prices, update.seq, update)
    for u in ready:
        j, out = join.on_curves(params, j, u.t, u.curves)
        steps += out
    leads = state.leads
    if update.curves:
        slot = max(c.visible_step for c in update.curves)
        leads = ingress_check.lead_step(leads, PRICE, slot)
    new = replace(state, join=j, prices=inbox, leads=leads, clock=clock)
    return new, (*_route(params, state.schedule, steps), *starts)


def on_forecast(
    params: RunParams, state: HouseholdState, update: ForecastUpdate
) -> Out[HouseholdState]:
    inbox, ready = in_order(state.forecasts, update.seq, update)
    j, steps = state.join, ()
    for u in ready:
        j, out = join.on_run(params, j, u.t, u.run)
        steps += out
    leads = ingress_check.lead_step(state.leads, FORECAST, update.run.visible_step)
    return replace(state, join=j, forecasts=inbox, leads=leads), _route(
        params, state.schedule, steps
    )


def on_deadline(
    params: RunParams, state: HouseholdState, d: Deadline, now_s: float
) -> Out[HouseholdState]:
    """Live mode: apply the wall clock. Every step that ended at least ``grace_s`` ago is closed (a
    late timer releases them all at once), then the next deadline is set for when the clock step
    next increases. A deadline for another step than the expected one is stale.
    """
    clock = state.clock
    if d.t != clock.expected:
        return replace(state, clock=replace(clock, stale=clock.stale + 1)), ()
    c = math.floor((now_s - params.grace_s) / QUARTER_S)
    k = min(c, params.end_step)
    j, steps = join.on_clock(params, state.join, k)
    routed = _route(params, state.schedule, steps)
    if k >= params.end_step:
        done = replace(clock, expected=None, stopped=True, firings=clock.firings + 1)
        return replace(state, join=j, clock=done), routed
    nxt = max(k, params.first_step) + 1
    delay = max(QUARTER_S * nxt + params.grace_s + MARGIN_S - now_s, MIN_DELAY_S)
    clock = replace(
        clock, expected=nxt, firings=clock.firings + 1, early=clock.early + (nxt == d.t)
    )
    again = Send("Household", "on_deadline", params.household_key, Deadline(nxt), delay_s=delay)
    return replace(state, join=j, clock=clock), (*routed, again)


def on_change(
    params: RunParams, state: HouseholdState, change: StrategyChange
) -> Out[HouseholdState]:
    """The operator's strategy change: added to the schedule, or refused with its reason."""
    schedule = accept(params, state.schedule, state.join.next, change.t, change.strategies)
    return replace(state, schedule=schedule), ()


def _route(params: RunParams, schedule: Schedule, steps: tuple[Step, ...]) -> tuple[Send, ...]:
    """Every released step to the strategies in force at its own ``t`` and to Ems, by service
    name.
    """
    return tuple(
        Send(target, "on_step", params.household_key, s)
        for s in steps
        for target in (*strategies_at(params, schedule, s.t), "Ems")
    )


# ── Restate ──────────────────────────────────────────────────────────────────────────────────────

household = VirtualObject("Household")
STATE = serde_for(HouseholdState)


@household.handler(input_serde=shell.PARAMS_SERDE)
async def configure(ctx: ObjectContext, params: RunParams) -> None:
    await shell.configure(ctx, ("Household", "configure"), params, start, STATE, kafka_fed=True)


@household.handler(input_serde=BytesSerde(), accept="*/*")
async def on_measurement(ctx: ObjectContext, raw: bytes) -> None:
    """`ems.production.v1` and `ems.consumption.v1`, through `ingress-meters`."""
    await shell.ingest(ctx, ("Household", "on_measurement"), raw, STATE, on_record)


@household.handler(name="on_prices", input_serde=serde_for(PriceUpdate))
async def on_prices_handler(ctx: ObjectContext, update: PriceUpdate) -> None:
    names = ("Household", "on_prices")
    await shell.handle(ctx, names, STATE, lambda p, s: on_prices(p, s, update), update.t)


@household.handler(name="on_forecast", input_serde=serde_for(ForecastUpdate))
async def on_forecast_handler(ctx: ObjectContext, update: ForecastUpdate) -> None:
    names = ("Household", "on_forecast")
    await shell.handle(ctx, names, STATE, lambda p, s: on_forecast(p, s, update), update.t)


@household.handler(name="on_deadline", input_serde=serde_for(Deadline))
async def on_deadline_handler(ctx: ObjectContext, d: Deadline) -> None:
    """Live mode's delayed self-send; it reads the journaled wall time."""
    names = ("Household", "on_deadline")
    await shell.handle_timed(ctx, names, STATE, lambda p, s, now: on_deadline(p, s, d, now), d.t)


@household.handler(name="set_strategies", input_serde=serde_for(StrategyChange))
async def set_strategies(ctx: ObjectContext, change: StrategyChange) -> None:
    """The operator's strategy change (HTTP ingress, with an ``Idempotency-Key``)."""
    names = ("Household", "set_strategies")
    await shell.handle(ctx, names, STATE, lambda p, s: on_change(p, s, change), change.t)


@household.handler(kind="shared", output_serde=STATE)
async def status(ctx: ObjectSharedContext) -> HouseholdState | None:
    return await ctx.get(shell.STATE, serde=STATE)
