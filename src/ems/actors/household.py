"""Household: one site's join of its four input streams (ADR 0017; key ``[run_id:]location_id``).

Fed by Kafka (production and consumption), by ``Tariff`` (price curves) and by ``Weather`` (forecast
runs). It releases the steps the join allows and sends each one to every strategy of the run and to
``Ems``. A household that was never configured is a site the EMS does not manage: it keeps only the
ingress log and the record counter.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any

from restate import ObjectContext, ObjectSharedContext, VirtualObject
from restate.serde import BytesSerde

from ems.actors import ingress_check, shell
from ems.actors.messages import ForecastUpdate, PriceUpdate, Subscribe
from ems.actors.reorder import Inbox, begin, in_order
from ems.actors.serde import serde_for
from ems.actors.shell import Out, Send
from ems.assembler import join
from ems.assembler.join import Join, Step
from ems.assembler.mapping import Event, reading
from ems.assembler.params import RunParams

PRICE, FORECAST = "price", "weather_forecast"  # stream names of the leads, as on the wire


@dataclass(frozen=True)
class HouseholdState:
    join: Join
    # Numbered by Tariff; the snapshot sets the start.
    prices: Inbox[PriceUpdate] = field(default_factory=Inbox)
    # Weather numbers its runs from 1.
    forecasts: Inbox[ForecastUpdate] = field(default_factory=lambda: Inbox(expected=1))
    # How far each stream runs ahead of the others (measured, never compared: arrival order).
    leads: dict[str, Any] = field(default_factory=ingress_check.new_leads, compare=False)


def start(params: RunParams) -> Out[HouseholdState]:
    subscribe = Subscribe(params.first_step, params.household_key)
    return HouseholdState(join.start(params)), (
        Send("Tariff", "subscribe", params.market_key, subscribe),
    )


def on_record(params: RunParams, state: HouseholdState, event: Event | None) -> Out[HouseholdState]:
    r = reading(event)
    j, steps = join.on_reading(params, state.join, r)
    leads = state.leads if r is None else ingress_check.lead_step(state.leads, r.stream, r.t + 1)
    return replace(state, join=j, leads=leads), _route(params, steps)


def on_prices(params: RunParams, state: HouseholdState, update: PriceUpdate) -> Out[HouseholdState]:
    j, steps = state.join, ()
    if update.snapshot and state.prices.expected is None:
        j, steps = join.on_curves(params, j, update.t, update.curves)
        inbox, ready = begin(state.prices, update.seq + 1)
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
    return replace(state, join=j, prices=inbox, leads=leads), _route(params, steps)


def on_forecast(
    params: RunParams, state: HouseholdState, update: ForecastUpdate
) -> Out[HouseholdState]:
    inbox, ready = in_order(state.forecasts, update.seq, update)
    j, steps = state.join, ()
    for u in ready:
        j, out = join.on_run(params, j, u.t, u.run)
        steps += out
    leads = ingress_check.lead_step(state.leads, FORECAST, update.run.visible_step)
    return replace(state, join=j, forecasts=inbox, leads=leads), _route(params, steps)


def _route(params: RunParams, steps: tuple[Step, ...]) -> tuple[Send, ...]:
    """Every released step to each strategy of the run and to Ems, by service name."""
    targets = (*params.strategies, "Ems")
    return tuple(Send(t, "on_step", params.household_key, s) for s in steps for t in targets)


# ── Restate ──────────────────────────────────────────────────────────────────────────────────────

household = VirtualObject("Household")
STATE = serde_for(HouseholdState)


@household.handler(input_serde=shell.PARAMS_SERDE)
async def configure(ctx: ObjectContext, params: RunParams) -> None:
    await shell.configure(ctx, params, start, STATE, kafka_fed=True)


@household.handler(input_serde=BytesSerde(), accept="*/*")
async def on_measurement(ctx: ObjectContext, raw: bytes) -> None:
    """`ems.production.v1` and `ems.consumption.v1`, through `ingress-meters`."""
    await shell.ingest(ctx, raw, STATE, on_record)


@household.handler(name="on_prices", input_serde=serde_for(PriceUpdate))
async def on_prices_handler(ctx: ObjectContext, update: PriceUpdate) -> None:
    await shell.handle(ctx, STATE, lambda p, s: on_prices(p, s, update))


@household.handler(name="on_forecast", input_serde=serde_for(ForecastUpdate))
async def on_forecast_handler(ctx: ObjectContext, update: ForecastUpdate) -> None:
    await shell.handle(ctx, STATE, lambda p, s: on_forecast(p, s, update))


@household.handler(kind="shared", output_serde=STATE)
async def status(ctx: ObjectSharedContext) -> HouseholdState | None:
    return await ctx.get(shell.STATE, serde=STATE)
