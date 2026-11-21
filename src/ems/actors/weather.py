"""Weather: the forecast runs of one location (key ``[run_id:]location_id``).

Fed by Kafka (``ems.weather.forecast.v1``). It owns the runs, their visibility and the forecast
watermark, and sends each accepted run's issue time and visibility to the household of its
location (no payload in stage 3). A run of a model without a known delay or cadence is counted and
dropped, as if it were missing (ADR 0018). An unconfigured location keeps only the ingress log and
the counter.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace

from restate import ObjectContext, ObjectSharedContext, VirtualObject
from restate.serde import BytesSerde

from ems.actors import shell
from ems.actors.messages import ForecastUpdate
from ems.actors.serde import serde_for
from ems.actors.shell import Out, Send
from ems.assembler.mapping import Event, run
from ems.assembler.params import RunParams
from ems.assembler.sparse_streams import ForecastStream, forecast_apply


@dataclass(frozen=True)
class WeatherState:
    stream: ForecastStream = field(default_factory=ForecastStream)
    unusable: int = 0  # records that are no usable run (an unknown model among them)


def start(params: RunParams) -> Out[WeatherState]:
    return WeatherState(), ()


def on_record(params: RunParams, state: WeatherState, event: Event | None) -> Out[WeatherState]:
    r = run(event)
    if r is None:
        return replace(state, unusable=state.unusable + 1), ()
    stream, accepted = forecast_apply(state.stream, r)
    if accepted is None:
        return replace(state, stream=stream), ()
    update = ForecastUpdate(stream.covered, accepted.seq, accepted)
    return replace(state, stream=stream), (
        Send("Household", "on_forecast", params.household_key, update),
    )


# ── Restate ──────────────────────────────────────────────────────────────────────────────────────

weather = VirtualObject("Weather")
STATE = serde_for(WeatherState)


@weather.handler(input_serde=shell.PARAMS_SERDE)
async def configure(ctx: ObjectContext, params: RunParams) -> None:
    await shell.configure(ctx, params, start, STATE, kafka_fed=True)


@weather.handler(input_serde=BytesSerde(), accept="*/*")
async def on_forecast(ctx: ObjectContext, raw: bytes) -> None:
    """`ems.weather.forecast.v1`, through `ingress-forecasts`."""
    await shell.ingest(ctx, raw, STATE, on_record)


@weather.handler(kind="shared", output_serde=STATE)
async def status(ctx: ObjectSharedContext) -> WeatherState | None:
    return await ctx.get(shell.STATE, serde=STATE)
