"""Wire events into the assembler's inputs: the one mapping the actors and the offline side share.

Every function is total: anything it cannot use (a record that does not parse, another resolution,
a unit or market without a rule, a value beyond the bounds) gives None, and the caller counts it. A
Kafka-fed handler that raised would be retried and block its key.

Times become integer steps: a record may be read by the steps from ``ceil(available_at / 900 s)``
on, and a watermark covers the steps below ``floor(watermark / 900 s)``.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime

from ems.contracts.availability import forecast_cadence, forecast_delay, price_window_opens
from ems.contracts.envelope import (
    MARKET_KEYS,
    Aggregation,
    AssetType,
    Direction,
    MeasurementEvent,
    PriceEvent,
    Quality,
    Quantity,
    Resolution,
    Stream,
    WeatherForecastEvent,
    parse_event,
)
from ems.contracts.time import QUARTER_S
from ems.domain.units import pln_per_mwh_to_pln_per_kwh

PRODUCTION, CONSUMPTION = "production", "consumption"
# Far above any physical or market value, low enough that no sum can overflow.
MAX_ENTRY_KWH = 1e4
MAX_PRICE_PLN_PER_MWH = 1e5

_PRODUCTION = ((AssetType.PV,), Direction.GENERATION)
_CONSUMPTION = ((AssetType.LOAD, AssetType.HEAT_PUMP, AssetType.EV_CHARGER), Direction.CONSUMPTION)

Event = MeasurementEvent | WeatherForecastEvent | PriceEvent


@dataclass(frozen=True)
class Reading:
    """One meter record: the energy of one quarter-hour of one stream."""

    stream: str  # PRODUCTION or CONSUMPTION
    t: int
    event_id: str
    kwh: float
    skipped: int  # entries of a wanted asset type that were not usable energy


@dataclass(frozen=True)
class CurveIn:
    """One published price curve of a delivery day."""

    event_id: str
    t0: int  # the day's first quarter-hour
    day: date  # the local delivery day
    rce_pln_per_kwh: tuple[float, ...]
    issued_s: int  # publication time, unix seconds
    issued_step: int  # the first step that may read it
    opens_step: int  # the watermark the curve sets: the next publication window opens there


@dataclass(frozen=True)
class RunIn:
    """One forecast run (its payload reaches a policy only from stage 5)."""

    issue_s: int
    usable_step: int  # the first step that may read it
    next_covered: int  # the watermark it sets: the next run cannot be usable earlier


def parse(raw: bytes) -> Event | None:
    try:
        return parse_event(raw)
    except Exception:  # invalid JSON, a contract violation, a date out of range
        return None


def _ceil_step(dt: datetime) -> int:
    return -(-math.ceil(dt.timestamp()) // QUARTER_S)


def _floor_step(dt: datetime) -> int:
    return math.floor(dt.timestamp()) // QUARTER_S


def reading(event: Event | None) -> Reading | None:
    try:
        return _reading(event)
    except Exception:
        return None


def _reading(event: Event | None) -> Reading | None:
    if not isinstance(event, MeasurementEvent) or event.resolution is not Resolution.PT15M:
        return None
    if event.stream is Stream.PRODUCTION:
        stream, (types, direction) = PRODUCTION, _PRODUCTION
    else:
        stream, (types, direction) = CONSUMPTION, _CONSUMPTION
    values: list[float] = []
    skipped = 0
    for m in event.payload.measurements:
        if m.value > MAX_ENTRY_KWH:
            return None
        if m.asset_type not in types or m.direction is not direction:
            continue
        usable = (
            m.quantity is Quantity.ACTIVE_ENERGY
            and m.aggregation is Aggregation.INTERVAL_SUM
            and m.unit == "kWh"
            and m.quality is not Quality.MISSING
        )
        if usable:
            values.append(m.value)
        else:
            skipped += 1
    return Reading(stream, event.t, str(event.event_id), math.fsum(values), skipped)


def curve(event: Event | None) -> CurveIn | None:
    try:
        return _curve(event)
    except Exception:  # a market without a publication window, a date out of range
        return None


def _curve(event: Event | None) -> CurveIn | None:
    if not isinstance(event, PriceEvent) or event.issued_at is None:
        return None
    p = event.payload
    if p.unit != "PLN/MWh" or p.resolution is not Resolution.PT15M:
        return None
    if any(abs(s.price) > MAX_PRICE_PLN_PER_MWH for s in p.slots):
        return None
    opens = price_window_opens(MARKET_KEYS[p.market], p.delivery_day)
    return CurveIn(
        event_id=str(event.event_id),
        t0=event.t,
        day=p.delivery_day,
        rce_pln_per_kwh=tuple(pln_per_mwh_to_pln_per_kwh(s.price) for s in p.slots),
        issued_s=math.floor(event.issued_at.timestamp()),
        issued_step=_ceil_step(event.issued_at),
        opens_step=_floor_step(opens),
    )


def run(event: Event | None) -> RunIn | None:
    try:
        return _run(event)
    except Exception:  # a model without a delay or a cadence, a date out of range
        return None


def _run(event: Event | None) -> RunIn | None:
    if not isinstance(event, WeatherForecastEvent) or event.issued_at is None:
        return None
    model = event.payload.forecast_model
    available = event.issued_at + forecast_delay(model)
    return RunIn(
        issue_s=math.floor(event.issued_at.timestamp()),
        usable_step=_ceil_step(available),
        next_covered=_floor_step(available + forecast_cadence(model)),
    )
