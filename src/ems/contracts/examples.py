"""Builders for contract-valid events, used by the generator, tests and the golden examples."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from typing import Any, Literal

from ems.contracts.envelope import (
    MAJOR,
    MARKET_KEYS,
    Aggregation,
    AssetType,
    Direction,
    ForecastPayload,
    ForecastStep,
    Market,
    Measurement,
    MeasurementEvent,
    MeasurementPayload,
    PriceEvent,
    PricePayload,
    PriceSlot,
    Quality,
    Quantity,
    Resolution,
    Stream,
    WeatherForecastEvent,
)
from ems.contracts.ids import event_id, make_gsrn
from ems.contracts.time import (
    QUARTER,
    iso_duration,
    local_day_bounds,
    local_day_slots,
    quarter_index,
)

GOLDEN_INGESTED_AT = datetime(2026, 10, 20, 0, 15, 5, tzinfo=UTC)


def measurement_event(
    *,
    stream: Stream,
    location_id: str,
    interval_start: datetime,
    measurements: Sequence[Measurement],
    ingested_at: datetime,
    source_system: str = "synthetic",
    run_id: str | None = None,
    metering_point_id: str | None = None,
    seed: int | None = None,
    synthetic: bool = True,
) -> MeasurementEvent:
    return MeasurementEvent(
        event_id=event_id(
            stream=stream.value,
            subject=location_id,
            interval_start=interval_start,
            issued_at=None,
            source_system=source_system,
            run_id=run_id,
            major=MAJOR,
        ),
        stream=stream,  # ty: ignore[invalid-argument-type]
        source_system=source_system,
        location_id=location_id,
        metering_point_id=metering_point_id,
        run_id=run_id,
        interval_start=interval_start,
        interval_end=interval_start + QUARTER,
        resolution=Resolution.PT15M,
        t=quarter_index(interval_start),
        ingested_at=ingested_at,
        synthetic=synthetic,
        seed=seed,
        payload=MeasurementPayload(measurements=list(measurements)),
    )


def energy(asset_id: str, asset_type: AssetType, direction: Direction, kwh: float) -> Measurement:
    return Measurement(
        asset_id=asset_id,
        asset_type=asset_type,
        quantity=Quantity.ACTIVE_ENERGY,
        direction=direction,
        aggregation=Aggregation.INTERVAL_SUM,
        value=kwh,
        unit="kWh",
        quality=Quality.SYNTHETIC,
    )


def forecast_event(
    *,
    location_id: str,
    issued_at: datetime,
    steps: Sequence[dict[str, Any]],
    ingested_at: datetime,
    forecast_model: str = "synthetic",
    source_system: str = "synthetic",
    run_id: str | None = None,
    seed: int | None = None,
    synthetic: bool = True,
) -> WeatherForecastEvent:
    """``steps`` hold per-step weather values; times and lead times follow from ``issued_at``."""
    built = []
    for k, values in enumerate(steps, start=1):
        valid_end = issued_at + k * QUARTER
        built.append(
            ForecastStep(
                valid_start=valid_end - QUARTER,
                valid_end=valid_end,
                lead_time=iso_duration(valid_end - issued_at),
                **values,
            )
        )
    start = built[0].valid_start
    return WeatherForecastEvent(
        event_id=event_id(
            stream=Stream.WEATHER_FORECAST.value,
            subject=location_id,
            interval_start=start,
            issued_at=issued_at,
            source_system=source_system,
            run_id=run_id,
            major=MAJOR,
        ),
        stream=Stream.WEATHER_FORECAST,
        source_system=source_system,
        location_id=location_id,
        run_id=run_id,
        interval_start=start,
        interval_end=built[-1].valid_end,
        resolution=Resolution.PT15M,
        t=quarter_index(start),
        issued_at=issued_at,
        ingested_at=ingested_at,
        synthetic=synthetic,
        seed=seed,
        payload=ForecastPayload(forecast_model=forecast_model, steps=built),
    )


def price_event(
    *,
    market: Market,
    delivery_day: date,
    prices: Sequence[float],
    published_at: datetime,
    ingested_at: datetime,
    resolution: Resolution = Resolution.PT15M,
    source_system: str = "synthetic",
    run_id: str | None = None,
    seed: int | None = None,
    synthetic: bool = True,
    currency: Literal["PLN", "EUR"] = "PLN",
) -> PriceEvent:
    slots_t = local_day_slots(delivery_day, resolution.delta)
    if len(prices) != len(slots_t):
        raise ValueError(f"{delivery_day}: {len(slots_t)} prices needed, {len(prices)} given")
    start, end = local_day_bounds(delivery_day)
    return PriceEvent(
        event_id=event_id(
            stream=Stream.PRICE.value,
            subject=MARKET_KEYS[market],
            interval_start=start,
            issued_at=published_at,
            source_system=source_system,
            run_id=run_id,
            major=MAJOR,
        ),
        source_system=source_system,
        run_id=run_id,
        interval_start=start,
        interval_end=end,
        resolution=resolution,
        t=quarter_index(start),
        issued_at=published_at,
        ingested_at=ingested_at,
        synthetic=synthetic,
        seed=seed,
        payload=PricePayload(
            market=market,
            currency=currency,
            unit="PLN/MWh" if currency == "PLN" else "EUR/MWh",
            delivery_day=delivery_day,
            publication_time=published_at,
            resolution=resolution,
            slots=[
                PriceSlot(interval_start=s, interval_end=e, price=float(p))
                for (s, e), p in zip(slots_t, prices, strict=True)
            ],
        ),
    )


def golden_examples() -> dict[str, MeasurementEvent | WeatherForecastEvent | PriceEvent]:
    """Fixed example per stream, written to ``schemas/examples/`` and checked in tests (P9)."""
    start = datetime(2026, 10, 20, 0, 0, tzinfo=UTC)
    loc = "PL-LOC-000001"
    production = measurement_event(
        stream=Stream.PRODUCTION,
        location_id=loc,
        interval_start=start,
        measurements=[energy("pv-1", AssetType.PV, Direction.GENERATION, 0.0)],
        ingested_at=GOLDEN_INGESTED_AT,
        metering_point_id=make_gsrn("3224", 1),
        seed=42,
    )
    consumption = measurement_event(
        stream=Stream.CONSUMPTION,
        location_id=loc,
        interval_start=start,
        measurements=[
            energy("load-1", AssetType.LOAD, Direction.CONSUMPTION, 0.071),
            energy("hp-1", AssetType.HEAT_PUMP, Direction.CONSUMPTION, 0.402),
        ],
        ingested_at=GOLDEN_INGESTED_AT,
        metering_point_id=make_gsrn("3224", 1),
        seed=42,
    )
    step = {
        "ghi_wm2": 0.0,
        "dni_wm2": 0.0,
        "dhi_wm2": 0.0,
        "temp_air_c": 7.4,
        "cloud_cover_pct": 88,
        "wind_speed_ms": {"10": 3.1, "100": 6.0, "120": 6.2},
        "wind_dir_deg": 240,
    }
    forecast = forecast_event(
        location_id=loc,
        issued_at=start,
        steps=[step, step],
        ingested_at=GOLDEN_INGESTED_AT,
        seed=42,
    )
    day = date(2026, 10, 21)
    price = price_event(
        market=Market.PL_RCE,
        delivery_day=day,
        prices=[400.0 + i for i in range(96)],
        published_at=datetime(2026, 10, 20, 11, 59, 15, tzinfo=UTC),
        ingested_at=GOLDEN_INGESTED_AT + timedelta(hours=12),
    )
    return {
        "production.json": production,
        "consumption.json": consumption,
        "weather_forecast.json": forecast,
        "price.json": price,
    }
