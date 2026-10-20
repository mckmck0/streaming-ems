"""Wire contract v1: the common envelope and the three stream payloads.

Producers build these models and validate strictly (``validate_strict``); consumers parse tolerantly
(``parse_event``), ignoring unknown fields, so an additive minor version never breaks an older
reader.
"""

from __future__ import annotations

import itertools
import math
import uuid
from datetime import date, datetime, timedelta
from enum import StrEnum
from typing import Annotated, Any, Literal, Self

from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    model_validator,
)

from ems.contracts.ids import (
    ID_PATTERN,
    LOCATION_PATTERN,
    RUN_ID_PATTERN,
    event_id,
    is_valid_gsrn,
)
from ems.contracts.time import (
    RESOLUTIONS,
    iso_duration,
    local_day_slots,
    quarter_index,
    require_utc,
)

SCHEMA_VERSION = "1.0.0"
MAJOR = 1


def _utc(dt: datetime) -> datetime:
    return require_utc(dt)


UtcDatetime = Annotated[AwareDatetime, AfterValidator(_utc)]
Id = Annotated[str, Field(pattern=ID_PATTERN.pattern, max_length=128)]


class _Model(BaseModel):
    model_config = ConfigDict(
        extra="ignore",
        allow_inf_nan=False,
        frozen=True,
        populate_by_name=True,
        serialize_by_alias=True,
        validate_by_alias=True,
        validate_by_name=True,
    )


class Stream(StrEnum):
    PRODUCTION = "production"
    CONSUMPTION = "consumption"
    WEATHER_FORECAST = "weather_forecast"
    WEATHER_ACTUAL = "weather_actual"
    PRICE = "price"


class Resolution(StrEnum):
    PT1M = "PT1M"
    PT15M = "PT15M"
    PT1H = "PT1H"
    P1D = "P1D"

    @property
    def delta(self) -> timedelta:
        return RESOLUTIONS[self.value]


class EventTimeKind(StrEnum):
    INTERVAL = "interval"
    INSTANT = "instant"
    FORECAST = "forecast"


class AssetType(StrEnum):
    PV = "pv"
    WIND = "wind"
    BESS = "bess"
    CHP = "chp"
    HYDRO = "hydro"
    BIOGAS = "biogas"
    BIOMASS = "biomass"
    HEAT_PUMP = "heat_pump"
    EV_CHARGER = "ev_charger"
    LOAD = "load"
    GRID_CONNECTION = "grid_connection"


class Quantity(StrEnum):
    ACTIVE_ENERGY = "active_energy"
    ACTIVE_POWER = "active_power"
    REACTIVE_ENERGY = "reactive_energy"
    SOC = "soc"
    HEAT_ENERGY = "heat_energy"
    FUEL_GAS_VOLUME = "fuel_gas_volume"


class Direction(StrEnum):
    GENERATION = "generation"
    CONSUMPTION = "consumption"
    IMPORT = "import"
    EXPORT = "export"
    CHARGE = "charge"
    DISCHARGE = "discharge"


class Aggregation(StrEnum):
    INTERVAL_SUM = "interval_sum"
    INTERVAL_MEAN = "interval_mean"
    INSTANT = "instant"
    REGISTER = "register"


class Quality(StrEnum):
    MEASURED = "measured"
    ESTIMATED = "estimated"
    SUBSTITUTED = "substituted"
    MISSING = "missing"
    SYNTHETIC = "synthetic"


class Market(StrEnum):
    PL_RCE = "PL_RCE"
    PL_SDAC = "PL_SDAC"
    ENTSOE_A44_PL = "ENTSOE_A44_PL"
    PL_DAM_TGE = "PL_DAM_TGE"


MARKET_KEYS: dict[Market, str] = {
    Market.PL_RCE: "PL:RCE",
    Market.PL_SDAC: "PL:SDAC",
    Market.ENTSOE_A44_PL: "ENTSOE:PL",
    Market.PL_DAM_TGE: "PL:TGE",
}

PRODUCTION_DIRECTIONS = {Direction.GENERATION, Direction.DISCHARGE}
CONSUMPTION_DIRECTIONS = {
    Direction.CONSUMPTION,
    Direction.CHARGE,
    Direction.IMPORT,
    Direction.EXPORT,
}


# ── payloads ──────────────────────────────────────────────────────────────────────────────────────


class Measurement(_Model):
    asset_id: Id
    asset_type: AssetType
    quantity: Quantity
    direction: Direction
    aggregation: Aggregation
    value: float = Field(ge=0)
    unit: str
    phase: str = "total"
    obis: str | None = None
    quality: Quality
    quality_raw: str | None = None
    version: int = Field(default=1, ge=1)


class MeasurementPayload(_Model):
    measurements: list[Measurement] = Field(min_length=1)


class ForecastStep(_Model):
    valid_start: UtcDatetime
    valid_end: UtcDatetime
    lead_time: str = Field(pattern=r"^PT(\d+H)?(\d+M)?(\d+S)?$")
    ghi_wm2: float = Field(ge=0)
    dni_wm2: float = Field(ge=0)
    dhi_wm2: float = Field(ge=0)
    temp_air_c: float
    cloud_cover_pct: int = Field(ge=0, le=100)
    wind_speed_ms: dict[Literal["10", "100", "120"], Annotated[float, Field(ge=0)]]
    wind_dir_deg: int = Field(ge=0, le=359)
    radiation_label: Literal["preceding_mean"] = "preceding_mean"


class ForecastPayload(_Model):
    forecast_model: Id
    steps: list[ForecastStep] = Field(min_length=1)


class PriceSlot(_Model):
    interval_start: UtcDatetime
    interval_end: UtcDatetime
    price: float


class PricePayload(_Model):
    market: Market
    bidding_zone: Literal["PL"] = "PL"
    currency: Literal["PLN", "EUR"]
    unit: Literal["PLN/MWh", "EUR/MWh"]
    delivery_day: date
    publication_time: UtcDatetime
    resolution: Resolution
    slots: list[PriceSlot] = Field(min_length=1)

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.unit.split("/")[0] != self.currency:
            raise ValueError("unit currency must match currency")
        if self.resolution not in (Resolution.PT15M, Resolution.PT1H):
            raise ValueError("price resolution must be PT15M or PT1H")
        expected = [s for s, _ in local_day_slots(self.delivery_day, self.resolution.delta)]
        got = [s.interval_start for s in self.slots]
        if got != expected:
            raise ValueError(
                f"slots must cover delivery day {self.delivery_day} in order "
                f"({len(expected)} expected, {len(got)} given)"
            )
        for s in self.slots:
            if s.interval_end - s.interval_start != self.resolution.delta:
                raise ValueError("slot length must equal the resolution")
        return self


# ── envelope ──────────────────────────────────────────────────────────────────────────────────────


class EnvelopeBase(_Model):
    schema_name: str = Field(alias="schema")
    schema_version: str = Field(default=SCHEMA_VERSION, pattern=r"^1\.\d+\.\d+$")
    event_id: uuid.UUID
    stream: Stream
    source_system: str = Field(pattern=r"^(replay:)?[a-z0-9-]+$")
    source_record_ref: str | None = None
    location_id: str | None = None
    metering_point_id: str | None = None
    algorithm_id: None = None
    run_id: str | None = Field(default=None, pattern=RUN_ID_PATTERN.pattern)
    interval_start: UtcDatetime
    interval_end: UtcDatetime
    resolution: Resolution
    t: int
    event_time_kind: EventTimeKind
    issued_at: UtcDatetime | None = None
    ingested_at: UtcDatetime
    synthetic: bool
    seed: int | None = None

    @property
    def subject(self) -> str:
        """Location id, or the market key (without run prefix) for prices."""
        raise NotImplementedError

    def expected_event_id(self) -> uuid.UUID:
        return event_id(
            stream=self.stream.value,
            subject=self.subject,
            interval_start=self.interval_start,
            issued_at=self.issued_at,
            source_system=self.source_system,
            run_id=self.run_id,
            major=int(self.schema_version.split(".")[0]),
        )

    @model_validator(mode="after")
    def _common(self) -> Self:
        if self.interval_end <= self.interval_start:
            raise ValueError("interval_end must be after interval_start")
        if self.t != quarter_index(self.interval_start):
            raise ValueError("t must be the UTC quarter-hour index of interval_start")
        if self.metering_point_id is not None and not is_valid_gsrn(self.metering_point_id):
            raise ValueError("metering_point_id must be an 18-digit GSRN with a valid check digit")
        if self.stream is Stream.PRICE:
            if self.location_id is not None:
                raise ValueError("location_id must be null for price events")
        elif self.location_id is None or not LOCATION_PATTERN.match(self.location_id):
            raise ValueError("location streams need a location_id like PL-LOC-000001")
        return self


class MeasurementEvent(EnvelopeBase):
    schema_name: Literal["ems.measurement"] = Field(default="ems.measurement", alias="schema")
    stream: Literal[Stream.PRODUCTION, Stream.CONSUMPTION]
    event_time_kind: Literal[EventTimeKind.INTERVAL] = EventTimeKind.INTERVAL
    payload: MeasurementPayload

    @property
    def subject(self) -> str:
        assert self.location_id is not None
        return self.location_id

    @model_validator(mode="after")
    def _measurement(self) -> Self:
        if self.interval_end - self.interval_start != self.resolution.delta:
            raise ValueError("resolution must equal the interval length")
        allowed = (
            PRODUCTION_DIRECTIONS if self.stream is Stream.PRODUCTION else CONSUMPTION_DIRECTIONS
        )
        for m in self.payload.measurements:
            if m.direction not in allowed:
                raise ValueError(
                    f"direction {m.direction} does not belong on the {self.stream} stream"
                )
        return self


class WeatherForecastEvent(EnvelopeBase):
    schema_name: Literal["ems.weather_forecast"] = Field(
        default="ems.weather_forecast", alias="schema"
    )
    stream: Literal[Stream.WEATHER_FORECAST, Stream.WEATHER_ACTUAL]
    event_time_kind: Literal[EventTimeKind.FORECAST] = EventTimeKind.FORECAST
    issued_at: UtcDatetime
    payload: ForecastPayload

    @property
    def subject(self) -> str:
        assert self.location_id is not None
        return self.location_id

    @model_validator(mode="after")
    def _forecast(self) -> Self:
        steps = self.payload.steps
        if steps[0].valid_start != self.interval_start or steps[-1].valid_end != self.interval_end:
            raise ValueError("envelope interval must span the forecast steps")
        step = self.resolution.delta
        for prev, cur in itertools.pairwise(steps):
            if cur.valid_start != prev.valid_end:
                raise ValueError("forecast steps must be contiguous")
        for s in steps:
            if s.valid_end - s.valid_start != step:
                raise ValueError("step length must equal the resolution")
            if s.lead_time != iso_duration(s.valid_end - self.issued_at):
                raise ValueError("lead_time must equal valid_end - issued_at")
        return self


class PriceEvent(EnvelopeBase):
    schema_name: Literal["ems.price"] = Field(default="ems.price", alias="schema")
    stream: Literal[Stream.PRICE] = Stream.PRICE
    event_time_kind: Literal[EventTimeKind.INTERVAL] = EventTimeKind.INTERVAL
    issued_at: UtcDatetime
    payload: PricePayload

    @property
    def subject(self) -> str:
        return MARKET_KEYS[self.payload.market]

    @model_validator(mode="after")
    def _price(self) -> Self:
        slots = self.payload.slots
        if (
            slots[0].interval_start != self.interval_start
            or slots[-1].interval_end != self.interval_end
        ):
            raise ValueError("envelope interval must be the delivery day in UTC")
        if self.issued_at != self.payload.publication_time:
            raise ValueError("issued_at must equal publication_time")
        if self.resolution != self.payload.resolution:
            raise ValueError("envelope resolution must equal the slot resolution")
        return self


Event = Annotated[
    MeasurementEvent | WeatherForecastEvent | PriceEvent, Field(discriminator="schema_name")
]
EVENT_TYPES: dict[str, type[EnvelopeBase]] = {
    "ems.measurement": MeasurementEvent,
    "ems.weather_forecast": WeatherForecastEvent,
    "ems.price": PriceEvent,
}
_EVENT_ADAPTER: TypeAdapter[MeasurementEvent | WeatherForecastEvent | PriceEvent] = TypeAdapter(
    Event
)


class ContractError(ValueError):
    """An event violates the wire contract."""


def parse_event(
    data: bytes | str | dict[str, Any],
) -> MeasurementEvent | WeatherForecastEvent | PriceEvent:
    """Tolerant parse (consumer side): unknown fields are ignored."""
    if isinstance(data, dict):
        return _EVENT_ADAPTER.validate_python(data)
    return _EVENT_ADAPTER.validate_json(data)


def _unknown_keys(given: Any, known: Any, path: str = "") -> list[str]:
    if isinstance(given, dict) and isinstance(known, dict):
        out: list[str] = []
        for k, v in given.items():
            if k not in known:
                out.append(f"{path}{k}")
            else:
                out += _unknown_keys(v, known[k], f"{path}{k}.")
        return out
    if isinstance(given, list) and isinstance(known, list):
        out = []
        for i, (g, k) in enumerate(zip(given, known, strict=False)):
            out += _unknown_keys(g, k, f"{path}{i}.")
        return out
    return []


def validate_strict(data: dict[str, Any]) -> MeasurementEvent | WeatherForecastEvent | PriceEvent:
    """Producer-side validation: no unknown fields, finite values, deterministic event id."""
    try:
        event = parse_event(data)
    except ValueError as exc:
        raise ContractError(str(exc)) from exc
    extra = _unknown_keys(data, event.model_dump(mode="json"))
    if extra:
        raise ContractError(f"unknown fields: {', '.join(extra)}")
    check_event(event)
    return event


def check_event(event: EnvelopeBase) -> None:
    """Checks a model can only fail on after construction: the version and the deterministic id."""
    if int(event.schema_version.split(".")[0]) != MAJOR:
        raise ContractError(f"unsupported major version {event.schema_version}")
    if event.event_id != event.expected_event_id():
        raise ContractError("event_id does not match the identity rule")
    for value in _floats(event.model_dump(mode="python")):
        if not math.isfinite(value):
            raise ContractError("non-finite value")


def _floats(obj: Any) -> list[float]:
    if isinstance(obj, float):
        return [obj]
    if isinstance(obj, dict):
        return [f for v in obj.values() for f in _floats(v)]
    if isinstance(obj, list):
        return [f for v in obj for f in _floats(v)]
    return []


def to_json_bytes(event: EnvelopeBase) -> bytes:
    return event.model_dump_json(by_alias=True).encode()
