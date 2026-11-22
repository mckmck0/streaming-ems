"""AC-011-07, AC-011-26: the one event mapping the actors and the offline side share.

- Readings: PV is the ``math.fsum`` of the ``pv`` generation entries, load the ``math.fsum`` of the
  ``load``, ``heat_pump`` and ``ev_charger`` consumption entries; battery, grid and SoC entries are
  ignored and not counted; an entry of a wanted type that is not ``active_energy``,
  ``interval_sum``, ``kWh``, or that is ``missing``, is left out and counted in ``skipped``.
- Curves: every slot through ``pln_per_mwh_to_pln_per_kwh``; ``t0``, the delivery day, the
  publication second and step (ceil) and the step the next publication window opens (13:00
  Europe/Warsaw).
- Runs: usable from the step of ``issued_at`` + the model's delay (ceil), the next run not before
  that + the cadence (floor).
- Anything else (another resolution, a non-``PLN/MWh`` or non-RCE curve, a value beyond the bounds,
  an unknown model, a mapping that would raise) maps to None, never an exception.

The sums are compared with ``math.fsum`` of the entries themselves, not with ``Chunk.consumption``,
which adds in another order.
"""

from __future__ import annotations

import math
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest

from ems.assembler import mapping
from ems.assembler.mapping import CurveIn, Reading, RunIn
from ems.contracts.envelope import (
    Aggregation,
    AssetType,
    Direction,
    Market,
    Measurement,
    MeasurementEvent,
    PriceEvent,
    Quality,
    Quantity,
    Resolution,
    Stream,
    WeatherForecastEvent,
    parse_event,
    to_json_bytes,
)
from ems.contracts.examples import (
    GOLDEN_INGESTED_AT,
    energy,
    forecast_event,
    golden_examples,
    measurement_event,
    price_event,
)
from ems.domain.units import pln_per_mwh_to_pln_per_kwh
from tests.unit.assembler.cases import generated, seconds_at, step_at

LOC = "PL-LOC-000001"
START = datetime(2026, 10, 20, 10, 0, tzinfo=UTC)
STEP = {
    "ghi_wm2": 0.0,
    "dni_wm2": 0.0,
    "dhi_wm2": 0.0,
    "temp_air_c": 7.4,
    "cloud_cover_pct": 88,
    "wind_speed_ms": {"10": 3.1, "100": 6.0, "120": 6.2},
    "wind_dir_deg": 240,
}
WANTED = {
    Stream.PRODUCTION: ({AssetType.PV}, Direction.GENERATION),
    Stream.CONSUMPTION: (
        {AssetType.LOAD, AssetType.HEAT_PUMP, AssetType.EV_CHARGER},
        Direction.CONSUMPTION,
    ),
}


def _wire(event: Any) -> Any:
    """``event`` as the Kafka sink publishes it, parsed back by the mapping."""
    parsed = mapping.parse(to_json_bytes(event))
    assert parsed is not None
    return parsed


def _meter(stream: Stream, *entries: Measurement) -> MeasurementEvent:
    return measurement_event(
        stream=stream,
        location_id=LOC,
        interval_start=START,
        measurements=list(entries),
        ingested_at=GOLDEN_INGESTED_AT,
        run_id="r1",
    )


def _usable(m: Measurement) -> bool:
    return (
        m.quantity is Quantity.ACTIVE_ENERGY
        and m.aggregation is Aggregation.INTERVAL_SUM
        and m.unit == "kWh"
        and m.quality is not Quality.MISSING
    )


def _expected(event: MeasurementEvent) -> Reading:
    types, direction = WANTED[event.stream]
    wanted = [
        m for m in event.payload.measurements if m.asset_type in types and m.direction is direction
    ]
    return Reading(
        event.stream.value,
        event.t,
        str(event.event_id),
        math.fsum(m.value for m in wanted if _usable(m)),
        sum(not _usable(m) for m in wanted),
    )


def _price(
    day: date,
    n: int,
    published: datetime,
    *,
    market: Market = Market.PL_RCE,
    resolution: Resolution = Resolution.PT15M,
    currency: Any = "PLN",
    prices: list[float] | None = None,
) -> PriceEvent:
    return price_event(
        market=market,
        delivery_day=day,
        prices=prices if prices is not None else [400.0 + i for i in range(n)],
        published_at=published,
        ingested_at=GOLDEN_INGESTED_AT,
        resolution=resolution,
        currency=currency,
    )


def _run(issued: datetime, model: str = "synthetic") -> WeatherForecastEvent:
    return forecast_event(
        location_id=LOC,
        issued_at=issued,
        steps=[STEP, STEP],
        ingested_at=GOLDEN_INGESTED_AT,
        forecast_model=model,
    )


def test_constants() -> None:
    assert (mapping.PRODUCTION, mapping.CONSUMPTION) == ("production", "consumption")
    assert mapping.MAX_ENTRY_KWH == 1e4 and mapping.MAX_PRICE_PLN_PER_MWH == 1e5


# ── parse ─────────────────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("name", sorted(golden_examples()))
def test_ac_011_26_parse_reads_what_the_sink_writes(name: str) -> None:
    raw = to_json_bytes(golden_examples()[name])
    assert mapping.parse(raw) == parse_event(raw)


@pytest.mark.parametrize(
    "raw", [b"", b"\xff\xfe", b"not json", b"{}", b"[]", b"null", b'{"schema": "ems.price"}']
)
def test_ac_011_26_parse_refuses_without_raising(raw: bytes) -> None:
    assert mapping.parse(raw) is None


# ── readings ──────────────────────────────────────────────────────────────────────────────────────


def test_ac_011_07_generated_readings_are_the_fsum_of_their_entries() -> None:
    day = generated()
    events = [e for s in ("production", "consumption") for e in day.of(s)]
    assert len(events) == 768 + 960
    grid = 0
    for event in events:
        assert isinstance(event, MeasurementEvent)
        got = mapping.reading(mapping.parse(to_json_bytes(event)))
        assert got == _expected(event), (event.location_id, event.stream, event.t)
        assert got is not None and got.skipped == 0
        grid += any(m.asset_type is AssetType.GRID_CONNECTION for m in event.payload.measurements)
    assert grid > 0  # unmanaged sites carry grid entries, which must not count


def test_ac_011_07_load_adds_load_heat_pump_and_ev_and_ignores_battery_grid_soc() -> None:
    soc = Measurement(
        asset_id="bess-1",
        asset_type=AssetType.BESS,
        quantity=Quantity.SOC,
        direction=Direction.CHARGE,
        aggregation=Aggregation.INSTANT,
        value=4.2,
        unit="kWh",
        quality=Quality.SYNTHETIC,
    )
    event = _meter(
        Stream.CONSUMPTION,
        energy("load", AssetType.LOAD, Direction.CONSUMPTION, 0.1),
        energy("hp-1", AssetType.HEAT_PUMP, Direction.CONSUMPTION, 0.2),
        energy("ev-1", AssetType.EV_CHARGER, Direction.CONSUMPTION, 0.3),
        energy("bess-1", AssetType.BESS, Direction.CHARGE, 1.5),
        soc,
        energy("grid", AssetType.GRID_CONNECTION, Direction.IMPORT, 2.0),
        energy("grid", AssetType.GRID_CONNECTION, Direction.EXPORT, 0.7),
    )
    got = mapping.reading(_wire(event))
    assert got == Reading(
        "consumption", event.t, str(event.event_id), math.fsum([0.1, 0.2, 0.3]), 0
    )


def test_ac_011_07_pv_adds_pv_generation_and_ignores_battery_discharge() -> None:
    event = _meter(
        Stream.PRODUCTION,
        energy("pv-1", AssetType.PV, Direction.GENERATION, 0.4),
        energy("pv-2", AssetType.PV, Direction.GENERATION, 0.35),
        energy("bess-1", AssetType.BESS, Direction.DISCHARGE, 1.0),
    )
    got = mapping.reading(_wire(event))
    assert got == Reading("production", event.t, str(event.event_id), math.fsum([0.4, 0.35]), 0)


@pytest.mark.parametrize(
    "quality", [Quality.MEASURED, Quality.ESTIMATED, Quality.SUBSTITUTED, Quality.SYNTHETIC]
)
def test_ac_011_07_every_quality_but_missing_counts(quality: Quality) -> None:
    entry = energy("load", AssetType.LOAD, Direction.CONSUMPTION, 0.3)
    event = _meter(Stream.CONSUMPTION, entry.model_copy(update={"quality": quality}))
    got = mapping.reading(_wire(event))
    assert got is not None and (got.kwh, got.skipped) == (0.3, 0)


WANTED_ENTRIES = [
    pytest.param(Stream.PRODUCTION, AssetType.PV, Direction.GENERATION, id="pv"),
    pytest.param(Stream.CONSUMPTION, AssetType.LOAD, Direction.CONSUMPTION, id="load"),
    pytest.param(Stream.CONSUMPTION, AssetType.HEAT_PUMP, Direction.CONSUMPTION, id="heat_pump"),
    pytest.param(Stream.CONSUMPTION, AssetType.EV_CHARGER, Direction.CONSUMPTION, id="ev_charger"),
]
UNUSABLE = [
    pytest.param({"quantity": Quantity.ACTIVE_POWER}, id="active_power"),
    pytest.param({"quantity": Quantity.REACTIVE_ENERGY}, id="reactive_energy"),
    pytest.param({"aggregation": Aggregation.INTERVAL_MEAN}, id="interval_mean"),
    pytest.param({"aggregation": Aggregation.REGISTER}, id="register"),
    pytest.param({"aggregation": Aggregation.INSTANT}, id="instant"),
    pytest.param({"unit": "Wh"}, id="Wh"),
    pytest.param({"unit": "MWh"}, id="MWh"),
    pytest.param({"unit": "kwh"}, id="kwh lower case"),
    pytest.param({"quality": Quality.MISSING}, id="missing"),
]


@pytest.mark.parametrize("change", UNUSABLE)
@pytest.mark.parametrize(("stream", "asset", "direction"), WANTED_ENTRIES)
def test_ac_011_07_an_unusable_entry_is_left_out_and_counted(
    stream: Stream, asset: AssetType, direction: Direction, change: dict[str, Any]
) -> None:
    good = energy("a-1", asset, direction, 0.5)
    bad = energy("a-2", asset, direction, 7.0).model_copy(update=change)
    event = _meter(stream, good, bad)
    got = mapping.reading(_wire(event))
    assert got == Reading(stream.value, event.t, str(event.event_id), 0.5, 1)


def test_ac_011_07_only_unusable_entries_give_zero_and_their_count() -> None:
    bad = energy("pv-1", AssetType.PV, Direction.GENERATION, 2.0)
    event = _meter(
        Stream.PRODUCTION,
        bad.model_copy(update={"quality": Quality.MISSING}),
        bad.model_copy(update={"unit": "Wh"}),
    )
    got = mapping.reading(_wire(event))
    assert got == Reading("production", event.t, str(event.event_id), 0.0, 2)


def test_ac_011_07_an_entry_at_the_bound_is_used() -> None:
    event = _meter(Stream.CONSUMPTION, energy("load", AssetType.LOAD, Direction.CONSUMPTION, 1e4))
    got = mapping.reading(_wire(event))
    assert got is not None and got.kwh == 1e4


@pytest.mark.parametrize(
    ("asset", "direction"),
    [
        (AssetType.LOAD, Direction.CONSUMPTION),
        (AssetType.HEAT_PUMP, Direction.CONSUMPTION),
        (AssetType.GRID_CONNECTION, Direction.IMPORT),  # every entry is bounded, used or not
    ],
)
def test_ac_011_07_an_entry_beyond_the_bound_refuses_the_event(
    asset: AssetType, direction: Direction
) -> None:
    big = math.nextafter(1e4, math.inf)
    event = _meter(
        Stream.CONSUMPTION,
        energy("load", AssetType.LOAD, Direction.CONSUMPTION, 0.2),
        energy("x-1", asset, direction, big),
    )
    assert mapping.reading(_wire(event)) is None


@pytest.mark.parametrize("resolution", [Resolution.PT1M, Resolution.PT1H, Resolution.P1D])
def test_ac_011_07_a_measurement_not_at_pt15m_is_refused(resolution: Resolution) -> None:
    event = _meter(Stream.CONSUMPTION, energy("load", AssetType.LOAD, Direction.CONSUMPTION, 0.2))
    other = event.model_copy(
        update={"resolution": resolution, "interval_end": START + resolution.delta}
    )
    raw = to_json_bytes(other)
    assert parse_event(raw).resolution is resolution  # still a valid v1 event
    assert mapping.reading(mapping.parse(raw)) is None


# ── curves ────────────────────────────────────────────────────────────────────────────────────────


def test_ac_011_07_the_golden_curve() -> None:
    event = golden_examples()["price.json"]  # 2026-10-21, published 2026-10-20T11:59:15Z
    got = mapping.curve(_wire(event))
    assert got == CurveIn(
        event_id=str(event.event_id),
        t0=step_at("2026-10-20T22:00"),
        day=date(2026, 10, 21),
        rce_pln_per_kwh=tuple(pln_per_mwh_to_pln_per_kwh(400.0 + i) for i in range(96)),
        issued_s=seconds_at("2026-10-20T11:59:15"),
        issued_step=step_at("2026-10-20T12:00"),
        opens_step=step_at("2026-10-21T11:00"),
    )


def test_ac_011_07_a_curve_published_on_a_quarter_is_issued_on_it() -> None:
    event = _price(date(2026, 10, 21), 96, datetime(2026, 10, 20, 13, 15, tzinfo=UTC))
    got = mapping.curve(_wire(event))
    assert got is not None
    assert (got.issued_s, got.issued_step) == (
        seconds_at("2026-10-20T13:15"),
        step_at("2026-10-20T13:15"),
    )


@pytest.mark.parametrize(
    ("day", "n", "first", "opens"),
    [
        (date(2026, 10, 25), 100, "2026-10-24T22:00", "2026-10-25T12:00"),
        (date(2026, 3, 29), 92, "2026-03-28T23:00", "2026-03-29T11:00"),
        (date(2026, 3, 30), 96, "2026-03-29T22:00", "2026-03-30T11:00"),
    ],
)
def test_ac_011_07_dst_curves(day: date, n: int, first: str, opens: str) -> None:
    published = datetime.combine(day - timedelta(days=1), datetime.min.time(), UTC) + timedelta(
        hours=12, minutes=7
    )
    event = _price(day, n, published, prices=[-50.0 + 3 * i for i in range(n)])
    got = mapping.curve(_wire(event))
    assert got is not None
    assert (got.t0, got.day, got.opens_step) == (step_at(first), day, step_at(opens))
    assert got.rce_pln_per_kwh == tuple(pln_per_mwh_to_pln_per_kwh(-50.0 + 3 * i) for i in range(n))


def test_ac_011_07_generated_curves() -> None:
    curves = [e for e in generated().of("price") if isinstance(e, PriceEvent)]
    assert [c.payload.delivery_day for c in curves] == [date(2026, 10, 20), date(2026, 10, 21)]
    for event in curves:
        got = mapping.curve(mapping.parse(to_json_bytes(event)))
        assert got is not None
        stamp = event.issued_at.timestamp()
        assert got.event_id == str(event.event_id) and got.t0 == event.t
        assert got.rce_pln_per_kwh == tuple(
            pln_per_mwh_to_pln_per_kwh(s.price) for s in event.payload.slots
        )
        assert got.issued_s == math.floor(stamp) and got.issued_step == math.ceil(stamp / 900)
        day = event.payload.delivery_day
        opens = datetime.combine(day, datetime.min.time(), UTC) + timedelta(hours=11)  # CEST
        assert got.opens_step == int(opens.timestamp()) // 900


@pytest.mark.parametrize("price", [1e5, -1e5])
def test_ac_011_07_a_price_at_the_bound_is_used(price: float) -> None:
    prices = [price] + [100.0] * 95
    event = _price(date(2026, 10, 21), 96, datetime(2026, 10, 20, 12, tzinfo=UTC), prices=prices)
    got = mapping.curve(_wire(event))
    assert got is not None and got.rce_pln_per_kwh[0] == pln_per_mwh_to_pln_per_kwh(price)


@pytest.mark.parametrize("price", [math.nextafter(1e5, math.inf), -math.nextafter(1e5, math.inf)])
def test_ac_011_07_a_price_beyond_the_bound_refuses_the_curve(price: float) -> None:
    prices = [100.0] * 95 + [price]
    event = _price(date(2026, 10, 21), 96, datetime(2026, 10, 20, 12, tzinfo=UTC), prices=prices)
    assert mapping.curve(_wire(event)) is None


@pytest.mark.parametrize(
    "event",
    [
        pytest.param(
            _price(
                date(2026, 10, 21),
                24,
                datetime(2026, 10, 20, 12, tzinfo=UTC),
                resolution=Resolution.PT1H,
            ),
            id="PT1H",
        ),
        pytest.param(
            _price(date(2026, 10, 21), 96, datetime(2026, 10, 20, 12, tzinfo=UTC), currency="EUR"),
            id="EUR/MWh",
        ),
        pytest.param(
            _price(
                date(2026, 10, 21),
                96,
                datetime(2026, 10, 20, 12, tzinfo=UTC),
                market=Market.PL_SDAC,
            ),
            id="PL:SDAC (no window)",
        ),
        pytest.param(
            _price(
                date(2026, 10, 21),
                96,
                datetime(2026, 10, 20, 12, tzinfo=UTC),
                market=Market.ENTSOE_A44_PL,
                currency="EUR",
            ),
            id="ENTSOE:PL",
        ),
    ],
)
def test_ac_011_07_a_curve_the_household_cannot_use_is_refused(event: PriceEvent) -> None:
    assert mapping.curve(_wire(event)) is None


# ── runs ──────────────────────────────────────────────────────────────────────────────────────────


def test_ac_011_07_the_first_generated_run() -> None:
    first = generated().of("weather_forecast", LOC)[0]
    assert first.issued_at == datetime(2026, 10, 20, tzinfo=UTC)
    got = mapping.run(mapping.parse(to_json_bytes(first)))
    assert got == RunIn(
        issue_s=seconds_at("2026-10-20T00:00"),
        usable_step=step_at("2026-10-20T02:00"),
        next_covered=step_at("2026-10-20T05:00"),
    )


@pytest.mark.parametrize(
    ("model", "issued", "usable", "next_covered"),
    [
        ("synthetic", "2026-10-20T03:00", "2026-10-20T05:00", "2026-10-20T08:00"),
        ("icon_d2", "2026-10-20T03:00", "2026-10-20T05:00", "2026-10-20T08:00"),
        ("dmi_harmonie_arome_europe", "2026-10-20T00:00", "2026-10-20T03:15", "2026-10-20T06:15"),
        # between issue times: usable rounds up, the next run's watermark rounds down
        ("synthetic", "2026-10-20T01:07:30", "2026-10-20T03:15", "2026-10-20T06:00"),
        ("icon_d2", "2026-10-25T00:00", "2026-10-25T02:00", "2026-10-25T05:00"),  # UTC: no DST
    ],
)
def test_ac_011_07_run_usable_step_and_next_watermark(
    model: str, issued: str, usable: str, next_covered: str
) -> None:
    event = _run(datetime.fromisoformat(issued).replace(tzinfo=UTC), model)
    got = mapping.run(_wire(event))
    assert got == RunIn(seconds_at(issued), step_at(usable), step_at(next_covered))


@pytest.mark.parametrize("model", ["gfs_seamless", "best_match", "SYNTHETIC"])
def test_ac_011_15_a_run_of_an_unknown_model_is_refused(model: str) -> None:
    assert mapping.run(_wire(_run(datetime(2026, 10, 20, tzinfo=UTC), model))) is None


def test_ac_011_07_a_run_whose_watermark_leaves_the_calendar_is_refused() -> None:
    # Usable at 9999-12-31T22:00Z; one cadence later is past datetime.max.
    event = _run(datetime(9999, 12, 31, 20, 0, tzinfo=UTC))
    assert mapping.run(_wire(event)) is None


# ── cross-type and None ───────────────────────────────────────────────────────────────────────────


def test_ac_011_26_each_function_maps_only_its_own_kind() -> None:
    examples = {name: _wire(e) for name, e in golden_examples().items()}
    for name, event in examples.items():
        assert (mapping.reading(event) is not None) == (
            name in {"production.json", "consumption.json"}
        )
        assert (mapping.curve(event) is not None) == (name == "price.json")
        assert (mapping.run(event) is not None) == (name == "weather_forecast.json")
    assert mapping.reading(None) is None
    assert mapping.curve(None) is None
    assert mapping.run(None) is None
