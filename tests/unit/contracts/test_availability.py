"""AC-008-08: one forecast availability delay per ``forecast_model`` (spec 008, plan §4).

The same table drives synthetic forecast emission, the history build and the rule in
``08-contracts.md``; ``available_at`` applies it per stream.

AC-011-14, -16, -20 (spec 011, plan §3): the schedule constants the assembler's watermarks use, a
run cadence per ``forecast_model`` and the opening of the price window (13:00 Europe/Warsaw on the
day before delivery), checked against the generator's schedule and on both DST days.
"""

from __future__ import annotations

import math
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any

import pytest
from ruamel.yaml import YAML

from ems.contracts.availability import (
    FORECAST_CADENCE_MIN,
    FORECAST_DELAY_MIN,
    GRACE_MIN,
    MEASURED_MAX_MIN,
    PRICE_WINDOW_OPENS,
    available_at,
    forecast_cadence,
    forecast_delay,
    price_window_opens,
)
from ems.contracts.envelope import MARKET_KEYS, AssetType, Direction, Market, Stream
from ems.contracts.examples import energy, forecast_event, measurement_event, price_event
from ems.contracts.time import (
    QUARTER,
    WARSAW,
    from_quarter_index,
    local_day_bounds,
    quarter_index,
)
from ems.ingest.generator import FORECAST_CADENCE_Q
from ems.ingest.live.openmeteo import AVAILABILITY_GRACE, DMI_HARMONIE, ICON_D2
from ems.ingest.synthetic.prices import price_params, publication_time

ISSUED = datetime(2026, 5, 15, 3, tzinfo=UTC)
INGESTED = datetime(2026, 11, 13, 12, tzinfo=UTC)
STEP: dict[str, Any] = {
    "ghi_wm2": 0.0,
    "dni_wm2": 0.0,
    "dhi_wm2": 0.0,
    "temp_air_c": 7.4,
    "cloud_cover_pct": 88,
    "wind_speed_ms": {"10": 3.1, "100": 6.0, "120": 6.2},
    "wind_dir_deg": 240,
}
REAL_MODELS = (ICON_D2.request, DMI_HARMONIE.request)


def _forecast(model: str, first_step: int = 0) -> Any:
    return forecast_event(
        location_id="PL-LOC-000001",
        issued_at=ISSUED,
        steps=[STEP, STEP],
        ingested_at=INGESTED,
        forecast_model=model,
        first_step=first_step,
    )


def test_ac_008_08_table_values() -> None:
    assert FORECAST_DELAY_MIN == {
        "synthetic": 120,
        "icon_d2": 120,
        "dmi_harmonie_arome_europe": 195,
    }
    assert MEASURED_MAX_MIN == {"icon_d2": 106.7, "dmi_harmonie_arome_europe": 170.4}
    assert GRACE_MIN == 10
    assert forecast_delay("synthetic") == timedelta(minutes=120)
    assert forecast_delay("icon_d2") == timedelta(minutes=120)
    assert forecast_delay("dmi_harmonie_arome_europe") == timedelta(minutes=195)


@pytest.mark.parametrize("model", REAL_MODELS)
def test_ac_008_08_delay_covers_measured_plus_grace_rounded_up_to_a_quarter(model: str) -> None:
    """Each delay is >= the largest measured delay + the grace, on the quarter-hour grid, and is
    the smallest such value (F6 and T07: 106.7 + 10 -> 120; 170.4 + 10 -> 195; revised 2026-11-13).
    """
    delay_min = FORECAST_DELAY_MIN[model]
    needed_min = MEASURED_MAX_MIN[model] + GRACE_MIN
    assert delay_min >= needed_min
    assert delay_min % 15 == 0
    assert delay_min == 15 * math.ceil(needed_min / 15)
    assert forecast_delay(model) % QUARTER == timedelta(0)


def test_ac_008_08_grace_is_the_live_adapters_and_the_table_covers_its_models() -> None:
    assert timedelta(minutes=GRACE_MIN) == AVAILABILITY_GRACE
    assert set(REAL_MODELS) == set(MEASURED_MAX_MIN)
    assert set(REAL_MODELS) | {"synthetic"} == set(FORECAST_DELAY_MIN)


def test_ac_008_08_unknown_model_raises_naming_it() -> None:
    with pytest.raises(ValueError, match="ecmwf_ifs025"):
        forecast_delay("ecmwf_ifs025")


def test_ac_008_08_weather_yaml_holds_no_second_definition(repo: Path) -> None:
    """One definition: ``forecast.availability_delay_min`` leaves ``weather.yaml``."""
    params = YAML(typ="safe").load((repo / "config" / "calibration" / "weather.yaml").read_text())
    assert "availability_delay_min" not in params["forecast"]


# ── available_at per stream ───────────────────────────────────────────────────────────────────


def test_ac_008_08_measurement_is_available_at_interval_end_plus_meter_delay() -> None:
    start = datetime(2026, 5, 15, 10, 15, tzinfo=UTC)
    for stream, measurement in (
        (Stream.PRODUCTION, energy("pv-1", AssetType.PV, Direction.GENERATION, 0.4)),
        (Stream.CONSUMPTION, energy("load-1", AssetType.LOAD, Direction.CONSUMPTION, 0.1)),
    ):
        event = measurement_event(
            stream=stream,
            location_id="PL-LOC-000001",
            interval_start=start,
            measurements=[measurement],
            ingested_at=INGESTED,
        )
        assert available_at(event) == start + QUARTER
        assert available_at(event, timedelta(seconds=45)) == start + QUARTER + timedelta(seconds=45)


@pytest.mark.parametrize(
    ("model", "delay_min"),
    [("synthetic", 120), ("icon_d2", 120), ("dmi_harmonie_arome_europe", 195)],
)
def test_ac_008_08_forecast_is_available_at_issued_at_plus_its_models_delay(
    model: str, delay_min: int
) -> None:
    event = _forecast(model)
    expected = ISSUED + timedelta(minutes=delay_min)
    assert available_at(event) == expected
    assert available_at(event, timedelta(minutes=5)) == expected  # the meter delay is not used


def test_ac_008_08_forecast_starting_late_is_still_timed_from_issued_at() -> None:
    """An archived DMI run starts at +45 min (``first_step=2``); availability counts from the run
    time, not from the first step.
    """
    event = _forecast("dmi_harmonie_arome_europe", first_step=2)
    assert event.interval_start == ISSUED + 2 * QUARTER
    assert available_at(event) == ISSUED + timedelta(minutes=195)


def test_ac_008_08_forecast_with_unknown_model_raises() -> None:
    with pytest.raises(ValueError, match="gfs_seamless"):
        available_at(_forecast("gfs_seamless"))


def test_ac_008_08_price_is_available_at_issued_at() -> None:
    published = datetime(2026, 10, 20, 11, 42, 11, 720000, tzinfo=UTC)
    event = price_event(
        market=Market.PL_RCE,
        delivery_day=date(2026, 10, 21),
        prices=[400.0 + i for i in range(96)],
        published_at=published,
        ingested_at=INGESTED,
    )
    assert available_at(event) == published
    assert available_at(event, timedelta(minutes=5)) == published


# ── forecast_event(first_step=…) (AC-008-05, used by archived runs) ───────────────────────────


def test_ac_008_05_forecast_event_first_step_offsets_the_window() -> None:
    """Step k (1-based) ends at ``issued_at + (first_step + k) * 15 min``; the event's window, ``t``
    and lead times follow; ``first_step=0`` is today's event.
    """
    event = _forecast("dmi_harmonie_arome_europe", first_step=2)
    steps = event.payload.steps
    assert [s.valid_end for s in steps] == [ISSUED + 3 * QUARTER, ISSUED + 4 * QUARTER]
    assert [s.valid_start for s in steps] == [ISSUED + 2 * QUARTER, ISSUED + 3 * QUARTER]
    assert [s.lead_time for s in steps] == ["PT45M", "PT1H"]
    assert (event.interval_start, event.interval_end) == (
        ISSUED + 2 * QUARTER,
        ISSUED + 4 * QUARTER,
    )
    assert event.t == quarter_index(ISSUED + 2 * QUARTER) and event.issued_at == ISSUED
    default = forecast_event(
        location_id="PL-LOC-000001",
        issued_at=ISSUED,
        steps=[STEP, STEP],
        ingested_at=INGESTED,
        forecast_model="dmi_harmonie_arome_europe",
    )
    assert _forecast("dmi_harmonie_arome_europe", first_step=0) == default
    assert default.interval_start == ISSUED and default.payload.steps[0].lead_time == "PT15M"


# ── AC-011-14, -16, -20: schedule constants (spec 011, plan §3) ──────────────────────────────────

ALL_MODELS = ("synthetic", "icon_d2", "dmi_harmonie_arome_europe")
DAYS_2026 = [date(2026, 1, 1) + timedelta(days=i) for i in range(365)]
SEED = 4242


def test_ac_011_14_cadence_table_values() -> None:
    assert FORECAST_CADENCE_MIN == {
        "synthetic": 180,
        "icon_d2": 180,
        "dmi_harmonie_arome_europe": 180,
    }
    for model in ALL_MODELS:
        assert forecast_cadence(model) == timedelta(minutes=180)
        assert forecast_cadence(model) % QUARTER == timedelta(0)


def test_ac_011_14_every_model_with_a_delay_has_a_cadence() -> None:
    assert set(FORECAST_CADENCE_MIN) == set(FORECAST_DELAY_MIN)


@pytest.mark.parametrize("model", ["ecmwf_ifs025", "gfs_seamless", "ICON_D2", ""])
def test_ac_011_14_unknown_model_has_no_cadence(model: str) -> None:
    with pytest.raises(ValueError):
        forecast_cadence(model)


def test_ac_011_14_synthetic_issue_times_lie_on_the_cadence() -> None:
    """The generator issues a run at every ``t`` with ``t % FORECAST_CADENCE_Q == 0``
    (``emissions``): over 2026 each issue time is a multiple of the cadence, eight a day at
    00, 03, … 21 UTC as the ICON-D2 runs."""
    cadence = forecast_cadence("synthetic")
    assert cadence == FORECAST_CADENCE_Q * QUARTER
    epoch = from_quarter_index(0)
    first = quarter_index(datetime(2026, 1, 1, tzinfo=UTC))
    end = quarter_index(datetime(2027, 1, 1, tzinfo=UTC))
    issues = [from_quarter_index(t) for t in range(first, end) if t % FORECAST_CADENCE_Q == 0]
    assert len(issues) == 365 * 8
    for issued in issues:
        assert (issued - epoch) % cadence == timedelta(0), issued
        assert (issued.hour % 3, issued.minute, issued.second) == (0, 0, 0), issued


def test_ac_011_16_price_window_table() -> None:
    assert MARKET_KEYS[Market.PL_RCE] == "PL:RCE"
    assert dict(PRICE_WINDOW_OPENS) == {"PL:RCE": time(13, 0)}


@pytest.mark.parametrize(
    ("day", "opens"),
    [
        (date(2026, 3, 29), datetime(2026, 3, 29, 11, tzinfo=UTC)),  # 92 slots, CEST from 02:00
        (date(2026, 10, 25), datetime(2026, 10, 25, 12, tzinfo=UTC)),  # 100 slots, CET from 03:00
        (date(2026, 3, 28), datetime(2026, 3, 28, 12, tzinfo=UTC)),
        (date(2026, 10, 24), datetime(2026, 10, 24, 11, tzinfo=UTC)),
    ],
)
def test_ac_011_16_20_price_window_opens_at_13_warsaw_across_dst(
    day: date, opens: datetime
) -> None:
    got = price_window_opens("PL:RCE", day)
    assert got == opens
    assert got.utcoffset() == timedelta(0)
    if day in (date(2026, 3, 29), date(2026, 10, 25)):
        # A fixed offset from the day's first slot would miss by an hour on the DST days.
        assert local_day_bounds(day)[0] + timedelta(hours=13) != opens


def test_ac_011_16_price_window_is_13_warsaw_on_every_day_of_2026() -> None:
    for day in DAYS_2026:
        local = price_window_opens("PL:RCE", day).astimezone(WARSAW)
        assert (local.date(), local.time()) == (day, time(13, 0)), day


@pytest.mark.parametrize("market", ["PL:SDAC", "PL:TGE", "ENTSOE:PL", "PL_RCE", ""])
def test_ac_011_16_unknown_market_has_no_window(market: str) -> None:
    with pytest.raises(ValueError):
        price_window_opens(market, date(2026, 10, 20))


def test_ac_011_16_synthetic_publications_never_precede_the_window(repo: Path) -> None:
    """The generator publishes delivery day D on D-1 (``publication_time``); for every D of 2026
    that instant is at or after the window opens on D-1, so the watermark never hides a curve."""
    params = price_params(repo / "config" / "calibration" / "prices.yaml")
    for day in DAYS_2026:
        published = publication_time(day, SEED, params)
        assert published >= price_window_opens("PL:RCE", day - timedelta(days=1)), day
