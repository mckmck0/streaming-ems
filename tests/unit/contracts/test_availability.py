"""AC-008-08: one forecast availability delay per ``forecast_model`` (spec 008, plan §4).

The same table drives synthetic forecast emission, the history build and the rule in
``08-contracts.md``; ``available_at`` applies it per stream.
"""

from __future__ import annotations

import math
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from ruamel.yaml import YAML

from ems.contracts.availability import (
    FORECAST_DELAY_MIN,
    GRACE_MIN,
    MEASURED_MAX_MIN,
    available_at,
    forecast_delay,
)
from ems.contracts.envelope import AssetType, Direction, Market, Stream
from ems.contracts.examples import energy, forecast_event, measurement_event, price_event
from ems.contracts.time import QUARTER, quarter_index
from ems.ingest.live.openmeteo import AVAILABILITY_GRACE, DMI_HARMONIE, ICON_D2

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
