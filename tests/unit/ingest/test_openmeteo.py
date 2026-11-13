"""AC-002-06/-07: Open-Meteo mapping on recorded fixtures, run-anchored events, unchanged runs.

Spec 008: AC-008-05 (live events unchanged; archived runs, leading gaps), AC-008-11
(``source_system`` and ``source_record_ref`` of archived runs) and AC-008-01 (Single Runs API
requests).
"""

from __future__ import annotations

import asyncio
import copy
import dataclasses
import hashlib
import itertools
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl

import httpx
import pytest
import respx

import ems.ingest.live.openmeteo as openmeteo
from ems.contracts.envelope import WeatherForecastEvent, to_json_bytes, validate_strict
from ems.contracts.time import QUARTER, quarter_index
from ems.ingest.catalog.build import load_overrides
from ems.ingest.catalog.sites import Site
from ems.ingest.generator import load_generator_config, load_sites
from ems.ingest.live.http import Fetcher
from ems.ingest.live.openmeteo import (
    DMI_HARMONIE,
    ICON_D2,
    Model,
    OpenMeteoSource,
    build_event,
    forecast_params,
    model_for,
    parse_meta,
)

REPO = Path(__file__).resolve().parents[3]
FIX = REPO / "tests" / "fixtures" / "openmeteo"
MAPPINGS = REPO / "mappings"
SITE = load_overrides(REPO / "config" / "sites.yaml", 4242)["PL-LOC-000001"]
META = json.loads((FIX / "meta_dwd_icon_d2.json").read_text())
FORECAST = json.loads((FIX / "forecast_PL-LOC-000001.json").read_text())
ISSUED = parse_meta(META).issued_at
NOW = ISSUED + timedelta(hours=3)

# 008/T01: SHA-256 of the event build_event gives for forecast_PL-LOC-000001.json (issued_at from
# its .meta.json), as model_dump(mode="json", by_alias=True) without ingested_at, json.dumps with
# sorted keys. Pinned on 48fafee before spec 008: live events must stay byte-identical after
# 008/T16 (AC-008-05).
LIVE_EVENT_DIGEST = "596a6c1ee781a8dc994ea65d72719f97c650f6c066e0d5763e5f13f2813e8faa"


def test_ac_002_06_forecast_event_from_fixture() -> None:
    event = build_event(SITE, ICON_D2, ISSUED, FORECAST, MAPPINGS, NOW)
    assert event is not None
    steps = event.payload.steps
    assert 180 <= len(steps) <= 192  # truncated before the first step with a missing value
    assert steps[0].valid_start == ISSUED and steps[0].lead_time == "PT15M"
    assert event.issued_at == ISSUED and event.payload.forecast_model == "icon_d2"
    assert event.source_system == "open-meteo" and not event.synthetic
    assert all(s.radiation_label == "preceding_mean" for s in steps)
    assert set(steps[0].wind_speed_ms) == {"10", "100", "120"}
    validate_strict(json.loads(to_json_bytes(event)))


def test_model_rule_by_longitude() -> None:
    assert model_for(SITE) is ICON_D2
    warsaw = SITE.model_copy(update={"lat": 52.23, "lon": 21.01})
    assert model_for(warsaw) is DMI_HARMONIE


def test_request_is_run_anchored() -> None:
    params = forecast_params(SITE, ICON_D2, ISSUED)
    assert params["start_minutely_15"] == (ISSUED + timedelta(minutes=15)).strftime(
        "%Y-%m-%dT%H:%M"
    )
    assert params["end_minutely_15"] == (ISSUED + timedelta(hours=48)).strftime("%Y-%m-%dT%H:%M")
    assert params["models"] == "icon_d2" and params["timeformat"] == "unixtime"


async def no_sleep(_s: float) -> None:
    return None


def source(key: str | None = None) -> OpenMeteoSource:
    f = Fetcher(
        "open-meteo", client=httpx.AsyncClient(), sleep=no_sleep, secrets=[key] if key else []
    )
    return OpenMeteoSource(f, MAPPINGS, api_key=key)


@respx.mock
def test_ac_002_07_unchanged_run_is_not_emitted_twice() -> None:
    respx.get("https://api.open-meteo.com/data/dwd_icon_d2/static/meta.json").mock(
        return_value=httpx.Response(200, json=META)
    )
    fc = respx.get(url__startswith="https://api.open-meteo.com/v1/forecast").mock(
        return_value=httpx.Response(200, json=FORECAST)
    )
    src = source()
    first = asyncio.run(src.poll(SITE, NOW, NOW))
    second = asyncio.run(src.poll(SITE, NOW + timedelta(minutes=15), NOW))
    assert first is not None and second is None and fc.call_count == 1


@respx.mock
def test_run_not_used_before_availability_grace() -> None:
    respx.get("https://api.open-meteo.com/data/dwd_icon_d2/static/meta.json").mock(
        return_value=httpx.Response(200, json=META)
    )
    available = parse_meta(META).available_at
    assert asyncio.run(source().poll(SITE, available + timedelta(minutes=5), NOW)) is None


@respx.mock
def test_customer_api_uses_header_key() -> None:
    key = "om-key-xyz"
    meta = respx.get("https://customer-api.open-meteo.com/data/dwd_icon_d2/static/meta.json").mock(
        return_value=httpx.Response(200, json=META)
    )
    respx.get(url__startswith="https://customer-api.open-meteo.com/v1/forecast").mock(
        return_value=httpx.Response(200, json=FORECAST)
    )
    event = asyncio.run(source(key).poll(SITE, NOW, NOW))
    assert event is not None
    assert meta.calls[0].request.headers["X-Api-Key"] == key
    assert key not in str(meta.calls[0].request.url)


# ── spec 008 ─────────────────────────────────────────────────────────────────────────────────────


def test_ac_008_05_live_event_unchanged() -> None:
    """008/T01: the live fixture's event minus ``ingested_at`` equals the one pinned on 48fafee."""
    meta = json.loads((FIX / "forecast_PL-LOC-000001.meta.json").read_text())
    issued = datetime.fromisoformat(meta["issued_at"])
    event = build_event(SITE, ICON_D2, issued, FORECAST, MAPPINGS, NOW)
    assert event is not None
    row = event.model_dump(mode="json", by_alias=True)
    row.pop("ingested_at")
    assert hashlib.sha256(json.dumps(row, sort_keys=True).encode()).hexdigest() == (
        LIVE_EVENT_DIGEST
    )


def _history_site(location_id: str) -> Site:
    """A site of the default catalog with 20 synthetic locations (``generator.history.yaml``)."""
    cfg = load_generator_config(REPO / "config" / "generator.yaml")
    cfg = dataclasses.replace(cfg, synthetic_locations=20)
    return {s.location_id: s for s in load_sites(REPO / "config", cfg)}[location_id]


def test_ac_008_01_single_run_params_are_the_live_ones_for_a_fixed_run() -> None:
    run = datetime(2026, 5, 15, 3, tzinfo=UTC)
    live = forecast_params(SITE, ICON_D2, run)
    expected = {k: v for k, v in live.items() if k not in ("start_minutely_15", "end_minutely_15")}
    expected |= {"run": "2026-05-15T03:00", "forecast_minutely_15": 193}
    assert openmeteo.single_run_params(SITE, ICON_D2, run) == expected


@pytest.mark.parametrize(
    ("fixture", "location_id", "model"),
    [
        ("single_run_icon_d2_PL-LOC-000001_20260515T0300", "PL-LOC-000001", ICON_D2),
        ("single_run_dmi_PL-LOC-000004_20260515T0300", "PL-LOC-000004", DMI_HARMONIE),
    ],
    ids=["icon_d2", "dmi"],
)
def test_ac_008_01_single_run_params_give_the_recorded_query(
    fixture: str, location_id: str, model: Model
) -> None:
    """The captured archive fixtures were fetched with the very query single_run_params builds."""
    site = _history_site(location_id)
    assert model_for(site) is model
    meta = json.loads((FIX / f"{fixture}.meta.json").read_text())
    params = openmeteo.single_run_params(site, model, datetime.fromisoformat(meta["issued_at"]))
    assert {k: str(v) for k, v in params.items()} == dict(parse_qsl(meta["query"]))


def test_ac_008_01_single_runs_hosts() -> None:
    assert openmeteo.SINGLE_RUNS_HOST == "https://single-runs-api.open-meteo.com"
    assert openmeteo.SINGLE_RUNS_CUSTOMER_HOST == "https://customer-single-runs-api.open-meteo.com"


# ── 008/T09: events from archived runs (AC-008-05, -11) ──────────────────────────────────────────

ICON_RUN = "single_run_icon_d2_PL-LOC-000001_20260515T0300"
DMI_RUN = "single_run_dmi_PL-LOC-000004_20260515T0300"
HISTORY = "open-meteo-single-runs"
RUN_ID = "spring-2026"
FETCHED = datetime(2026, 11, 13, 12, 0, 5, tzinfo=UTC)  # a recorded fetch time, not the clock
# One column per leading gap, so "any missing value" is a gap, not only missing radiation.
GAP_COLUMNS = (
    "shortwave_radiation",
    "temperature_2m",
    "cloud_cover",
    "wind_speed_10m",
    "direct_normal_irradiance",
)


def _archived(name: str) -> tuple[dict[str, Any], datetime]:
    payload = json.loads((FIX / f"{name}.json").read_text())
    meta = json.loads((FIX / f"{name}.meta.json").read_text())
    return payload, datetime.fromisoformat(meta["issued_at"])


def _archived_event(
    name: str, location_id: str, model: Model, payload: dict[str, Any] | None = None
) -> WeatherForecastEvent | None:
    stored, issued = _archived(name)
    return openmeteo.build_event(
        _history_site(location_id),
        model,
        issued,
        stored if payload is None else payload,
        MAPPINGS,
        FETCHED,
        run_id=RUN_ID,
        source_system=HISTORY,
    )


def _with_gaps(payload: dict[str, Any], rows: range) -> dict[str, Any]:
    """A copy of ``payload`` with one value nulled in each of ``rows``."""
    out = copy.deepcopy(payload)
    for j, row in enumerate(rows):
        out["minutely_15"][GAP_COLUMNS[j % len(GAP_COLUMNS)]][row] = None
    return out


@pytest.mark.parametrize(
    ("fixture", "location_id", "model", "first_row", "n_steps", "first_lead"),
    [
        # step 0 at the run time has null radiation (dropped); cloud_cover is null at the last step
        (ICON_RUN, "PL-LOC-000001", ICON_D2, 1, 191, "PT15M"),
        # radiation is null at steps 0-2: the run-time step is dropped, two leading gaps skipped
        (DMI_RUN, "PL-LOC-000004", DMI_HARMONIE, 3, 190, "PT45M"),
    ],
    ids=["icon_d2", "dmi"],
)
def test_ac_008_05_archived_run_gives_a_run_anchored_event(
    fixture: str, location_id: str, model: Model, first_row: int, n_steps: int, first_lead: str
) -> None:
    """ICON-D2: 191 steps from +15 min. DMI: the first step ends at +45 min, 190 steps."""
    payload, issued = _archived(fixture)
    event = _archived_event(fixture, location_id, model)
    assert event is not None
    steps = event.payload.steps
    assert len(steps) == n_steps
    assert steps[0].valid_end == issued + first_row * QUARTER
    assert steps[0].valid_start == issued + (first_row - 1) * QUARTER
    assert steps[0].lead_time == first_lead
    assert steps[-1].valid_end == issued + (first_row + n_steps - 1) * QUARTER
    assert all(b.valid_start == a.valid_end for a, b in itertools.pairwise(steps))
    m = payload["minutely_15"]
    assert steps[0].ghi_wm2 == m["shortwave_radiation"][first_row]  # the values of that row
    assert steps[0].temp_air_c == m["temperature_2m"][first_row]
    assert event.interval_start == issued + (first_row - 1) * QUARTER
    assert event.interval_end == steps[-1].valid_end
    assert event.t == quarter_index(event.interval_start)
    assert event.issued_at == issued and event.payload.forecast_model == model.request
    assert isinstance(validate_strict(json.loads(to_json_bytes(event))), WeatherForecastEvent)


@pytest.mark.parametrize(
    ("fixture", "location_id", "model", "ref"),
    [
        (
            ICON_RUN,
            "PL-LOC-000001",
            ICON_D2,
            "open-meteo-single-runs:icon_d2:2026-05-15T03:00:00+00:00",
        ),
        (
            DMI_RUN,
            "PL-LOC-000004",
            DMI_HARMONIE,
            "open-meteo-single-runs:dmi_harmonie_arome_europe:2026-05-15T03:00:00+00:00",
        ),
    ],
    ids=["icon_d2", "dmi"],
)
def test_ac_008_11_archived_run_source_system_and_record_ref(
    fixture: str, location_id: str, model: Model, ref: str
) -> None:
    event = _archived_event(fixture, location_id, model)
    assert event is not None
    assert event.source_system == HISTORY and event.source_record_ref == ref
    assert event.run_id == RUN_ID and not event.synthetic
    assert event.ingested_at == FETCHED


def test_ac_008_05_at_most_four_leading_gaps_are_skipped_in_an_archived_run() -> None:
    assert openmeteo.LEADING_GAP_MAX == 4
    payload, issued = _archived(ICON_RUN)
    four = _archived_event(ICON_RUN, "PL-LOC-000001", ICON_D2, _with_gaps(payload, range(1, 5)))
    assert four is not None
    assert four.payload.steps[0].valid_end == issued + timedelta(minutes=75)
    assert four.payload.steps[0].lead_time == "PT1H15M"
    assert four.interval_start == issued + timedelta(minutes=60)
    assert len(four.payload.steps) == 191 - 4
    assert four.payload.steps[0].temp_air_c == payload["minutely_15"]["temperature_2m"][5]
    five = _archived_event(ICON_RUN, "PL-LOC-000001", ICON_D2, _with_gaps(payload, range(1, 6)))
    assert five is None


def test_ac_008_05_live_response_with_leading_gaps() -> None:
    """The documented live edge case (plan §3): up to four leading gaps now give an event."""
    base = build_event(SITE, ICON_D2, ISSUED, FORECAST, MAPPINGS, NOW)
    assert base is not None
    four = build_event(SITE, ICON_D2, ISSUED, _with_gaps(FORECAST, range(4)), MAPPINGS, NOW)
    assert four is not None
    assert four.payload.steps[0].valid_end == ISSUED + timedelta(minutes=75)
    assert four.payload.steps == base.payload.steps[4:]
    assert four.source_system == "open-meteo"
    assert build_event(SITE, ICON_D2, ISSUED, _with_gaps(FORECAST, range(5)), MAPPINGS, NOW) is None


def test_ac_008_05_a_gap_after_the_first_complete_step_truncates() -> None:
    """Only leading gaps are skipped; after the first complete step the run ends at a gap."""
    payload, issued = _archived(ICON_RUN)
    gappy = _with_gaps(payload, range(1, 2))
    gappy["minutely_15"]["wind_speed_100m"][3] = None
    event = _archived_event(ICON_RUN, "PL-LOC-000001", ICON_D2, gappy)
    assert event is not None
    assert [s.valid_end for s in event.payload.steps] == [issued + 2 * QUARTER]


def test_ac_008_05_a_missing_row_in_an_archived_run_raises() -> None:
    """``valid_end == issued_at + (first + k + 1) * 15 min`` is checked (build counts the run as
    malformed).
    """
    payload, _ = _archived(ICON_RUN)
    holed = copy.deepcopy(payload)
    for column in holed["minutely_15"].values():
        del column[10]
    with pytest.raises(ValueError):
        _archived_event(ICON_RUN, "PL-LOC-000001", ICON_D2, holed)
