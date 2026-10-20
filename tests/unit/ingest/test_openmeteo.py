"""AC-002-06/-07: Open-Meteo mapping on recorded fixtures, run-anchored events, unchanged runs."""

from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from pathlib import Path

import httpx
import respx

from ems.contracts.envelope import to_json_bytes, validate_strict
from ems.ingest.catalog.build import load_overrides
from ems.ingest.live.http import Fetcher
from ems.ingest.live.openmeteo import (
    DMI_HARMONIE,
    ICON_D2,
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
