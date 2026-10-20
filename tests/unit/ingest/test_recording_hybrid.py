"""AC-002-10/-11/-12: recordings without secrets, replay keeps ids, routing and hybrid mode."""

from __future__ import annotations

import asyncio
import dataclasses
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from ems.contracts.envelope import AssetType, Stream, check_event
from ems.ingest.generator import load_generator_config, load_sites
from ems.ingest.kafka.sink import SinkStats
from ems.ingest.live.openmeteo import parse_meta
from ems.ingest.runner import record, replay
from ems.ingest.sources.live_runner import LiveRunner, live_weather_override, mask_synthetic
from ems.ingest.sources.recording import as_replay, read_events
from ems.ingest.sources.router import RoutingError, route
from ems.settings import EmsSettings

REPO = Path(__file__).resolve().parents[3]
FIX = REPO / "tests" / "fixtures"
META = json.loads((FIX / "openmeteo" / "meta_dwd_icon_d2.json").read_text())
NOW = parse_meta(META).issued_at + timedelta(hours=3)  # 2026-10-20 21:00 UTC
SECRET = "om-secret-0001"


def live_cfg() -> Any:
    return load_generator_config(REPO / "config" / "generator.live.yaml")


def mock_sources() -> None:
    respx.get(
        url__regex=r"https://(customer-)?api\.open-meteo\.com/data/dwd_icon_d2/static/meta\.json"
    ).mock(return_value=httpx.Response(200, json=META))
    respx.get(url__regex=r"https://(customer-)?api\.open-meteo\.com/v1/forecast.*").mock(
        return_value=httpx.Response(
            200, json=json.loads((FIX / "openmeteo" / "forecast_PL-LOC-000001.json").read_text())
        )
    )
    rce = json.loads((FIX / "pse" / "rce-pln_2026-10-21.json").read_text())
    csdac = json.loads((FIX / "pse" / "csdac-pln_2026-10-21.json").read_text())
    empty = {"value": []}

    def pse(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "2026-10-21" in url:
            return httpx.Response(200, json=rce if "rce-pln" in url else csdac)
        return httpx.Response(200, json=empty)

    respx.get(url__startswith="https://api.raporty.pse.pl/api/").mock(side_effect=pse)


class Collect:
    """In-memory sink (satisfies the runner's Sink protocol)."""

    def __init__(self) -> None:
        self.events: list[Any] = []
        self.stats = SinkStats()

    def publish(self, event: Any) -> bool:
        self.events.append(event)
        return True

    def poll(self, timeout: float = 0) -> None:
        return None

    def close(self, timeout_s: float = 30.0) -> SinkStats:
        return self.stats


def settings(tmp: Path) -> EmsSettings:
    return EmsSettings(config_dir=REPO / "config", data_dir=tmp, open_meteo_api_key=SECRET)


def test_ac_002_12_routing() -> None:
    cfg = live_cfg()
    sites = load_sites(REPO / "config", cfg)
    r = route(cfg, sites)
    assert [s.location_id for s in r.live_sites] == ["PL-LOC-000001"]
    assert {s.location_id for s in r.synthetic_sites} == {"PL-LOC-000002", "PL-LOC-000003"}
    assert r.hybrid and r.price == "live"
    synth = mask_synthetic(cfg, price_live=True)
    assert (
        synth.stream_mode("price") == "off" and synth.stream_mode("weather_forecast") == "synthetic"
    )
    offline = load_generator_config(REPO / "config" / "generator.yaml")
    assert not route(offline, load_sites(REPO / "config", offline)).needs_live
    bad = dataclasses.replace(cfg, streams={**cfg.streams, "production": {"mode": "live"}})
    with pytest.raises(RoutingError):
        route(bad, sites)


@respx.mock
def test_ac_002_10_record_once_without_secrets(tmp_path: Path) -> None:
    mock_sources()
    root = asyncio.run(record(settings(tmp_path), live_cfg(), name="t", clock=lambda: NOW))
    events = list(read_events(root))
    streams = sorted({e.stream.value for e in events})
    assert streams == ["price", "weather_forecast"]
    markets = sorted(e.payload.market.value for e in events if e.stream is Stream.PRICE)
    assert markets == ["PL_RCE", "PL_SDAC"]  # D+1 published; today not in the fixtures
    for path in root.rglob("*"):
        if path.is_file():
            assert SECRET not in path.read_text(errors="ignore"), path
    raw = list((root / "raw").rglob("*.meta.json"))
    assert {p.parent.name for p in raw} == {
        "pse-rce",
        "pse-csdac",
        "open-meteo",
    }  # key went in a header
    manifest = json.loads((root / "manifest.json").read_text())
    assert manifest["counts"] == {"price": 2, "weather_forecast": 1} and manifest["live_sites"] == [
        "PL-LOC-000001"
    ]


@respx.mock
def test_ac_002_11_replay_keeps_ids(tmp_path: Path) -> None:
    mock_sources()
    root = asyncio.run(record(settings(tmp_path), live_cfg(), name="t", clock=lambda: NOW))
    recorded = list(read_events(root))

    sink = Collect()
    asyncio.run(replay(settings(tmp_path), "t", sink=sink, clock=lambda: NOW + timedelta(days=1)))
    assert [e.event_id for e in sink.events] == [e.event_id for e in recorded]
    for e in sink.events:
        assert e.source_system.startswith("replay:")
        check_event(e)  # the identity rule still holds for the replayed source
    assert as_replay(sink.events[0], NOW).source_system.count("replay:") == 1


@respx.mock
def test_ac_002_12_live_site_meters_follow_live_weather(tmp_path: Path) -> None:
    mock_sources()
    cfg = live_cfg()
    sites = load_sites(REPO / "config", cfg)
    r = route(cfg, sites)

    sink = Collect()
    runner = LiveRunner(
        settings(tmp_path), cfg, sink, r.live_sites, price_live=True, clock=lambda: NOW
    )
    asyncio.run(runner.once())
    forecast = runner.state.forecasts["PL-LOC-000001"]
    noon = datetime(2026, 10, 21, 10, 0, tzinfo=UTC)
    runner.live_meters(noon)
    meters = [e for e in sink.events if e.stream in (Stream.PRODUCTION, Stream.CONSUMPTION)]
    assert {e.location_id for e in meters} == {"PL-LOC-000001"}
    pv = next(
        m.value for e in meters for m in e.payload.measurements if m.asset_type is AssetType.PV
    )
    step = next(s for s in forecast.payload.steps if s.valid_start == noon)
    if step.ghi_wm2 < 1:
        assert pv == 0
    else:
        assert pv > 0
    # live weather replaced the synthetic truth for the live site only
    override = live_weather_override(r.live_sites, runner.state.forecasts)
    assert callable(override)
    assert all(e.synthetic for e in meters) and all(e.source_system == "synthetic" for e in meters)
    assert date(2026, 10, 21) in {
        e.payload.delivery_day for e in sink.events if e.stream is Stream.PRICE
    }
