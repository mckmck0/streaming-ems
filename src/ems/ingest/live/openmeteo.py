"""Open-Meteo weather forecasts: one event per model run and site, 15-minute steps, 48 h.

The forecast response carries neither the model nor the run time, so the run time comes from the
model's metadata (``/data/<model>/static/meta.json``) and the request asks for an explicit model and
a window anchored to that run. Re-polling an unchanged run therefore never emits a second event.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import structlog

from ems.contracts.envelope import WeatherForecastEvent
from ems.contracts.examples import forecast_event
from ems.contracts.time import QUARTER
from ems.ingest.catalog.sites import Site
from ems.ingest.live.http import Fetcher
from ems.ingest.mapping.engine import apply, builtin

log = structlog.get_logger(__name__)

FREE_HOST = "https://api.open-meteo.com"
CUSTOMER_HOST = "https://customer-api.open-meteo.com"
VARIABLES = (
    "shortwave_radiation,direct_normal_irradiance,diffuse_radiation,temperature_2m,cloud_cover,"
    "wind_speed_10m,wind_speed_100m,wind_speed_120m,wind_direction_10m"
)
STEPS = 192
AVAILABILITY_GRACE = timedelta(minutes=10)
# Open-Meteo's ICON-D2 grid (verified 2026-10-20): 43.18-58.08 N, 3.94 W-20.34 E.
ICON_D2_BOX = (43.18, 58.08, -3.94, 20.34)


@dataclass(frozen=True)
class Model:
    request: str  # value of the models= parameter, also the event's forecast_model
    meta: str  # name in /data/<meta>/static/meta.json


ICON_D2 = Model("icon_d2", "dwd_icon_d2")
DMI_HARMONIE = Model("dmi_harmonie_arome_europe", "dmi_harmonie_arome_europe")


def model_for(site: Site) -> Model:
    """ICON-D2 (native 15-minute radiation) inside its domain; DMI HARMONIE elsewhere in Poland."""
    lat0, lat1, lon0, lon1 = ICON_D2_BOX
    return ICON_D2 if lat0 <= site.lat <= lat1 and lon0 <= site.lon <= lon1 else DMI_HARMONIE


@dataclass(frozen=True)
class RunInfo:
    issued_at: datetime  # model initialisation time
    available_at: datetime


def parse_meta(payload: dict[str, Any]) -> RunInfo:
    return RunInfo(
        issued_at=datetime.fromtimestamp(int(payload["last_run_initialisation_time"]), tz=UTC),
        available_at=datetime.fromtimestamp(int(payload["last_run_availability_time"]), tz=UTC),
    )


def forecast_params(site: Site, model: Model, issued_at: datetime) -> dict[str, Any]:
    fmt = "%Y-%m-%dT%H:%M"
    return {
        "latitude": site.lat,
        "longitude": site.lon,
        "elevation": site.elevation_m,
        "models": model.request,
        "minutely_15": VARIABLES,
        "timezone": "UTC",
        "wind_speed_unit": "ms",
        "timeformat": "unixtime",
        "start_minutely_15": (issued_at + QUARTER).strftime(fmt),
        "end_minutely_15": (issued_at + STEPS * QUARTER).strftime(fmt),
    }


def build_event(
    site: Site,
    model: Model,
    issued_at: datetime,
    payload: dict[str, Any],
    mappings_dir: Path,
    ingested_at: datetime,
    run_id: str | None = None,
) -> WeatherForecastEvent | None:
    rows = apply(builtin("openmeteo.forecast.v1", mappings_dir), payload)
    steps: list[dict[str, Any]] = []
    for k, row in enumerate(rows):
        if row.get("quality") == "missing":
            break  # horizon ended early: keep the complete prefix
        if row["valid_end"] != issued_at + (k + 1) * QUARTER:
            raise ValueError(f"step {k} at {row['valid_end']}, expected the run-anchored window")
        steps.append(
            {
                "ghi_wm2": max(0.0, row["ghi_wm2"]),
                "dni_wm2": max(0.0, row["dni_wm2"]),
                "dhi_wm2": max(0.0, row["dhi_wm2"]),
                "temp_air_c": row["temp_air_c"],
                "cloud_cover_pct": round(min(100.0, max(0.0, row["cloud_cover_pct"]))),
                "wind_speed_ms": {
                    "10": max(0.0, row["wind_10"]),
                    "100": max(0.0, row["wind_100"]),
                    "120": max(0.0, row["wind_120"]),
                },
                "wind_dir_deg": round(row["wind_dir_deg"]) % 360,
            }
        )
    if not steps:
        return None
    return forecast_event(
        location_id=site.location_id,
        issued_at=issued_at,
        steps=steps,
        ingested_at=ingested_at,
        forecast_model=model.request,
        source_system="open-meteo",
        run_id=run_id,
        synthetic=False,
        source_record_ref=f"open-meteo:{model.request}:{issued_at.isoformat()}",
    )


@dataclass
class OpenMeteoSource:
    fetcher: Fetcher
    mappings_dir: Path
    api_key: str | None = None
    last_issued: dict[str, datetime] = field(default_factory=dict)

    @property
    def host(self) -> str:
        return CUSTOMER_HOST if self.api_key else FREE_HOST

    @property
    def headers(self) -> dict[str, str]:
        return {"X-Api-Key": self.api_key} if self.api_key else {}

    async def run_info(self, model: Model) -> RunInfo:
        fetched = await self.fetcher.get(
            f"{self.host}/data/{model.meta}/static/meta.json", headers=self.headers
        )
        return parse_meta(json.loads(fetched.body))

    async def poll(
        self, site: Site, now: datetime, ingested_at: datetime, run_id: str | None = None
    ) -> WeatherForecastEvent | None:
        """New event for the site's latest available run, or None when there is nothing new."""
        model = model_for(site)
        info = await self.run_info(model)
        if now < info.available_at + AVAILABILITY_GRACE:
            return None  # let every API server catch up with the run
        if self.last_issued.get(site.location_id) == info.issued_at:
            return None
        fetched = await self.fetcher.get(
            f"{self.host}/v1/forecast",
            params=forecast_params(site, model, info.issued_at),
            headers=self.headers,
        )
        event = build_event(
            site,
            model,
            info.issued_at,
            json.loads(fetched.body),
            self.mappings_dir,
            ingested_at,
            run_id,
        )
        if event is not None:
            self.last_issued[site.location_id] = info.issued_at
            log.info(
                "openmeteo_run",
                site=site.location_id,
                model=model.request,
                issued_at=str(info.issued_at),
            )
        return event
