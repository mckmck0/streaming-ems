"""Fetch a real-data window once (spec 008, plan section 2): raw responses into a recording.

For each real-data site (``live.locations``): ERA5 temperature, wind and GHI, the satellite's
10-minute GHI, DNI and DHI, and every archived forecast run issued in ``[start, end)``; for the
market, every PSE ``rce-pln`` business day the window touches. Responses are stored under
deterministic names (``data/recordings/<name>/raw/...``), so a rerun requests only what is missing
and a crash loses nothing. ``manifest.json`` is derived from the raw folder after each fetch.

Requests are paced below Open-Meteo's free-tier limits and the up-front estimate keeps one fetch
below its daily limit. A final 429 (other traffic, or two fetches in a day) stops the fetch with
``complete: false`` and exit code 4; a rerun resumes. The build step (``history.build``) works
offline on what is stored here.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import platform
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import structlog

from ems.contracts.time import local_date
from ems.ingest.catalog.sites import Site
from ems.ingest.generator import load_generator_config, load_sites
from ems.ingest.live.http import Fetched, Fetcher, NonRetryableError, RetryableError
from ems.ingest.live.openmeteo import (
    SINGLE_RUN_STEPS,
    SINGLE_RUNS_CUSTOMER_HOST,
    SINGLE_RUNS_HOST,
    model_for,
    single_run_params,
)
from ems.ingest.live.pse import REPORTS, PseSource
from ems.ingest.sources.recording import write_atomic, write_raw
from ems.settings import EmsSettings

log = structlog.get_logger(__name__)

SINGLE_RUNS_ARCHIVE_START = date(2026, 4, 2)
ERA5_LAG_DAYS = 6  # ERA5's documented five-day delay plus one day
OPEN_METEO_MIN_INTERVAL_S = 0.8  # <= 75 a minute, <= 4,500 an hour (free tier: 600, 5,000)
PSE_MIN_INTERVAL_S = 1.0  # PSE publishes no limit (VERIFY)
DAILY_BUDGET = 9000.0  # weighted Open-Meteo calls one fetch may need (free tier: 10,000 a day)
RUN_CADENCE = timedelta(hours=3)
MARKET = "rce-pln"
NOT_AVAILABLE = "The requested model run is not available"

ERA5_HOURLY = ("temperature_2m", "wind_speed_10m", "shortwave_radiation")
SATELLITE_HOURLY = ("shortwave_radiation", "direct_normal_irradiance", "diffuse_radiation")
FREE_HOSTS = {
    "era5": "https://archive-api.open-meteo.com",
    "satellite": "https://satellite-api.open-meteo.com",
    "open-meteo-single-runs": SINGLE_RUNS_HOST,
}
CUSTOMER_HOSTS = {
    "era5": "https://customer-archive-api.open-meteo.com",
    "satellite": "https://customer-satellite-api.open-meteo.com",
    "open-meteo-single-runs": SINGLE_RUNS_CUSTOMER_HOST,
}
ATTRIBUTION = {
    "open-meteo-single-runs": {
        "licence": "CC BY 4.0",
        "attribution": "Weather data by Open-Meteo.com (DWD ICON-D2, DMI HARMONIE AROME Europe)",
    },
    "satellite": {
        "licence": "VERIFY (DWD / EUMETSAT MTG via Open-Meteo; risk L6)",
        "attribution": "Satellite radiation by Open-Meteo.com (DWD, EUMETSAT MTG)",
    },
    "era5": {
        "licence": "CC BY 4.0",
        "attribution": "Contains modified Copernicus Climate Change Service information 2026; "
        "Hersbach et al., ERA5 hourly data on single levels, DOI 10.24381/cds.adbb2d47; "
        "via Open-Meteo.com",
    },
    "pse-rce": {
        "licence": "unpublished (risk L3); kept out of git",
        "attribution": "Polskie Sieci Elektroenergetyczne S.A., Raporty API (rce-pln)",
    },
}

Wall = Callable[[], datetime]
Monotonic = Callable[[], float]
Sleep = Callable[[float], Awaitable[None]]


class FetchRefused(ValueError):
    """The fetch must not run (or go on) as asked; nothing is stored for it. CLI exit 2."""


@dataclass(frozen=True)
class FetchResult:
    root: Path
    complete: bool
    exit_code: int  # 0 complete, 4 incomplete after a final 429 or 5xx (rerun later)


def call_weight(n_variables: int, days: float) -> float:
    """Open-Meteo's published call weight of one request."""
    return max(1.0, n_variables / 10) * max(1.0, days / 14)


@dataclass
class Pacer:
    """At least ``min_interval_s`` between the requests of one host family."""

    min_interval_s: float
    monotonic: Monotonic
    sleep: Sleep
    _last: float | None = None

    async def wait(self) -> None:
        if self._last is not None:
            gap = self.min_interval_s - (self.monotonic() - self._last)
            if gap > 0:
                await self.sleep(gap)
        self._last = self.monotonic()


class PacedFetcher(Fetcher):
    """A ``Fetcher`` that waits for its ``Pacer`` before each request (retries keep their own
    back-off).
    """

    def __init__(self, source: str, pacer: Pacer, **kwargs: Any) -> None:
        super().__init__(source, **kwargs)
        self.pacer = pacer

    async def get(
        self,
        url: str,
        params: Any = None,
        headers: Any = None,
    ) -> Fetched:
        await self.pacer.wait()
        return await super().get(url, params=params, headers=headers)


# ── the plan of requests ─────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Request:
    folder: str  # raw folder, also the meta "source"
    stem: str  # path under the folder, without extension
    weight: float  # Open-Meteo call weight (0 for PSE)
    site: Site | None = None
    issued_at: datetime | None = None
    day: date | None = None


@dataclass
class Plan:
    start: date
    end: date
    sites: list[Site]
    requests: list[Request] = field(default_factory=list)


def runs_in(start: date, end: date) -> list[datetime]:
    t = datetime.combine(start, datetime.min.time(), UTC)
    stop = datetime.combine(end, datetime.min.time(), UTC)
    out = []
    while t < stop:
        out.append(t)
        t += RUN_CADENCE
    return out


def business_days(start: date, end: date) -> list[date]:
    """Local delivery days ``local_date(start)`` … ``local_date(end - 1 s)``. For whole-UTC-day
    windows the day after is always published after ``end``, so it is never needed.
    """
    first = local_date(datetime.combine(start, datetime.min.time(), UTC))
    last = local_date(datetime.combine(end, datetime.min.time(), UTC) - timedelta(seconds=1))
    return [first + timedelta(days=i) for i in range((last - first).days + 1)]


def make_plan(start: date, end: date, sites: list[Site]) -> Plan:
    plan = Plan(start, end, sites)
    span = (end - start).days + 1  # both archive requests include the end date
    for site in sites:
        loc = site.location_id
        plan.requests.append(Request("era5", loc, call_weight(len(ERA5_HOURLY), span), site=site))
        plan.requests.append(
            Request("satellite", loc, call_weight(len(SATELLITE_HOURLY), span), site=site)
        )
    for day in business_days(start, end):
        plan.requests.append(Request("pse-rce", f"{day.isoformat()}-p01", 0.0, day=day))
    run_weight = call_weight(9, SINGLE_RUN_STEPS / 96)
    for site in sites:
        for issued in runs_in(start, end):
            stem = f"{site.location_id}/{issued:%Y%m%dT%H%M}"
            plan.requests.append(
                Request("open-meteo-single-runs", stem, run_weight, site=site, issued_at=issued)
            )
    return plan


# ── stored state ─────────────────────────────────────────────────────────────────────────────────


def _base(root: Path, req: Request) -> Path:
    return root / "raw" / req.folder / req.stem


def _stored(root: Path, req: Request) -> bool:
    meta = _base(root, req).with_name(Path(req.stem).name + ".meta.json")
    if not meta.exists():
        return False
    return int(json.loads(meta.read_text()).get("status", 0)) == 200


def _missing_marker(root: Path, req: Request) -> Path:
    return _base(root, req).with_name(Path(req.stem).name + ".missing.json")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


# ── checks ───────────────────────────────────────────────────────────────────────────────────────


def check_window(start: date, end: date, today: date) -> None:
    if start >= end:
        raise FetchRefused(f"empty window: start {start} must be before end {end}")
    if start < SINGLE_RUNS_ARCHIVE_START:
        raise FetchRefused(
            f"window starts {start}, before the Single Runs archive start "
            f"{SINGLE_RUNS_ARCHIVE_START}"
        )
    latest = today - timedelta(days=ERA5_LAG_DAYS)
    if end > latest:
        raise FetchRefused(
            f"window ends {end}, after {latest}: ERA5 lags real time, so end must be at least "
            f"{ERA5_LAG_DAYS} days before today ({today})"
        )


def check_sites(settings: EmsSettings, cfg_path: Path) -> list[Site]:
    cfg = load_generator_config(cfg_path)
    catalog = {s.location_id: s for s in load_sites(settings.config_dir, cfg)}
    if not cfg.live_locations:
        raise FetchRefused(f"{cfg_path}: live.locations is empty (no real-data site)")
    sites = []
    for loc in cfg.live_locations:
        site = catalog.get(loc)
        if site is None:
            raise FetchRefused(f"real-data site {loc} is not in the catalog of {cfg_path}")
        if site.bess is not None and site.bess.control == "site":
            raise FetchRefused(
                f"real-data site {loc} has a site-controlled battery, which would dispatch on "
                "synthetic prices while the recording carries real RCE; use an EMS-managed site"
            )
        sites.append(site)
    return sites


def check_rerun(root: Path, cfg_path: Path, start: date, end: date, sites: list[Site]) -> None:
    copy = root / "generator.yaml"
    if copy.exists() and copy.read_bytes() != cfg_path.read_bytes():
        raise FetchRefused(
            f"recording {root.name} was fetched with another generator config; use a new name"
        )
    manifest = root / "manifest.json"
    if not manifest.exists():
        return
    old = json.loads(manifest.read_text())
    if old["window"] != {"start": start.isoformat(), "end": end.isoformat()}:
        raise FetchRefused(
            f"recording {root.name} holds the window {old['window']['start']} … "
            f"{old['window']['end']}; use a new name for another window"
        )
    if [s["location_id"] for s in old["sites"]] != [s.location_id for s in sites]:
        raise FetchRefused(f"recording {root.name} holds other sites; use a new name")


def check_budget(root: Path, plan: Plan) -> None:
    need = sum(r.weight for r in plan.requests if not _stored(root, r))
    if need > DAILY_BUDGET:
        raise FetchRefused(
            f"this fetch needs about {need:.0f} weighted Open-Meteo calls, more than the "
            f"{DAILY_BUDGET:.0f} one fetch may use (free tier: 10,000 a day); split the window "
            "or the sites and fetch over several days"
        )


def check_era5(body: bytes, site: Site, start: date, end: date) -> None:
    """Temperature and wind must exist for every hour in ``[start, end]`` before ERA5 is stored."""
    hourly = json.loads(body)["hourly"]
    values = {int(t): i for i, t in enumerate(hourly["time"])}
    t = datetime.combine(start, datetime.min.time(), UTC)
    stop = datetime.combine(end, datetime.min.time(), UTC)
    while t <= stop:
        i = values.get(int(t.timestamp()))
        if i is None or any(hourly[v][i] is None for v in ("temperature_2m", "wind_speed_10m")):
            raise FetchRefused(
                f"ERA5 at {site.location_id} has no temperature or wind for "
                f"{t:%Y-%m-%dT%H:%M}Z yet (ERA5 lags real time by about 5 days); retry later "
                "or end the window earlier"
            )
        t += timedelta(hours=1)


# ── fetching ─────────────────────────────────────────────────────────────────────────────────────


@dataclass
class Clients:
    open_meteo: PacedFetcher
    pse: PseSource
    hosts: dict[str, str]
    headers: dict[str, str]
    secrets: list[str]

    async def aclose(self) -> None:
        await self.open_meteo.aclose()
        await self.pse.fetcher.aclose()


def _clients(settings: EmsSettings, wall: Wall, monotonic: Monotonic, sleep: Sleep) -> Clients:
    key = settings.open_meteo_api_key.get_secret_value() if settings.open_meteo_api_key else None
    secrets = [key] if key else []
    common: dict[str, Any] = {"clock": wall, "sleep": sleep, "secrets": secrets}
    open_meteo = PacedFetcher(
        "open-meteo", Pacer(OPEN_METEO_MIN_INTERVAL_S, monotonic, sleep), **common
    )
    pse_fetcher = PacedFetcher("pse-rce", Pacer(PSE_MIN_INTERVAL_S, monotonic, sleep), **common)
    return Clients(
        open_meteo=open_meteo,
        pse=PseSource(pse_fetcher, REPORTS[MARKET], settings.mappings_dir),
        hosts=CUSTOMER_HOSTS if key else FREE_HOSTS,
        headers={"X-Api-Key": key} if key else {},
        secrets=secrets,
    )


def _params(req: Request, start: date, end: date) -> tuple[str, dict[str, Any]]:
    assert req.site is not None
    site = req.site
    dates = {"start_date": start.isoformat(), "end_date": end.isoformat()}
    if req.folder == "era5":
        return "/v1/archive", {
            "latitude": site.lat,
            "longitude": site.lon,
            "elevation": site.elevation_m,
            **dates,
            "hourly": ",".join(ERA5_HOURLY),
            "models": "era5",
            "wind_speed_unit": "ms",
            "timezone": "UTC",
            "timeformat": "unixtime",
        }
    if req.folder == "satellite":
        return "/v1/archive", {
            "latitude": site.lat,
            "longitude": site.lon,
            **dates,
            "hourly": ",".join(SATELLITE_HOURLY),
            "models": "satellite_radiation_seamless",
            "temporal_resolution": "native",
            "timezone": "UTC",
            "timeformat": "unixtime",
        }
    assert req.issued_at is not None
    return "/v1/forecast", single_run_params(site, model_for(site), req.issued_at)


async def _fetch_one(
    root: Path, req: Request, clients: Clients, start: date, end: date, wall: Wall
) -> None:
    if req.folder == "pse-rce":
        assert req.day is not None
        pages = await clients.pse.fetch_pages(req.day)
        for n, page in enumerate(pages, start=1):
            base = root / "raw" / "pse-rce" / f"{req.day.isoformat()}-p{n:02d}"
            write_raw(base, page, clients.secrets, source="pse-rce")
        return
    path, params = _params(req, start, end)
    url = clients.hosts[req.folder] + path
    try:
        fetched = await clients.open_meteo.get(url, params=params, headers=clients.headers)
    except NonRetryableError as exc:
        if exc.status in (401, 403):
            host = httpx.URL(exc.url).host
            raise FetchRefused(
                f"{host} answered {exc.status}: the API key does not cover this API "
                "(the Historical and Satellite APIs need a Professional plan); unset "
                "EMS_OPEN_METEO_API_KEY to use the free tier"
            ) from None
        if (
            req.folder == "open-meteo-single-runs"
            and exc.status == 400
            and NOT_AVAILABLE in exc.body
        ):
            marker = {
                "source": req.folder,
                "url": exc.url,
                "status": exc.status,
                "reason": json.loads(exc.body).get("reason", exc.body[:300]),
                "location_id": req.site.location_id if req.site else None,
                "run": req.issued_at.isoformat() if req.issued_at else None,
                "fetched_at": wall().isoformat(),
            }
            write_atomic(_missing_marker(root, req), (json.dumps(marker, indent=2) + "\n").encode())
            log.warning("history_run_missing", stem=req.stem)
            return
        raise
    if req.folder == "era5":
        assert req.site is not None
        check_era5(fetched.body, req.site, start, end)
    write_raw(_base(root, req), fetched, clients.secrets, source=req.folder)
    _missing_marker(root, req).unlink(missing_ok=True)


# ── manifest ─────────────────────────────────────────────────────────────────────────────────────


def _grid_cells(root: Path) -> dict[str, dict[str, Any]]:
    cells: dict[str, dict[str, Any]] = {}
    for folder in ("era5", "satellite", "open-meteo-single-runs"):
        for meta in sorted((root / "raw" / folder).rglob("*.meta.json")):
            body = meta.with_name(meta.name.removesuffix(".meta.json") + ".json")
            data = json.loads(body.read_bytes())
            name = body.relative_to(root / "raw").as_posix()
            cells[name] = {k: data.get(k) for k in ("latitude", "longitude", "elevation")}
    return cells


def write_manifest(
    root: Path,
    plan: Plan,
    cfg_path: Path,
    clients: Clients,
    elapsed_s: float,
    today: date,
) -> Path:
    from ems.ingest.runner import _git_commit, _versions

    stored = {(r.folder, r.stem): _stored(root, r) for r in plan.requests}
    marked = {
        (r.folder, r.stem): _missing_marker(root, r).exists()
        for r in plan.requests
        if r.folder == "open-meteo-single-runs"
    }
    missing = [
        json.loads(_missing_marker(root, r).read_text())
        for r in plan.requests
        if marked.get((r.folder, r.stem)) and not stored[(r.folder, r.stem)]
    ]
    complete = all(stored[k] or marked.get(k, False) for k in stored)
    counts: dict[str, int] = {}
    for (folder, _stem), ok in stored.items():
        if ok:
            counts[folder] = counts.get(folder, 0) + 1
    hosts = clients.hosts
    manifest = {
        "kind": "ems-gen-history",
        "window": {"start": plan.start.isoformat(), "end": plan.end.isoformat()},
        "market": MARKET,
        "sites": [
            {
                "location_id": s.location_id,
                "lat": s.lat,
                "lon": s.lon,
                "elevation_m": s.elevation_m,
                "model": model_for(s).request,
            }
            for s in plan.sites
        ],
        "config_digest": _sha256(cfg_path),
        "sources": {
            "era5": {
                "endpoint": hosts["era5"] + "/v1/archive",
                "hourly": list(ERA5_HOURLY),
                **ATTRIBUTION["era5"],
            },
            "satellite": {
                "endpoint": hosts["satellite"] + "/v1/archive",
                "hourly": list(SATELLITE_HOURLY),
                "model": "satellite_radiation_seamless",
                **ATTRIBUTION["satellite"],
            },
            "open-meteo-single-runs": {
                "endpoint": hosts["open-meteo-single-runs"] + "/v1/forecast",
                "forecast_minutely_15": SINGLE_RUN_STEPS,
                **ATTRIBUTION["open-meteo-single-runs"],
            },
            "pse-rce": {"endpoint": clients.pse.base_url + "/" + MARKET, **ATTRIBUTION["pse-rce"]},
        },
        "expected": len(plan.requests),
        "counts": counts,
        "missing_runs": missing,
        "complete": complete,
        "grid_cells": _grid_cells(root),
        "era5t": (today - plan.end).days < 92,  # preliminary ERA5, possibly revised later
        "fetch_elapsed_s": round(elapsed_s, 1),
        "written_at": datetime.now(UTC).isoformat(),
        "git_commit": _git_commit(Path.cwd()),
        "versions": _versions(),
        "hardware": {
            "machine": platform.machine(),
            "system": platform.system(),
            "release": platform.release(),
            "cpus": os.cpu_count(),
        },
    }
    path = root / "manifest.json"
    write_atomic(path, (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode())
    return path


def _copy_config(root: Path, cfg_path: Path) -> None:
    """Keep the generator config the fetch used; build and replay --background read it."""
    root.mkdir(parents=True, exist_ok=True)
    copy = root / "generator.yaml"
    if not copy.exists():
        write_atomic(copy, cfg_path.read_bytes())


# ── entry point ──────────────────────────────────────────────────────────────────────────────────


async def fetch(
    settings: EmsSettings,
    cfg_path: Path,
    name: str,
    start: date,
    end: date,
    *,
    today: date,
    wall: Wall = lambda: datetime.now(UTC),
    monotonic: Monotonic = time.monotonic,
    sleep: Sleep = asyncio.sleep,
) -> FetchResult:
    """Fetch the raw data of ``[start, end)`` for the real-data sites into recording ``name``."""
    check_window(start, end, today)
    sites = check_sites(settings, cfg_path)
    root = settings.data_dir / "recordings" / name
    check_rerun(root, cfg_path, start, end, sites)
    plan = make_plan(start, end, sites)
    check_budget(root, plan)

    _copy_config(root, cfg_path)
    clients = _clients(settings, wall, monotonic, sleep)
    t0 = monotonic()
    exit_code = 0
    try:
        for req in plan.requests:
            if _stored(root, req):
                continue
            try:
                await _fetch_one(root, req, clients, start, end, wall)
            except RetryableError as exc:
                log.warning("history_fetch_stopped", stem=req.stem, status=exc.status)
                exit_code = 4
                break
    finally:
        await clients.aclose()
    write_manifest(root, plan, cfg_path, clients, monotonic() - t0, today)
    complete = bool(json.loads((root / "manifest.json").read_text())["complete"])
    if exit_code == 0 and not complete:
        exit_code = 4
    return FetchResult(root=root, complete=complete, exit_code=exit_code)
