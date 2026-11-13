"""Raw history recordings for tests (spec 008): ``fetch()`` against respx with synthetic bodies.

``make_recording`` runs the real ``ems.ingest.history.fetch.fetch`` exactly as
``test_history_fetch.py`` does (respx, a fake clock, an injected "today"), but every API answers
with a SYNTHETIC body built from the request, so any whole-UTC-day window works:

- ERA5 (hourly, ``timeformat=unixtime``): temperature and 10 m wind as smooth functions of time,
  GHI as 0.8 x the hour's mean clear-sky GHI (labelled at the hour's end, as ERA5);
- satellite (10-minute end labels): GHI 0.85 x clear-sky, DNI 0.5 x clear-sky DNI, DHI closing the
  sum, zero at night; ``Gap`` spans are null (all three variables);
- Single Runs: 193 ``minutely_15`` steps from the requested ``run``, the nine live variables; step 0
  has no radiation (as the archive), DMI also lacks GHI and DHI for the steps ending +15 and
  +30 min, ICON-D2 lacks cloud cover at +48 h (F3);
- PSE ``rce-pln``: one page per business day from ``local_day_slots``, published on D-1 at a chosen
  local time; optional revised rows, dropped slots or a real fixture body for a day.

The clear-sky values come from ``ems.ingest.synthetic.solar`` (Ineichen), so "daylight" here is the
build's daylight. No network: sockets are blocked while the fetch runs and respx answers every
request (any other request fails the test).
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import math
import shutil
import socket
from collections.abc import Callable, Collection, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any, NamedTuple
from urllib.parse import parse_qs, urlsplit

import httpx
import numpy as np
import pytest
import respx
from numpy.typing import NDArray
from ruamel.yaml import YAML

from ems.contracts.envelope import parse_event
from ems.contracts.time import WARSAW, local_day_slots
from ems.ingest.catalog.sites import Site
from ems.ingest.generator import load_generator_config, load_sites
from ems.ingest.history.fetch import NOT_AVAILABLE, fetch
from ems.ingest.synthetic.solar import solar_geometry
from ems.settings import EmsSettings

REPO = Path(__file__).resolve().parents[3]
CONFIG = REPO / "config"
MAPPINGS = REPO / "mappings"
FIX = REPO / "tests" / "fixtures"
PSE_FALL_BACK_DAY = FIX / "pse" / "history_rce-pln_2026-10-25.json"  # real, 100 slots (L3)

POZNAN, RADOM = "PL-LOC-000001", "PL-LOC-000004"  # ICON-D2 and DMI HARMONIE real-data sites
UNMANAGED = "PL-LOC-000003"  # PV + heat pump, no EMS battery: emits grid flows

ERA5_URL = "https://archive-api.open-meteo.com/v1/archive"
SATELLITE_URL = "https://satellite-api.open-meteo.com/v1/archive"
RUNS_URL = "https://single-runs-api.open-meteo.com/v1/forecast"
PSE_URL = "https://api.raporty.pse.pl/api/rce-pln"
DMI = "dmi_harmonie_arome_europe"

F64 = NDArray[np.float64]


# ── what the APIs answer ─────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Gap:
    """The satellite has no data for ``[start, end)`` at ``location_id`` (10-minute aligned):
    every slot ``[L - 10 min, L)`` inside the span is null.
    """

    location_id: str
    start: datetime
    end: datetime


@dataclass(frozen=True)
class Revision:
    """PSE republished ``slots`` (indices into the day's slots) at ``published`` with new prices:
    the day's page carries both rows for those slots.
    """

    slots: Sequence[int]
    published: datetime
    price_delta_pln_mwh: float = 37.5


def default_publication(day: date) -> datetime:
    """D-1 between 13:00 and 15:59 local, deterministic per day, millisecond precision."""
    o = day.toordinal()
    local = datetime.combine(
        day - timedelta(days=1), time(13 + o % 3, (o * 7) % 60, (o * 13) % 60), WARSAW
    )
    return local.replace(microsecond=((o * 37) % 1000) * 1000)


def _calendar(unix: F64) -> tuple[NDArray[np.int64], NDArray[np.int64]]:
    dt = unix.astype("datetime64[s]")
    years = dt.astype("datetime64[Y]")
    doy = (dt.astype("datetime64[D]") - years.astype("datetime64[D]")).astype(np.int64) + 1
    month = (dt.astype("datetime64[M]") - years.astype("datetime64[M]")).astype(np.int64) + 1
    return doy, month


def clear_sky(lat: float, lon: float, elevation_m: float, unix_mid: F64) -> tuple[F64, F64, F64]:
    """Clear-sky GHI and DNI (W/m2) and cos(zenith) at ``unix_mid`` for one site."""
    doy, month = _calendar(unix_mid)
    geo = solar_geometry(
        unix_mid, doy, month, np.array([lat]), np.array([lon]), np.array([elevation_m])
    )
    return geo.cs_ghi[0], geo.cs_dni[0], np.cos(np.radians(geo.zenith[0]))


def temperature_c(unix: float) -> float:
    hour = (unix % 86400) / 3600
    return round(
        12.0 + 6.0 * math.sin(2 * math.pi * (hour - 9) / 24) + 2.0 * math.sin(unix / 432000), 1
    )


def wind10_ms(unix: float) -> float:
    return round(max(0.0, 3.0 + 1.5 * math.sin(2 * math.pi * unix / 86400 + 1.0)), 2)


def _hourly_range(first: date, last: date, step_s: int) -> list[int]:
    t0 = int(datetime.combine(first, time(), UTC).timestamp())
    t1 = int(datetime.combine(last + timedelta(days=1), time(), UTC).timestamp())
    return list(range(t0, t1, step_s))


def _grid_cell(lat: float, lon: float, elevation_m: float) -> dict[str, Any]:
    return {
        "latitude": round(lat + 0.01, 2),
        "longitude": round(lon + 0.01, 6),
        "generationtime_ms": 1.0,
        "utc_offset_seconds": 0,
        "timezone": "GMT",
        "timezone_abbreviation": "GMT",
        "elevation": elevation_m,
    }


def era5_body(site: Site, first: date, last: date) -> dict[str, Any]:
    labels = _hourly_range(first, last, 3600)
    # GHI labelled H is the mean over [H - 1 h, H): the mean of its four quarter midpoints.
    mids = np.array([h - 3600 + 450 + 900 * q for h in labels for q in range(4)], dtype=np.float64)
    cs, _dni, _cosz = clear_sky(site.lat, site.lon, site.elevation_m, mids)
    ghi_h = 0.8 * cs.reshape(-1, 4).mean(axis=1)
    return {
        **_grid_cell(site.lat, site.lon, site.elevation_m),
        "hourly_units": {
            "time": "unixtime",
            "temperature_2m": "°C",
            "wind_speed_10m": "m/s",
            "shortwave_radiation": "W/m²",
        },
        "hourly": {
            "time": labels,
            "temperature_2m": [temperature_c(t) for t in labels],
            "wind_speed_10m": [wind10_ms(t) for t in labels],
            "shortwave_radiation": [round(float(v), 1) for v in ghi_h],
        },
    }


def satellite_body(site: Site, first: date, last: date, gaps: Sequence[Gap]) -> dict[str, Any]:
    labels = _hourly_range(first, last, 600)  # end labels of 10-minute backward means
    mids = np.array(labels, dtype=np.float64) - 300
    cs_ghi, cs_dni, cosz = clear_sky(site.lat, site.lon, site.elevation_m, mids)
    ghi = np.round(0.85 * cs_ghi, 1)
    dni = np.round(0.5 * cs_dni, 1)
    dhi = np.round(np.maximum(0.0, ghi - dni * np.clip(cosz, 0.0, None)), 1)
    spans = [
        (int(g.start.timestamp()), int(g.end.timestamp()))
        for g in gaps
        if g.location_id == site.location_id
    ]

    def column(values: F64) -> list[float | None]:
        return [
            None if any(lo <= t - 600 and t <= hi for lo, hi in spans) else float(v)
            for t, v in zip(labels, values, strict=True)
        ]

    return {
        **_grid_cell(site.lat, site.lon, site.elevation_m),
        "hourly_units": {
            "time": "unixtime",
            "shortwave_radiation": "W/m²",
            "direct_normal_irradiance": "W/m²",
            "diffuse_radiation": "W/m²",
        },
        "hourly": {
            "time": labels,
            "shortwave_radiation": column(ghi),
            "direct_normal_irradiance": column(dni),
            "diffuse_radiation": column(dhi),
        },
    }


def run_body(site: Site, model: str, run: datetime, *, malformed: bool = False) -> dict[str, Any]:
    """An archived run: 193 quarter-hour labels from ``run``. ``malformed``: the labels jump
    30 minutes after step 96, so they leave the run-anchored grid (``build_event`` raises).
    """
    t0 = int(run.timestamp())
    labels = [t0 + 900 * k + (1800 if malformed and k > 96 else 0) for k in range(193)]
    mids = np.array(labels, dtype=np.float64) - 450
    cs_ghi, cs_dni, cosz = clear_sky(site.lat, site.lon, site.elevation_m, mids)
    ghi: list[float | None] = [float(v) for v in np.round(0.7 * cs_ghi, 1)]
    dni: list[float | None] = [float(v) for v in np.round(0.45 * cs_dni, 1)]
    dhi: list[float | None] = [
        round(max(0.0, (g or 0.0) - (d or 0.0) * max(0.0, float(c))), 1)
        for g, d, c in zip(ghi, dni, cosz, strict=True)
    ]
    cloud: list[int | None] = [int(30 + 20 * math.sin(t / 20000)) for t in labels]
    ghi[0] = dni[0] = dhi[0] = None  # step 0 (labelled at the run time) has no radiation
    if model == DMI:
        for k in (1, 2):  # DMI: no GHI or DHI for the steps ending +15 and +30 min
            ghi[k] = dhi[k] = None
    else:
        cloud[-1] = None  # ICON-D2: cloud cover missing at +48 h, so 191 complete steps
    wind10 = [wind10_ms(t) for t in labels]
    return {
        **_grid_cell(site.lat, site.lon, site.elevation_m),
        "minutely_15_units": {
            "time": "unixtime",
            "shortwave_radiation": "W/m²",
            "direct_normal_irradiance": "W/m²",
            "diffuse_radiation": "W/m²",
            "temperature_2m": "°C",
            "cloud_cover": "%",
            "wind_speed_10m": "m/s",
            "wind_speed_100m": "m/s",
            "wind_speed_120m": "m/s",
            "wind_direction_10m": "°",
        },
        "minutely_15": {
            "time": labels,
            "shortwave_radiation": ghi,
            "direct_normal_irradiance": dni,
            "diffuse_radiation": dhi,
            "temperature_2m": [temperature_c(t) for t in labels],
            "cloud_cover": cloud,
            "wind_speed_10m": wind10,
            "wind_speed_100m": [round(w * 1.8, 2) for w in wind10],
            "wind_speed_120m": [round(w * 1.9, 2) for w in wind10],
            "wind_direction_10m": [int(200 + 60 * math.sin(t / 30000)) % 360 for t in labels],
        },
    }


def _ms(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{dt.microsecond // 1000:03d}"


def price_pln_mwh(day: date, i: int, n: int) -> float:
    return round(
        420.0
        + 180.0 * math.sin(2 * math.pi * i / n)
        - 90.0 * math.cos(4 * math.pi * i / n)
        + 10.0 * (day.toordinal() % 7),
        2,
    )


def pse_rows(day: date, published: datetime, revision: Revision | None = None) -> list[dict]:
    """Rows of one business day in the ``rce-pln`` format (``dtime_utc`` = interval end, UTC)."""

    def row(s: datetime, e: datetime, price: float, pub: datetime) -> dict[str, Any]:
        sl, el = s.astimezone(WARSAW), e.astimezone(WARSAW)
        end_local = "24:00" if el.time() == time() else f"{el:%H:%M}"
        return {
            "dtime": f"{el:%Y-%m-%d %H:%M:%S}",
            "period": f"{sl:%H:%M} - {end_local}",
            "rce_pln": price,
            "dtime_utc": f"{e.astimezone(UTC):%Y-%m-%d %H:%M:%S}",
            "period_utc": f"{s.astimezone(UTC):%H:%M} - {e.astimezone(UTC):%H:%M}",
            "business_date": day.isoformat(),
            "publication_ts": _ms(pub.astimezone(WARSAW)),
            "publication_ts_utc": _ms(pub.astimezone(UTC)),
        }

    slots = local_day_slots(day)
    rows = [
        row(s, e, price_pln_mwh(day, i, len(slots)), published) for i, (s, e) in enumerate(slots)
    ]
    if revision is not None:
        for i in revision.slots:
            s, e = slots[i]
            price = round(price_pln_mwh(day, i, len(slots)) + revision.price_delta_pln_mwh, 2)
            rows.append(row(s, e, price, revision.published))
    return rows


# ── the fetch ────────────────────────────────────────────────────────────────────────────────────


class Recording(NamedTuple):
    settings: EmsSettings
    name: str

    @property
    def root(self) -> Path:
        return self.settings.data_dir / "recordings" / self.name


@dataclass
class FakeClock:
    """Simulated time: ``sleep`` advances the monotonic and the wall clock without waiting."""

    wall0: datetime
    t: float = 0.0

    def monotonic(self) -> float:
        return self.t

    def wall(self) -> datetime:
        return self.wall0 + timedelta(seconds=self.t)

    async def sleep(self, seconds: float) -> None:
        self.t += max(0.0, seconds)


@contextlib.contextmanager
def no_network() -> Iterator[None]:
    """Any socket connect fails (the unit tier's rule, also inside module-scoped fixtures)."""

    def refuse(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("network access is not allowed while building history recordings")

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(socket.socket, "connect", refuse)
        mp.setattr(socket.socket, "connect_ex", refuse)
        mp.setattr(socket, "create_connection", refuse)
        yield


def write_config(
    dest: Path,
    locations: Sequence[str],
    *,
    synthetic_locations: int,
    base: Path = CONFIG / "generator.yaml",
    seed: int | None = None,
) -> Path:
    """A generator config with the given real-data sites (written as JSON, which is YAML)."""
    raw = YAML(typ="safe").load(base.read_text())
    raw["live"]["locations"] = list(locations)
    raw["scale"]["synthetic_locations"] = synthetic_locations
    if seed is not None:
        raw["seed"] = seed
    dest.mkdir(parents=True, exist_ok=True)
    path = dest / "generator.yaml"
    path.write_text(json.dumps(raw, indent=2) + "\n")
    return path


def _param(request: httpx.Request, key: str) -> str:
    return request.url.params[key]


def make_recording(
    tmp: Path,
    start: date,
    end: date,
    *,
    name: str = "hist",
    locations: Sequence[str] = (POZNAN,),
    synthetic_locations: int = 1,
    base_config: Path = CONFIG / "generator.yaml",
    config_dir: Path = CONFIG,
    seed: int | None = None,
    sat_gaps: Sequence[Gap] = (),
    publication: Callable[[date], datetime] = default_publication,
    pse_revisions: Mapping[date, Revision] | None = None,
    pse_drop_slots: Mapping[date, int] | None = None,
    pse_bodies: Mapping[date, bytes] | None = None,
    missing_runs: Collection[tuple[str, datetime]] = (),
    malformed_runs: Collection[tuple[str, datetime]] = (),
) -> Recording:
    """Fetch a complete raw history recording ``name`` for ``[start, end)`` under ``tmp``.

    ``publication(day)`` gives each PSE business day's publication time; ``pse_revisions`` adds a
    later publication for some slots of a day; ``pse_drop_slots`` drops a day's last N slots
    (incomplete); ``pse_bodies`` serves a given body (for example the real 2026-10-25 fixture);
    ``missing_runs`` answer "run not available" (marker); ``malformed_runs`` leave the run grid.
    """
    revisions = dict(pse_revisions or {})
    drops = dict(pse_drop_slots or {})
    bodies = dict(pse_bodies or {})
    missing = {(loc, run.astimezone(UTC)) for loc, run in missing_runs}
    malformed = {(loc, run.astimezone(UTC)) for loc, run in malformed_runs}

    cfg_path = write_config(
        tmp / "cfg", locations, synthetic_locations=synthetic_locations, base=base_config, seed=seed
    )
    catalog = load_sites(config_dir, load_generator_config(cfg_path))
    sites = [s for s in catalog if s.location_id in set(locations)]

    def site_at(request: httpx.Request) -> Site:
        lat, lon = float(_param(request, "latitude")), float(_param(request, "longitude"))
        for site in sites:
            if (site.lat, site.lon) == (lat, lon):
                return site
        raise AssertionError(f"no real-data site at {lat}, {lon}")

    def dates(request: httpx.Request) -> tuple[date, date]:
        return (
            date.fromisoformat(_param(request, "start_date")),
            date.fromisoformat(_param(request, "end_date")),
        )

    def json_response(status: int, body: bytes | dict[str, Any]) -> httpx.Response:
        content = body if isinstance(body, bytes) else json.dumps(body).encode()
        return httpx.Response(
            status, content=content, headers={"content-type": "application/json; charset=utf-8"}
        )

    def era5(request: httpx.Request) -> httpx.Response:
        return json_response(200, era5_body(site_at(request), *dates(request)))

    def satellite(request: httpx.Request) -> httpx.Response:
        return json_response(200, satellite_body(site_at(request), *dates(request), sat_gaps))

    def runs(request: httpx.Request) -> httpx.Response:
        site = site_at(request)
        model = _param(request, "models")
        run = datetime.fromisoformat(_param(request, "run")).replace(tzinfo=UTC)
        if (site.location_id, run) in missing:
            reason = f"{NOT_AVAILABLE}. Model: {model}, run: {run:%Y-%m-%dT%H:%M}"
            return json_response(400, {"error": True, "reason": reason})
        return json_response(
            200, run_body(site, model, run, malformed=(site.location_id, run) in malformed)
        )

    def pse(request: httpx.Request) -> httpx.Response:
        query = parse_qs(urlsplit(str(request.url)).query)
        day = date.fromisoformat(query["$filter"][0].split("'")[1])
        if day in bodies:
            return json_response(200, bodies[day])
        rows = pse_rows(day, publication(day), revisions.get(day))
        if day in drops:
            rows = rows[: len(local_day_slots(day)) - drops[day]]
        return json_response(200, {"value": rows})

    def unexpected(request: httpx.Request) -> httpx.Response:
        raise AssertionError(f"unexpected request {request.url}")

    settings = EmsSettings(
        config_dir=config_dir,
        mappings_dir=MAPPINGS,
        data_dir=tmp / "data",
        topics_profile="sim",
        topic_prefix="",
        open_meteo_api_key=None,
        entsoe_token=None,
    )
    today = end + timedelta(days=30)
    clock = FakeClock(datetime.combine(today, time(9, 30), UTC))
    with no_network(), respx.mock(assert_all_called=False) as router:
        router.get(url__startswith=ERA5_URL).mock(side_effect=era5)
        router.get(url__startswith=SATELLITE_URL).mock(side_effect=satellite)
        router.get(url__startswith=RUNS_URL).mock(side_effect=runs)
        router.get(url__startswith=PSE_URL).mock(side_effect=pse)
        router.route().mock(side_effect=unexpected)
        result = asyncio.run(
            fetch(
                settings,
                cfg_path,
                name,
                start,
                end,
                today=today,
                wall=clock.wall,
                monotonic=clock.monotonic,
                sleep=clock.sleep,
            )
        )
    assert (result.complete, result.exit_code) == (True, 0), result
    return Recording(settings, name)


def copy_recording(
    rec: Recording, tmp: Path, *, raw_only: bool = False, **settings_update: Any
) -> Recording:
    """The same recording (same name, same bytes) under another data dir; ``raw_only`` leaves
    out what build wrote (``events.jsonl``, ``build.json``).
    """
    data_dir = tmp / "data"
    ignore = shutil.ignore_patterns("events.jsonl", "build.json") if raw_only else None
    shutil.copytree(rec.root, data_dir / "recordings" / rec.name, ignore=ignore)
    settings = rec.settings.model_copy(update={"data_dir": data_dir, **settings_update})
    return Recording(settings, rec.name)


def copy_config(tmp: Path) -> Path:
    """A private copy of ``config/`` (to change a calibration file without touching the repo)."""
    dest = tmp / "config"
    shutil.copytree(CONFIG, dest)
    return dest


# ── reading a recording ──────────────────────────────────────────────────────────────────────────


def build(rec: Recording, **kwargs: Any) -> Any:
    """``ems.ingest.history.build.build`` (spec 008, T18), without network."""
    from ems.ingest.history.build import build as do_build

    with no_network():
        return do_build(rec.settings, rec.name, **kwargs)


def event_lines(root: Path) -> list[bytes]:
    return [line for line in (root / "events.jsonl").read_bytes().split(b"\n") if line]


def read_built(root: Path) -> list[Any]:
    return [parse_event(line) for line in event_lines(root)]


def build_info(root: Path) -> dict[str, Any]:
    return json.loads((root / "build.json").read_text())


def manifest(root: Path) -> dict[str, Any]:
    return json.loads((root / "manifest.json").read_text())


def window(root: Path) -> tuple[datetime, datetime]:
    w = manifest(root)["window"]
    return (
        datetime.combine(date.fromisoformat(w["start"][:10]), time(), UTC),
        datetime.combine(date.fromisoformat(w["end"][:10]), time(), UTC),
    )


def meta(body: Path) -> dict[str, Any]:
    return json.loads(body.with_name(body.name.removesuffix(".json") + ".meta.json").read_text())


def fetched_at(body: Path) -> datetime:
    return datetime.fromisoformat(meta(body)["fetched_at"])


def raw_runs(root: Path) -> dict[tuple[str, datetime], Path]:
    """Stored archived runs by (location, the ``run`` parameter of their request)."""
    out: dict[tuple[str, datetime], Path] = {}
    for meta_path in sorted((root / "raw" / "open-meteo-single-runs").rglob("*.meta.json")):
        body = meta_path.with_name(meta_path.name.removesuffix(".meta.json") + ".json")
        run = parse_qs(urlsplit(meta(body)["url"]).query)["run"][0]
        out[(meta_path.parent.name, datetime.fromisoformat(run).replace(tzinfo=UTC))] = body
    return out


def raw_pse_days(root: Path) -> dict[date, list[Path]]:
    """Stored PSE pages by business date, in page order."""
    out: dict[date, list[Path]] = {}
    for body in sorted((root / "raw" / "pse-rce").glob("*-p*.json")):
        if body.name.endswith(".meta.json"):
            continue
        out.setdefault(date.fromisoformat(body.name[:10]), []).append(body)
    return out


def raw_publications(pages: Sequence[Path]) -> list[datetime]:
    """Every ``publication_ts_utc`` in a day's raw pages."""
    return [
        datetime.fromisoformat(row["publication_ts_utc"]).replace(tzinfo=UTC)
        for page in pages
        for row in json.loads(page.read_text())["value"]
    ]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
