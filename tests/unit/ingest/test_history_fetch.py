"""AC-008-01 … -04: ``ems-gen history fetch`` of a real-data window (spec 008, T03).

No network: respx answers for every host the fetch may use (Open-Meteo ERA5, satellite and Single
Runs, free or customer, and PSE) and logs each request with the time of a fake monotonic clock; any
other request is logged as ``unexpected``. "Today" and the wall clock are injected.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import shutil
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, time, timedelta
from itertools import pairwise
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl

import httpx
import pytest
import respx
from pydantic import SecretStr
from ruamel.yaml import YAML
from typer.testing import CliRunner

from ems.ingest import cli
from ems.ingest.catalog.sites import Site
from ems.ingest.generator import load_generator_config, load_sites
from ems.ingest.history import fetch as history_fetch
from ems.ingest.history.fetch import (
    DAILY_BUDGET,
    ERA5_LAG_DAYS,
    OPEN_METEO_MIN_INTERVAL_S,
    PSE_MIN_INTERVAL_S,
    SINGLE_RUNS_ARCHIVE_START,
    FetchRefused,
    FetchResult,
    call_weight,
    fetch,
)
from ems.ingest.live.http import SourceHTTPError
from ems.ingest.live.openmeteo import DMI_HARMONIE, ICON_D2, model_for, single_run_params
from ems.settings import EmsSettings

REPO = Path(__file__).resolve().parents[3]
CONFIG = REPO / "config"
MAPPINGS = REPO / "mappings"
FIX = REPO / "tests" / "fixtures"

NAME = "test-window"
START, END = date(2026, 5, 15), date(2026, 5, 16)  # one whole UTC day
TODAY = date(2026, 6, 15)
WALL0 = datetime(2026, 6, 15, 12, tzinfo=UTC)
POZNAN, RADOM = "PL-LOC-000001", "PL-LOC-000004"
RUNS = [datetime(2026, 5, 15, h, tzinfo=UTC) for h in range(0, 24, 3)]  # 00, 03, … 21 UTC
# Local business dates local_date(start) … local_date(end - 1 s): 02:00 and 01:59 CEST.
PSE_DAYS = (date(2026, 5, 15), date(2026, 5, 16))
EPS = 1e-9  # float slack on the fake clock

FREE = {
    "era5": "https://archive-api.open-meteo.com/v1/archive",
    "satellite": "https://satellite-api.open-meteo.com/v1/archive",
    "runs": "https://single-runs-api.open-meteo.com/v1/forecast",
}
CUSTOMER = {k: v.replace("https://", "https://customer-") for k, v in FREE.items()}
PSE_URL = "https://api.raporty.pse.pl/api/rce-pln"
OPEN_METEO = tuple(FREE)
FOLDER = {
    "era5": "era5",
    "satellite": "satellite",
    "runs": "open-meteo-single-runs",
    "pse": "pse-rce",
}

ERA5 = (FIX / "openmeteo" / "era5_PL-LOC-000001_20260515-16.json").read_bytes()
ICON_RUN = (FIX / "openmeteo" / "single_run_icon_d2_PL-LOC-000001_20260515T0300.json").read_bytes()
DMI_RUN = (FIX / "openmeteo" / "single_run_dmi_PL-LOC-000004_20260515T0300.json").read_bytes()
PSE_DAY = (FIX / "pse" / "rce-pln_2026-10-21.json").read_bytes()  # any PSE body will do
OUTSIDE_DOMAIN = json.loads((FIX / "openmeteo" / "error_400_outside_domain.json").read_text())
NOT_AVAILABLE = {
    "error": True,
    "reason": "The requested model run is not available. Model: icon_d2, run: 2026-05-15T06:00",
}


# ── fakes ────────────────────────────────────────────────────────────────────────────────────────


@dataclass
class FakeClock:
    """Simulated time: ``sleep`` advances the monotonic and the wall clock without waiting."""

    t: float = 0.0
    sleeps: list[float] = field(default_factory=list)

    def monotonic(self) -> float:
        return self.t

    def wall(self) -> datetime:
        return WALL0 + timedelta(seconds=self.t)

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.t += max(0.0, seconds)


@dataclass(frozen=True)
class Call:
    source: str  # era5 | satellite | runs | pse | unexpected
    t: float  # fake monotonic time of the request
    request: httpx.Request

    @property
    def params(self) -> dict[str, str]:
        return dict(self.request.url.params)


Responder = Callable[[httpx.Request], httpx.Response]


def json_response(status: int, body: bytes | dict[str, Any]) -> httpx.Response:
    content = body if isinstance(body, bytes) else json.dumps(body).encode()
    return httpx.Response(
        status, content=content, headers={"content-type": "application/json; charset=utf-8"}
    )


def satellite_body(note: str | None = None) -> bytes:
    """Hand-written body in the Satellite API format: 10-minute steps over both dates, zeros."""
    t0 = int(datetime.combine(START, time(), UTC).timestamp())
    n = ((END - START).days + 1) * 144
    zeros = [0.0] * n
    body: dict[str, Any] = {
        "latitude": 52.4,
        "longitude": 16.92,
        "generationtime_ms": 1.0,
        "utc_offset_seconds": 0,
        "timezone": "GMT",
        "timezone_abbreviation": "GMT",
        "elevation": 85.0,
        "hourly_units": {
            "time": "unixtime",
            "shortwave_radiation": "W/m²",
            "direct_normal_irradiance": "W/m²",
            "diffuse_radiation": "W/m²",
        },
        "hourly": {
            "time": [t0 + 600 * i for i in range(n)],
            "shortwave_radiation": zeros,
            "direct_normal_irradiance": zeros,
            "diffuse_radiation": zeros,
        },
    }
    if note is not None:
        body["note"] = note
    return json.dumps(body).encode()


def run_by_model(request: httpx.Request) -> httpx.Response:
    """The archived run of the requested model (the captured fixtures, whatever the run time)."""
    dmi = request.url.params.get("models") == DMI_HARMONIE.request
    return json_response(200, DMI_RUN if dmi else ICON_RUN)


def runs_answering(special: dict[str, tuple[int, dict[str, Any]]]) -> Responder:
    """Archived runs, except the runs in ``special`` (keyed by the ``run`` parameter)."""

    def respond(request: httpx.Request) -> httpx.Response:
        answer = special.get(request.url.params.get("run", ""))
        return json_response(*answer) if answer else run_by_model(request)

    return respond


class Apis:
    """respx routes for every host the fetch may use, answering like the real APIs."""

    def __init__(self, *, customer: bool = False, clock: FakeClock | None = None) -> None:
        self.clock = clock or FakeClock()
        self.calls: list[Call] = []
        self.respond: dict[str, Responder] = {
            "era5": lambda _r: json_response(200, ERA5),
            "satellite": lambda _r: json_response(200, satellite_body()),
            "runs": run_by_model,
            "pse": lambda _r: json_response(200, PSE_DAY),
            "unexpected": lambda _r: httpx.Response(404),
        }
        self.router = respx.mock(assert_all_called=False)
        for source, url in {**(CUSTOMER if customer else FREE), "pse": PSE_URL}.items():
            self.router.get(url__startswith=url).mock(side_effect=self._handle(source))
        self.router.route().mock(side_effect=self._handle("unexpected"))

    def _handle(self, source: str) -> Responder:
        def handle(request: httpx.Request) -> httpx.Response:
            self.calls.append(Call(source, self.clock.t, request))
            return self.respond[source](request)

        return handle

    def of(self, *sources: str) -> list[Call]:
        return [c for c in self.calls if c.source in sources]


# ── helpers ──────────────────────────────────────────────────────────────────────────────────────


def write_config(
    dest: Path, locations: list[str], *, synthetic_locations: int = 1, seed: int | None = None
) -> Path:
    """``config/generator.yaml`` with the given real-data sites (written as JSON, which is YAML)."""
    raw = YAML(typ="safe").load((CONFIG / "generator.yaml").read_text())
    raw["live"]["locations"] = locations
    raw["scale"]["synthetic_locations"] = synthetic_locations
    if seed is not None:
        raw["seed"] = seed
    dest.mkdir(parents=True, exist_ok=True)
    path = dest / "generator.yaml"
    path.write_text(json.dumps(raw, indent=2) + "\n")
    return path


def settings(tmp_path: Path, *, config_dir: Path = CONFIG, key: str | None = None) -> EmsSettings:
    return EmsSettings(
        config_dir=config_dir,
        mappings_dir=MAPPINGS,
        data_dir=tmp_path / "data",
        open_meteo_api_key=SecretStr(key) if key else None,
    )


def run(
    s: EmsSettings,
    cfg: Path,
    apis: Apis,
    *,
    start: date = START,
    end: date = END,
    today: date = TODAY,
) -> FetchResult:
    with apis.router:
        return asyncio.run(
            fetch(
                s,
                cfg,
                NAME,
                start,
                end,
                today=today,
                wall=apis.clock.wall,
                monotonic=apis.clock.monotonic,
                sleep=apis.clock.sleep,
            )
        )


def recording(s: EmsSettings) -> Path:
    return s.data_dir / "recordings" / NAME


def read_manifest(root: Path) -> dict[str, Any]:
    return json.loads((root / "manifest.json").read_text())


def raw_files(root: Path) -> dict[str, tuple[bytes, int]]:
    """Every file under ``raw/`` with its bytes and modification time (untouched means both)."""
    raw = root / "raw"
    return {
        p.relative_to(raw).as_posix(): (p.read_bytes(), p.stat().st_mtime_ns)
        for p in sorted(raw.rglob("*"))
        if p.is_file()
    }


def stems(location: str = POZNAN) -> set[str]:
    """Raw names (without extension) of a complete one-day fetch for one site."""
    return {
        *(f"open-meteo-single-runs/{location}/{r:%Y%m%dT%H%M}" for r in RUNS),
        f"satellite/{location}",
        f"era5/{location}",
        *(f"pse-rce/{d.isoformat()}-p01" for d in PSE_DAYS),
    }


def stored_stems(files: dict[str, Any]) -> set[str]:
    """Raw names that have a stored response (a meta file)."""
    return {k.removesuffix(".meta.json") for k in files if k.endswith(".meta.json")}


def stem_of(call: Call, location: str = POZNAN) -> str:
    """The raw name a request of a one-site fetch is stored under."""
    if call.source == "runs":
        issued = datetime.fromisoformat(call.params["run"])
        return f"open-meteo-single-runs/{location}/{issued:%Y%m%dT%H%M}"
    if call.source == "pse":
        day = re.search(r"\d{4}-\d{2}-\d{2}", call.params["$filter"])
        assert day is not None
        return f"pse-rce/{day[0]}-p01"
    return f"{call.source}/{location}"


def grid_cells(manifest: dict[str, Any]) -> set[tuple[float, float, float]]:
    """(latitude, longitude, elevation) of every stored response, whether listed or keyed."""
    cells = manifest["grid_cells"]
    entries = cells.values() if isinstance(cells, dict) else cells
    return {(c["latitude"], c["longitude"], c["elevation"]) for c in entries}


def catalog_site(location_id: str) -> Site:
    """A site of the default catalog with 20 synthetic locations (``generator.history.yaml``)."""
    cfg = replace(load_generator_config(CONFIG / "generator.yaml"), synthetic_locations=20)
    return {s.location_id: s for s in load_sites(CONFIG, cfg)}[location_id]


# ── constants (AC-008-02, -04) ───────────────────────────────────────────────────────────────────


def test_ac_008_02_window_limits() -> None:
    assert date(2026, 4, 2) == SINGLE_RUNS_ARCHIVE_START
    assert ERA5_LAG_DAYS == 6  # ERA5's documented five-day delay plus one day


def test_ac_008_04_free_tier_pacing_and_call_weight() -> None:
    assert OPEN_METEO_MIN_INTERVAL_S == 0.8  # ≤ 75 a minute, ≤ 4,500 an hour
    assert PSE_MIN_INTERVAL_S == 1.0
    assert DAILY_BUDGET == 9000.0  # below the free tier's 10,000 a day
    # Weighted as published: max(1, variables / 10) * max(1, days / 14).
    assert call_weight(9, 2) == 1.0  # one archived run (nine variables, 193 quarter-hours)
    assert call_weight(3, 91) == 6.5  # ERA5 or satellite over a 90-day window
    assert call_weight(20, 28) == 4.0


# ── AC-008-01: layout, provenance, requests ──────────────────────────────────────────────────────


def test_ac_008_01_fetch_writes_the_recording_layout(tmp_path: Path) -> None:
    cfg = write_config(tmp_path / "cfg", [POZNAN])
    s = settings(tmp_path)
    apis = Apis()
    result = run(s, cfg, apis)

    root = recording(s)
    assert (result.root, result.complete, result.exit_code) == (root, True, 0)
    assert not apis.of("unexpected")

    raw = root / "raw"
    files = raw_files(root)
    assert set(files) == {f"{n}{ext}" for n in stems() for ext in (".json", ".meta.json")}
    assert (raw / "era5" / f"{POZNAN}.json").read_bytes() == ERA5  # stored as received
    assert (raw / "open-meteo-single-runs" / POZNAN / "20260515T0300.json").read_bytes() == ICON_RUN
    assert (root / "generator.yaml").read_bytes() == cfg.read_bytes()
    assert not list(root.rglob("*.tmp"))  # atomic writes
    assert not (root / "events.jsonl").exists() and not (root / "build.json").exists()

    urls = {FOLDER[k]: v for k, v in {**FREE, "pse": PSE_URL}.items()}
    for meta_path in raw.rglob("*.meta.json"):
        meta = json.loads(meta_path.read_text())
        body = meta_path.with_name(meta_path.name.removesuffix(".meta.json") + ".json")
        folder = meta_path.relative_to(raw).parts[0]
        assert meta["sha256"] == hashlib.sha256(body.read_bytes()).hexdigest(), meta_path
        assert meta["source"] == folder
        assert meta["url"].startswith(urls[folder])
        assert meta["status"] == 200
        assert "json" in meta["content_type"]
        assert WALL0 <= datetime.fromisoformat(meta["fetched_at"]) <= apis.clock.wall()

    manifest = read_manifest(root)
    assert manifest["kind"] == "ems-gen-history"
    assert str(manifest["window"]["start"]).startswith("2026-05-15")
    assert str(manifest["window"]["end"]).startswith("2026-05-16")
    assert manifest["market"] == "rce-pln"
    keys = ("location_id", "lat", "lon", "elevation_m", "model")
    assert [{k: site[k] for k in keys} for site in manifest["sites"]] == [
        {"location_id": POZNAN, "lat": 52.41, "lon": 16.93, "elevation_m": 85.0, "model": "icon_d2"}
    ]
    assert manifest["config_digest"] == hashlib.sha256(cfg.read_bytes()).hexdigest()
    assert manifest["missing_runs"] == []
    assert manifest["complete"] is True
    assert {"git_commit", "versions", "hardware"} <= manifest.keys()
    # Grid cells as the bodies report them (ERA5, ICON-D2 run, satellite).
    assert {(52.5, 17.0, 85.0), (52.42, 16.939999, 85.0), (52.4, 16.92, 85.0)} <= grid_cells(
        manifest
    )


def test_ac_008_01_requests_cover_the_window(tmp_path: Path) -> None:
    cfg = write_config(tmp_path / "cfg", [POZNAN])
    apis = Apis()
    run(settings(tmp_path), cfg, apis)
    site = catalog_site(POZNAN)

    (era5,) = apis.of("era5")
    assert {k: era5.params[k] for k in ("hourly", "models", "start_date", "end_date")} == {
        "hourly": "temperature_2m,wind_speed_10m,shortwave_radiation",
        "models": "era5",
        "start_date": "2026-05-15",
        "end_date": "2026-05-16",  # covers the hour at end
    }
    assert {k: era5.params[k] for k in ("wind_speed_unit", "timezone", "timeformat")} == {
        "wind_speed_unit": "ms",
        "timezone": "UTC",
        "timeformat": "unixtime",
    }
    assert [float(era5.params[k]) for k in ("latitude", "longitude", "elevation")] == [
        site.lat,
        site.lon,
        site.elevation_m,
    ]

    (sat,) = apis.of("satellite")
    assert {
        k: sat.params[k]
        for k in ("hourly", "models", "temporal_resolution", "start_date", "end_date")
    } == {
        "hourly": "shortwave_radiation,direct_normal_irradiance,diffuse_radiation",
        "models": "satellite_radiation_seamless",
        "temporal_resolution": "native",
        "start_date": "2026-05-15",
        "end_date": "2026-05-16",
    }
    assert [float(sat.params[k]) for k in ("latitude", "longitude")] == [site.lat, site.lon]

    assert sorted(c.params["$filter"] for c in apis.of("pse")) == [
        f"business_date eq '{d.isoformat()}'" for d in PSE_DAYS
    ]

    runs = sorted(apis.of("runs"), key=lambda c: c.params["run"])
    assert [c.params for c in runs] == [
        {k: str(v) for k, v in single_run_params(site, ICON_D2, r).items()} for r in RUNS
    ]
    assert all("X-Api-Key" not in c.request.headers for c in apis.calls)  # free tier: no key
    assert not apis.of("unexpected")


def test_ac_008_01_each_site_gets_the_runs_of_its_model(tmp_path: Path) -> None:
    cfg = write_config(tmp_path / "cfg", [POZNAN, RADOM], synthetic_locations=20)
    s = settings(tmp_path)
    apis = Apis()
    result = run(s, cfg, apis)
    assert result.complete

    by_model: dict[str, set[str]] = {}
    for c in apis.of("runs"):
        by_model.setdefault(c.params["models"], set()).add(c.params["latitude"])
    poznan, radom = catalog_site(POZNAN), catalog_site(RADOM)
    assert by_model == {"icon_d2": {str(poznan.lat)}, DMI_HARMONIE.request: {str(radom.lat)}}
    assert model_for(radom) is DMI_HARMONIE

    raw = recording(s) / "raw"
    for location in (POZNAN, RADOM):
        assert set(raw_files(recording(s))) >= {
            f"{n}{ext}" for n in stems(location) for ext in (".json", ".meta.json")
        }
    assert (raw / "open-meteo-single-runs" / RADOM / "20260515T0300.json").read_bytes() == DMI_RUN
    models = {site["location_id"]: site["model"] for site in read_manifest(recording(s))["sites"]}
    assert models == {POZNAN: ICON_D2.request, RADOM: DMI_HARMONIE.request}


def test_ac_008_01_api_key_goes_only_in_a_header(tmp_path: Path) -> None:
    key = "om-test-key-5f3a9c"
    cfg = write_config(tmp_path / "cfg", [POZNAN])
    s = settings(tmp_path, key=key)
    apis = Apis(customer=True)
    # A response that echoes the key: the stored body must be scrubbed too.
    apis.respond["satellite"] = lambda _r: json_response(200, satellite_body(note=f"key {key}"))
    result = run(s, cfg, apis)
    assert result.complete
    assert not apis.of("unexpected")  # no free host, only customer-*.open-meteo.com

    for c in apis.of(*OPEN_METEO):
        assert c.request.url.host.startswith("customer-")
        assert c.request.headers["X-Api-Key"] == key
        assert key not in str(c.request.url)
    assert all("X-Api-Key" not in c.request.headers for c in apis.of("pse"))

    root = recording(s)
    written = [p for p in root.rglob("*") if p.is_file()]
    assert written
    for path in written:
        assert key.encode() not in path.read_bytes(), path
    for meta_path in (root / "raw").rglob("*.meta.json"):
        meta = json.loads(meta_path.read_text())
        body = meta_path.with_name(meta_path.name.removesuffix(".meta.json") + ".json")
        assert meta["sha256"] == hashlib.sha256(body.read_bytes()).hexdigest()
        if meta["source"] != "pse-rce":
            assert meta["url"].startswith("https://customer-")


def test_ac_008_01_history_config_sites() -> None:
    """``config/generator.history.yaml``: the Poznań site (ICON-D2) and a Radom household (DMI)."""
    cfg = load_generator_config(CONFIG / "generator.history.yaml")
    assert cfg.live_locations == (POZNAN, RADOM)
    assert cfg.synthetic_locations == 20
    assert cfg.site_controlled_bess_share == 0.0
    sites = {s.location_id: s for s in load_sites(CONFIG, cfg)}
    poznan, radom = sites[POZNAN], sites[RADOM]
    assert model_for(poznan) is ICON_D2
    assert radom.lon > 20.34 and model_for(radom) is DMI_HARMONIE
    for site in (poznan, radom):
        assert site.bess is None or site.bess.control != "site"
    assert len(sites) - 2 == 18  # the background fleet
    # The captured DMI fixture was fetched for this very site.
    meta = json.loads(
        (FIX / "openmeteo" / "single_run_dmi_PL-LOC-000004_20260515T0300.meta.json").read_text()
    )
    query = dict(parse_qsl(meta["query"]))
    assert [float(query[k]) for k in ("latitude", "longitude", "elevation")] == [
        radom.lat,
        radom.lon,
        radom.elevation_m,
    ]


# ── AC-008-02: refusals ──────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("start", "end", "today", "named"),
    [
        pytest.param(date(2026, 4, 1), date(2026, 4, 2), TODAY, "2026-04-02", id="before-archive"),
        pytest.param(
            date(2026, 6, 9), date(2026, 6, 10), TODAY, "2026-06-09|6 days", id="too-recent"
        ),
        pytest.param(START, END, date(2026, 5, 21), "2026-05-15|6 days", id="end-5-days-ago"),
        pytest.param(START, START, TODAY, None, id="empty"),
        pytest.param(END, START, TODAY, None, id="reversed"),
    ],
)
def test_ac_008_02_window_is_refused_before_any_request(
    tmp_path: Path, start: date, end: date, today: date, named: str | None
) -> None:
    cfg = write_config(tmp_path / "cfg", [POZNAN])
    s = settings(tmp_path)
    apis = Apis()
    with pytest.raises(FetchRefused, match=named):
        run(s, cfg, apis, start=start, end=end, today=today)
    assert apis.calls == []
    assert not (recording(s) / "manifest.json").exists()  # a corrected rerun is not a "rerun"


def test_ac_008_02_end_six_days_before_today_is_allowed(tmp_path: Path) -> None:
    cfg = write_config(tmp_path / "cfg", [POZNAN])
    result = run(settings(tmp_path), cfg, Apis(), today=END + timedelta(days=6))
    assert (result.complete, result.exit_code) == (True, 0)


def test_ac_008_02_unknown_site_is_refused(tmp_path: Path) -> None:
    cfg = write_config(tmp_path / "cfg", [POZNAN, "PL-LOC-000099"], synthetic_locations=20)
    apis = Apis()
    with pytest.raises(FetchRefused, match="PL-LOC-000099"):
        run(settings(tmp_path), cfg, apis)
    assert apis.calls == []


def test_ac_008_02_site_controlled_battery_is_refused(tmp_path: Path) -> None:
    """A real-data site whose battery the site controls would dispatch on synthetic prices."""
    config_dir = tmp_path / "config"
    shutil.copytree(CONFIG, config_dir)
    sites_yaml = config_dir / "sites.yaml"
    text = sites_yaml.read_text()
    assert text.count("control: ems") == 1
    sites_yaml.write_text(text.replace("control: ems", "control: site"))
    cfg = write_config(tmp_path / "cfg", [POZNAN])
    apis = Apis()
    with pytest.raises(FetchRefused, match=POZNAN):
        run(settings(tmp_path, config_dir=config_dir), cfg, apis)
    assert apis.calls == []


def test_ac_008_02_estimate_above_daily_budget_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # One day, one site: ERA5 1 + satellite 1 + 8 runs of 1 = about 10 weighted calls.
    monkeypatch.setattr(history_fetch, "DAILY_BUDGET", 5.0)
    cfg = write_config(tmp_path / "cfg", [POZNAN])
    apis = Apis()
    with pytest.raises(FetchRefused, match=r"(?i)split"):
        run(settings(tmp_path), cfg, apis)
    assert apis.calls == []


def test_ac_008_02_budget_counts_only_calls_still_needed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = write_config(tmp_path / "cfg", [POZNAN])
    s = settings(tmp_path)
    assert run(s, cfg, Apis()).complete
    monkeypatch.setattr(history_fetch, "DAILY_BUDGET", 5.0)
    apis = Apis()
    result = run(s, cfg, apis)  # nothing left to fetch: within any budget
    assert (result.complete, result.exit_code, apis.calls) == (True, 0, [])


@pytest.mark.parametrize("change", ["window", "config", "sites"])
def test_ac_008_02_rerun_with_other_window_or_config_is_refused(
    tmp_path: Path, change: str
) -> None:
    cfg = write_config(tmp_path / "cfg", [POZNAN])
    s = settings(tmp_path)
    assert run(s, cfg, Apis()).complete
    manifest_before = (recording(s) / "manifest.json").read_bytes()
    raw_before = raw_files(recording(s))

    start = START
    if change == "window":
        start = date(2026, 5, 14)  # a valid window on its own
    elif change == "config":
        cfg = write_config(tmp_path / "cfg2", [POZNAN], seed=4243)
    else:
        cfg = write_config(tmp_path / "cfg2", [POZNAN, RADOM], synthetic_locations=20)
    apis = Apis()
    with pytest.raises(FetchRefused):
        run(s, cfg, apis, start=start)
    assert apis.calls == []
    assert (recording(s) / "manifest.json").read_bytes() == manifest_before
    assert raw_files(recording(s)) == raw_before


def era5_without_end_hour() -> bytes:
    """The ERA5 fixture before the daily update reaches ``end``: nulls from 05-16 00:00."""
    body = json.loads(ERA5)
    hourly = body["hourly"]
    i = hourly["time"].index(int(datetime.combine(END, time(), UTC).timestamp()))
    for var in ("temperature_2m", "wind_speed_10m", "shortwave_radiation"):
        hourly[var][i:] = [None] * (len(hourly[var]) - i)
    return json.dumps(body).encode()


def test_ac_008_02_era5_without_the_hour_at_end_is_refused(tmp_path: Path) -> None:
    cfg = write_config(tmp_path / "cfg", [POZNAN])
    apis = Apis()
    apis.respond["era5"] = lambda _r: json_response(200, era5_without_end_hour())
    with pytest.raises(FetchRefused, match=r"2026-05-16[T ]00:00"):
        run(settings(tmp_path), cfg, apis)
    assert len(apis.of("era5")) == 1
    assert apis.of("runs") == []  # cheap requests first: a bad window fails before the runs


def test_ac_008_03_rerun_after_an_era5_refusal_requests_era5_again(tmp_path: Path) -> None:
    """The refused ERA5 response must not count as stored, or every rerun would skip it."""
    cfg = write_config(tmp_path / "cfg", [POZNAN])
    s = settings(tmp_path)
    early = Apis()
    early.respond["era5"] = lambda _r: json_response(200, era5_without_end_hour())
    with pytest.raises(FetchRefused):
        run(s, cfg, early)

    later = Apis()  # the daily ERA5 update has reached end
    result = run(s, cfg, later)
    assert (result.complete, result.exit_code) == (True, 0)
    assert len(later.of("era5")) == 1
    assert (recording(s) / "raw" / "era5" / f"{POZNAN}.json").read_bytes() == ERA5


@pytest.mark.parametrize("status", [401, 403])
def test_ac_008_02_unauthorised_customer_host_is_refused_without_retry(
    tmp_path: Path, status: int
) -> None:
    key = "om-test-key-5f3a9c"
    cfg = write_config(tmp_path / "cfg", [POZNAN])
    apis = Apis(customer=True)
    denied = {"error": True, "reason": "API key does not include this API"}
    for source in OPEN_METEO:
        apis.respond[source] = lambda _r: json_response(status, denied)
    with pytest.raises(FetchRefused, match=r"customer-[a-z0-9-]+\.open-meteo\.com") as refused:
        run(settings(tmp_path, key=key), cfg, apis)
    assert len(apis.of(*OPEN_METEO)) == 1  # not retried, nothing after it
    assert key not in str(refused.value)


# ── AC-008-03: resume and missing runs ───────────────────────────────────────────────────────────


def test_ac_008_03_second_fetch_sends_nothing(tmp_path: Path) -> None:
    cfg = write_config(tmp_path / "cfg", [POZNAN])
    s = settings(tmp_path)
    assert run(s, cfg, Apis()).complete
    before = raw_files(recording(s))

    apis = Apis()
    result = run(s, cfg, apis)
    assert apis.calls == []
    assert (result.complete, result.exit_code) == (True, 0)
    assert raw_files(recording(s)) == before  # stored responses untouched
    assert read_manifest(recording(s))["complete"] is True


def test_ac_008_03_missing_run_is_marked_and_retried_next_time(tmp_path: Path) -> None:
    cfg = write_config(tmp_path / "cfg", [POZNAN])
    s = settings(tmp_path)
    apis = Apis()
    apis.respond["runs"] = runs_answering({"2026-05-15T06:00": (400, NOT_AVAILABLE)})
    result = run(s, cfg, apis)

    # Every request is accounted for: the missing run by its marker.
    assert (result.complete, result.exit_code) == (True, 0)
    assert len(apis.of("runs")) == len(RUNS)  # one request each, and the fetch went on
    runs_dir = recording(s) / "raw" / "open-meteo-single-runs" / POZNAN
    assert (runs_dir / "20260515T0600.missing.json").exists()
    assert not (runs_dir / "20260515T0600.json").exists()
    assert not (runs_dir / "20260515T0600.meta.json").exists()
    assert (runs_dir / "20260515T0900.json").exists()
    manifest = read_manifest(recording(s))
    assert manifest["complete"] is True
    (missing,) = manifest["missing_runs"]
    assert POZNAN in json.dumps(missing)
    assert re.search(r"2026-?05-?15T06:?00", json.dumps(missing))

    retry = Apis()
    again = run(s, cfg, retry)
    assert [stem_of(c) for c in retry.calls] == [f"open-meteo-single-runs/{POZNAN}/20260515T0600"]
    assert (again.complete, again.exit_code) == (True, 0)
    assert (runs_dir / "20260515T0600.json").read_bytes() == ICON_RUN
    assert not (runs_dir / "20260515T0600.missing.json").exists()
    assert read_manifest(recording(s))["missing_runs"] == []


def test_ac_008_03_other_bad_request_aborts(tmp_path: Path) -> None:
    cfg = write_config(tmp_path / "cfg", [POZNAN])
    s = settings(tmp_path)
    apis = Apis()
    apis.respond["runs"] = runs_answering({"2026-05-15T06:00": (400, OUTSIDE_DOMAIN)})
    with pytest.raises((FetchRefused, SourceHTTPError), match="No data is available"):
        run(s, cfg, apis)
    requested = [c.params["run"] for c in apis.of("runs")]
    assert requested.count("2026-05-15T06:00") == 1  # a 400 is never retried
    assert "2026-05-15T09:00" not in requested  # aborted, not skipped
    assert not list((recording(s) / "raw").rglob("*.missing.json"))


# ── AC-008-04: pacing and the server's limits ────────────────────────────────────────────────────


def test_ac_008_04_open_meteo_and_pse_requests_are_paced(tmp_path: Path) -> None:
    cfg = write_config(tmp_path / "cfg", [POZNAN])
    apis = Apis()
    assert run(settings(tmp_path), cfg, apis).complete

    open_meteo = [c.t for c in apis.of(*OPEN_METEO)]
    pse = [c.t for c in apis.of("pse")]
    assert (len(open_meteo), len(pse)) == (2 + len(RUNS), len(PSE_DAYS))
    # Across all three Open-Meteo hosts (the limits are per client): ≤ 75 a minute, ≤ 4,500 an hour.
    assert all(b - a >= 0.8 - EPS for a, b in pairwise(open_meteo)), open_meteo
    assert all(b - a >= 1.0 - EPS for a, b in pairwise(pse)), pse


def test_ac_008_04_final_429_writes_the_manifest_exits_4_and_a_rerun_resumes(
    tmp_path: Path,
) -> None:
    cfg = write_config(tmp_path / "cfg", [POZNAN])
    s = settings(tmp_path)
    limited = {
        "error": True,
        "reason": "Daily API request limit exceeded. Please try again tomorrow.",
    }
    apis = Apis()
    apis.respond["runs"] = runs_answering({"2026-05-15T09:00": (429, limited)})
    result = run(s, cfg, apis)

    assert (result.root, result.complete, result.exit_code) == (recording(s), False, 4)
    assert read_manifest(recording(s))["complete"] is False
    attempts = [c.t for c in apis.of("runs") if c.params["run"] == "2026-05-15T09:00"]
    assert len(attempts) > 1  # 429 is retried with back-off by the Fetcher …
    assert attempts[-1] > attempts[0]  # … whose waits go through the injected sleep
    assert not list(recording(s).rglob("*.tmp"))
    stored = raw_files(recording(s))
    assert f"open-meteo-single-runs/{POZNAN}/20260515T0900" not in stored_stems(stored)

    rerun = Apis()
    again = run(s, cfg, rerun)
    assert (again.complete, again.exit_code) == (True, 0)
    requested = [stem_of(c) for c in rerun.calls]
    assert len(requested) == len(set(requested))
    assert set(requested).isdisjoint(stored_stems(stored))  # stored files are not re-requested
    assert set(requested) | stored_stems(stored) == stems()
    after = raw_files(recording(s))
    assert {k: after[k] for k in stored} == stored  # and stay untouched
    assert read_manifest(recording(s))["complete"] is True


# ── CLI ──────────────────────────────────────────────────────────────────────────────────────────


def test_ac_008_02_cli_refusal_exits_2(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = write_config(tmp_path / "cfg", [POZNAN])
    monkeypatch.setenv("EMS_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("EMS_CONFIG_DIR", str(CONFIG))
    monkeypatch.setenv("EMS_MAPPINGS_DIR", str(MAPPINGS))
    monkeypatch.delenv("EMS_OPEN_METEO_API_KEY", raising=False)
    args = ["history", "fetch", "--name", NAME, "--start", "2026-03-01", "--end", "2026-03-02"]
    with respx.mock(assert_all_called=False) as router:
        route = router.route().mock(return_value=httpx.Response(500))
        result = CliRunner().invoke(cli.app, [*args, "--config", str(cfg)])
    assert result.exit_code == 2, result.output
    assert "2026-04-02" in result.output  # names the archive start
    assert route.call_count == 0
    assert not (tmp_path / "data" / "recordings" / NAME / "manifest.json").exists()
