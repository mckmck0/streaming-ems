"""Build a fetched real-data window into contract events, offline (spec 008, plan section 7).

Inputs are only what ``history.fetch`` stored (raw responses, ``manifest.json``, the generator
config copy) plus the repo's calibration, catalog and mappings, whose digests go into
``build.json``. Output:

- ``events.jsonl``: archived forecast runs (``open-meteo-single-runs``), PSE RCE days (``pse-rce``)
  and the real-data sites' meters (``synthetic-history``: the generator's models driven by the
  observed truth), in availability order (``emission_key``). Rebuilding from the same inputs gives
  the same bytes: ``ingested_at`` is the recorded fetch time, never the build time.
- ``build.json`` (written last): seed, ``run_id``, digests, the events hash, counts, gap shares,
  flagged and incomplete price days, the complete local delivery days and diagnostics.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import os
import platform
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import structlog

from ems.contracts.availability import available_at
from ems.contracts.envelope import (
    AssetType,
    EnvelopeBase,
    MeasurementEvent,
    Quality,
    to_json_bytes,
)
from ems.contracts.ids import RUN_ID_PATTERN
from ems.contracts.time import WARSAW, local_date, local_day_bounds, quarter_index
from ems.ingest import rng
from ems.ingest.catalog.sites import Site
from ems.ingest.generator import (
    STREAM_ORDER,
    SyntheticModel,
    emissions,
    load_generator_config,
    load_sites,
)
from ems.ingest.history.observed import ObservedSeries, ObservedTruth, parse_series
from ems.ingest.live.openmeteo import build_event, model_for
from ems.ingest.live.pse import REPORTS, IncompleteDay, build_day_event
from ems.ingest.mapping.engine import apply, builtin
from ems.ingest.sources.recording import write_atomic
from ems.ingest.synthetic.truth import SyntheticTruth
from ems.ingest.synthetic.weather import TimeAxis, load_params
from ems.settings import EmsSettings

log = structlog.get_logger(__name__)

FORECAST_SOURCE = "open-meteo-single-runs"
METER_SOURCE = "synthetic-history"
MAPPINGS = ("openmeteo.forecast.v1", "pse.rce.v2")
WEATHER_ASSETS = {AssetType.PV, AssetType.HEAT_PUMP}
# Fallback strength for quality_raw: the strongest fallback a measurement depended on.
STRENGTH = {"erbs": 1, "era5": 2, "synthetic": 3}
PV_MASKS = {
    "radiation_erbs": "erbs",
    "radiation_era5": "era5",
    "radiation_synthetic": "synthetic",
    "temperature_synthetic": "synthetic",
    "wind_synthetic": "synthetic",
}
HP_MASKS = {"temperature_synthetic": "synthetic"}


class BuildRefused(ValueError):
    """The recording cannot be built as asked. CLI exit 2."""


@dataclass(frozen=True)
class BuildResult:
    root: Path
    events: int
    sha256: str


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def emission_key(
    event: EnvelopeBase, start: datetime, meter_delay: timedelta
) -> tuple[datetime, int, str, datetime, str]:
    """Total order of a history recording: when the event becomes usable (never before ``start``),
    then stream, subject, interval and id.
    """
    delay = meter_delay if isinstance(event, MeasurementEvent) else timedelta(0)
    at = max(available_at(event, delay), start)
    return (
        at,
        STREAM_ORDER[event.stream.value],
        event.subject,
        event.interval_start,
        str(event.event_id),
    )


# ── inputs and their digests ─────────────────────────────────────────────────────────────────────


def raw_set_digest(root: Path) -> str:
    """SHA-256 over every stored response (name and body hash) and missing-run marker."""
    raw = root / "raw"
    lines = []
    for meta in sorted(raw.rglob("*.meta.json")):
        name = meta.relative_to(raw).as_posix().removesuffix(".meta.json")
        lines.append(f"{name} {json.loads(meta.read_text())['sha256']}")
    for marker in sorted(raw.rglob("*.missing.json")):
        lines.append(f"{marker.relative_to(raw).as_posix()} {_sha256(marker.read_bytes())}")
    return _sha256("\n".join(lines).encode())


def input_digests(settings: EmsSettings, root: Path) -> dict[str, str]:
    """Digests of everything besides raw data that the meters depend on."""
    out = {"generator.yaml": _sha256((root / "generator.yaml").read_bytes())}
    for path in sorted((settings.config_dir / "calibration").glob("*.yaml")):
        out[f"calibration/{path.name}"] = _sha256(path.read_bytes())
    out["sites.yaml"] = _sha256((settings.config_dir / "sites.yaml").read_bytes())
    for name in MAPPINGS:
        out[f"mappings/{name}.yaml"] = _sha256(
            (settings.mappings_dir / f"{name}.yaml").read_bytes()
        )
    return out


def _check_bodies(root: Path) -> None:
    for meta_path in sorted((root / "raw").rglob("*.meta.json")):
        meta = json.loads(meta_path.read_text())
        body = meta_path.with_name(meta_path.name.removesuffix(".meta.json") + ".json")
        if not body.exists() or _sha256(body.read_bytes()) != meta["sha256"]:
            raise BuildRefused(f"{body.relative_to(root)} no longer matches its fetch (SHA-256)")


def _meta(body: Path) -> dict[str, Any]:
    return json.loads(body.with_name(body.name.removesuffix(".json") + ".meta.json").read_text())


def _fetched_at(body: Path) -> datetime:
    return datetime.fromisoformat(_meta(body)["fetched_at"])


def _manifest(root: Path) -> dict[str, Any]:
    path = root / "manifest.json"
    if not path.exists():
        raise BuildRefused(f"{root.name}: no manifest.json; run `ems-gen history fetch` first")
    manifest = json.loads(path.read_text())
    if manifest.get("kind") != "ems-gen-history":
        raise BuildRefused(f"{root.name} is not a history recording")
    if not manifest.get("complete"):
        raise BuildRefused(
            f"{root.name}: the fetch is not complete; rerun `ems-gen history fetch` to resume"
        )
    return manifest


def _real_sites(manifest: dict[str, Any], catalog: list[Site]) -> list[Site]:
    by_id = {s.location_id: s for s in catalog}
    sites = []
    for entry in manifest["sites"]:
        site = by_id.get(entry["location_id"])
        if site is None or (site.lat, site.lon, site.elevation_m) != (
            entry["lat"],
            entry["lon"],
            entry["elevation_m"],
        ):
            raise BuildRefused(
                f"real-data site {entry['location_id']} differs from the one fetched; build with "
                "the catalog the fetch used"
            )
        sites.append(site)
    return sites


def _check_rerun(root: Path, run_id: str, seed: int, digests: dict[str, str], raw: str) -> None:
    path = root / "build.json"
    if not path.exists():
        return
    old = json.loads(path.read_text())
    if old.get("run_id") != run_id:
        return
    if (old.get("digests"), old.get("raw_set_digest"), old.get("seed")) != (digests, raw, seed):
        raise BuildRefused(
            f"{root.name} was built under run_id {run_id!r} from other inputs (config, "
            "calibration, raw data or seed); the same ids would carry new values (risk G11). "
            "Pass a new --run-id"
        )


# ── events ───────────────────────────────────────────────────────────────────────────────────────


def _forecasts(
    root: Path, sites: list[Site], settings: EmsSettings, run_id: str
) -> tuple[list[EnvelopeBase], list[str], list[str]]:
    events: list[EnvelopeBase] = []
    empty: list[str] = []
    malformed: list[str] = []
    for site in sites:
        model = model_for(site)
        folder = root / "raw" / FORECAST_SOURCE / site.location_id
        for body in sorted(folder.glob("*.json")):
            if body.name.endswith((".meta.json", ".missing.json")):
                continue
            name = f"{site.location_id}/{body.stem}"
            issued = datetime.strptime(body.stem, "%Y%m%dT%H%M").replace(tzinfo=UTC)
            try:
                event = build_event(
                    site,
                    model,
                    issued,
                    json.loads(body.read_bytes()),
                    settings.mappings_dir,
                    _fetched_at(body),
                    run_id,
                    source_system=FORECAST_SOURCE,
                )
            except (ValueError, KeyError, TypeError) as exc:
                log.warning("history_run_malformed", run=name, error=str(exc))
                malformed.append(name)
                continue
            if event is None:
                empty.append(name)
            else:
                events.append(event)
    return events, empty, malformed


def _page_number(path: Path) -> int:
    return int(path.stem.rsplit("-p", 1)[1])


def _prices(
    root: Path, settings: EmsSettings, run_id: str, start: datetime, end: datetime
) -> tuple[list[EnvelopeBase], list[str], list[str]]:
    report = REPORTS["rce-pln"]
    mapping = builtin(report.mapping, settings.mappings_dir)
    pages: dict[str, list[Path]] = defaultdict(list)
    for body in (root / "raw" / "pse-rce").glob("*.json"):
        if not body.name.endswith(".meta.json"):
            pages[body.stem.rsplit("-p", 1)[0]].append(body)
    events: list[EnvelopeBase] = []
    flagged: list[str] = []
    incomplete: list[str] = []
    for key in sorted(pages):
        day = date.fromisoformat(key)
        bodies = sorted(pages[key], key=_page_number)
        rows = [r for b in bodies for r in apply(mapping, json.loads(b.read_bytes()))]
        ingested = max(_fetched_at(b) for b in bodies)
        try:
            event = build_day_event(report, day, rows, ingested, run_id)
        except IncompleteDay:
            incomplete.append(day.isoformat())
            continue
        lo, hi = local_day_bounds(day)
        assert event.issued_at is not None
        if not (lo < end and hi > start) and event.issued_at >= end:
            continue
        published = {r["published"] for r in rows if r.get("business_date") == day}
        latest = max(published)
        if len(published) > 1 or local_date(latest) != day - timedelta(days=1):
            flagged.append(day.isoformat())
        events.append(event)
    return events, flagged, incomplete


def _series(root: Path, site: Site) -> ObservedSeries:
    loc = site.location_id
    satellite = json.loads((root / "raw" / "satellite" / f"{loc}.json").read_bytes())
    era5 = json.loads((root / "raw" / "era5" / f"{loc}.json").read_bytes())
    labels = np.asarray(satellite["hourly"]["time"], dtype=np.int64)
    if labels.size > 1 and not (np.diff(labels) == 600).all():
        raise BuildRefused(f"satellite data at {loc} is not on a regular 10-minute grid")
    return parse_series(loc, satellite, era5)


def _mark(event: EnvelopeBase, assets: dict[AssetType, str]) -> EnvelopeBase:
    """The event with the given measurements marked ``estimated`` (``quality_raw`` names the
    truth's fallback). The id does not change: identity carries no values.
    """
    if not assets or not isinstance(event, MeasurementEvent):
        return event
    measurements = [
        m.model_copy(
            update={"quality": Quality.ESTIMATED, "quality_raw": f"truth:{assets[m.asset_type]}"}
        )
        if m.asset_type in assets
        else m
        for m in event.payload.measurements
    ]
    payload = event.payload.model_copy(update={"measurements": measurements})
    return event.model_copy(update={"payload": payload})


def _strongest(kinds: list[str]) -> str | None:
    return max(kinds, key=STRENGTH.__getitem__) if kinds else None


def _marks(masks: dict[str, Any], i: int, site: Site) -> dict[AssetType, str]:
    pv = _strongest([kind for name, kind in PV_MASKS.items() if masks[name][i]])
    hp = _strongest([kind for name, kind in HP_MASKS.items() if masks[name][i]])
    out: dict[AssetType, str] = {}
    if pv and site.pv is not None:
        out[AssetType.PV] = pv
    if hp and site.heat_pump is not None:
        out[AssetType.HEAT_PUMP] = hp
    flows = _strongest([k for k in (out.get(AssetType.PV), out.get(AssetType.HEAT_PUMP)) if k])
    if flows:  # grid flows (unmanaged sites) close the balance on PV and heat pump
        out[AssetType.GRID_CONNECTION] = flows
    return out


@dataclass(frozen=True)
class _Meters:
    events: list[EnvelopeBase]
    gap_shares: dict[str, dict[str, float]]
    diagnostics: dict[str, dict[str, float]]


def _meters(
    root: Path,
    settings: EmsSettings,
    cfg: Any,
    sites: list[Site],
    run_id: str,
    start: datetime,
    end: datetime,
) -> _Meters:
    params = load_params(settings.config_dir / "calibration" / "weather.yaml")
    observed = ObservedTruth(
        location_by_key={int(rng.location_key(s.location_id)): s.location_id for s in sites},
        series={s.location_id: _series(root, s) for s in sites},
        fallback=SyntheticTruth(cfg.seed, params),
    )
    model = SyntheticModel(
        sites, cfg.seed, settings.config_dir, cfg.site_chunk, truth_source=observed
    )
    t0 = quarter_index(start)
    axis = TimeAxis.from_range(t0, quarter_index(end) - t0)
    truth = observed(model.arrays, axis)
    masks = observed.masks(model.arrays, axis)
    diagnostics = observed.diagnostics(model.arrays, axis)
    gap_shares: dict[str, dict[str, float]] = {}
    for row, site in enumerate(sites):
        daylight = truth.geometry.cs_ghi[row] > 0
        n_day = int(daylight.sum())
        gap_shares[site.location_id] = {
            name: (float(m[daylight].sum()) / n_day if n_day else 0.0)
            for name, m in masks[site.location_id].items()
        }
    ingested = {
        s.location_id: max(
            _fetched_at(root / "raw" / "satellite" / f"{s.location_id}.json"),
            _fetched_at(root / "raw" / "era5" / f"{s.location_id}.json"),
        )
        for s in sites
    }
    by_id = {s.location_id: s for s in sites}
    streams = {
        **cfg.streams,
        "weather_forecast": {"mode": "off"},
        "weather_actual": {"mode": "off"},
        "price": {"mode": "off"},
    }
    meters_cfg = dataclasses.replace(cfg, streams=streams)
    events: list[EnvelopeBase] = []
    for _emit_at, _stream, make in emissions(
        model, meters_cfg, start, end, run_id, meter_source=METER_SOURCE
    ):
        event = make(start)  # ingested_at is replaced by the site's fetch time below
        assert isinstance(event, MeasurementEvent) and event.location_id is not None
        loc = event.location_id
        event = event.model_copy(update={"ingested_at": ingested[loc]})
        marks = _marks(masks[loc], event.t - t0, by_id[loc])
        events.append(_mark(event, marks))
    return _Meters(events=events, gap_shares=gap_shares, diagnostics=diagnostics)


def complete_local_days(start: datetime, end: datetime) -> dict[str, str] | None:
    """First and last Europe/Warsaw delivery day lying wholly inside ``[start, end)``."""
    days = []
    day = local_date(start)
    while local_day_bounds(day)[0] < end:
        lo, hi = local_day_bounds(day)
        if lo >= start and hi <= end:
            days.append(day)
        day += timedelta(days=1)
    if not days:
        return None
    return {"first": days[0].isoformat(), "last": days[-1].isoformat()}


# ── entry point ──────────────────────────────────────────────────────────────────────────────────


def build(
    settings: EmsSettings, name: str, *, seed: int | None = None, run_id: str | None = None
) -> BuildResult:
    """Build ``data/recordings/<name>`` into ``events.jsonl`` and ``build.json``."""
    t_wall = time.monotonic()
    root = settings.data_dir / "recordings" / name
    manifest = _manifest(root)
    run_id = run_id or name
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise BuildRefused(f"run_id {run_id!r} must match {RUN_ID_PATTERN.pattern}")
    _check_bodies(root)
    cfg = load_generator_config(root / "generator.yaml")
    if seed is not None:
        cfg = dataclasses.replace(cfg, seed=seed)
    sites = _real_sites(manifest, load_sites(settings.config_dir, cfg))
    digests = input_digests(settings, root)
    raw_digest = raw_set_digest(root)
    _check_rerun(root, run_id, cfg.seed, digests, raw_digest)

    start = datetime.combine(
        date.fromisoformat(manifest["window"]["start"]), datetime.min.time(), UTC
    )
    end = datetime.combine(date.fromisoformat(manifest["window"]["end"]), datetime.min.time(), UTC)
    forecasts, empty, malformed = _forecasts(root, sites, settings, run_id)
    prices, flagged, incomplete = _prices(root, settings, run_id, start, end)
    meters = _meters(root, settings, cfg, sites, run_id, start, end)
    meter_delay = timedelta(seconds=cfg.meter_delay_s)
    events = sorted(
        [*forecasts, *prices, *meters.events], key=lambda e: emission_key(e, start, meter_delay)
    )
    data = b"".join(to_json_bytes(e) + b"\n" for e in events)
    write_atomic(root / "events.jsonl", data)
    counts: dict[str, int] = {}
    for e in events:
        counts[e.stream.value] = counts.get(e.stream.value, 0) + 1

    from ems.ingest.runner import _git_commit, _versions

    info = {
        "kind": "ems-gen-history-build",
        "recording": name,
        "window": manifest["window"],
        "seed": cfg.seed,
        "run_id": run_id,
        "raw_set_digest": raw_digest,
        "digests": digests,
        "events_sha256": _sha256(data),
        "counts": counts,
        "gap_shares": meters.gap_shares,
        "diagnostics": meters.diagnostics,
        "price_days": {"flagged": flagged, "incomplete": incomplete},
        "empty_runs": empty,
        "malformed_runs": malformed,
        "complete_local_days": complete_local_days(start, end),
        "local_time_zone": str(WARSAW),
        "meter_source": METER_SOURCE,
        "forecast_source": FORECAST_SOURCE,
        "build_elapsed_s": round(time.monotonic() - t_wall, 1),
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
    write_atomic(root / "build.json", (json.dumps(info, indent=2, sort_keys=True) + "\n").encode())
    log.info("history_built", recording=name, events=len(events), run_id=run_id)
    return BuildResult(root=root, events=len(events), sha256=info["events_sha256"])


def read_build_info(root: Path) -> dict[str, Any]:
    path = root / "build.json"
    if not path.exists():
        raise BuildRefused(f"{root.name} has no build.json; run `ems-gen history build` first")
    return json.loads(path.read_text())


def window(manifest: dict[str, Any]) -> tuple[datetime, datetime]:
    start = date.fromisoformat(manifest["window"]["start"])
    end = date.fromisoformat(manifest["window"]["end"])
    return (
        datetime.combine(start, datetime.min.time(), UTC),
        datetime.combine(end, datetime.min.time(), UTC),
    )


def observed_truth_series(settings: EmsSettings, name: str) -> dict[str, Any]:
    """Per real-data site the observed truth of a built recording on its window, for the
    forecast-error fit: quarters filled from ERA5 or the synthetic truth are NaN (GHI), and so are
    synthetic temperature and wind. Returns ``{location_id: TruthSeries}``.
    """
    from ems.ingest.calibration import TruthSeries

    root = settings.data_dir / "recordings" / name
    manifest = _manifest(root)
    info = read_build_info(root)
    cfg = dataclasses.replace(
        load_generator_config(root / "generator.yaml"), seed=int(info["seed"])
    )
    sites = _real_sites(manifest, load_sites(settings.config_dir, cfg))
    params = load_params(settings.config_dir / "calibration" / "weather.yaml")
    observed = ObservedTruth(
        location_by_key={int(rng.location_key(s.location_id)): s.location_id for s in sites},
        series={s.location_id: _series(root, s) for s in sites},
        fallback=SyntheticTruth(cfg.seed, params),
    )
    arrays = SyntheticModel(sites, cfg.seed, settings.config_dir).arrays
    start, end = window(manifest)
    t0 = quarter_index(start)
    axis = TimeAxis.from_range(t0, quarter_index(end) - t0)
    truth = observed(arrays, axis)
    masks = observed.masks(arrays, axis)
    out: dict[str, Any] = {}
    for row, site in enumerate(sites):
        m = masks[site.location_id]
        filled = m["radiation_era5"] | m["radiation_synthetic"]
        out[site.location_id] = TruthSeries(
            t0=t0,
            ghi=np.where(filled, np.nan, truth.ghi[row]),
            cs_ghi=truth.geometry.cs_ghi[row],
            temp_c=np.where(m["temperature_synthetic"], np.nan, truth.temp_c[row]),
            wind10=np.where(m["wind_synthetic"], np.nan, truth.wind10[row]),
        )
    return out
