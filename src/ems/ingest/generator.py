"""Turn the synthetic models into contract events, in emission-time order.

``SyntheticModel`` holds the site catalog as arrays and computes chunks of truth, PV, load and
heat-pump energy. ``emissions`` yields ``(emit_at, build)`` pairs sorted by emission time;
``build(ingested_at)`` creates the event, so ``ingested_at`` is the real produce time and never
enters identity.

Emission schedule (simulated time):
- meters: at interval end + availability delay, one production and one consumption event per site
  and interval (production only for sites with generation);
- forecasts: runs issued every 3 h (00, 03, ... UTC), 192 quarter-hour steps, emitted 1.5 h later;
- prices: one event per local delivery day, published D-1 in the early afternoon local time; the
  day(s) already published when the run starts are emitted at the start.
"""

from __future__ import annotations

import heapq
import itertools
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
from numpy.typing import NDArray
from ruamel.yaml import YAML

from ems.contracts.envelope import (
    AssetType,
    Direction,
    Market,
    Measurement,
    MeasurementEvent,
    PriceEvent,
    Stream,
    WeatherForecastEvent,
)
from ems.contracts.examples import energy, forecast_event, measurement_event, price_event
from ems.contracts.time import (
    QUARTER,
    from_quarter_index,
    local_date,
    quarter_index,
    require_utc,
)
from ems.ingest import rng
from ems.ingest.catalog.build import build_catalog, load_overrides
from ems.ingest.catalog.sites import Site
from ems.ingest.catalog.voivodeships import BY_TERYT
from ems.ingest.localtime import local_hour
from ems.ingest.synthetic.balance import grid_flows
from ems.ingest.synthetic.heat_pump import heat_pump_kwh
from ems.ingest.synthetic.load import base_load_kwh
from ems.ingest.synthetic.prices import day_prices, price_params, publication_time
from ems.ingest.synthetic.pv import PvArrays, pv_energy_kwh
from ems.ingest.synthetic.weather import (
    ForecastRun,
    SiteArrays,
    TimeAxis,
    Truth,
    forecast_run,
    generate_truth,
    load_params,
)

F64 = NDArray[np.float64]
Event = MeasurementEvent | WeatherForecastEvent | PriceEvent
Builder = Callable[[datetime], Event]

FORECAST_STEPS = 192
FORECAST_CADENCE_Q = 12  # 3 h in quarter-hours
STREAM_ORDER = {"price": 0, "weather_forecast": 1, "production": 2, "consumption": 3}


@dataclass(frozen=True)
class GeneratorConfig:
    seed: int = 4242
    mode: str = "backfill"
    speed: float = 96.0
    start: datetime | None = None
    end: datetime | None = None
    streams: dict[str, dict[str, Any]] = field(default_factory=dict)
    meter_delay_s: int = 0
    synthetic_locations: int = 1
    catalog_seed: int = 7
    chunk_days: int = 1
    live: dict[str, Any] = field(default_factory=dict)

    @property
    def live_locations(self) -> tuple[str, ...]:
        return tuple(str(x) for x in self.live.get("locations", []))

    def stream_mode(self, name: str) -> str:
        return str(self.streams.get(name, {}).get("mode", "synthetic"))

    def stream_on(self, name: str) -> bool:
        return str(self.streams.get(name, {}).get("mode", "synthetic")) != "off"

    @property
    def emit_initial_day(self) -> bool:
        return bool(self.streams.get("price", {}).get("emit_initial_day", True))

    def as_dict(self) -> dict[str, Any]:
        out = self.__dict__.copy()
        out["start"] = self.start.isoformat() if self.start else None
        out["end"] = self.end.isoformat() if self.end else None
        return out


def _dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return require_utc(value)
    return require_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))


def load_generator_config(path: Path) -> GeneratorConfig:
    raw: dict[str, Any] = YAML(typ="safe").load(path.read_text()) or {}
    clock = raw.get("clock", {})
    scale = raw.get("scale", {})
    return GeneratorConfig(
        seed=int(raw.get("seed", 4242)),
        mode=str(clock.get("mode", "backfill")),
        speed=float(clock.get("speed", 96.0)),
        start=_dt(clock.get("start")),
        end=_dt(clock.get("end")),
        streams={str(k): dict(v) for k, v in (raw.get("streams") or {}).items()},
        meter_delay_s=int((raw.get("meters") or {}).get("availability_delay_s", 0)),
        synthetic_locations=int(scale.get("synthetic_locations", 1)),
        catalog_seed=int(scale.get("catalog_seed", 7)),
        chunk_days=int(raw.get("chunk_days", 1)),
        live=dict(raw.get("live") or {}),
    )


def load_sites(config_dir: Path, cfg: GeneratorConfig) -> list[Site]:
    overrides = load_overrides(config_dir / "sites.yaml", cfg.seed)
    return build_catalog(
        cfg.synthetic_locations,
        catalog_seed=cfg.catalog_seed,
        global_seed=cfg.seed,
        overrides=overrides,
    )


@dataclass(frozen=True)
class Chunk:
    t0: int
    n: int  # meter intervals in this chunk
    pv_kwh: F64
    load_kwh: F64
    hp_kwh: F64
    truth_axis: TimeAxis
    forecasts: dict[int, ForecastRun]


class SyntheticModel:
    def __init__(self, sites: list[Site], seed: int, config_dir: Path) -> None:
        self.sites = sites
        self.seed = seed
        self.weather_params = load_params(config_dir / "calibration" / "weather.yaml")
        self.price_params = price_params(config_dir / "calibration" / "prices.yaml")
        alpha = self.weather_params["wind"]["shear_alpha"]
        self.arrays = SiteArrays(
            keys=np.array([rng.location_key(s.location_id) for s in sites], dtype=np.uint64),
            lat=np.array([s.lat for s in sites]),
            lon=np.array([s.lon for s in sites]),
            elevation_m=np.array([s.elevation_m for s in sites], dtype=np.float64),
            shear_alpha=np.array([float(alpha[s.site_type.value]) for s in sites]),
            wind_scale_ms=np.array([BY_TERYT[s.voivodeship].wind_scale_ms for s in sites]),
        )
        pv = [s.pv for s in sites]
        self.pv = PvArrays(
            kwp=np.array([p.kwp if p else 0.0 for p in pv]),
            tilt_deg=np.array([p.tilt_deg if p else 0.0 for p in pv]),
            azimuth_deg=np.array([p.azimuth_deg if p else 180.0 for p in pv]),
            ac_kw=np.array([p.ac_kw if p else 1.0 for p in pv]),
            gamma_pdc=np.array([p.gamma_pdc if p else -0.0037 for p in pv]),
            losses=np.array([p.losses if p else 0.0 for p in pv]),
        )
        hp = [s.heat_pump for s in sites]
        self.hp_loss = np.array([h.heat_loss_kw_per_k if h else 0.0 for h in hp])
        self.hp_dhw = np.array([h.dhw_kwh_per_year if h else 0.0 for h in hp])
        self.hp_base = np.array([h.base_temp_c if h else 15.0 for h in hp])
        self.hp_eta = np.array([h.carnot_eta if h else 0.45 for h in hp])
        self.annual_kwh = np.array([s.annual_base_load_kwh for s in sites])
        self.profiles = [s.profile for s in sites]

    def chunk(
        self,
        t0: int,
        n: int,
        forecast_issues: list[int],
        weather_override: Callable[[Truth], Truth] | None = None,
    ) -> Chunk:
        """Energy for meter intervals ``t0 .. t0+n-1`` and the forecast runs issued in that span."""
        horizon_end = max([t0 + n, *[t + FORECAST_STEPS for t in forecast_issues]])
        axis = TimeAxis.from_range(t0, horizon_end - t0)
        truth = generate_truth(self.arrays, axis, self.seed, self.weather_params)
        if weather_override is not None:
            truth = weather_override(truth)
        pv = pv_energy_kwh(truth, self.pv)[:, :n]
        t = axis.t[:n]
        load = base_load_kwh(self.seed, self.arrays.keys, self.profiles, self.annual_kwh, t)
        hour_local = local_hour(axis.unix_mid[:n])
        hp = heat_pump_kwh(
            truth.temp_c[:, :n], hour_local, self.hp_loss, self.hp_dhw, self.hp_base, self.hp_eta
        )
        forecasts = {
            issue: forecast_run(
                truth, issue, FORECAST_STEPS, self.seed, self.arrays.keys, self.weather_params
            )
            for issue in forecast_issues
        }
        return Chunk(
            t0=t0, n=n, pv_kwh=pv, load_kwh=load, hp_kwh=hp, truth_axis=axis, forecasts=forecasts
        )

    def day_prices(self, day: date) -> F64:
        return day_prices(day, self.seed, self.price_params, self.weather_params)

    def publication(self, day: date) -> datetime:
        return publication_time(day, self.seed, self.price_params)


# ── event builders ────────────────────────────────────────────────────────────────────────────────


def _meter_builders(
    model: SyntheticModel, chunk: Chunk, i: int, run_id: str | None
) -> list[tuple[str, Builder]]:
    start = from_quarter_index(chunk.t0 + i)
    out: list[tuple[str, Builder]] = []
    for s_idx, site in enumerate(model.sites):
        gens: list[Measurement] = []
        if site.pv is not None:
            gens.append(
                energy(
                    site.pv.asset_id,
                    AssetType.PV,
                    Direction.GENERATION,
                    float(chunk.pv_kwh[s_idx, i]),
                )
            )
        loads = [
            energy("load", AssetType.LOAD, Direction.CONSUMPTION, float(chunk.load_kwh[s_idx, i]))
        ]
        if site.heat_pump is not None:
            loads.append(
                energy(
                    site.heat_pump.asset_id,
                    AssetType.HEAT_PUMP,
                    Direction.CONSUMPTION,
                    float(chunk.hp_kwh[s_idx, i]),
                )
            )
        if not site.ems_managed:
            gen_total = float(chunk.pv_kwh[s_idx, i])
            load_total = float(chunk.load_kwh[s_idx, i] + chunk.hp_kwh[s_idx, i])
            imp, exp = grid_flows(np.array([load_total]), np.array([gen_total]))
            loads.append(energy("grid", AssetType.GRID_CONNECTION, Direction.IMPORT, float(imp[0])))
            loads.append(energy("grid", AssetType.GRID_CONNECTION, Direction.EXPORT, float(exp[0])))

        def mk(stream: Stream, ms: list[Measurement], site: Site = site) -> Builder:
            def build(ingested_at: datetime) -> Event:
                return measurement_event(
                    stream=stream,
                    location_id=site.location_id,
                    interval_start=start,
                    measurements=ms,
                    ingested_at=ingested_at,
                    run_id=run_id,
                    metering_point_id=site.metering_point_id,
                    seed=site.seed,
                )

            return build

        if gens:
            out.append(("production", mk(Stream.PRODUCTION, gens)))
        out.append(("consumption", mk(Stream.CONSUMPTION, loads)))
    return out


def _forecast_builders(
    model: SyntheticModel, run: ForecastRun, run_id: str | None
) -> list[Builder]:
    issued_at = from_quarter_index(run.issue_t)
    out: list[Builder] = []
    for s_idx, site in enumerate(model.sites):
        steps = [
            {
                "ghi_wm2": round(float(run.ghi[s_idx, k]), 1),
                "dni_wm2": round(float(run.dni[s_idx, k]), 1),
                "dhi_wm2": round(float(run.dhi[s_idx, k]), 1),
                "temp_air_c": round(float(run.temp_c[s_idx, k]), 1),
                "cloud_cover_pct": int(run.cloud_pct[s_idx, k]),
                "wind_speed_ms": {
                    "10": round(float(run.wind10[s_idx, k]), 2),
                    "100": round(float(run.wind100[s_idx, k]), 2),
                    "120": round(float(run.wind120[s_idx, k]), 2),
                },
                "wind_dir_deg": int(run.wind_dir[s_idx, k]) % 360,
            }
            for k in range(run.steps)
        ]

        def build(
            ingested_at: datetime, site: Site = site, steps: list[dict[str, Any]] = steps
        ) -> Event:
            return forecast_event(
                location_id=site.location_id,
                issued_at=issued_at,
                steps=steps,
                ingested_at=ingested_at,
                run_id=run_id,
                seed=site.seed,
            )

        out.append(build)
    return out


def _price_builder(model: SyntheticModel, day: date, run_id: str | None) -> Builder:
    prices = model.day_prices(day)
    published = model.publication(day)

    def build(ingested_at: datetime) -> Event:
        return price_event(
            market=Market.PL_RCE,
            delivery_day=day,
            prices=[float(p) for p in prices],
            published_at=published,
            ingested_at=ingested_at,
            run_id=run_id,
            seed=model.seed,
        )

    return build


# ── schedule ──────────────────────────────────────────────────────────────────────────────────────


@dataclass(order=True)
class _Pending:
    emit_at: datetime
    order: int
    seq: int
    stream: str = field(compare=False)
    build: Builder = field(compare=False)


def emissions(
    model: SyntheticModel,
    cfg: GeneratorConfig,
    start: datetime,
    end: datetime | None,
    run_id: str | None = None,
) -> Iterator[tuple[datetime, str, Builder]]:
    """Yield ``(emit_at, stream, build)`` in emission order for meter intervals starting in
    ``[start, end)`` (``end=None``: forever), forecast runs issued in that span and the price days
    published in it.
    """
    start = require_utc(start)
    t_start = quarter_index(start)
    if from_quarter_index(t_start) != start:
        raise ValueError("start must be aligned to a quarter hour")
    t_end = quarter_index(require_utc(end)) if end is not None else None
    delay = timedelta(seconds=cfg.meter_delay_s)
    chunk_q = 96 * max(1, cfg.chunk_days)
    seq = itertools.count()
    heap: list[_Pending] = []

    def push(emit_at: datetime, stream: str, build: Builder) -> None:
        heapq.heappush(heap, _Pending(emit_at, STREAM_ORDER[stream], next(seq), stream, build))

    if cfg.stream_on("price") and cfg.emit_initial_day:
        day = local_date(start)
        while model.publication(day) <= start:
            push(start, "price", _price_builder(model, day, run_id))
            day += timedelta(days=1)

    c0 = t_start
    while t_end is None or c0 < t_end:
        c1 = c0 + chunk_q if t_end is None else min(c0 + chunk_q, t_end)
        issues = [t for t in range(c0, c1) if t % FORECAST_CADENCE_Q == 0]
        if not cfg.stream_on("weather_forecast"):
            issues = []
        chunk = model.chunk(c0, c1 - c0, issues)
        for i in range(chunk.n):
            interval_end = from_quarter_index(c0 + i) + QUARTER
            for stream, build in _meter_builders(model, chunk, i, run_id):
                if cfg.stream_on(stream):
                    push(interval_end + delay, stream, build)
        avail = timedelta(minutes=float(model.weather_params["forecast"]["availability_delay_min"]))
        for issue in issues:
            for build in _forecast_builders(model, chunk.forecasts[issue], run_id):
                push(from_quarter_index(issue) + avail, "weather_forecast", build)
        if cfg.stream_on("price"):
            chunk_start, chunk_end = from_quarter_index(c0), from_quarter_index(c1)
            day = local_date(chunk_start)
            last = local_date(chunk_end) + timedelta(days=2)
            while day <= last:
                pub = model.publication(day)
                if chunk_start <= pub < chunk_end and pub > start:
                    push(pub, "price", _price_builder(model, day, run_id))
                day += timedelta(days=1)
        # Everything due before the next chunk's first meter emission is final: release it in order.
        horizon = from_quarter_index(c1) + QUARTER + delay
        while heap and (heap[0].emit_at < horizon):
            p = heapq.heappop(heap)
            yield p.emit_at, p.stream, p.build
        c0 = c1
    while heap:
        p = heapq.heappop(heap)
        yield p.emit_at, p.stream, p.build


def record_time_bounds(
    model: SyntheticModel, cfg: GeneratorConfig, start: datetime, end: datetime
) -> tuple[datetime, datetime]:
    """Earliest and latest record timestamps a run over ``[start, end)`` produces (for
    preflight).
    """
    lo = start
    if cfg.stream_on("price") and cfg.emit_initial_day:
        lo = min(lo, model.publication(local_date(start)))
    hi = end + QUARTER + timedelta(seconds=cfg.meter_delay_s)
    return lo, max(hi, end)
