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

import dataclasses
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
    Aggregation,
    AssetType,
    Direction,
    Market,
    Measurement,
    MeasurementEvent,
    PriceEvent,
    Quality,
    Quantity,
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
from ems.ingest.localtime import local_hour, local_unix
from ems.ingest.synthetic.balance import grid_flows
from ems.ingest.synthetic.bess import BessArrays, dispatch
from ems.ingest.synthetic.ev import ev_kwh
from ems.ingest.synthetic.heat_pump import heat_pump_kwh
from ems.ingest.synthetic.load import base_load_kwh
from ems.ingest.synthetic.outages import availability
from ems.ingest.synthetic.plants import biogas_kwh, biomass_kwh, chp_kwh, hydro_kwh
from ems.ingest.synthetic.prices import day_prices, price_curve, price_params, publication_time
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
from ems.ingest.synthetic.wind import wind_energy_kwh

F64 = NDArray[np.float64]
Event = MeasurementEvent | WeatherForecastEvent | PriceEvent
Builder = Callable[[datetime], Event]

FORECAST_STEPS = 192
FORECAST_CADENCE_Q = 12  # 3 h in quarter-hours
STREAM_ORDER = {"price": 0, "weather_forecast": 1, "production": 2, "consumption": 3}
# Outage defaults (spec 004 assumptions): forced share of days, planned days per year.
OUTAGES = {
    "wind": (0.02, 0),
    "chp": (0.01, 14),
    "biogas": (0.01, 14),
    "biomass": (0.01, 14),
    "hydro": (0.005, 7),
}
# Generation assets beyond PV: chunk field, catalog property, contract asset type.
PLANT_KINDS = (
    ("wind_kwh", "wind", AssetType.WIND),
    ("chp_kwh", "chp", AssetType.CHP),
    ("biogas_kwh", "biogas", AssetType.BIOGAS),
    ("biomass_kwh", "biomass", AssetType.BIOMASS),
    ("hydro_kwh", "hydro", AssetType.HYDRO),
)


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
    site_chunk: int = 2000
    site_controlled_bess_share: float = 0.0
    emit_registers: bool = False
    faults: dict[str, Any] = field(default_factory=dict)
    site_mix: dict[str, float] = field(default_factory=dict)

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
        emit_registers=bool((raw.get("meters") or {}).get("emit_registers", False)),
        site_chunk=int(scale.get("site_chunk", 2000)),
        site_controlled_bess_share=float(scale.get("site_controlled_bess_share", 0.0)),
        site_mix={str(k): float(v) for k, v in (scale.get("site_mix") or {}).items()},
        faults=dict(raw.get("faults") or {}),
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
        site_controlled_bess_share=cfg.site_controlled_bess_share,
        site_mix=cfg.site_mix or None,
    )


def _zeros() -> F64:
    return np.zeros((0, 0))


@dataclass(frozen=True)
class Chunk:
    t0: int
    n: int  # meter intervals in this chunk
    pv_kwh: F64
    load_kwh: F64
    hp_kwh: F64
    truth_axis: TimeAxis
    forecasts: dict[int, ForecastRun]
    bess_charge_kwh: F64 = field(default_factory=lambda: np.zeros((0, 0)))
    bess_discharge_kwh: F64 = field(default_factory=lambda: np.zeros((0, 0)))
    bess_soc_kwh: F64 = field(default_factory=lambda: np.zeros((0, 0)))
    import_register_kwh: F64 = field(default_factory=lambda: np.zeros((0, 0)))
    wind_kwh: F64 = field(default_factory=_zeros)
    chp_kwh: F64 = field(default_factory=_zeros)
    chp_heat_kwh: F64 = field(default_factory=_zeros)
    biogas_kwh: F64 = field(default_factory=_zeros)
    biomass_kwh: F64 = field(default_factory=_zeros)
    hydro_kwh: F64 = field(default_factory=_zeros)
    ev_kwh: F64 = field(default_factory=_zeros)

    def generation(self, s_idx: int, i: int) -> float:
        """All site generation in one interval (PV plus plants), kWh."""
        total = float(self.pv_kwh[s_idx, i])
        for name, _, _ in PLANT_KINDS:
            arr = getattr(self, name)
            if arr.size:
                total += float(arr[s_idx, i])
        return total

    def consumption(self, s_idx: int, i: int) -> float:
        """Uncontrolled site consumption in one interval (base load, heat pump, EV), kWh."""
        total = float(self.load_kwh[s_idx, i] + self.hp_kwh[s_idx, i])
        return total + (float(self.ev_kwh[s_idx, i]) if self.ev_kwh.size else 0.0)


def _take(obj: Any, idx: slice) -> Any:
    """Row subset of a dataclass of per-site arrays."""
    return dataclasses.replace(
        obj, **{f.name: getattr(obj, f.name)[idx] for f in dataclasses.fields(obj)}
    )


def _concat_runs(parts: list[ForecastRun]) -> ForecastRun:
    first = parts[0]
    arrays = {
        f.name: np.concatenate([getattr(p, f.name) for p in parts], axis=0)
        for f in dataclasses.fields(first)
        if isinstance(getattr(first, f.name), np.ndarray)
    }
    return dataclasses.replace(first, **arrays)


class SyntheticModel:
    def __init__(
        self, sites: list[Site], seed: int, config_dir: Path, site_chunk: int = 2000
    ) -> None:
        self.sites = sites
        self.turbine_curves = config_dir / "turbines" / "power_curves.csv"
        self.seed = seed
        self.site_chunk = max(1, site_chunk)
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
        # Site-controlled batteries (background sites); EMS-managed batteries are never simulated.
        bess = [
            s.bess if (s.bess is not None and s.bess.control == "site") else None for s in sites
        ]
        self.bess = BessArrays(
            present=np.array([b is not None for b in bess]),
            capacity_kwh=np.array([b.capacity_kwh if b else 1.0 for b in bess]),
            p_max_kw=np.array([b.p_max_kw if b else 1.0 for b in bess]),
            eta=np.array([b.eta if b else 1.0 for b in bess]),
            soc_min_kwh=np.array([b.soc_min_kwh if b else 0.0 for b in bess]),
            soc_max_kwh=np.array([b.soc_max_kwh if b else 1.0 for b in bess]),
        )
        # Other assets (spec 004): absent assets are zero-rated rows.
        wind = [s.wind for s in sites]
        self.wind_types: list[str | None] = [w.turbine_type if w else None for w in wind]
        self.wind_count = np.array([float(w.count) if w else 0.0 for w in wind])
        self.wind_hub = np.array([w.hub_height_m if w else 100.0 for w in wind])
        chp = [s.chp for s in sites]
        self.chp_rated = np.array([c.rated_kw if c else 0.0 for c in chp])
        self.chp_p2h = np.array([c.power_to_heat if c else 1.0 for c in chp])
        self.chp_min = np.array([c.min_load if c else 0.5 for c in chp])
        self.chp_heat_k = np.array([c.heat_kw_per_k if c else 0.0 for c in chp])
        self.chp_heat_base = np.array([c.heat_base_kw if c else 0.0 for c in chp])
        self.biogas_rated = np.array([b.rated_kw if (b := s.biogas) else 0.0 for s in sites])
        bio = [s.biomass for s in sites]
        self.biomass_rated = np.array([b.rated_kw if b else 0.0 for b in bio])
        self.biomass_min = np.array([b.min_load if b else 0.5 for b in bio])
        self.biomass_ramp = np.array([b.ramp_kw_per_step if b else 0.0 for b in bio])
        self.hydro_rated = np.array([h.rated_kw if (h := s.hydro) else 0.0 for s in sites])
        ev = [s.ev for s in sites]
        self.ev_patterns: list[str | None] = [e.pattern if e else None for e in ev]
        self.ev_p = np.array([e.p_max_kw if e else 1.0 for e in ev])
        self.ev_need = np.array([e.daily_need_kwh if e else 0.0 for e in ev])
        # State carried from one chunk to the next (SoC, cumulative import register, biomass level).
        self._carry_t: int | None = None
        self._soc = self.bess.initial_soc()
        self._register = np.zeros(len(sites))
        self._biomass_level: F64 | None = None

    def _subset(
        self, idx: slice, axis: TimeAxis, n: int, issues: list[int], override: Any
    ) -> tuple[F64, F64, F64, dict[int, ForecastRun], Truth, dict[str, F64]]:
        arrays = _take(self.arrays, idx)
        truth = generate_truth(arrays, axis, self.seed, self.weather_params)
        if override is not None:
            truth = override(truth)
        pv = pv_energy_kwh(truth, _take(self.pv, idx))[:, :n]
        t = axis.t[:n]
        load = base_load_kwh(self.seed, arrays.keys, self.profiles[idx], self.annual_kwh[idx], t)
        hour_local = local_hour(axis.unix_mid[:n])
        hp = heat_pump_kwh(
            truth.temp_c[:, :n],
            hour_local,
            self.hp_loss[idx],
            self.hp_dhw[idx],
            self.hp_base[idx],
            self.hp_eta[idx],
        )
        runs = {
            issue: forecast_run(
                truth, issue, FORECAST_STEPS, self.seed, arrays.keys, self.weather_params
            )
            for issue in issues
        }
        return pv, load, hp, runs, truth, self._other_assets(idx, arrays, axis, n, truth)

    def _avail(self, keys: NDArray[np.uint64], t: NDArray[np.int64], kind: str) -> F64:
        forced, planned = OUTAGES[kind]
        return availability(self.seed, keys, t, kind, forced, planned)

    def _other_assets(
        self, idx: slice, arrays: SiteArrays, axis: TimeAxis, n: int, truth: Truth
    ) -> dict[str, F64]:
        """Wind, CHP, biogas, hydro and EV energy for one slice (biomass needs the full price
        curve and runs over all sites in ``chunk``).
        """
        keys, t = arrays.keys, axis.t[:n]
        out: dict[str, F64] = {}
        if any(self.wind_types[idx]):
            out["wind_kwh"] = wind_energy_kwh(
                self.turbine_curves,
                self.wind_types[idx],
                self.wind_count[idx],
                self.wind_hub[idx],
                truth.wind10[:, :n],
                arrays.shear_alpha,
                truth.temp_c[:, :n],
                arrays.elevation_m,
                self._avail(keys, t, "wind"),
            )
        local = local_unix(axis.unix_start[:n])
        if self.chp_rated[idx].any():
            weekday = ((np.floor(local / 86400.0).astype(np.int64) + 3) % 7) < 5
            out["chp_kwh"], out["chp_heat_kwh"] = chp_kwh(
                truth.temp_c[:, :n],
                weekday,
                self.chp_rated[idx],
                self.chp_p2h[idx],
                self.chp_min[idx],
                self.chp_heat_k[idx],
                self.chp_heat_base[idx],
                self._avail(keys, t, "chp"),
            )
        if self.biogas_rated[idx].any():
            out["biogas_kwh"] = biogas_kwh(
                self.seed,
                keys,
                int(t[0]),
                n,
                self.biogas_rated[idx],
                self._avail(keys, t, "biogas"),
            )
        if self.hydro_rated[idx].any():
            doy = axis.calendar(axis.unix_mid[:n])[0].astype(np.float64)
            out["hydro_kwh"] = hydro_kwh(
                self.seed,
                keys,
                int(t[0]),
                n,
                doy,
                self.hydro_rated[idx],
                self._avail(keys, t, "hydro"),
            )
        if any(self.ev_patterns[idx]):
            out["ev_kwh"] = ev_kwh(
                self.seed, keys, local, self.ev_p[idx], self.ev_need[idx], self.ev_patterns[idx]
            )
        return out

    def chunk(
        self,
        t0: int,
        n: int,
        forecast_issues: list[int],
        weather_override: Callable[[Truth], Truth] | None = None,
    ) -> Chunk:
        """Energy for meter intervals ``t0 .. t0+n-1`` and the forecast runs issued in that span.

        Sites are processed in slices of ``site_chunk`` to bound memory; every per-site computation
        is row-wise, so slicing never changes a value.
        """
        horizon_end = max([t0 + n, *[t + FORECAST_STEPS for t in forecast_issues]])
        axis = TimeAxis.from_range(t0, horizon_end - t0)
        n_loc = len(self.sites)
        pv_parts, load_parts, hp_parts = [], [], []
        other_parts: list[tuple[int, dict[str, F64]]] = []
        run_parts: dict[int, list[ForecastRun]] = {i: [] for i in forecast_issues}
        if weather_override is not None and n_loc > self.site_chunk:
            raise ValueError("weather overrides need all sites in one slice")
        for lo in range(0, max(n_loc, 1), self.site_chunk):
            idx = slice(lo, min(lo + self.site_chunk, n_loc))
            pv, load, hp, runs, _truth, other = self._subset(
                idx, axis, n, forecast_issues, weather_override
            )
            other_parts.append((idx.stop - idx.start, other))
            pv_parts.append(pv)
            load_parts.append(load)
            hp_parts.append(hp)
            for issue, run in runs.items():
                run_parts[issue].append(run)
        pv_all = np.concatenate(pv_parts, axis=0)
        load_all = np.concatenate(load_parts, axis=0)
        hp_all = np.concatenate(hp_parts, axis=0)
        other_all: dict[str, F64] = {}
        for name in ("wind_kwh", "chp_kwh", "chp_heat_kwh", "biogas_kwh", "hydro_kwh", "ev_kwh"):
            if any(name in o for _, o in other_parts):
                other_all[name] = np.concatenate(
                    [o.get(name, np.zeros((rows, n))) for rows, o in other_parts], axis=0
                )
        charge = np.zeros_like(pv_all)
        discharge = np.zeros_like(pv_all)
        soc = np.zeros_like(pv_all)
        if self._carry_t != t0:  # not a continuation: start batteries and registers fresh
            self._soc = self.bess.initial_soc()
            self._register = np.zeros(n_loc)
            self._biomass_level = None
        day_index = (local_unix(axis.unix_start[:n]) // 86400).astype(np.int64)
        if self.biomass_rated.any():
            keys = self.arrays.keys
            other_all["biomass_kwh"], self._biomass_level = biomass_kwh(
                self.price_curve(t0, n),
                self.biomass_rated,
                self.biomass_min,
                self.biomass_ramp,
                self._avail(keys, axis.t[:n], "biomass"),
                self._biomass_level,
            )
        gen_all = pv_all + sum(
            (other_all[k] for k, _, _ in PLANT_KINDS if k in other_all), np.zeros_like(pv_all)
        )
        cons_all = load_all + hp_all + other_all.get("ev_kwh", np.zeros_like(pv_all))
        if self.bess.present.any():
            prices = self.price_curve(t0, n)
            hour_local = local_hour(axis.unix_mid[:n])
            charge, discharge, soc = dispatch(
                self.bess, cons_all - gen_all, prices, hour_local, day_index, self._soc
            )
            self._soc = soc[:, -1].copy()
        imports = np.clip(cons_all + charge - gen_all - discharge, 0.0, None)
        register = self._register.reshape(-1, 1) + np.cumsum(imports, axis=1)
        self._register = register[:, -1].copy()
        self._carry_t = t0 + n
        return Chunk(
            t0=t0,
            n=n,
            pv_kwh=pv_all,
            load_kwh=load_all,
            hp_kwh=hp_all,
            truth_axis=axis,
            forecasts={i: _concat_runs(parts) for i, parts in run_parts.items()},
            bess_charge_kwh=charge,
            bess_discharge_kwh=discharge,
            bess_soc_kwh=soc,
            import_register_kwh=register,
            **other_all,
        )

    def price_curve(self, t0: int, n: int) -> F64:
        return price_curve(t0, n, self.seed, self.price_params, self.weather_params)

    def day_prices(self, day: date) -> F64:
        return day_prices(day, self.seed, self.price_params, self.weather_params)

    def publication(self, day: date) -> datetime:
        return publication_time(day, self.seed, self.price_params)


# ── event builders ────────────────────────────────────────────────────────────────────────────────


def _meter_builders(
    model: SyntheticModel, chunk: Chunk, i: int, run_id: str | None, emit_registers: bool = False
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
        for name, prop, asset_type in PLANT_KINDS:
            asset = getattr(site, prop)
            arr = getattr(chunk, name)
            if asset is not None and arr.size:
                gens.append(
                    energy(asset.asset_id, asset_type, Direction.GENERATION, float(arr[s_idx, i]))
                )
        if site.chp is not None and chunk.chp_heat_kwh.size:
            gens.append(
                Measurement(
                    asset_id=site.chp.asset_id,
                    asset_type=AssetType.CHP,
                    quantity=Quantity.HEAT_ENERGY,
                    direction=Direction.GENERATION,
                    aggregation=Aggregation.INTERVAL_SUM,
                    value=float(chunk.chp_heat_kwh[s_idx, i]),
                    unit="kWh",
                    quality=Quality.SYNTHETIC,
                )
            )
        loads = [
            energy("load", AssetType.LOAD, Direction.CONSUMPTION, float(chunk.load_kwh[s_idx, i]))
        ]
        if site.ev is not None and chunk.ev_kwh.size:
            loads.append(
                energy(
                    site.ev.asset_id,
                    AssetType.EV_CHARGER,
                    Direction.CONSUMPTION,
                    float(chunk.ev_kwh[s_idx, i]),
                )
            )
        if site.heat_pump is not None:
            loads.append(
                energy(
                    site.heat_pump.asset_id,
                    AssetType.HEAT_PUMP,
                    Direction.CONSUMPTION,
                    float(chunk.hp_kwh[s_idx, i]),
                )
            )
        site_bess = site.bess is not None and site.bess.control == "site"
        if site_bess and chunk.bess_charge_kwh.size:
            assert site.bess is not None
            charge = float(chunk.bess_charge_kwh[s_idx, i])
            discharge = float(chunk.bess_discharge_kwh[s_idx, i])
            gens.append(energy(site.bess.asset_id, AssetType.BESS, Direction.DISCHARGE, discharge))
            loads.append(energy(site.bess.asset_id, AssetType.BESS, Direction.CHARGE, charge))
            loads.append(
                Measurement(
                    asset_id=site.bess.asset_id,
                    asset_type=AssetType.BESS,
                    quantity=Quantity.SOC,
                    direction=Direction.CHARGE,
                    aggregation=Aggregation.INSTANT,
                    value=float(chunk.bess_soc_kwh[s_idx, i]),
                    unit="kWh",
                    quality=Quality.SYNTHETIC,
                )
            )
        else:
            charge = discharge = 0.0
        if not site.ems_managed:
            gen_total = chunk.generation(s_idx, i)
            load_total = chunk.consumption(s_idx, i)
            imp, exp = grid_flows(
                np.array([load_total]),
                np.array([gen_total]),
                np.array([charge]),
                np.array([discharge]),
            )
            loads.append(energy("grid", AssetType.GRID_CONNECTION, Direction.IMPORT, float(imp[0])))
            loads.append(energy("grid", AssetType.GRID_CONNECTION, Direction.EXPORT, float(exp[0])))
            if emit_registers and chunk.import_register_kwh.size:
                loads.append(
                    Measurement(
                        asset_id="grid-register",
                        asset_type=AssetType.GRID_CONNECTION,
                        quantity=Quantity.ACTIVE_ENERGY,
                        direction=Direction.IMPORT,
                        aggregation=Aggregation.REGISTER,
                        value=float(chunk.import_register_kwh[s_idx, i]),
                        unit="kWh",
                        quality=Quality.SYNTHETIC,
                    )
                )

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
            for stream, build in _meter_builders(model, chunk, i, run_id, cfg.emit_registers):
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
