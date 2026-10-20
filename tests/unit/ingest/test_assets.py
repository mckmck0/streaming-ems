"""AC-004-01 … -08: wind, plants, EV charging and site types."""

from __future__ import annotations

import itertools
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import pytest

from ems.contracts.envelope import (
    Aggregation,
    AssetType,
    MeasurementEvent,
    Quantity,
    validate_strict,
)
from ems.contracts.time import quarter_index
from ems.ingest import rng
from ems.ingest.catalog.build import (
    TURBINES,
    build_catalog,
    generate_plant,
    location_id,
    site_type_for,
)
from ems.ingest.catalog.sites import (
    BiomassAsset,
    ChpAsset,
    EvChargerAsset,
    HydroAsset,
    Site,
    SiteType,
    WindAsset,
)
from ems.ingest.generator import GeneratorConfig, SyntheticModel, emissions
from ems.ingest.localtime import local_hour, local_unix
from ems.ingest.synthetic.ev import ev_kwh
from ems.ingest.synthetic.plants import biomass_kwh, hydro_flow_factor
from ems.ingest.synthetic.wind import air_density, power_curves, turbine_power_kw

CONFIG = Path(__file__).resolve().parents[3] / "config"
CURVES = CONFIG / "turbines" / "power_curves.csv"
YEAR_T0 = quarter_index(datetime(2025, 1, 1, tzinfo=UTC))
YEAR_N = 96 * 365
MIX = {
    "household": 0.6,
    "commercial": 0.1,
    "industrial": 0.05,
    "wind_farm": 0.08,
    "solar_farm": 0.07,
    "biogas_plant": 0.05,
    "hydro_plant": 0.05,
}


def _plant(site_type: SiteType, index: int = 1, **update: object) -> Site:
    site = generate_plant(index, site_type, catalog_seed=7, global_seed=4242)
    return site.model_copy(update=update)


def _year(site: Site) -> SyntheticModel:
    return SyntheticModel([site], 4242, CONFIG)


# ── wind ──────────────────────────────────────────────────────────────────────────────────────────


def test_ac_004_02_curve_bounds_and_density() -> None:
    curves = power_curves(CURVES)
    assert {"E-82/2350", "V90/2000", "N117/2400", "V126/3450"} <= set(curves)
    speeds = np.linspace(0.0, 40.0, 801)
    for curve in curves.values():
        cut_in = curve[0][np.flatnonzero(curve[1] > 0)[0]]
        cut_out = curve[0][-1]
        for rho in (1.10, 1.225, 1.35):
            p = turbine_power_kw(curve, speeds, np.full_like(speeds, rho))
            assert p.max() <= curve[2] + 1e-9
            assert (p[speeds < cut_in - 1.0] == 0).all()
            assert (p[speeds > cut_out + 2.0] == 0).all()
    # Denser air yields more power below rated speed; reference density at 15 °C, sea level.
    assert air_density(np.array([15.0]), np.array([0.0]))[0] == pytest.approx(1.225, abs=0.002)
    curve = curves["V90/2000"]
    v = np.array([7.0])
    assert (
        turbine_power_kw(curve, v, np.array([1.3]))[0]
        > turbine_power_kw(curve, v, np.array([1.2]))[0]
    )


def test_ac_004_01_pomeranian_capacity_factor() -> None:
    """A Pomeranian farm of each catalog turbine type: fleet mean about 25 %, each type
    plausible.
    """
    factors = []
    for turbine, rated, hub in TURBINES:
        farm = _plant(
            SiteType.WIND_FARM,
            voivodeship="22",
            lat=54.3,
            lon=17.5,
            elevation_m=80,
            assets=[
                WindAsset(
                    asset_id="wind-1",
                    turbine_type=turbine,
                    count=10,
                    hub_height_m=hub,
                    rated_kw=rated,
                )
            ],
        )
        c = _year(farm).chunk(YEAR_T0, YEAR_N, [])
        assert c.wind_kwh.max() <= rated * 10 * 0.25 + 1e-6
        factors.append(c.wind_kwh.sum() / (rated * 10 * 8760))
    assert 0.22 <= float(np.mean(factors)) <= 0.35, factors
    assert all(0.15 <= f <= 0.40 for f in factors), factors


# ── plants ────────────────────────────────────────────────────────────────────────────────────────


def test_ac_004_03_chp_seasonal_min_load_and_heat() -> None:
    chp = ChpAsset(asset_id="chp-1", rated_kw=1000.0, heat_kw_per_k=80.0, heat_base_kw=300.0)
    c = _year(_plant(SiteType.INDUSTRIAL, assets=[chp])).chunk(YEAR_T0, YEAR_N, [])
    e = c.chp_kwh[0]
    month = (np.arange(YEAR_N) // (96 * 30.5)).astype(int)
    winter = e[(month <= 1) | (month == 11)].mean()
    summer = e[(month >= 5) & (month <= 7)].mean()
    assert winter > summer
    running = e > 0
    assert (e[running] >= chp.min_load * chp.rated_kw * 0.25 - 1e-9).all()
    assert (e <= chp.rated_kw * 0.25 + 1e-9).all()
    heat = c.chp_heat_kwh[0]
    assert np.allclose(heat[running], e[running] / chp.power_to_heat)
    assert (heat[~running] == 0).all()


def test_ac_004_04_biogas_capacity_factor() -> None:
    site = _plant(SiteType.BIOGAS_PLANT)
    assert site.biogas is not None
    c = _year(site.model_copy(update={"assets": [site.biogas]})).chunk(YEAR_T0, YEAR_N, [])
    cf = c.biogas_kwh.sum() / (site.biogas.rated_kw * 8760)
    assert 0.80 <= cf <= 0.95, cf


def test_ac_004_05_biomass_ramp_limit() -> None:
    n = 96 * 7
    price = np.sin(np.arange(n) / 4.0) * 300 + 400
    ramp = np.array([50.0, 200.0])
    out, level = biomass_kwh(
        price, np.array([1000.0, 1000.0]), np.array([0.3, 0.3]), ramp, np.ones((2, n))
    )
    kw = out / 0.25
    assert (np.abs(np.diff(kw, axis=1)) <= ramp[:, None] + 1e-9).all()
    assert (kw >= 0).all() and (kw <= 1000.0 + 1e-9).all()
    # Carrying the level makes two chunks equal one long run.
    a, la = biomass_kwh(
        price[:300], np.array([1000.0]), np.array([0.3]), ramp[:1], np.ones((1, 300))
    )
    b, _ = biomass_kwh(
        price[300:],
        np.array([1000.0]),
        np.array([0.3]),
        ramp[:1],
        np.ones((1, n - 300)),
        la,
    )
    whole, _ = biomass_kwh(price, np.array([1000.0]), np.array([0.3]), ramp[:1], np.ones((1, n)))
    assert np.allclose(np.concatenate([a, b], axis=1), whole)
    assert level.shape == (2,)


def test_ac_004_05_biomass_ramp_in_model_across_chunks() -> None:
    bm = BiomassAsset(asset_id="biomass-1", rated_kw=1000.0, ramp_kw_per_step=40.0)
    model = _year(_plant(SiteType.BIOGAS_PLANT, assets=[bm]))
    first = model.chunk(YEAR_T0, 96, [])
    second = model.chunk(YEAR_T0 + 96, 96, [])
    kw = np.concatenate([first.biomass_kwh[0], second.biomass_kwh[0]]) / 0.25
    running = kw > 0
    steps = np.abs(np.diff(kw))[running[1:] & running[:-1]]
    assert (steps <= 40.0 + 1e-6).all()


def test_ac_004_06_hydro_spring_above_late_summer() -> None:
    doy = np.arange(1, 366, dtype=np.float64)
    flow = hydro_flow_factor(doy)
    assert flow[95:125].mean() > flow[213:243].mean()
    hydro = HydroAsset(asset_id="hydro-1", rated_kw=500.0)
    c = _year(_plant(SiteType.HYDRO_PLANT, assets=[hydro])).chunk(YEAR_T0, YEAR_N, [])
    april = c.hydro_kwh[0, 96 * 90 : 96 * 120].sum()
    august = c.hydro_kwh[0, 96 * 212 : 96 * 243].sum()
    assert april > august
    assert (c.hydro_kwh <= 500.0 * 0.25 + 1e-9).all()


def test_plants_produce_zero_in_outages() -> None:
    hydro = HydroAsset(asset_id="hydro-1", rated_kw=500.0)
    model = _year(_plant(SiteType.HYDRO_PLANT, assets=[hydro]))
    c = model.chunk(YEAR_T0, YEAR_N, [])
    days = c.hydro_kwh[0].reshape(-1, 96)
    off = (days == 0).all(axis=1)
    assert 7 <= off.sum() <= 20  # one 7-day planned block plus a few forced days


# ── EV ────────────────────────────────────────────────────────────────────────────────────────────


def test_ac_004_07_ev_window_power_and_need() -> None:
    keys = np.array([rng.location_key(location_id(i)) for i in range(1, 51)], dtype=np.uint64)
    t = np.arange(YEAR_T0, YEAR_T0 + 96 * 14, dtype=np.int64)
    unix = (t * 900).astype(np.float64)
    local = local_unix(unix)
    p = np.full(keys.size, 7.4)
    need = np.full(keys.size, 8.0)
    e = ev_kwh(4242, keys, local, p, need, ["overnight"] * keys.size)
    assert (e >= 0).all() and (e <= 7.4 * 0.25 + 1e-9).all()
    hours = local_hour(unix + 450.0)
    assert (
        e[:, (hours > 10.0) & (hours < 15.0)].sum() == 0
    )  # overnight sessions never charge midday
    # Each night delivers the (noisy) need: about 8 kWh per day on average.
    per_day = e[:, 96:].sum(axis=1) / 13
    assert 6.0 < float(per_day.mean()) < 11.0
    day = ev_kwh(
        4242,
        keys,
        local,
        np.full(keys.size, 22.0),
        np.full(keys.size, 20.0),
        ["daytime"] * keys.size,
    )
    assert day[:, (hours < 5.0) | (hours > 22.0)].sum() == 0


# ── site types ────────────────────────────────────────────────────────────────────────────────────


def test_ac_004_08_site_mix_shares_and_stability() -> None:
    sites = build_catalog(4000, catalog_seed=7, global_seed=4242, site_mix=MIX)
    counts = Counter(s.site_type.value for s in sites)
    for name, share in MIX.items():
        assert abs(counts[name] / 4000 - share) < 0.02, (name, counts[name])
    # Without a mix every generated site is a household, unchanged from earlier specs.
    plain = build_catalog(50, catalog_seed=7, global_seed=4242)
    assert all(s.site_type is SiteType.HOUSEHOLD for s in plain)
    mixed = {
        s.location_id: s for s in build_catalog(50, catalog_seed=7, global_seed=4242, site_mix=MIX)
    }
    for s in plain:
        if mixed[s.location_id].site_type is SiteType.HOUSEHOLD:
            assert mixed[s.location_id] == s
    assert site_type_for(7, "PL-LOC-000001", None) is SiteType.HOUSEHOLD


def test_site_types_have_matching_assets_and_tariffs() -> None:
    by_type = {
        SiteType.WIND_FARM: WindAsset,
        SiteType.INDUSTRIAL: ChpAsset,
        SiteType.HYDRO_PLANT: HydroAsset,
    }
    for kind, asset_cls in by_type.items():
        for i in range(1, 6):
            site = generate_plant(i, kind, catalog_seed=7, global_seed=4242)
            assert any(isinstance(a, asset_cls) for a in site.assets)
            assert not site.ems_managed
    commercial = [
        generate_plant(i, SiteType.COMMERCIAL, catalog_seed=7, global_seed=4242)
        for i in range(1, 30)
    ]
    assert {s.tariff_group.value for s in commercial} <= {"C11", "C21"}
    assert any(
        isinstance(a, EvChargerAsset) and a.pattern == "daytime"
        for s in commercial
        for a in s.assets
    )
    assert (
        generate_plant(3, SiteType.INDUSTRIAL, catalog_seed=7, global_seed=4242).tariff_group.value
        == "C21"
    )


def test_ac_004_08_mixed_day_events_validate_and_balance() -> None:
    sites = build_catalog(
        120, catalog_seed=7, global_seed=4242, site_mix=MIX, site_controlled_bess_share=0.5
    )
    model = SyntheticModel(sites, 4242, CONFIG)
    start = datetime(2026, 1, 15, tzinfo=UTC)
    seen: set[AssetType] = set()
    heat = False
    flows: dict[tuple[str, datetime], dict[str, float]] = {}
    for at, _stream, build in emissions(
        model, GeneratorConfig(), start, start + timedelta(hours=6)
    ):
        event = build(at)
        validate_strict(event.model_dump(mode="json"))
        if not isinstance(event, MeasurementEvent):
            continue
        acc = flows.setdefault((event.subject, event.interval_start), {})
        for m in event.payload.measurements:
            seen.add(m.asset_type)
            heat = heat or m.quantity is Quantity.HEAT_ENERGY
            if m.quantity is Quantity.ACTIVE_ENERGY and m.aggregation is Aggregation.INTERVAL_SUM:
                acc[m.direction.value] = acc.get(m.direction.value, 0.0) + m.value
    assert {
        AssetType.WIND,
        AssetType.CHP,
        AssetType.BIOGAS,
        AssetType.HYDRO,
        AssetType.EV_CHARGER,
    } <= seen
    assert heat
    # Unmanaged sites close their balance with every asset included.
    managed = {s.location_id for s in sites if s.ems_managed}
    checked = 0
    for (loc, _), f in flows.items():
        if loc in managed:
            continue
        supply = f.get("generation", 0) + f.get("discharge", 0) + f.get("import", 0)
        demand = f.get("consumption", 0) + f.get("charge", 0) + f.get("export", 0)
        assert supply == pytest.approx(demand, rel=1e-9, abs=1e-6)
        checked += 1
    assert checked > 1000


def test_ac_004_07_ev_sessions_charge_at_limit_until_need_met() -> None:
    """Within a session: full-power intervals, then at most one partial one, then nothing."""
    keys = np.array([rng.location_key(location_id(i)) for i in range(1, 31)], dtype=np.uint64)
    t = np.arange(YEAR_T0, YEAR_T0 + 96 * 10, dtype=np.int64)
    local = local_unix((t * 900).astype(np.float64))
    full = 7.4 * 0.25
    e = ev_kwh(
        4242, keys, local, np.full(keys.size, 7.4), np.full(keys.size, 6.0), ["overnight"] * 30
    )
    hours = (local % 86400.0) / 3600.0
    noon = np.flatnonzero(np.isclose(hours, 12.0))
    sessions = 0
    for lo, hi in itertools.pairwise(noon):  # one overnight session per noon-to-noon
        for row in e[:, lo:hi]:
            on = np.flatnonzero(row > 0)
            if on.size == 0:
                continue
            sessions += 1
            block = row[on[0] : on[-1] + 1]
            assert (block > 0).all()  # contiguous from arrival
            assert np.allclose(block[:-1], full)  # at the charger limit until the need is met
            assert 0 < block[-1] <= full + 1e-9
    assert sessions > 200
