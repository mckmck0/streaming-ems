"""AC-004-07 and AC-005-04: EV charging and the site types kept by spec 005."""

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
    Stream,
    validate_strict,
)
from ems.contracts.time import quarter_index
from ems.ingest import rng
from ems.ingest.catalog.build import (
    build_catalog,
    generate_plant,
    location_id,
    site_type_for,
)
from ems.ingest.catalog.sites import EvChargerAsset, PvAsset, SiteType
from ems.ingest.generator import GeneratorConfig, SyntheticModel, emissions
from ems.ingest.localtime import local_hour, local_unix
from ems.ingest.synthetic.ev import ev_kwh

CONFIG = Path(__file__).resolve().parents[3] / "config"
YEAR_T0 = quarter_index(datetime(2025, 1, 1, tzinfo=UTC))
MIX = {"household": 0.6, "commercial": 0.2, "solar_farm": 0.2}


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


def test_ac_005_04_site_mix_shares_and_stability() -> None:
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
    for i in range(1, 6):
        farm = generate_plant(i, SiteType.SOLAR_FARM, catalog_seed=7, global_seed=4242)
        assert [type(a) for a in farm.assets] == [PvAsset]
        assert farm.tariff_group.value == "C21"
        assert not farm.ems_managed
    assert all(not s.ems_managed for s in commercial)


def test_ac_005_04_mixed_day_events_validate_and_balance() -> None:
    """A full June day (PV, evening battery discharge) of a household/commercial/solar-farm mix."""
    sites = build_catalog(
        120, catalog_seed=7, global_seed=4242, site_mix=MIX, site_controlled_bess_share=0.5
    )
    assert {s.site_type for s in sites} == {
        SiteType.HOUSEHOLD,
        SiteType.COMMERCIAL,
        SiteType.SOLAR_FARM,
    }
    model = SyntheticModel(sites, 4242, CONFIG)
    start = datetime(2026, 6, 15, tzinfo=UTC)
    cfg = GeneratorConfig(streams={"weather_forecast": {"mode": "off"}})
    seen: set[AssetType] = set()
    production: set[AssetType] = set()
    flows: dict[tuple[str, datetime], dict[str, float]] = {}
    for at, _stream, build in emissions(model, cfg, start, start + timedelta(days=1)):
        event = build(at)
        validate_strict(event.model_dump(mode="json"))
        if not isinstance(event, MeasurementEvent):
            continue
        acc = flows.setdefault((event.subject, event.interval_start), {})
        for m in event.payload.measurements:
            seen.add(m.asset_type)
            if event.stream is Stream.PRODUCTION:
                production.add(m.asset_type)
            if m.quantity is Quantity.ACTIVE_ENERGY and m.aggregation is Aggregation.INTERVAL_SUM:
                acc[m.direction.value] = acc.get(m.direction.value, 0.0) + m.value
    assert production == {AssetType.PV, AssetType.BESS}
    assert {AssetType.EV_CHARGER, AssetType.HEAT_PUMP, AssetType.GRID_CONNECTION} <= seen
    # Unmanaged sites close their balance in every interval.
    managed = {s.location_id for s in sites if s.ems_managed}
    unmanaged = [s for s in sites if s.location_id not in managed]
    checked = 0
    for (loc, _), f in flows.items():
        if loc in managed:
            continue
        supply = f.get("generation", 0) + f.get("discharge", 0) + f.get("import", 0)
        demand = f.get("consumption", 0) + f.get("charge", 0) + f.get("export", 0)
        assert supply == pytest.approx(demand, rel=1e-9, abs=1e-6)
        checked += 1
    assert checked == 96 * len(unmanaged)


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
