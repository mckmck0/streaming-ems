"""Build the site catalog deterministically from a seed; hand-defined sites override generated
ones.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import numpy as np
from faker import Faker
from ruamel.yaml import YAML

from ems.contracts.ids import location_seed, make_gsrn, stable_hash64
from ems.ingest.catalog.sites import (
    BessAsset,
    EvChargerAsset,
    HeatPumpAsset,
    PvAsset,
    Site,
    SiteType,
    tariff_for,
)
from ems.ingest.catalog.voivodeships import DSO_OPERATOR_PREFIX, VOIVODESHIPS, Voivodeship


def location_id(index: int) -> str:
    return f"PL-LOC-{index:06d}"


def _rng(catalog_seed: int, loc: str) -> np.random.Generator:
    # One generator per site: a site's metadata never depends on how many sites are built.
    return np.random.default_rng(stable_hash64("catalog", catalog_seed, loc))


def _bess_control(catalog_seed: int, loc: str, share: float) -> Literal["ems", "site"]:
    """Separate hash: changing the share never changes any other catalog draw."""
    u = stable_hash64("bess-control", catalog_seed, loc) / 2**64
    return "site" if u < share else "ems"


def _uniform(catalog_seed: int, loc: str, what: str) -> float:
    return stable_hash64(what, catalog_seed, loc) / 2**64


def _pick_region(rng: np.random.Generator, weight: str) -> Voivodeship:
    w = np.array([getattr(v, weight) for v in VOIVODESHIPS])
    return VOIVODESHIPS[int(rng.choice(len(VOIVODESHIPS), p=w / w.sum()))]


def _pv(kwp: float, tilt: float, dc_ac: float) -> PvAsset:
    return PvAsset(
        asset_id="pv-1", kwp=kwp, tilt_deg=tilt, azimuth_deg=180.0, ac_kw=round(kwp / dc_ac, 1)
    )


def _plant_assets(
    site_type: SiteType, rng: np.random.Generator
) -> tuple[list[Any], float, float, float]:
    """Assets, annual base load (kWh), contracted power (kW) and fuse (A) per non-household type."""
    assets: list[Any] = []
    if site_type is SiteType.COMMERCIAL:
        contracted = float(rng.choice([25.0, 40.0, 80.0, 150.0]))
        fuse = 63.0 if contracted <= 40 else 250.0
        annual = float(rng.uniform(15_000, 150_000))
        if rng.random() < 0.6:
            assets.append(_pv(round(float(rng.uniform(20, 150)), 1), 15.0, 1.1))
        if rng.random() < 0.5:
            need = round(float(rng.uniform(15, 40)), 1)
            assets.append(
                EvChargerAsset(
                    asset_id="ev-1", p_max_kw=22.0, daily_need_kwh=need, pattern="daytime"
                )
            )
        return assets, annual, contracted, fuse
    if site_type is SiteType.SOLAR_FARM:
        kwp = round(float(rng.uniform(1000, 20000)))
        assets.append(_pv(float(kwp), 25.0, 1.25))
        return assets, 1e-3 * kwp * 1000, kwp / 1.25, 1e4
    raise ValueError(f"no generator for site type {site_type}")


def generate_plant(index: int, site_type: SiteType, *, catalog_seed: int, global_seed: int) -> Site:
    """Commercial sites and solar farms (specs 004, 005); background, never EMS-managed."""
    loc = location_id(index)
    rng = _rng(catalog_seed, loc)
    v = _pick_region(rng, "pv_weight" if site_type is SiteType.SOLAR_FARM else "household_weight")
    fake = Faker("pl_PL")
    fake.seed_instance(stable_hash64("faker", catalog_seed, loc))
    assets, annual, contracted, fuse = _plant_assets(site_type, rng)
    p_lo, p_hi = v.postcode_prefixes
    return Site(
        location_id=loc,
        site_type=site_type,
        lat=round(float(rng.uniform(*v.lat)), 4),
        lon=round(float(rng.uniform(*v.lon)), 4),
        elevation_m=round(float(np.clip(rng.normal(v.elevation_m, 40), 0, 900))),
        voivodeship=v.teryt,
        city=str(rng.choice(v.cities)),
        street=fake.street_address(),
        postcode=f"{int(rng.integers(p_lo, p_hi + 1)):02d}-{int(rng.integers(0, 1000)):03d}",
        dso=v.dso,
        metering_point_id=make_gsrn(DSO_OPERATOR_PREFIX[v.dso], int(rng.integers(0, 10**10))),
        tariff_group=tariff_for(site_type, contracted, fuse, False),
        contracted_kw=round(contracted, 1),
        fuse_a=fuse,
        annual_base_load_kwh=round(max(annual, 100.0)),
        assets=assets,
        seed=location_seed(global_seed, loc),
    )


def check_site_mix(mix: dict[str, float] | None) -> None:
    """Fail fast on site types the generator does not model (spec 005 removed the plant types)."""
    unknown = sorted(set(mix or {}) - {t.value for t in SiteType})
    if unknown:
        allowed = ", ".join(sorted(t.value for t in SiteType))
        raise ValueError(
            f"scale.site_mix: unknown site type(s) {', '.join(unknown)}; allowed: {allowed}"
        )


def site_type_for(catalog_seed: int, loc: str, mix: dict[str, float] | None) -> SiteType:
    """Site type from its own hash, so changing the mix only moves sites across the
    changed shares.
    """
    if not mix:
        return SiteType.HOUSEHOLD
    names = sorted(mix)
    u = _uniform(catalog_seed, loc, "site-type") * sum(mix.values())
    acc = 0.0
    for name in names:
        acc += mix[name]
        if u < acc:
            return SiteType(name)
    return SiteType(names[-1])


def generate_household(
    index: int, *, catalog_seed: int, global_seed: int, site_controlled_bess_share: float = 0.0
) -> Site:
    loc = location_id(index)
    rng = _rng(catalog_seed, loc)
    weights = np.array([v.household_weight for v in VOIVODESHIPS])
    v: Voivodeship = VOIVODESHIPS[int(rng.choice(len(VOIVODESHIPS), p=weights / weights.sum()))]
    lat = float(rng.uniform(*v.lat))
    lon = float(rng.uniform(*v.lon))
    fake = Faker("pl_PL")
    fake.seed_instance(stable_hash64("faker", catalog_seed, loc))
    two_zone = bool(rng.random() < 0.4)
    contracted = float(rng.choice([7.0, 10.0, 12.5, 14.0]))
    tariff = tariff_for(SiteType.HOUSEHOLD, contracted, 25.0, two_zone)
    assets: list[Any] = []
    if rng.random() < 0.7:
        kwp = float(np.clip(rng.lognormal(np.log(7.0), 0.35), 2.5, 20.0))
        assets.append(
            PvAsset(
                asset_id="pv-1",
                kwp=round(kwp, 2),
                tilt_deg=round(float(rng.uniform(20, 40)), 1),
                azimuth_deg=round(float(np.clip(rng.normal(180, 30), 90, 270)), 1),
                ac_kw=round(kwp * float(rng.uniform(0.8, 1.0)), 2),
            )
        )
    if rng.random() < 0.3:
        assets.append(
            HeatPumpAsset(
                asset_id="hp-1",
                heat_loss_kw_per_k=round(float(rng.uniform(0.10, 0.22)), 3),
                dhw_kwh_per_year=round(float(rng.uniform(1800, 3200))),
            )
        )
    if _uniform(catalog_seed, loc, "ev") < 0.2:  # own hash: existing households keep their draws
        ev_rng = np.random.default_rng(stable_hash64("catalog.ev", catalog_seed, loc))
        assets.append(
            EvChargerAsset(
                asset_id="ev-1",
                p_max_kw=float(ev_rng.choice([3.7, 7.4, 11.0])),
                daily_need_kwh=round(float(ev_rng.uniform(5.0, 12.0)), 1),
                pattern="overnight",
            )
        )
    if rng.random() < 0.3:
        cap = float(rng.choice([5.0, 10.0, 15.0]))
        assets.append(
            BessAsset(
                asset_id="bess-1",
                capacity_kwh=cap,
                p_max_kw=cap / 2,
                soc_min_kwh=0.1 * cap,
                soc_max_kwh=0.95 * cap,
                control=_bess_control(catalog_seed, loc, site_controlled_bess_share),
            )
        )
    p_lo, p_hi = v.postcode_prefixes
    return Site(
        location_id=loc,
        site_type=SiteType.HOUSEHOLD,
        lat=round(lat, 4),
        lon=round(lon, 4),
        elevation_m=round(float(np.clip(rng.normal(v.elevation_m, 40), 0, 900))),
        voivodeship=v.teryt,
        city=str(rng.choice(v.cities)),
        street=fake.street_address(),
        postcode=f"{int(rng.integers(p_lo, p_hi + 1)):02d}-{int(rng.integers(0, 1000)):03d}",
        dso=v.dso,
        metering_point_id=make_gsrn(DSO_OPERATOR_PREFIX[v.dso], int(rng.integers(0, 10**10))),
        tariff_group=tariff,
        contracted_kw=contracted,
        fuse_a=25.0,
        annual_base_load_kwh=round(float(np.clip(rng.lognormal(np.log(2300), 0.35), 900, 7000))),
        assets=assets,
        seed=location_seed(global_seed, loc),
    )


def load_overrides(path: Path, global_seed: int) -> dict[str, Site]:
    """Hand-defined sites (``config/sites.yaml``); their seed is always derived, never written."""
    if not path.exists():
        return {}
    data: Any = YAML(typ="safe").load(path.read_text()) or {}
    out: dict[str, Site] = {}
    for raw in data.get("sites", []):
        raw = dict(raw)
        raw["seed"] = location_seed(global_seed, str(raw["location_id"]))
        site = Site.model_validate(raw)
        out[site.location_id] = site
    return out


def build_catalog(
    n: int,
    *,
    catalog_seed: int,
    global_seed: int,
    overrides: dict[str, Site] | None = None,
    site_controlled_bess_share: float = 0.0,
    site_mix: dict[str, float] | None = None,
) -> list[Site]:
    """Sites PL-LOC-000001 .. n, each replaced by its hand-defined override if one exists.

    Without ``site_mix`` every generated site is a household; site types and household EVs use their
    own hashes, so households keep every earlier draw.
    """
    check_site_mix(site_mix)
    overrides = overrides or {}
    sites: list[Site] = []
    for i in range(1, n + 1):
        loc = location_id(i)
        if loc in overrides:
            sites.append(overrides[loc])
            continue
        kind = site_type_for(catalog_seed, loc, site_mix)
        if kind is SiteType.HOUSEHOLD:
            sites.append(
                generate_household(
                    i,
                    catalog_seed=catalog_seed,
                    global_seed=global_seed,
                    site_controlled_bess_share=site_controlled_bess_share,
                )
            )
        else:
            sites.append(
                generate_plant(i, kind, catalog_seed=catalog_seed, global_seed=global_seed)
            )
    extra = sorted(set(overrides) - {s.location_id for s in sites})
    sites += [overrides[k] for k in extra]
    return sites


def catalog_frame(sites: list[Site]) -> Any:
    import pandas as pd

    rows = []
    for s in sites:
        row = s.model_dump(exclude={"assets"})
        row["assets"] = [a.model_dump() for a in s.assets]
        row["ems_managed"] = s.ems_managed
        rows.append(row)
    return pd.DataFrame(rows)


def export_catalog(sites: list[Site], out: Path) -> Path:
    out.parent.mkdir(parents=True, exist_ok=True)
    frame = catalog_frame(sites)
    frame["assets"] = frame["assets"].map(lambda a: __import__("json").dumps(a))
    frame.to_parquet(out, index=False)
    return out
