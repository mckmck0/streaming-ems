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
    assets: list[PvAsset | HeatPumpAsset | BessAsset] = []
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
) -> list[Site]:
    """Sites PL-LOC-000001 .. n, each replaced by its hand-defined override if one exists."""
    overrides = overrides or {}
    sites = []
    for i in range(1, n + 1):
        loc = location_id(i)
        sites.append(
            overrides.get(loc)
            or generate_household(
                i,
                catalog_seed=catalog_seed,
                global_seed=global_seed,
                site_controlled_bess_share=site_controlled_bess_share,
            )
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
