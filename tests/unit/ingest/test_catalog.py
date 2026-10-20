"""AC-001-18: deterministic catalog, valid metering points, tariff rule, overrides."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from ems.contracts.ids import is_valid_gsrn
from ems.ingest.catalog.build import build_catalog, export_catalog, load_overrides
from ems.ingest.catalog.sites import SiteType, TariffGroup, tariff_for
from ems.ingest.catalog.voivodeships import BY_TERYT


def test_ac_001_18_catalog_is_deterministic_and_valid() -> None:
    a = build_catalog(40, catalog_seed=7, global_seed=4242)
    b = build_catalog(40, catalog_seed=7, global_seed=4242)
    assert [s.model_dump() for s in a] == [s.model_dump() for s in b]
    for site in a:
        assert is_valid_gsrn(site.metering_point_id) and site.metering_point_id.startswith("590")
        v = BY_TERYT[site.voivodeship]
        assert v.lat[0] <= site.lat <= v.lat[1] and v.lon[0] <= site.lon <= v.lon[1]
    assert build_catalog(10, catalog_seed=7, global_seed=4242)[3] == a[3]  # independent of n
    assert build_catalog(5, catalog_seed=8, global_seed=4242)[1] != a[1]


@pytest.mark.parametrize(
    ("site_type", "kw", "fuse", "two_zone", "want"),
    [
        (SiteType.HOUSEHOLD, 12.0, 25, False, TariffGroup.G11),
        (SiteType.HOUSEHOLD, 12.0, 25, True, TariffGroup.G12),
        (SiteType.COMMERCIAL, 40.0, 63, False, TariffGroup.C11),
        (SiteType.COMMERCIAL, 40.0, 80, False, TariffGroup.C21),
        (SiteType.COMMERCIAL, 45.0, 63, False, TariffGroup.C21),
    ],
)
def test_tariff_rule(
    site_type: SiteType, kw: float, fuse: float, two_zone: bool, want: TariffGroup
) -> None:
    assert tariff_for(site_type, kw, fuse, two_zone) is want


def test_test_site_override(repo: Path) -> None:
    overrides = load_overrides(repo / "config" / "sites.yaml", 4242)
    site = build_catalog(3, catalog_seed=7, global_seed=4242, overrides=overrides)[0]
    assert site.location_id == "PL-LOC-000001" and site.city == "Poznań"
    assert site.tariff_group is TariffGroup.G12 and site.ems_managed
    assert site.pv is not None and site.pv.kwp == 10.0
    assert site.bess is not None and site.bess.capacity_kwh == 10.0 and site.heat_pump is not None


def test_catalog_export(tmp_path: Path) -> None:
    out = export_catalog(build_catalog(5, catalog_seed=7, global_seed=4242), tmp_path / "c.parquet")
    frame = pd.read_parquet(out)
    assert len(frame) == 5 and set(frame["location_id"]) == {f"PL-LOC-{i:06d}" for i in range(1, 6)}
