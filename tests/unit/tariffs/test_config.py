"""AC-009-01, -02: tariff config validates at the edge; sites resolve or fail naming the site."""

from __future__ import annotations

import json
import shutil
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest
from ruamel.yaml import YAML

from ems.contracts.time import quarter_index
from ems.domain.netbilling import Cohort
from ems.ingest.catalog.build import load_overrides
from ems.ingest.catalog.sites import NetBilling, Site, TariffGroup
from ems.tariffs import (
    SiteConfigError,
    TariffConfigError,
    load_book,
    load_rcem,
    site_model,
)

CONFIG = Path(__file__).resolve().parents[3] / "config"
TARIFFS = CONFIG / "tariffs"


def test_ac_009_01_real_config_loads() -> None:
    book = load_book(TARIFFS)
    assert [d.dso for d in book.dsos] == ["Enea", "Energa", "PGE", "Tauron"]
    for d in book.dsos:
        for _group, versions in d.operator_versions:
            assert [v.valid_from for v in versions] == ["2026-01", "2026-10"]
            assert versions[1].subscription_fee_pln_per_month == 0.0
        for _group, versions in d.seller_versions:
            assert [v.valid_from for v in versions] == ["2026-01"]
    assert [q.valid_from for q in book.national.quality] == ["2026-01", "2026-02"]
    assert book.national.excise_pln_per_kwh == 0.005
    assert (book.rules.deposit_factor, book.rules.validity_months) == (1.23, 12)
    assert len(load_rcem(TARIFFS / "rcem.yaml")) == 13


def _broken(tmp_path: Path, name: str, mutate: Callable[[Any], None]) -> Path:
    root = tmp_path / "tariffs"
    shutil.copytree(TARIFFS, root)
    path = root / "2026" / name if name != "rcem.yaml" else root / name
    data = YAML(typ="safe").load(path.read_text())
    mutate(data)
    path.write_text(json.dumps(data, default=str))  # JSON is YAML
    return root


def _op(data: Any, i: int = 0) -> Any:
    return data["operator"]["versions"][i]


CASES: list[tuple[str, str, Callable[[Any], None], str]] = [
    (
        "missing-ref",
        "enea.yaml",
        lambda d: _op(d)["groups"]["G11"]["fixed_fee"].pop("ref"),
        "operator.versions.0.groups.G11.fixed_fee.ref",
    ),
    ("missing-source", "pge.yaml", lambda d: _op(d).pop("source"), "operator.versions.0.source"),
    (
        "unknown-component",
        "tauron.yaml",
        lambda d: _op(d)["groups"]["G12"].update(
            trading_fee={"value": 1.0, "unit": "PLN/month", "ref": "x"}
        ),
        "trading_fee",
    ),
    (
        "unknown-seller-zone",
        "energa.yaml",
        lambda d: d["seller"]["versions"][0]["groups"]["G12"]["energy"].update(
            evening={"value": 0.5, "unit": "PLN/kWh", "ref": "x"}
        ),
        "seller.versions[0].groups.G12.energy.evening",
    ),
    (
        "missing-zone-rate",
        "enea.yaml",
        lambda d: _op(d)["groups"]["G12w"]["variable_fee"].pop("offpeak"),
        "missing zone 'offpeak'",
    ),
    (
        "overlap",
        "enea.yaml",
        lambda d: _op(d, 1).update(valid_from="2026-01-01"),
        "operator.versions[1].valid_from: overlaps",
    ),
    (
        "gap",
        "pge.yaml",
        lambda d: _op(d).update(valid_from="2026-02-01"),
        "operator.versions[0].valid_from: gap",
    ),
    (
        "not-on-the-1st",
        "tauron.yaml",
        lambda d: _op(d, 1).update(valid_from="2026-10-15"),
        "not the 1st",
    ),
    (
        "wrong-unit",
        "energa.yaml",
        lambda d: _op(d)["groups"]["G11"]["fixed_fee"].update(unit="PLN/kWh"),
        "operator.versions.0.groups.G11.fixed_fee.unit",
    ),
    (
        "missing-group",
        "pge.yaml",
        lambda d: _op(d)["groups"].pop("G12w"),
        "operator.versions[0].groups.G12w: missing group",
    ),
    ("national-ref", "national.yaml", lambda d: d["vat"].pop("ref"), "vat.ref"),
    (
        "national-quality-gap",
        "national.yaml",
        lambda d: d["quality_fee"].pop(0),
        "quality_fee[0].valid_from: gap",
    ),
    ("other-year", "national.yaml", lambda d: d.update(year=2027), "only 2026"),
    (
        "rcem-unit",
        "rcem.yaml",
        lambda d: d["months"][0]["publications"][0].update(unit="PLN/kWh"),
        "months.0.publications.0.unit",
    ),
    ("rcem-order", "rcem.yaml", lambda d: d["months"].reverse(), "in order"),
    (
        "rcem-dates",
        "rcem.yaml",
        lambda d: d["months"][0]["publications"].reverse(),
        "increasing dates",
    ),
]


@pytest.mark.parametrize(
    ("name", "mutate", "field"), [c[1:] for c in CASES], ids=[c[0] for c in CASES]
)
def test_ac_009_01_broken_config_fails_naming_file_and_field(
    tmp_path: Path, name: str, mutate: Callable[[Any], None], field: str
) -> None:
    root = _broken(tmp_path, name, mutate)
    with pytest.raises(TariffConfigError) as err:
        if name == "rcem.yaml":
            load_rcem(root / name)
        else:
            load_book(root)
    assert name in str(err.value)
    assert field in str(err.value)


# ── AC-009-02: site resolution fails before settling and names the site ─────────────────────────

BOOK = load_book(TARIFFS)
SITE = load_overrides(CONFIG / "sites.yaml", 4242)["PL-LOC-000001"]
MAY = (
    quarter_index(datetime(2026, 5, 1, tzinfo=UTC)),
    quarter_index(datetime(2026, 6, 1, tzinfo=UTC)),
)


def test_ac_009_02_test_site_resolves() -> None:
    model = site_model(SITE, BOOK, MAY)
    assert model.location_id == "PL-LOC-000001"
    assert (model.tariff.operator, model.tariff.seller, model.tariff.group) == (
        "Enea Operator",
        "Enea S.A.",
        "G12",
    )
    assert model.cohort == Cohort(date(2024, 9, 1))
    assert model.fixed_inputs.annual_consumption_kwh == 2500
    assert (model.battery.capacity_kwh, model.battery.p_max_kw) == (10.0, 5.0)


def _bess_site() -> dict[str, Any]:
    return SITE.model_dump()


BAD_SITES: list[tuple[str, dict[str, Any], str]] = [
    ("stoen", {"dso": "Stoen"}, "no tariff for DSO 'Stoen'"),
    ("c11", {"tariff_group": TariffGroup.C11}, "C11"),
    ("no-net-billing", {"net_billing": None}, "net_billing"),
    ("net-metering", {"net_billing": NetBilling(first_feed_in=date(2021, 5, 1))}, "net-metering"),
    ("one-phase", {"contracted_kw": 5.0}, "1-phase"),
]


@pytest.mark.parametrize(
    ("update", "match"), [b[1:] for b in BAD_SITES], ids=[b[0] for b in BAD_SITES]
)
def test_ac_009_02_sites_fail_naming_the_site(update: dict[str, Any], match: str) -> None:
    site = SITE.model_copy(update=update)
    with pytest.raises(SiteConfigError, match=match) as err:
        site_model(site, BOOK, MAY)
    assert str(err.value).startswith("PL-LOC-000001")


def test_ac_009_02_site_without_an_ems_battery_fails() -> None:
    raw = SITE.model_dump()
    raw["assets"] = [dict(a, control="site") if a["kind"] == "bess" else a for a in raw["assets"]]
    with pytest.raises(SiteConfigError, match="not EMS-managed"):
        site_model(Site.model_validate(raw), BOOK, MAY)


def test_ac_009_02_window_outside_2026_fails() -> None:
    t0 = quarter_index(datetime(2026, 12, 31, 0, tzinfo=UTC))
    t1 = quarter_index(datetime(2027, 1, 2, 0, tzinfo=UTC))
    with pytest.raises(SiteConfigError, match="2027-01") as err:
        site_model(SITE, BOOK, (t0, t1))
    assert str(err.value).startswith("PL-LOC-000001")
    site_model(SITE, BOOK, (t0 - 96 * 30, t0))  # inside 2026: fine


def _zones(d: Any) -> Any:
    return d["operator"]["zones"]


ZONE_CASES: list[tuple[str, str, Callable[[Any], None], str]] = [
    (
        "bad-season",
        "pge.yaml",
        lambda d: _zones(d)["G12"]["rules"][0].update(season=["4-01", "10-01"]),
        "operator.zones.G12.rules.0.season",
    ),
    (
        "bad-hours",
        "tauron.yaml",
        lambda d: _zones(d)["G12"]["rules"][0].update(hours=[[13, 13]]),
        "operator.zones.G12.rules[0]",
    ),
    (
        "bad-zone-name",
        "energa.yaml",
        lambda d: _zones(d)["G12"]["rules"][0].update(zone="evening"),
        "operator.zones.G12: zones",
    ),
    (
        "duplicate-id",
        "enea.yaml",
        lambda d: d["operator"]["versions"][1].update(id="enea-op-2026-01"),
        "repeated: 'enea-op-2026-01'",
    ),
    ("rcem-gap", "rcem.yaml", lambda d: d["months"].pop(3), "contiguous"),
    (
        "rcem-early",
        "rcem.yaml",
        lambda d: d["months"][-1]["publications"][0].update(published="2026-10-30"),
        "before the month ended",
    ),
]


@pytest.mark.parametrize(
    ("name", "mutate", "field"), [c[1:] for c in ZONE_CASES], ids=[c[0] for c in ZONE_CASES]
)
def test_ac_009_01_zone_ids_and_rcem_checks(
    tmp_path: Path, name: str, mutate: Callable[[Any], None], field: str
) -> None:
    test_ac_009_01_broken_config_fails_naming_file_and_field(tmp_path, name, mutate, field)
