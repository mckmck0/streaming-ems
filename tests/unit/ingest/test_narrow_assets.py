"""AC-005-01 … -07: the generator keeps only PV prosumer sites; kept site types do not change."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator
from pydantic import ValidationError
from ruamel.yaml import YAML

from ems.contracts import schema_export
from ems.contracts.envelope import (
    SCHEMA_VERSION,
    ContractError,
    EnvelopeBase,
    to_json_bytes,
    validate_strict,
)
from ems.contracts.examples import golden_examples
from ems.contracts.topics import DLQ_TOPIC, STREAM_TOPICS
from ems.ingest.catalog.build import build_catalog, generate_plant, load_overrides
from ems.ingest.catalog.sites import Site, SiteType
from ems.ingest.generator import (
    GeneratorConfig,
    SyntheticModel,
    emissions,
    load_generator_config,
)

CONFIG = Path(__file__).resolve().parents[3] / "config"
DAY = datetime(2026, 1, 15, tzinfo=UTC)

# Captured at full float precision on the author's machine (arm64 macOS): a changed digest means
# changed output, not rounding. The catalog digest dates from commit 51560fa (before spec 005).
# The two event digests were refreshed by spec 007 (007/T12), whose model changes alter values on
# purpose (forecast errors, base load); event ids are unchanged (the id digests below).
HOUSEHOLDS_DIGEST = "699095f8cfc30e45bb928508d3512940ad6319726d46ba00ae5dc5052e848d9d"
PLANT_CATALOG_DIGEST = "24207803ef373456078935e2800a35a76e18225aa100251fb3f12a1d64a7bee2"
PLANT_EVENTS_DIGEST = "d35a1fe4cca18847d9a2a7d5cdb23ca83e008e860b967ba5e2844fde3372b87b"

# Spec 007 (AC-007-18): event ids only, in emission order, for the same sites and day as the two
# value digests above. Captured on d3d9f30 (before any spec 007 code); event identity carries no
# values, so these stay fixed when the value digests are refreshed.
HOUSEHOLDS_ID_DIGEST = "5ac1508ba5f9f371d4ba2e269be75c8f28ccaebf57d7395d39fde5138da3571d"
PLANT_ID_DIGEST = "148329e44fc55a86525fa134bbff5fa389ba0956e830b1866a877febaf3737da"


def _hash_rows(rows: Iterable[Any]) -> str:
    h = hashlib.sha256()
    for row in rows:
        h.update(json.dumps(row, sort_keys=True).encode())
        h.update(b"\n")
    return h.hexdigest()


def _events(sites: list[Site], cfg: GeneratorConfig) -> Iterable[EnvelopeBase]:
    """Every event of one day in emission order."""
    model = SyntheticModel(sites, 4242, CONFIG)
    for at, _stream, build in emissions(model, cfg, DAY, DAY + timedelta(days=1)):
        yield build(at)


def _event_rows(sites: list[Site], cfg: GeneratorConfig) -> Iterable[dict[str, Any]]:
    """Every event of one day in emission order, without the wall-clock ``ingested_at``."""
    for event in _events(sites, cfg):
        row = event.model_dump(mode="json", by_alias=True)
        row.pop("ingested_at")
        yield row


def _households() -> list[Site]:
    overrides = load_overrides(CONFIG / "sites.yaml", 4242)
    return build_catalog(
        20, catalog_seed=7, global_seed=4242, overrides=overrides, site_controlled_bess_share=0.5
    )


def test_ac_005_05_households_only_output_unchanged() -> None:
    sites = _households()
    assert sites[0].location_id == "PL-LOC-000001" and sites[0].ems_managed
    assert all(s.site_type is SiteType.HOUSEHOLD for s in sites)
    # Exercise battery dispatch on PV, EV charging and heat pumps, so the balance paths run.
    assert any(s.bess and s.bess.control == "site" and s.pv for s in sites)
    assert any(s.ev for s in sites) and any(s.heat_pump for s in sites)
    digest = _hash_rows(_event_rows(sites, GeneratorConfig(emit_registers=True)))
    assert digest == HOUSEHOLDS_DIGEST


def _plants() -> list[Site]:
    commercial = [
        generate_plant(i, SiteType.COMMERCIAL, catalog_seed=7, global_seed=4242)
        for i in range(1, 51)
    ]
    solar = [
        generate_plant(i, SiteType.SOLAR_FARM, catalog_seed=7, global_seed=4242)
        for i in range(51, 101)
    ]
    return commercial + solar


def test_ac_005_07_commercial_and_solar_farms_unchanged() -> None:
    plants = _plants()
    assert _hash_rows(s.model_dump(mode="json") for s in plants) == PLANT_CATALOG_DIGEST
    day_sites = plants[:10] + plants[50:55]
    assert any(s.ev for s in day_sites) and all(s.pv for s in plants[50:55])
    digest = _hash_rows(_event_rows(day_sites, GeneratorConfig(emit_registers=True)))
    assert digest == PLANT_EVENTS_DIGEST


def test_ac_007_18_event_ids_unchanged() -> None:
    cfg = GeneratorConfig(emit_registers=True)
    plants = _plants()
    households = _hash_rows(str(e.event_id) for e in _events(_households(), cfg))
    plant_ids = _hash_rows(str(e.event_id) for e in _events(plants[:10] + plants[50:55], cfg))
    assert households == HOUSEHOLDS_ID_DIGEST
    assert plant_ids == PLANT_ID_DIGEST


# ── removed site types, asset kinds and contract values are rejected ─────────────────────────────

ALLOWED = "allowed: commercial, household, solar_farm"
REMOVED_SITE_TYPES = ("industrial", "wind_farm", "biogas_plant", "hydro_plant", "nonsense")


@pytest.mark.parametrize("name", REMOVED_SITE_TYPES)
def test_ac_005_01_removed_site_types_fail_fast(name: str, tmp_path: Path) -> None:
    mix = {"household": 0.9, name: 0.1}
    path = tmp_path / "generator.yaml"
    path.write_text(json.dumps({"scale": {"site_mix": mix}}))
    with pytest.raises(ValueError, match=ALLOWED):
        load_generator_config(path)
    with pytest.raises(ValueError, match=ALLOWED):
        build_catalog(1, catalog_seed=7, global_seed=4242, site_mix=mix)


def test_ac_005_01_kept_site_types_load(tmp_path: Path) -> None:
    mix = {"household": 0.8, "commercial": 0.1, "solar_farm": 0.1}
    path = tmp_path / "generator.yaml"
    path.write_text(json.dumps({"scale": {"site_mix": mix}}))
    assert load_generator_config(path).site_mix == mix


def _test_site() -> dict[str, Any]:
    raw = YAML(typ="safe").load((CONFIG / "sites.yaml").read_text())["sites"][0]
    return dict(raw, location_id="PL-LOC-000002", metering_point_id="590900200000000025")


REMOVED_ASSETS = (
    {
        "kind": "wind",
        "asset_id": "wind-1",
        "turbine_type": "V90/2000",
        "count": 3,
        "hub_height_m": 105.0,
        "rated_kw": 2000.0,
    },
    {
        "kind": "chp",
        "asset_id": "chp-1",
        "rated_kw": 500.0,
        "heat_kw_per_k": 40.0,
        "heat_base_kw": 250.0,
    },
    {"kind": "biogas", "asset_id": "biogas-1", "rated_kw": 500.0},
    {"kind": "biomass", "asset_id": "biomass-1", "rated_kw": 500.0, "ramp_kw_per_step": 50.0},
    {"kind": "hydro", "asset_id": "hydro-1", "rated_kw": 500.0},
)


@pytest.mark.parametrize("asset", REMOVED_ASSETS, ids=lambda a: a["kind"])
def test_ac_005_02_removed_asset_kinds_rejected(asset: dict[str, Any], tmp_path: Path) -> None:
    site = _test_site()
    site["assets"] = [*site["assets"], asset]
    path = tmp_path / "sites.yaml"
    path.write_text(json.dumps({"sites": [site]}))
    with pytest.raises(ValidationError):
        load_overrides(path, 4242)


def test_ac_005_02_removed_site_type_rejected(tmp_path: Path) -> None:
    path = tmp_path / "sites.yaml"
    path.write_text(json.dumps({"sites": [dict(_test_site(), site_type="industrial")]}))
    with pytest.raises(ValidationError):
        load_overrides(path, 4242)


@pytest.mark.parametrize(
    ("field", "value"),
    [("asset_type", v) for v in ("wind", "chp", "hydro", "biogas", "biomass")]
    + [("quantity", v) for v in ("heat_energy", "fuel_gas_volume")],
)
def test_ac_005_03_contract_rejects_removed_values(field: str, value: str) -> None:
    data = json.loads(to_json_bytes(golden_examples()["production.json"]))
    data["payload"]["measurements"][0][field] = value
    with pytest.raises(ContractError):
        validate_strict(data)
    schema = schema_export.build_schemas()["ems.measurement.v1.json"]
    assert not Draft202012Validator(schema).is_valid(data)


def test_ac_005_06_version_topics_and_identity_kept() -> None:
    assert SCHEMA_VERSION == "1.0.0"
    assert sorted(STREAM_TOPICS.values()) == [
        "ems.consumption.v1",
        "ems.price.v1",
        "ems.production.v1",
        "ems.weather.actual.v1",
        "ems.weather.forecast.v1",
    ]
    assert DLQ_TOPIC == "ems.gen.dlq.v1"
    # The golden examples (and so the identity rule) are unchanged.
    assert schema_export.render_examples() == {
        p.name: p.read_text() for p in (CONFIG.parent / "schemas" / "examples").glob("*.json")
    }
