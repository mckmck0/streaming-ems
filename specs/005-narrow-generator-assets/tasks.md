# Tasks: Narrow the generator to PV prosumers

Spec: [spec.md](spec.md) · Plan: [plan.md](plan.md). Tests come first; T01 runs on unchanged code (`51560fa`).

## Phase 1: Regression baseline (before any code change)

- [x] T01 [test] [US3] `tests/unit/ingest/test_narrow_assets.py`: digests captured on `51560fa` and green there.
  (a) 20 households incl. `PL-LOC-000001` from `config/sites.yaml`, `site_controlled_bess_share=0.5`,
  `emit_registers=True`, one day from 2026-01-15: SHA-256 over every event in emission order, canonical JSON
  without `ingested_at`, full float precision; assert a site-controlled battery on a PV site, an EV and a heat pump
  are present (AC-005-05). (b) `generate_plant(i, COMMERCIAL | SOLAR_FARM).model_dump()` for i = 1…50, and one day
  of events for 10 commercial and 5 solar-farm sites (AC-005-07).

## Phase 2: Failing tests for the new behaviour

- [x] T02 [test] [US1] Same file: parametrized rejections. `site_mix` with `industrial`, `wind_farm`,
  `biogas_plant`, `hydro_plant`, `nonsense` → `ValueError` listing the allowed types from both
  `load_generator_config` (temp YAML) and `build_catalog`; a valid mix loads (AC-005-01). Temp `sites.yaml` with
  asset kind `wind`, `chp`, `biogas`, `biomass`, `hydro`, or `site_type: industrial` → `ValidationError` from
  `load_overrides` (AC-005-02).
- [x] T03 [test] [US2] Same file: a golden measurement event with `asset_type` in {wind, chp, hydro, biogas,
  biomass} or `quantity` in {heat_energy, fuel_gas_volume} fails `validate_strict` and the exported JSON Schema
  (AC-005-03); `SCHEMA_VERSION == "1.0.0"` and topic names unchanged (AC-005-06).
- [x] T04 [test] [US2] `tests/unit/contracts/test_topics_schemas.py`: `incompatibilities` detects an enum value and a
  property removed inside array `items`; every exported schema is compatible with `schemas/baseline/1.0.0/`
  (AC-005-08; the baseline files are created in T06).
- [x] T05 [test] [US1] `tests/unit/ingest/test_assets.py`: rework the mixed-day test for MIX `{household: 0.6,
  commercial: 0.2, solar_farm: 0.2}`, one full June day, forecasts off: every event validates, production asset
  types ⊆ {pv, bess}, every unmanaged site balances in every interval; shares ± 0.02 over 4,000 sites (AC-005-04).
  Delete AC-004-01…06 and `test_plants_produce_zero_in_outages`; update the site-type/asset/tariff test to
  commercial and solar farm; delete `tests/unit/ingest/test_outages.py`.

## Phase 3: Implementation

- [x] T06 [US2] Contract: drop `AssetType.WIND/CHP/HYDRO/BIOGAS/BIOMASS` and `Quantity.HEAT_ENERGY/FUEL_GAS_VOLUME`
  in `src/ems/contracts/envelope.py`; `incompatibilities` descends into `items` (`schema_export.py`);
  `uv run ems-gen schemas export`; copy the three schemas to `schemas/baseline/1.0.0/` (AC-005-03, -06, -08).
- [x] T07 [US1] Catalog: `SiteType` and asset classes in `catalog/sites.py`; `TURBINES`, removed branches, hydro
  override, wind weight and new `check_site_mix` in `catalog/build.py`; `wind_weight` (second-from-last value)
  in `catalog/voivodeships.py` (AC-005-01, -02, -04, -07).
- [x] T08 [US1] Models and generator: delete `synthetic/wind.py`, `plants.py`, `outages.py`; strip plants,
  outages, turbines and CHP heat from `generator.py`; `check_site_mix` in `load_generator_config`; config load
  inside the `try` in `cli.py run` (AC-005-01, -04, -05, -07).
- [x] T09 [US1] Config: `shear_alpha` keys in `config/calibration/weather.yaml`, `site_mix` comment in
  `config/generator.yaml`, delete `config/turbines/` (AC-005-04).

## Phase 4: Docs and decision

- [x] T10 ADR 0016 (`docs/adr/0016-pv-only-generator-and-v1-freeze.md`) + index row; `docs/context/08` (enums,
  enum removal is breaking, one-time amendment, baseline freeze), `13` (site types and assets, frozen range, how
  to add an asset, drop mixed-day timing, built-specs list), `00` (status, backlog, decision log, repo map), `09`
  (G5, G7 obsolete); `specs/001-data-pipeline-core/data-model.md` enum rows; `specs/004-assets-and-cluster/spec.md`
  status line; README ("Sites and assets" + Contents, `asset_type`/`quantity` rows, emitted-assets table,
  `--locations` bullet, `make reset` hint) (P15; all criteria).

## Phase 5: Verify

- [x] T11 `make format`, `make ci`; `git diff --stat schemas/` shows only the measurement enums plus the baseline;
  SC-001 grep (excluding `test_narrow_assets.py`); `make up`, `make gen-backfill DAYS=1`, `make validate`; a mixed
  200-site backfill with a scratch config validates; the same config with `wind_farm` exits 2 with the
  allowed-types message (AC-005-01, -04, -06; SC-001).

## Coverage

| Criterion | Tasks |
|---|---|
| AC-005-01 | T02, T07, T08, T11 |
| AC-005-02 | T02, T07 |
| AC-005-03 | T03, T06 |
| AC-005-04 | T05, T07, T08, T09, T11 |
| AC-005-05 | T01, T08 |
| AC-005-06 | T03, T06, T11 |
| AC-005-07 | T01, T07, T08 |
| AC-005-08 | T04, T06 |
