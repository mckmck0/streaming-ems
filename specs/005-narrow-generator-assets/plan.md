# Implementation Plan: Narrow the generator to PV prosumers

**Branch**: `005-narrow-generator-assets` | **Date**: 2026-11-08 | **Spec**: [spec.md](spec.md)

## Summary

Delete the wind, plant and outage models, the turbine curves, the four non-prosumer site types and the five plant
asset kinds; drop the matching `asset_type` and `quantity` values from wire contract v1 at 1.0.0; reject removed
names in config. Households, commercial sites and solar farms are untouched, so the default output stays identical.
ADR 0016 records the one-time in-place contract amendment and freezes the contract and the generator range; a
baseline copy of the 1.0.0 schemas and a schema checker that also looks inside arrays enforce the freeze.

## Principles Check (PASS, one recorded exception on P9)

| # | Touched? | How it is satisfied | Shown by |
|---|---|---|---|
| P1 | yes | spec approved 2026-11-08; plan and tasks gated; every change cites a task | review |
| P2 | no | `src/ems/domain/` untouched; generator models stay seeded pure functions | review |
| P3 | no | `ems-gen` stays the only writer of external inputs; fewer facts, same owner | review |
| P4–P8 | no | no actors, handlers, ingress, timers or battery-actor code | — |
| P9 | **yes** | removing enum values is breaking. One-time exception (user decision, spec 005): v1 amended in place at 1.0.0 before any consumer exists; topics and `event_id` unchanged; frozen afterwards and enforced by a baseline test | ADR 0016; AC-005-03, -06, -08 |
| P10 | no | no batch job changes | — |
| P11 | yes | removed site types in `scale.site_mix` fail at config load with the allowed list | AC-005-01 test |
| P12 | no | no dependency, image or tool change (pvlib stays for PV and the weather) | `uv lock --check` in `make lint` |
| P13 | no | no tracing | — |
| P14 | no | no experiment result changes; the recorded benchmark is households only | AC-005-05, -07 |
| P15 | yes | README, `docs/context/` 00, 08, 09, 13, ADR 0016 and index land in this change | review |
| P16 | no | no experiment | — |

## Contract changes (`08-contracts.md`)

- `AssetType`: remove `WIND`, `CHP`, `HYDRO`, `BIOGAS`, `BIOMASS`. Kept: `pv`, `bess`, `heat_pump`, `ev_charger`,
  `load`, `grid_connection`.
- `Quantity`: remove `HEAT_ENERGY`, `FUEL_GAS_VOLUME`. Kept: `active_energy`, `soc`, `active_power`,
  `reactive_energy`.
- `SCHEMA_VERSION` stays `"1.0.0"`; topic names, keys, partitioning, headers and the identity rule are unchanged,
  so event ids of all remaining events are unchanged.
- Regenerate `schemas/` with `uv run ems-gen schemas export`: only the two enums in `ems.measurement.v1.json`
  change; golden examples in `schemas/examples/` use none of the removed values and stay byte-identical.
- No consumer exists yet. Recordings in `data/recordings/` contain prices and weather only and stay valid.
- **Freeze (AC-005-08)**: `schema_export.incompatibilities` today stops at `properties` and never descends into
  array `items`, so it misses enum removals inside `payload.measurements[]` and `steps[]`. Make it descend into
  `items` (after `$ref` resolution). Copy the regenerated schemas once to `schemas/baseline/1.0.0/` and add a test
  that `incompatibilities(baseline, current) == []` for each schema; `stale()` only looks at the top level, so the
  baseline is not touched by `schemas export`. Unfreezing means editing the baseline in a spec with an ADR.
- `08-contracts.md` versioning rules: removing or renaming an enum value is breaking, like a field.

## Design

**Catalog** (`src/ems/ingest/catalog/`)
- `sites.py`: `SiteType` keeps `HOUSEHOLD`, `COMMERCIAL`, `SOLAR_FARM`. Delete `WindAsset`, `ChpAsset`,
  `BiogasAsset`, `BiomassAsset`, `HydroAsset`, their entries in `Asset` / `AssetModel`, and the `Site` properties
  `wind`, `chp`, `biogas`, `biomass`, `hydro`. `Site.typed()` stays (used by `ev`). An override with a removed
  `kind` then fails the pydantic discriminator (AC-005-02).
- `build.py`: delete `TURBINES`, the `INDUSTRIAL`, `WIND_FARM`, `BIOGAS_PLANT`, `HYDRO_PLANT` branches of
  `_plant_assets`, the hydro region override and the `WIND_FARM` weight in `generate_plant` (solar farms keep
  `pv_weight`, commercial keeps `household_weight`). Commercial and solar-farm branches keep their draw order, so
  their sites do not change.
- `voivodeships.py`: delete the `wind_weight` field and its value in each of the 16 positional entries: the
  **second from last** value, between `pv_weight` and `wind_scale_ms` (e.g. `0.6` in `02` dolnośląskie). Removing
  the wrong column would silently re-site solar farms; AC-005-07 catches it. Keep `wind_scale_ms` (weather).
  Docstring: weights steer households and PV siting.

**Models and generator**
- Delete `src/ems/ingest/synthetic/wind.py`, `plants.py`, `outages.py`.
- `generator.py`: delete `OUTAGES`, `PLANT_KINDS`, the `Chunk` fields `wind_kwh`, `chp_kwh`, `chp_heat_kwh`,
  `biogas_kwh`, `biomass_kwh`, `hydro_kwh`, `SyntheticModel.turbine_curves`, the plant arrays, `_avail`,
  `_biomass_level` and the biomass block in `chunk()`, the CHP heat builder and the `PLANT_KINDS` loop in
  `_meter_builders`, and the now-unused imports (`availability`, plant and wind functions, `Quantity` only if
  unused). `Chunk.generation()` returns PV; `_other_assets` becomes `_ev` returning only `ev_kwh`; the
  `other_all` concatenation keeps only `ev_kwh`; `gen_all = pv_all`. Battery dispatch, registers, grid flows,
  `price_curve` and the EV path are unchanged in order and arithmetic, so households stay bit-identical.
- `catalog/build.py`: new `check_site_mix(mix)` raising
  `ValueError("scale.site_mix: unknown site type(s) <names>; allowed: commercial, household, solar_farm")` for any
  key not in `SiteType`; called by `build_catalog` (before any site is drawn) and by `load_generator_config`
  right after parsing (AC-005-01). Today an unknown key fails only when some site happens to draw it.
- `cli.py` `run`: move `load_generator_config(config)` inside the existing `try`, so the error exits 2 with the
  message instead of a traceback (the other commands keep the traceback; they are developer tools).

**Config**
- `config/calibration/weather.yaml`: `shear_alpha` keeps `household: 0.22`, `commercial: 0.22`,
  `solar_farm: 0.18`.
- `config/generator.yaml`: `site_mix` comment example becomes `{household: 0.8, commercial: 0.1, solar_farm: 0.1}`.
- Delete `config/turbines/` (`power_curves.csv`, `NOTICE`); only `wind.py` and its tests read them.

## Risks (`09-risks-and-tech-debt.md`)

- G5 (windpowerlib unmaintained) and G7 (weak inland wind for turbines): become "Obsolete (spec 005): wind farms
  removed". The synthetic 10 m wind still feeds PV cell temperature and the price; G7's inland weakness does not
  matter there.
- G8 (household EVs): unchanged. No new risk.
- Local Kafka topics with events from an earlier mixed run fail `make validate`: run `make reset` once (README
  troubleshooting).

## Experiment impact

None. No thesis number is recorded from mixed catalogs; the benchmark (`benchmark-20261020T204019Z`) is
households only and AC-005-05 shows that output is unchanged. The mixed 1,000-site timing in `13` ("≈ 13 s")
described the old mix and has no reproducible command, so it is removed rather than re-measured.

## Test strategy

All unit tier (no infrastructure); written first by `test-engineer`.
- **AC-005-05** (`tests/unit/ingest/test_narrow_assets.py`): `build_catalog(20, …, overrides=load_overrides(…),
  site_controlled_bess_share=0.5)` → `emissions(model, GeneratorConfig(emit_registers=True), 2026-01-15, +1 day)`
  → SHA-256 over every event in emission order, as canonical JSON (`sort_keys`, full float `repr`) without
  `ingested_at`. Preconditions asserted so the balance and dispatch paths run: at least one site-controlled battery
  on a PV site, one EV and one heat pump. The constant is captured on `51560fa` before any code change and the test
  passes before and after. It is bit-exact on the author's machine; there is no CI on other hardware yet (CI
  platform is OPEN in 00), so portability is not claimed.
- **AC-005-07** (same file): digest of `generate_plant(i, COMMERCIAL | SOLAR_FARM).model_dump()` for i = 1…50, and of
  one day of events for 10 commercial and 5 solar-farm sites built that way, captured on `51560fa`.
- **AC-005-01**: parametrized over `industrial`, `wind_farm`, `biogas_plant`, `hydro_plant`, `nonsense`:
  `load_generator_config` on a temp YAML and `build_catalog(…, site_mix=…)` both raise `ValueError` naming the
  allowed types; a valid mix loads.
- **AC-005-02**: parametrized over asset kinds `wind`, `chp`, `biogas`, `biomass`, `hydro` and site type
  `industrial`: `load_overrides` on a temp `sites.yaml` raises `ValidationError`.
- **AC-005-03**: parametrized over the five asset types and two quantities: a golden measurement event with that
  value fails `validate_strict` and `jsonschema.Draft202012Validator` on the exported measurement schema.
- **AC-005-04**: rework `test_ac_004_08_mixed_day_events_validate_and_balance` for MIX
  `{household: 0.6, commercial: 0.2, solar_farm: 0.2}` over a **full day** in June (PV and the evening battery
  discharge run) with forecasts off (`streams.weather_forecast.mode: off`) for speed: every event validates,
  production asset types ⊆ {pv, bess}, every unmanaged site balances in every interval; shares ± 0.02 over 4,000
  sites.
- **AC-005-06**: existing `test_ac_001_15_exported_schemas_are_current`; assert `SCHEMA_VERSION == "1.0.0"` and the
  topic names.
- **AC-005-08** (`tests/unit/contracts/test_topics_schemas.py`): baseline test above, plus checker tests that an
  enum value removed inside `items` (measurement `asset_type`) and a removed property inside `items` are detected.
- Remove: AC-004-01…06 tests, `test_plants_produce_zero_in_outages`, `tests/unit/ingest/test_outages.py`; update
  `test_site_types_have_matching_assets_and_tariffs` (commercial C11/C21 with daytime EV; solar farm has PV, C21,
  not EMS-managed). EV tests (AC-004-07) stay.

## Docs and ADRs

- New `docs/adr/0016-pv-only-generator-and-v1-freeze.md` (+ index row in `docs/adr/README.md`): context, decision
  (PV prosumers only; v1 amended in place once at 1.0.0; contract and generator range frozen), consequences.
  "Frozen" means: site types, asset kinds and the contract change only through a new spec and ADR; contract changes
  from then on are additive (P9) and bump the minor version (1.1.0); removals need v2 and new topics.
- `08-contracts.md`: asset types and quantities; versioning rules: enum removal is breaking, the one-time
  amendment, the `schemas/baseline/1.0.0/` freeze.
- `specs/001-data-pipeline-core/data-model.md`: the `asset_type` and `quantity` rows (08 points here for the full
  field tables), marked "amended by spec 005".
- `13-subsystem-ingest.md`: "Site types and assets (specs 004, 005)", frozen range, "How to add an asset model"
  (spec first, additive contract value, no `PLANT_KINDS`), the intro's list of built specs; drop the mixed-day
  timing.
- `00-overview.md`: status row, backlog row 5, decision-log line, repo map "(specs 001–005)".
- `09-risks-and-tech-debt.md`: G5, G7 obsolete.
- `README.md`: "Sites and assets" subsection in Data + Contents entry; `asset_type`, `quantity` rows; remove the
  plant rows of the emitted-assets table; `--locations` bullet; `make reset` hint for stale mixed topics.
- `specs/004-assets-and-cluster/spec.md`: one status line "Partly superseded by spec 005 (2026-11-08):
  AC-004-01…06 retired, AC-004-08 replaced by AC-005-04".

## Complexity Tracking

None. Deletion, one config check and a stricter schema checker.
