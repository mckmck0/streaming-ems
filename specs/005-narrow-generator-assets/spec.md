# Feature Specification: Narrow the generator to PV prosumers

**Feature Branch**: `005-narrow-generator-assets` · **Created**: 2026-11-08 · **Status**: Approved 2026-11-08

**Input**: User review of spec 004 (2026-11-08): remove wind farms, CHP and biogas from the generator, and with them
biomass (it exists only on biogas plants), hydro and the industrial site type (defined by its CHP). Amend the wire
contract to match at version 1.0.0, then freeze the contract and the generator range.

## Goal

The thesis studies residential prosumers (PV, home battery, grid). Spec 004 added utility-scale and thermal plants
as background sites; they serve no EMS experiment and cost code, config, vendored data, contract surface and tests.
This spec removes them so that **PV is the only generation source** and the generator, the contract and the docs
describe only what the EMS uses.

## Scope

In scope:
- Remove the site types `industrial`, `wind_farm`, `biogas_plant`, `hydro_plant` and the asset kinds `wind`, `chp`,
  `biogas`, `biomass`, `hydro` from the catalog, the synthetic models and the event builders.
- Remove the wind power model, the vendored turbine curves (`config/turbines/`), the plant models and the outage
  model (only plants use it).
- Remove `asset_type` values `wind`, `chp`, `hydro`, `biogas`, `biomass` and `quantity` values `heat_energy`,
  `fuel_gas_volume` from wire contract v1, keeping `schema_version` 1.0.0.
- Reject removed site types and asset kinds in configuration instead of ignoring them.
- Freeze the contract and the generator range from this change on (ADR 0016).

Out of scope:
- Weather: wind speed and direction stay in the synthetic truth, the forecasts and the forecast contract. PV cell
  temperature (Faiman) uses the 10 m wind and the synthetic price uses the national wind index.
- Prices, live sources (PSE, Open-Meteo, ENTSO-E), record/replay, faults, stats, the cluster profile.
- Other reserved contract values (`active_power`, `reactive_energy`, `interval_mean`, `PT1M`, `P1D`, `instant`,
  `PL_DAM_TGE`, `weather_actual`, `algorithm_id`): they stay.
- Households, commercial sites and solar farms: their models and draws do not change.

## What remains

| Area | After this spec |
|---|---|
| Generation | PV only: household rooftop (70 % of households), commercial rooftop (60 %), ground-mounted solar farms (1–20 MWp) |
| Site types | `household`, `commercial`, `solar_farm` |
| Consumption | base load (BDEW H25/G25), heat pump, EV charger |
| Batteries and grid | EMS-managed battery (no measurements, ADR 0014); site-controlled battery charge, discharge and SoC; grid import/export and the optional import register on unmanaged sites |
| Contract `asset_type` | `pv`, `bess`, `heat_pump`, `ev_charger`, `load`, `grid_connection` |
| Contract `quantity` | `active_energy`, `soc` (emitted); `active_power`, `reactive_energy` (reserved) |

## Affected

- `ems-gen` (catalog, synthetic models, generator, config) and the wire contract (`src/ems/contracts/`, `schemas/`).
- Consumers of the contract: none yet (the Kafka ingestion spec is next). Local topics holding events from an
  earlier mixed run no longer validate and need `make reset`.
- Docs: README, `docs/context/00`, `08`, `09`, `13`, ADR index; spec 004 gets a "partly superseded" note and
  `specs/001-data-pipeline-core/data-model.md` (the field tables `08` points to) an "amended by spec 005" note.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Only PV prosumer sites (Priority: P1)
A mixed catalog contains only households, commercial sites and solar farms, and the production stream carries only
PV and battery readings. **Acceptance**: AC-005-01, -02, -04.

### User Story 2 - Contract matches the generator (Priority: P1)
The contract no longer accepts plant asset types or their quantities, still at 1.0.0 with the same topics and
event ids, and is frozen from then on. **Acceptance**: AC-005-03, -06, -08.

### User Story 3 - Nothing else changes (Priority: P1)
A households-only run, which is the default and the test site, gives exactly the same events as before, and
commercial sites and solar farms are unchanged. **Acceptance**: AC-005-05, -07.

### Edge Cases
- A config or `config/sites.yaml` written for spec 004 that names a removed type fails at load with a clear error,
  never silently at some later site.
- An unmanaged site's balance still closes with PV as the only generation.

## Requirements *(mandatory)*

- **FR-001**: The catalog MUST offer only the site types `household`, `commercial`, `solar_farm` and the asset kinds
  `pv`, `heat_pump`, `bess`, `ev_charger`. → AC-005-01, -02, -04
- **FR-002**: The generator MUST NOT contain wind, CHP, biogas, biomass, hydro or outage models, nor the turbine
  curves. → AC-005-04
- **FR-003**: Wire contract v1 MUST drop the removed asset types and quantities and keep `schema_version` 1.0.0,
  the topic names and the `event_id` rule. → AC-005-03, -06
- **FR-004**: Output of the kept site types MUST not change. → AC-005-05, -07
- **FR-005**: The 1.0.0 contract MUST be frozen after this change; removals fail the tests. → AC-005-08

### Acceptance criteria (EARS)
- **AC-005-01**: IF `scale.site_mix` names a site type other than `household`, `commercial` or `solar_farm`, THEN THE SYSTEM SHALL refuse to load the generator config and to build a catalog, with an error that lists the allowed types.
- **AC-005-02**: IF `config/sites.yaml` defines a removed site type or an asset of kind `wind`, `chp`, `biogas`, `biomass` or `hydro`, THEN THE SYSTEM SHALL reject the site when the catalog is loaded.
- **AC-005-03**: IF a measurement carries `asset_type` `wind`, `chp`, `hydro`, `biogas` or `biomass`, or `quantity` `heat_energy` or `fuel_gas_volume`, THEN THE SYSTEM SHALL reject the event in both the pydantic models and the exported JSON Schema.
- **AC-005-04**: WHEN a mixed catalog (`household`, `commercial`, `solar_farm`) is generated for a day, THE SYSTEM SHALL produce only those site types, emit only `pv` and `bess` measurements on the production stream, validate every event, and close the balance of every unmanaged site in every interval.
- **AC-005-05**: WHERE `scale.site_mix` is empty, THE SYSTEM SHALL produce events identical (every field except `ingested_at`, full float precision, emission order) to commit `51560fa` for a one-day run of 20 households including `PL-LOC-000001`, with site-controlled batteries and the import register enabled.
- **AC-005-06**: THE SYSTEM SHALL keep `schema_version` 1.0.0, the topic names and the `event_id` rule, and the committed schemas in `schemas/` SHALL match the models.
- **AC-005-07**: WHEN commercial and solar-farm sites are generated with the same seeds, THE SYSTEM SHALL produce catalog entries and one-day events identical to commit `51560fa`.
- **AC-005-08**: IF the models remove or rename a field or an enum value of the frozen 1.0.0 schemas, THEN the contract tests SHALL fail.

## Constraints (from the checklists)

- **P1**: this spec, its plan and tasks are approved before code; every change traces to a task.
- **P2**: no domain code changes (`src/ems/domain/` untouched).
- **P3–P8, P10, P13**: no actors, handlers, timers, jobs or tracing are touched.
- **P9**: removing enum values breaks v1. One-time exception by user decision: v1 is amended in place at 1.0.0
  before any consumer exists, then frozen; P9 applies strictly from this change on (ADR 0016).
- **P11**: removed site types in config fail fast (AC-005-01).
- **P12**: no dependency changes (pvlib stays; windpowerlib was never a dependency).
- **P14, P16**: no reported number changes; the recorded benchmark is households only (AC-005-05 shows it is
  unchanged). The mixed 1,000-site timing in `13-subsystem-ingest.md` has no reproducible command and described the
  old mix, so it is dropped.
- **P15**: README, `docs/context/` (00, 08, 09, 13), the spec 001 field tables and ADR 0016 land with the change.
- **Risks in scope** (`09`): G5 (windpowerlib unmaintained) and G7 (weak inland wind for turbines) become
  obsolete with the wind model; no new risk.

## Success Criteria
- **SC-001**: `make ci` is green; nothing in `src/`, `config/`, `tests/` or `schemas/` mentions the removed site types,
  asset kinds or quantities, except the AC-005-01…03 tests that feed them in as rejected input (checked with a grep
  that excludes `tests/unit/ingest/test_narrow_assets.py`).

## Assumptions
- Recordings in `data/recordings/` (prices and weather only) stay valid.

## Open questions
- None.
