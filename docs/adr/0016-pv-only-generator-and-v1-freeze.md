# 0016. The generator models PV prosumers only; wire contract v1 is frozen

- Status: accepted
- Date: 2026-11-08
- Spec: specs/005-narrow-generator-assets

## Context
Spec 004 added wind farms, industrial sites with CHP, biogas plants (some with biomass) and run-of-river hydro
plants as background sites, with turbine curves, plant and outage models, and the matching contract values. The
thesis studies residential prosumers (PV, home battery, grid); none of the plants feeds an EMS experiment. On
reviewing specs 001–004 the user narrowed the scope. Removing contract values breaks v1 under P9, but nothing
consumes v1 yet (the Kafka ingestion spec is next).

## Decision
- The generator models PV prosumer sites only: site types `household`, `commercial`, `solar_farm`; asset kinds
  `pv`, `heat_pump`, `bess`, `ev_charger`. PV is the only generation source. Weather keeps wind (PV cell
  temperature, the price model, the forecast contract).
- Wire contract v1 is amended in place, once, at version 1.0.0: `asset_type` loses `wind`, `chp`, `hydro`,
  `biogas`, `biomass`; `quantity` loses `heat_energy`, `fuel_gas_volume`. Topics, keys and the `event_id` rule
  do not change, so every remaining event keeps its id.
- From spec 005 on, the contract and the generator range are frozen. Site types, asset kinds and contract values
  change only through a new spec and ADR. Contract changes are additive (P9) and bump the minor version (1.1.0);
  removing or renaming a field or enum value needs v2 and new topics. `schemas/baseline/1.0.0/` holds the frozen
  schemas and a contract test fails on any removal against it.

## Consequences
Less code, config, vendored data and tests (no turbine curves, plant or outage models). Households, commercial
sites and solar farms produce exactly the same events as before (digest tests). Local topics holding events from
an earlier mixed run no longer validate (`make reset`). Spec 004's AC-004-01…06 are retired and AC-004-08 is
replaced by AC-005-04. Risks G5 and G7 are obsolete. Changed docs: README, `docs/context/` 00, 08, 09, 13, the
spec 001 field tables.

## Alternatives considered
Keep the plant models but stop generating them (dead code and contract surface); keep the contract values as
reserved (the user chose a contract that matches what exists); bump to v2 with new topics (churn with no
consumer to protect).
