# Implementation Plan: Other assets, site types and a 3-broker profile

**Branch**: `004-assets-and-cluster` | **Date**: 2026-10-20 | **Spec**: [spec.md](spec.md)

## Summary
New asset kinds in the catalog (`wind`, `chp`, `biogas`, `biomass`, `hydro`, `ev_charger`), vectorised models in
`src/ems/ingest/synthetic/{wind,plants,ev}.py`, outage masks in `synthetic/outages.py`, site-type generators with
`scale.site_mix`, meter builders for the new assets, a `cluster` compose profile (3 combined KRaft nodes) and its
integration test.

## Constitution Check (PASS)
P2 pure models (numpy, seeded via `ems.ingest.rng`); P3 plants are external inputs written by `ems-gen`; P9 only
existing enum values (`wind`, `chp`, `hydro`, `biogas`, `biomass`, `ev_charger`, `heat_energy`); P12 no new
dependency (windpowerlib replaced by vendored curves, ODbL notice); others unchanged.

## Design
- Wind: hub wind = v10 × (h/10)^α (site α), density ρ = p(h)/(287.058·T), v_eq = v (ρ/1.225)^(1/3),
  `np.interp(v_eq, curve)` per turbine type (zero outside the curve), × turbine count, × availability.
- CHP: heat demand ∝ heating degrees (base 15 °C) + DHW base, weekday factor; electric = clip(heat/ratio,
  min_load, rated) when running; heat_energy emitted on production.
- Biogas: rated × (0.92 + small AR noise) clipped to rated, × availability.
- Biomass: target = rated × clip(0.4 + 0.8 × price rank, 0.3, 1); ramp-limited recursion over time.
- Hydro: flow factor = seasonal curve (peak mid-April, low late August) × exp(AR noise); power = clip(rated × flow, 0,
  rated) × availability.
- EV: per site and local day, arrival/departure and energy need from keyed uniforms; charge at p_max between
  arrival and departure until the need is delivered (loop over time).
- Outages: forced (per site-day keyed uniform < rate) and planned (fixed consecutive days per year from a keyed
  offset) → availability mask [n_loc, n_steps].
- Catalog: `scale.site_mix` (shares per site type); generators for commercial (C11/C21, G25, PV, EV), industrial
  (C21, CHP), wind_farm, solar_farm, biogas_plant, hydro_plant; households may get an EV.
- Compose: profile `cluster` with `kafka-1..3` (combined broker+controller, host ports 127.0.0.1:19092..19094),
  topic profile `cluster` (RF 3, min ISR 2).

## Test strategy
property/unit: wind curve bounds and density; capacity factors (wind, biogas), seasonality (CHP, hydro), ramp
(biomass), EV window and need; site mix shares and contract validity of every new event; integration: cluster
profile up, stop 1 node → write OK, stop 2 → NOT_ENOUGH_REPLICAS.

## Docs
13 (new assets, site types), 05 (cluster profile), 09 (windpowerlib note closed), 00.

## Complexity Tracking
None.
