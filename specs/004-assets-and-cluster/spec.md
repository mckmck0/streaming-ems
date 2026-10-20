# Feature Specification: Other assets, site types and a 3-broker profile

**Feature Branch**: `004-assets-and-cluster` · **Created**: 2026-10-20 · **Status**: Approved 2026-10-20 (auto-run)

**Input**: Spec 004 of the approved data-generator plan (Plan 1 M3) plus the 3-broker profile: wind (vendored
power curves), CHP, biogas, biomass, run-of-river hydro, EV chargers, site types for farms and plants, and a
`cluster` compose profile to test replication factor 3 and `min.insync.replicas=2`.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Wind farms (Priority: P1)
Wind farms produce from hub-height wind through vendored power curves with air-density correction, cut-in and
cut-out, and outages; a simulated year has an onshore capacity factor of 22–35 % (fleet target about 25 %).
**Acceptance**: AC-004-01, -02.

### User Story 2 - Dispatchable and run-of-river plants (Priority: P2)
CHP (heat-led, with minimum load, weekday pattern, heat output), biogas (≈ 90 % capacity factor), biomass
(price-following with ramp limits) and hydro (seasonal flow with a spring peak and late-summer low) with planned
and forced outages. **Acceptance**: AC-004-03 … -06.

### User Story 3 - EV charging and site types (Priority: P2)
Households charge EVs overnight, commercial sites in the daytime, at the charger limit until the energy need is
met; the catalog mixes site types (household, commercial, industrial, wind farm, solar farm, biogas plant,
hydro plant) by configured shares. **Acceptance**: AC-004-07, -08.

### User Story 4 - Replicated broker profile (Priority: P3)
A 3-node KRaft cluster profile accepts writes with one node down and rejects them (`NOT_ENOUGH_REPLICAS`) with
two down. **Acceptance**: AC-004-09.

### Edge Cases
- Wind above cut-out or below cut-in produces zero; density correction never exceeds rated power.
- Outages are deterministic per seed; a plant in outage produces zero.
- Every event still validates (generation on production, consumption on consumption).

## Requirements *(mandatory)*

- **FR-001**: Wind power MUST use vendored power curves (`config/turbines/power_curves.csv`, ODbL notice) via
  numpy interpolation and IEC density correction (no windpowerlib dependency). → AC-004-01, -02
- **FR-002**: CHP, biogas, biomass and hydro models MUST be vectorised over sites and include planned and
  forced outages. → AC-004-03 … -06
- **FR-003**: EV charging MUST follow arrival/departure processes per site type and deliver the energy need at
  the charger limit. → AC-004-07
- **FR-004**: The catalog MUST generate site types by `scale.site_mix` shares with matching assets and tariffs.
  → AC-004-08
- **FR-005**: A `cluster` compose profile and topic profile MUST provide RF 3 and min ISR 2. → AC-004-09

### Acceptance criteria (EARS)
- **AC-004-01**: WHEN one simulated year is generated for a Pomeranian wind farm, THE SYSTEM SHALL yield a capacity factor between 22 % and 35 %.
- **AC-004-02**: THE SYSTEM SHALL produce zero wind power below cut-in and above cut-out and never more than the rated power.
- **AC-004-03**: WHEN a year is generated for a CHP plant, THE SYSTEM SHALL produce more in the heating season than in summer, respect the minimum load while running, and emit heat energy.
- **AC-004-04**: WHEN a year is generated for a biogas plant, THE SYSTEM SHALL yield a capacity factor between 80 % and 95 %.
- **AC-004-05**: WHILE a biomass plant runs, THE SYSTEM SHALL keep output changes within its ramp limit per interval.
- **AC-004-06**: WHEN a year is generated for a hydro plant, THE SYSTEM SHALL produce more in April than in August.
- **AC-004-07**: WHEN a day is generated for a household with an EV, THE SYSTEM SHALL charge only between arrival and departure, at most at the charger power, and deliver the energy need when time allows.
- **AC-004-08**: WHERE `scale.site_mix` is set, THE SYSTEM SHALL generate site types in those shares (± sampling error) and every event SHALL validate.
- **AC-004-09**: WHEN one of three brokers is stopped, THE SYSTEM SHALL accept writes to an RF-3 topic; WHEN two are stopped, writes SHALL fail with NOT_ENOUGH_REPLICAS.

## Success Criteria
- **SC-001**: All asset models pass their plausibility tests; a mixed 1,000-site day still generates in < 2 min.

## Assumptions
- Wind farm shear exponent 0.14 (open terrain); turbine hub heights as vendored; outage rates from literature
  defaults (forced ≈ 2 %/yr of days for wind, planned 14 days/yr for thermal plants).
