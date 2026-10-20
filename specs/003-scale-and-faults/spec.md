# Feature Specification: Scale and faults

**Feature Branch**: `003-scale-and-faults` · **Created**: 2026-10-20 · **Status**: Approved 2026-10-20 (auto-run)

**Input**: Spec 003 of the approved data-generator plan (Plan 1 M5): many locations generated vectorised,
unmanaged background sites with their own battery, deterministic fault injection for testing idempotency and
late data downstream, a `stats` tool, the `scale` topic profile in use, and a benchmark.

## Context

Specs 001–002 stream one or a few sites. Fleet-scale experiments (thesis RQ1) need thousands of sites, and the
ingestion spec needs realistic faults (duplicates, gaps, late and stuck data) to test exactly-once handling.
Background sites that control their own battery make the fleet realistic; their battery uses the domain battery
equations (ADR 0014), never a second model.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Generate a fleet (Priority: P1)
The author backfills a day for 1,000 or 10,000 generated sites; per-site values are identical to generating each
site alone, and generation time grows roughly linearly.
**Independent Test**: benchmark job and batch-independence tests.
**Acceptance Scenarios**:
1. **Given** a catalog of N sites, **When** a day is generated, **Then** every site's events equal those of the
   same site generated alone, and the run reports events/s.

### User Story 2 - Background sites with their own battery (Priority: P1)
Sites whose battery is site-controlled charge on PV surplus and in cheap slots and discharge in the evening peak
and in expensive slots; their measurements include battery charge/discharge and SoC, and the energy balance closes.
**Acceptance Scenarios**:
1. **Given** a site-controlled battery, **When** a year is simulated, **Then** SoC stays within bounds, power
   within limits, and import − export = loads + charge − generation − discharge within 1e-6 kWh per interval.

### User Story 3 - Fault injection and stats (Priority: P1)
With faults enabled, the generator drops, duplicates, delays and freezes events and resets registers
deterministically per seed; `ems-gen stats` reports gaps, duplicates and late events matching the configured
rates within tolerance.
**Acceptance Scenarios**:
1. **Given** fault rates and a seed, **When** a backfill runs twice, **Then** the same faults hit the same events.
2. **Given** the stats tool, **When** it reads the topics, **Then** reported duplicate, late and gap rates match
   the configured rates within tolerance.

### Edge Cases
- A duplicate is the same event (same id), not a new one; a late event keeps its event time.
- Faults never apply to EMS-managed battery facts (there are none) and never break the contract.
- Register resets only apply to register measurements (cumulative), never to interval sums.

## Requirements *(mandatory)*

- **FR-001**: Generation MUST stay vectorised over sites (loops over time only) and chunk sites to bound memory.
  → AC-003-01, -02
- **FR-002**: The pure battery model MUST live in `src/ems/domain/battery.py` (brief §4 equations: one-way
  efficiency on charge and discharge, SoC bounds, power limits, clamping) and be applied vectorised for
  site-controlled batteries with a rule policy. → AC-003-03, -04
- **FR-003**: Site-controlled battery sites MUST emit `bess` charge/discharge energy, an instant SoC reading and
  grid flows closing the balance. → AC-003-04
- **FR-004**: Fault injection MUST support missing, duplicate, late (up to `late_max`), stuck values, register
  resets and quality flags (estimated/substituted), deterministically per seed and off by default. → AC-003-05, -06
- **FR-005**: `ems-gen stats` MUST report counts per topic, key and partition, event-time range per key, gaps,
  duplicate ids and late events. → AC-003-07
- **FR-006**: A benchmark job MUST time one simulated day for 1k and 10k sites and producer throughput, writing
  results with metadata to `experiments/results/`. → AC-003-08
- **FR-007**: The catalog MUST support a share of site-controlled batteries for background sites. → AC-003-03

### Acceptance criteria (EARS)
- **AC-003-01**: WHEN N sites are generated, THE SYSTEM SHALL produce for each site the same values as generating it alone.
- **AC-003-02**: WHEN sites are chunked, THE SYSTEM SHALL produce the same events as without chunking.
- **AC-003-03**: WHERE `scale.site_controlled_bess_share` > 0, THE SYSTEM SHALL generate that share of battery sites with `control: site`.
- **AC-003-04**: WHILE a battery is site-controlled, THE SYSTEM SHALL keep SoC within [soc_min, soc_max] and power within ±p_max, and close the site balance within 1e-6 kWh per interval.
- **AC-003-05**: WHEN faults are enabled with a seed, THE SYSTEM SHALL inject the same faults on every run.
- **AC-003-06**: WHEN a duplicate fault fires, THE SYSTEM SHALL publish the identical event twice; WHEN a late fault fires, THE SYSTEM SHALL delay emission but keep event time and id.
- **AC-003-07**: WHEN `ems-gen stats` reads a topic, THE SYSTEM SHALL report gaps, duplicate ids and late events per key; with faults on, the rates SHALL match the configured ones within tolerance (integration test 4).
- **AC-003-08**: WHEN the benchmark runs, THE SYSTEM SHALL write events/s and wall time for 1k and 10k sites with config, versions, hardware and commit.

## Success Criteria
- **SC-001**: One simulated day for 1,000 sites generates in under 2 minutes on the author's Mac.
- **SC-002**: Fault statistics match configured rates within ±50 % relative (or ±3 events) on a 1,000-site day.

## Assumptions
- Background battery policy: charge from PV surplus, grid-charge in the cheapest 4 night slots, discharge in the
  evening peak (17–21 local) and when the price is in the top 10 % of the day.
- SoC readings use quantity `soc`, aggregation `instant`, direction `charge` on the consumption topic (the
  contract has no direction-free value).
