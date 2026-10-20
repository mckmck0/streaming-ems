# Feature Specification: Data pipeline core (synthetic household data → event streams)

**Feature Branch**: `001-data-pipeline-core`

**Created**: 2026-10-20

**Status**: Approved 2026-10-20 (plan reviewed)

**Input**: User description: "Spec 001 of the approved data-generator plan (Plan 1 milestones M0–M2): a local
event-streaming stack with automatic, idempotent topic creation, versioned wire contracts, a producer with
verification tools, and realistic synthetic Polish-market data (weather truth and forecasts, PV, base load, heat
pump, prices) for one EMS-managed household, streamed end to end. Source: the approved plan
`~/.claude/plans/pasted-content-id-8c05-plan-clever-pearl.md`, section 'Spec 001'."

## Context

The EMS needs a stream of realistic, reproducible inputs before any actor or strategy can be built ("data comes
first", `docs/context/00-overview.md`). This spec delivers the first slice: one household, synthetic data, end to
end into the event-streaming platform, with a wire contract that the later ingestion spec consumes unchanged.
Live data sources, scale, fault injection and non-household assets are later specs (002–004).

Decisions already taken with the user (2026-10-20) and binding for this spec: production and consumption are two
separate streams; prices are keyed by market; external data reaches the EMS only through the event streams; the
generator never simulates a battery that the EMS controls; repo conventions win over Plan 1 where they differ.

## User Scenarios & Testing *(mandatory)*

The "users" are the thesis author running experiments and the later ingestion spec (Plan 2) consuming the streams.

### User Story 1 - Start a local streaming stack with the right topics (Priority: P1)

The author starts the local infrastructure with one command. The broker comes up healthy, every topic the
contract defines exists with the configured partitions and settings, and nothing can create a topic by accident.
Running the start command again changes nothing.

**Why this priority**: every other story writes to these topics; wrong partition counts later break per-key
ordering and cannot be fixed without data loss.

**Independent Test**: start the stack on a clean machine, inspect topics, run topic creation a second time, try
to write to a topic that does not exist.

**Acceptance Scenarios**:

1. **Given** a clean machine with the stack stopped, **When** the author starts it, **Then** the broker, the
   topic-creation job and the inspection UI report healthy/completed and all enabled topics exist with the
   profile's partition counts and settings.
2. **Given** the topics exist, **When** topic creation runs again, **Then** it exits successfully and reports no
   change.
3. **Given** the stack is running, **When** a client writes to a topic not in the topic definition, **Then** the
   write fails and the topic is still absent afterwards.

---

### User Story 2 - Publish contract-valid events that downstream code can trust (Priority: P1)

Any event published to the streams follows one versioned contract: a common envelope, a deterministic event id, a
key that routes all data of a location to one partition, and a size well below the platform limit. Invalid
events never reach the main topics; they go to a dead-letter topic with the reason. The author can tail and
validate any topic.

**Why this priority**: the ingestion spec builds on this contract without changes; idempotency downstream depends
on deterministic ids.

**Independent Test**: publish hand-built valid and invalid events, then run the validation tool over the topics.

**Acceptance Scenarios**:

1. **Given** a valid event, **When** it is published, **Then** it lands on the topic and partition given by the
   contract's routing rule, with the documented headers and its event time as record time.
2. **Given** an event that violates the contract, **When** it is published, **Then** it is written to the
   dead-letter topic with an error description and not to the main topic.
3. **Given** the same event content is published twice (a retry), **When** both records are inspected, **Then**
   they carry the same event id.

---

### User Story 3 - Generate two realistic days for the test household and stream them (Priority: P1)

The author backfills a window (for example two days from 2026-10-20 00:00 UTC) for the test household
`PL-LOC-000001` near Poznań (10 kWp PV, 10 kWh battery controlled by the EMS, heat pump, tariff G12) and gets
weather forecasts, production, consumption and prices on the streams, physically consistent and reproducible from
a seed.

**Why this priority**: this is the first end-to-end data the EMS can consume; it is the MVP of the data pipeline.

**Independent Test**: run the backfill, consume the topics, count and validate events, rerun with the same seed
and compare.

**Acceptance Scenarios**:

1. **Given** a running stack, **When** the author backfills two days for the test household, **Then** the streams
   contain exactly 192 production events, 192 consumption events, 16 forecast runs and 3 price events (delivery
   days D, D+1, D+2), all valid, all of the household's records on one partition, in interval order.
2. **Given** the same seed, window and configuration, **When** the backfill runs twice under different run ids,
   **Then** the data content is identical and the event ids differ only through the run id.
3. **Given** the test household's battery is controlled by the EMS, **When** its events are generated, **Then**
   they contain PV, base load and heat-pump measurements and no battery or grid-connection measurements.

---

### User Story 4 - Trust the synthetic data's realism (Priority: P2)

The author can rely on the generated data being plausible for Poland, so that experiment results are meaningful.

**Why this priority**: required for thesis validity, but the pipeline works without the statistical checks.

**Independent Test**: generate one simulated year for one site and compare statistics with reference ranges.

**Acceptance Scenarios**:

1. **Given** one simulated year, **When** PV output is summed, **Then** the specific yield lies in 850–1150
   kWh/kWp.
2. **Given** one simulated year of prices, **When** negative quarter-hours are counted, **Then** their share lies
   in 1–8 % and is higher in April–August than in November–January.

---

### Edge Cases

- DST: a local delivery day has 92 quarter-hours on 2026-03-29 and 100 on 2026-10-25; prices and calendar logic
  must produce exactly those counts.
- Backfilled data far in the past and accelerated runs far in the future must not be silently deleted or rejected
  by the platform's time-based rules; the generator refuses to start when topic settings would cause either.
- A NaN or infinite value never becomes a silent null in an event.
- Re-running tests against a broker that still holds data from earlier runs must not change test outcomes.
- Restarting the broker keeps all topics and data.
- Generating one location alone or together with others gives identical values for that location.

## Requirements *(mandatory)*

Acceptance criteria use EARS form with ids `AC-001-NN`; each functional requirement lists the criteria that test it.

### Functional Requirements

**Infrastructure and topics**

- **FR-001**: The system MUST provide a one-command local stack (broker, topic-creation job, inspection UI)
  bound to localhost only, with persistent storage. → AC-001-01, AC-001-04
- **FR-002**: Topics MUST be created only by an idempotent topic-ensure routine driven by one topic definition
  file; broker auto-creation MUST be disabled. → AC-001-02, AC-001-03
- **FR-003**: The topic-ensure routine MUST create missing topics, fail when an existing topic has fewer
  partitions than configured (unless explicitly allowed to increase them, with a warning), warn when it has more,
  report configuration drift and apply it only on request, and refuse a replication factor above the broker
  count. → AC-001-05
- **FR-004**: The topic definition MUST support profiles (`live`, `sim`, `scale`, `cluster`) that set
  partitions, retention and record-timestamp limits, selected by one setting. → AC-001-06
- **FR-005**: Before streaming, the generator MUST check the target topics' time-based settings against the
  clock mode and window, and refuse to start if records would be rejected or expire. → AC-001-07

**Contract and publishing**

- **FR-006**: Every event MUST use the common envelope v1.0.0: schema name and version, deterministic event id,
  stream, source system, location id (null only for prices), metering-point id, interval start/end (UTC,
  half-open), resolution, event-time kind, issue time, ingestion time, synthetic flag, seed, quarter-hour index
  `t`, optional run id, and a stream-specific payload. → AC-001-08
- **FR-007**: The event id MUST be a deterministic function of stream, location or market, interval start, issue
  time, original source system, run id and major version, using one canonical UTC text form; ingestion time MUST
  NOT influence it. → AC-001-09, AC-001-10
- **FR-008**: Records MUST be routed by one shared rule: location streams keyed by location id (prefixed with the
  run id when set), prices keyed by market (e.g. `PL:RCE`); a key always maps to the same partition under the
  Java-compatible hash. → AC-001-11
- **FR-009**: The record time MUST be the event time (interval end; issue time for forecasts and prices).
  → AC-001-12
- **FR-010**: Events failing contract validation MUST go to the dead-letter topic with an error description;
  persistent delivery failures MUST be counted and make the process exit non-zero. → AC-001-13, AC-001-14
- **FR-011**: Machine-readable schemas MUST be exported from the contract models and kept in sync (a check fails
  when they are stale). → AC-001-15
- **FR-012**: The author MUST be able to tail a topic (key, partition, offset, headers, value) and validate all
  records of a topic against the schema, the routing rule and partition stability. → AC-001-16
- **FR-013**: Serialized events MUST stay below 900,000 bytes including key and headers. → AC-001-17

**Synthetic data for one household**

- **FR-014**: The generator MUST build a site catalog (Polish coordinates by voivodeship, a valid 18-digit PPE
  code starting with 590, tariff group, assets) deterministically from a seed, with hand-defined sites overriding
  generated ones; the test site `PL-LOC-000001` is hand-defined. → AC-001-18
- **FR-015**: Each site MUST declare who controls its battery (EMS or site). For EMS-controlled batteries the
  generator MUST NOT emit battery or grid-connection measurements. → AC-001-19
- **FR-016**: The generator MUST produce a "true weather" series (temperature, irradiance GHI/DNI/DHI, cloud
  cover, wind at 10/100/120 m) and forecasts derived from it with error growing with lead time, emitted every 3 h
  with 15-minute steps out to 48 h, field names and interval semantics matching the live source planned in 002.
  → AC-001-20, AC-001-21
- **FR-017**: Production MUST follow the true weather (PV), consumption MUST follow calendar, holidays,
  temperature and tariff group (base load, heat pump), and prices MUST follow season, day type and the national
  PV/wind index derived from the same weather process. → AC-001-22, AC-001-23, AC-001-24
- **FR-018**: Prices MUST be emitted once per market per local delivery day with 92/96/100 quarter-hour slots,
  published in the early afternoon local time for the next day, plus day D at start when configured.
  → AC-001-25
- **FR-019**: Output for a location MUST depend only on the seed, the location and the time, not on how many
  other locations are generated. → AC-001-26
- **FR-020**: The generator MUST run in `backfill`, `accelerated` and `realtime` clock modes, with the wall clock
  used only for pacing and the ingestion timestamp. → AC-001-27

### Acceptance criteria (EARS)

- **AC-001-01**: WHEN the author starts the stack on a clean machine, THE SYSTEM SHALL report broker and UI
  healthy and the topic job completed, with host ports bound to 127.0.0.1.
- **AC-001-02**: WHEN topic creation runs against a broker without the topics, THE SYSTEM SHALL create every
  enabled topic with the active profile's partitions and settings; WHEN it runs again, THE SYSTEM SHALL exit 0
  and report no change.
- **AC-001-03**: IF a client writes to a topic that is not defined, THEN THE SYSTEM SHALL fail the write and the
  topic SHALL still not exist.
- **AC-001-04**: WHEN the broker restarts, THE SYSTEM SHALL keep all topics and records.
- **AC-001-05**: WHEN an existing topic has fewer partitions than configured, THE SYSTEM SHALL fail with a clear
  message unless partition increase is allowed; WHEN it has more, warn; WHEN settings drift, print the diff and
  apply only on request; IF the replication factor exceeds the broker count, THEN fail.
- **AC-001-06**: WHERE the `sim` profile is selected, THE SYSTEM SHALL accept records timestamped far in the past
  or future and not delete them by age; WHERE the `live` profile is selected, THE SYSTEM SHALL reject a record
  timestamped more than 1 h in the future.
- **AC-001-07**: IF the clock mode and window would produce records the target topics reject or expire, THEN THE
  SYSTEM SHALL refuse to start and name the conflicting setting.
- **AC-001-08**: THE SYSTEM SHALL publish only events whose envelope validates against the exported v1.0.0
  schema, with `t` equal to the UTC quarter-hour index of the interval start.
- **AC-001-09**: WHEN identical event content is produced twice, THE SYSTEM SHALL assign the same event id; WHEN
  only the ingestion time differs, the id SHALL be unchanged.
- **AC-001-10**: WHEN a recorded event is re-emitted as a replay within its original run scope (same `run_id`, or
  none), THE SYSTEM SHALL keep its original event id; a replay under a new `run_id` is a new run and gets new ids.
- **AC-001-11**: THE SYSTEM SHALL write every record of a key to the partition given by the Java-compatible
  murmur2 hash of the key, and never write a null key to a location or price topic.
- **AC-001-12**: THE SYSTEM SHALL set each record's timestamp to the event's interval end (issue time for
  forecasts and prices).
- **AC-001-13**: IF an event fails contract validation, THEN THE SYSTEM SHALL write it to the dead-letter topic
  with an error header and not to the main topic.
- **AC-001-14**: IF records cannot be delivered within the delivery timeout, THEN THE SYSTEM SHALL count them and
  exit non-zero.
- **AC-001-15**: WHEN the exported schemas differ from the contract models, THE SYSTEM SHALL fail the check; an
  additive optional field SHALL pass both BACKWARD and FORWARD compatibility checks, and a v1.0 consumer SHALL
  accept a v1.1 record that only adds a field.
- **AC-001-16**: WHEN the author validates a topic from the beginning, THE SYSTEM SHALL report counts, schema
  errors, routing-rule errors and partition changes per key.
- **AC-001-17**: THE SYSTEM SHALL keep every serialized record, including a 192-step forecast, below 900,000
  bytes.
- **AC-001-18**: WHEN the catalog is built twice from the same seed, THE SYSTEM SHALL produce identical sites;
  every metering-point id SHALL have 18 digits, start with 590 and carry a valid GS1 check digit.
- **AC-001-19**: WHILE a site's battery is controlled by the EMS, THE SYSTEM SHALL emit no battery or
  grid-connection measurements for it; WHERE a site controls its own battery, THE SYSTEM SHALL emit import and
  export that close the energy balance within 1e-6 kWh.
- **AC-001-20**: THE SYSTEM SHALL keep generated irradiance within 0 and 1.1 × clear-sky, and PV output zero at
  night.
- **AC-001-21**: WHEN a forecast run is emitted, THE SYSTEM SHALL include 192 quarter-hour steps whose error
  spread grows with lead time.
- **AC-001-22**: WHEN one simulated year is generated for the test site, THE SYSTEM SHALL yield 850–1150 kWh per
  kWp of PV.
- **AC-001-23**: WHEN one simulated year is generated, THE SYSTEM SHALL keep annual household base-load
  consumption within ±5 % of the configured value.
- **AC-001-24**: WHEN one simulated year of prices is generated, THE SYSTEM SHALL produce a negative-slot share
  between 1 % and 8 %, higher in April–August than in November–January.
- **AC-001-25**: THE SYSTEM SHALL emit price events with 92 slots for 2026-03-29, 100 for 2026-10-25 and 96 for
  other days (Europe/Warsaw).
- **AC-001-26**: WHEN a location is generated alone and within a batch of N locations, THE SYSTEM SHALL produce
  identical values for it.
- **AC-001-28**: WHEN a run finishes, THE SYSTEM SHALL write a manifest with config, seed, window, run id,
  software versions and git commit next to the run's outputs.
- **AC-001-27**: WHEN the author backfills two days for `PL-LOC-000001` from 2026-10-20T00:00Z with a fresh run id,
  THE SYSTEM SHALL publish exactly 192 production, 192 consumption, 16 forecast and 3 price events, all valid,
  one partition per key, interval start monotonic per key.

### Key Entities

- **Envelope**: the common wrapper of every event (identity, stream, source, location or market, interval, time
  kinds, synthetic flag, seed, `t`, run id, payload).
- **Measurement event**: one location, one interval, many measurements (asset, quantity, direction, aggregation,
  value, unit, quality); split into production and consumption streams.
- **Weather forecast event**: one location, one forecast run, 192 steps (valid interval, lead time, irradiance,
  temperature, cloud cover, wind by height, direction).
- **Price event**: one market, one local delivery day, quarter-hour slots with price in native currency per MWh.
- **Site**: location id, coordinates, voivodeship, metering point, tariff group, site type, assets with
  parameters and battery control (EMS or site).
- **Topic definition**: topics, key rule, partitions and settings per profile.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: From a clean machine, the stack is up with all topics in under 2 minutes with one command.
- **SC-002**: A two-day backfill for the test household completes and validates in under 1 minute.
- **SC-003**: 100 % of published records pass validation; 0 records with a null key or wrong partition.
- **SC-004**: Rerunning the same seed and window reproduces identical data content (100 % of values equal).
- **SC-005**: Annual statistics for the test site fall inside all reference ranges (PV yield, consumption,
  negative-price share).
- **SC-006**: The ingestion spec can consume the streams using only the documented contract (no change to the
  wire format needed), confirmed by its review.

## Assumptions

- The streaming platform is Apache Kafka (KRaft) run with Docker Compose on the author's Mac; the repo's
  conventions apply (one uv project, `ty`, `infra/docker-compose.yml`, images by tag and digest, settings via
  environment). Technology choices and pins come from the approved plan and are fixed in `/speckit-plan`.
- Prices are an RCE-like synthetic curve (the RES-act net-billing reference price, PLN/MWh); conversion to the
  domain's PLN/kWh happens downstream.
- Base load uses BDEW 2025 standard profiles (H25 for G-tariffs, G25 for C11 and C21): official Polish DSO profiles are
  PDF annexes without reuse terms. Household annual consumption defaults to a lognormal with a median around
  2,000–2,500 kWh excluding heat pump and EV, which are modelled separately (a heat pump adds roughly
  5–6 MWh/yr); VERIFY against GUS BDL.
- Live sources, record/replay, fault injection, many locations, background sites with their own battery
  (beyond fixtures), and non-household assets are out of scope (specs 002–004). The Restate ingestion is a later
  spec.
- Kafka record headers are kept for tooling only; everything downstream needs is in the JSON payload.
