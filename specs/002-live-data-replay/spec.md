# Feature Specification: Live data, record and replay

**Feature Branch**: `002-live-data-replay`

**Created**: 2026-10-20

**Status**: Approved 2026-10-20 (user: run all Spec Kit phases automatically)

**Input**: Spec 002 of the approved data-generator plan (Plan 1 M4): a mapping engine for raw source payloads,
live adapters for Open-Meteo weather forecasts and PSE prices (RCE and the SDAC day-ahead price), ENTSO-E A44
behind a flag, recording of raw responses and canonical events, replay, hybrid mode (live and synthetic
locations on the same topics), and a calibration job. Fronius is not built (§16.3 = none).

## Context

Spec 001 streams synthetic data. The thesis needs the same streams from real sources for the live test site
`PL-LOC-000001` (Poznań), recordings that can be replayed deterministically into experiments, and
calibration of the synthetic models from real history. External data reaches the EMS only through Kafka
(ADR 0011); the wire contract v1 (ADR 0015) is unchanged except additive values.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Live prices from PSE (Priority: P1)

The author runs the generator with live prices: RCE and the SDAC day-ahead price for D (at start) and D+1
(after publication) appear on the price topic, with correct UTC slots on DST days, deterministic ids and
revisions detected.

**Why this priority**: prices drive every strategy; PSE is free and authoritative.

**Independent Test**: recorded PSE fixtures (normal, 92-slot and 100-slot days, empty response) mapped
offline; a mocked live poll publishes to Kafka.

**Acceptance Scenarios**:

1. **Given** a PSE response for a delivery day, **When** it is mapped, **Then** the price event has every
   local-day slot in UTC (92/96/100), PLN/MWh, `issued_at` = PSE publication time.
2. **Given** the same publication is polled twice, **When** events are built, **Then** both have the same
   event id; **given** PSE republishes with a new publication time, **Then** a new event is emitted.

---

### User Story 2 - Live weather forecasts from Open-Meteo (Priority: P1)

The author runs the generator with live forecasts for the test site: one event per model run with 192
quarter-hour steps, the same canonical fields as synthetic forecasts, and no duplicate events while the
model run is unchanged.

**Why this priority**: forecasts feed forecasters and strategies; the test site is inside ICON-D2.

**Independent Test**: recorded Open-Meteo fixtures (forecast and model metadata) mapped offline; repeated
polls of an unchanged run yield one event.

**Acceptance Scenarios**:

1. **Given** model metadata and a forecast response, **When** mapped, **Then** the event has 192 steps from
   the run time, radiation as preceding means, temperature/wind/cloud as instants, `forecast_model` = the
   requested model, `issued_at` = model run initialisation time.
2. **Given** an unchanged model run, **When** polled again, **Then** no new event is emitted.

---

### User Story 3 - Record and replay (Priority: P1)

The author records a live session (raw responses with secrets redacted, canonical events, manifest) and later
replays it into Kafka; replayed events keep their ids and are marked `replay:<source>`.

**Why this priority**: experiments must be reproducible from real data (principle 14).

**Independent Test**: record from mocked sources, replay into Kafka, compare ids and payloads.

**Acceptance Scenarios**:

1. **Given** a recording, **When** it is replayed, **Then** every event is published with its original id and
   payload, `source_system = replay:<orig>`, and the record timestamp = event time.

---

### User Story 4 - Hybrid mode (Priority: P2)

Live locations use live sources while other locations stay synthetic, at the same time and on the same topics;
the live location's synthetic production and consumption follow its live weather.

**Independent Test**: hybrid run with mocked live sources and two synthetic sites; check `source_system`,
`synthetic` and that only one price source is active.

---

### User Story 5 - ENTSO-E prices and calibration (Priority: P3)

With a token, ENTSO-E day-ahead prices (EUR) arrive under `ENTSOE:PL`; a calibration job fits price and weather
parameters from history into candidate calibration files.

**Independent Test**: hand-written A44 fixtures (A03 with omitted positions, mixed contract types, duplicate
series, PT60M history) mapped offline; calibration job run against recorded history.

### Edge Cases

- PSE fall-back day labels (`02a:15`) and the 2025 vs 2026 spring-forward label change: only UTC fields are used.
- PSE returns an empty list before publication; a partial day is never emitted.
- Open-Meteo returns HTTP 400 for coordinates outside a model domain; sites east of 20.34°E use DMI HARMONIE.
- Secrets (API key, ENTSO-E token) never appear in URLs, logs, recordings or `source_record_ref`.
- ENTSO-E: A03 positions omitted when values repeat; gaps between periods stay missing, never zero; 429 backs
  off at least 10 minutes.
- Network errors: retries with jitter on 429/5xx/timeouts only; 4xx other than 429 fail without retry.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: A mapping engine MUST turn raw payloads into canonical rows from versioned YAML mappings validated
  against a JSON Schema, supporting dotted and JSON-pointer paths (keys may contain dots), row and column
  layouts (columns zipped by index with a length check), time handling (`utc`, `local:Europe/Warsaw`,
  `epoch_s`, `epoch_ms`, start/end labels), unit conversion, transforms (`negate`, `scale`, `sign_split`,
  `enum_map`, `default`), null policies (`skip`, `missing`) and quality maps. → AC-002-01, -02
- **FR-002**: The PSE adapter MUST fetch `rce-pln` and `csdac-pln` per business date with paging, treat an
  empty list as not yet published, emit only complete days, set `issued_at` from `publication_ts_utc`, poll
  D+1 13:00–18:00 local every 15 minutes (then hourly) and re-poll D and D+1 hourly for revisions.
  → AC-002-03, -04, -05
- **FR-003**: The Open-Meteo adapter MUST request the nine canonical variables at 15 minutes with an explicit
  model per site (ICON-D2 inside its domain, DMI HARMONIE otherwise), site elevation, a run-anchored 192-step
  window, read the run time from the model metadata, emit one event per run, and use the customer API with a
  header key when one is configured. → AC-002-06, -07
- **FR-004**: The ENTSO-E adapter (off by default) MUST request A44 with contract type A01 and the token in a
  header, forward-fill A03 periods to the period end, keep native resolution, de-duplicate identical series,
  route conflicting ones to the DLQ, and back off ≥ 10 minutes on 429. → AC-002-08
- **FR-005**: Live HTTP calls MUST retry with exponential backoff and jitter on 429, 5xx and timeouts only.
  → AC-002-09
- **FR-006**: Recording MUST store raw responses (secrets redacted), canonical events and a manifest under
  `data/recordings/<name>/`; replay MUST re-emit recorded events with their ids. → AC-002-10, -11
- **FR-007**: A router MUST pick the source per stream and location from config; hybrid mode runs live and
  synthetic locations together; a live-weather site's synthetic production and consumption follow its live
  weather. → AC-002-12
- **FR-008**: A calibration job MUST fit price parameters from PSE history (quarter-hour dynamics only from
  2025-10-01) and temperature parameters from Open-Meteo archive data into candidate calibration files.
  → AC-002-13
- **FR-009**: Unit tests MUST NOT use the network: fixtures are recorded once and mocked. → AC-002-14
- **FR-010**: Data attribution and licences MUST be recorded (Open-Meteo CC BY 4.0 notice next to fixtures and
  recordings; ENTSO-E prices are not CC BY, so no raw ENTSO-E data is committed). → AC-002-15

### Acceptance criteria (EARS)

- **AC-002-01**: WHEN a mapping file is loaded, THE SYSTEM SHALL validate it against `mappings/_schema/mapping.schema.json` and reject unknown keys or ops.
- **AC-002-02**: WHEN a mapping is applied to its fixture, THE SYSTEM SHALL produce the expected canonical rows (units, UTC times, nulls, transforms), including dotted keys via JSON pointer.
- **AC-002-03**: WHEN PSE fixtures for 2025-03-30, 2025-10-26 and 2026-03-29 are mapped, THE SYSTEM SHALL produce 92, 100 and 92 unique UTC slots at 15-minute spacing.
- **AC-002-04**: IF PSE returns an empty or incomplete day, THEN THE SYSTEM SHALL emit nothing for that day and retry later.
- **AC-002-05**: WHEN the same PSE publication is polled twice, THE SYSTEM SHALL emit one event; WHEN the publication time changes, THE SYSTEM SHALL emit a new event.
- **AC-002-06**: WHEN an Open-Meteo forecast fixture is mapped, THE SYSTEM SHALL produce up to 192 steps starting at the run time (truncated before the first step with a missing value, since the model horizon can end early), with canonical field names and units and `forecast_model` equal to the requested model.
- **AC-002-07**: WHILE the model run is unchanged, THE SYSTEM SHALL not emit another forecast event for the site.
- **AC-002-08**: WHEN the A44 fixtures are mapped, THE SYSTEM SHALL forward-fill A03 positions to the period end, select contract type A01, keep PT60M history, de-duplicate identical series and flag conflicting ones.
- **AC-002-09**: IF a source returns 429 or 5xx, THEN THE SYSTEM SHALL retry with backoff; IF it returns 400/401/403, THEN THE SYSTEM SHALL fail without retry.
- **AC-002-10**: WHEN a session is recorded, THE SYSTEM SHALL store raw responses without any configured secret value, canonical events and a manifest.
- **AC-002-11**: WHEN a recording is replayed into Kafka, THE SYSTEM SHALL publish every event with its original id, `source_system = replay:<orig>`, and pass topic validation.
- **AC-002-12**: WHERE hybrid mode is configured, THE SYSTEM SHALL publish live events for live locations and synthetic events for the others on the same topics, with one active price source.
- **AC-002-13**: WHEN the calibration job runs on recorded history, THE SYSTEM SHALL write candidate calibration files with the fitted values and their data window.
- **AC-002-14**: THE SYSTEM SHALL run all unit tests with network access blocked.
- **AC-002-15**: THE SYSTEM SHALL keep a licence/attribution NOTICE next to committed fixtures and recordings.

### Key Entities

- **Mapping**: id, version, source, layout, record path, field specs (path, type, time, unit, transforms, null
  policy), quality map.
- **Source adapter**: stream, mode (synthetic, live, replay), poll schedule, raw fetch, mapping, event assembly.
- **Recording**: raw responses (redacted), canonical events (JSONL), manifest.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A live run for the test site publishes D and D+1 prices and the latest forecast run within one
  poll cycle of starting.
- **SC-002**: A replayed recording validates 100 % against the contract and reproduces every original event id.
- **SC-003**: No secret value appears in any recording, log or event (automated scan).
- **SC-004**: All mappings are covered by fixture tests; unit tests pass offline.

## Assumptions

- Free Open-Meteo API (non-commercial: public research at a public institution; §16.1 default) with a switch to
  the customer API; PSE is public and unauthenticated; ENTSO-E needs a token the author does not have yet, so
  its tests use hand-written fixtures.
- Prices: RCE is the net-billing reference (`PL:RCE`); csdac is the SDAC day-ahead price (`PL:SDAC`, additive
  market value already in v1.0.0).
- Hybrid mode requires the realtime clock.
