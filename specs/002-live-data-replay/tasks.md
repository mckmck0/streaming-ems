# Tasks: Live data, record and replay

Tests first per task pair. Format: `[ID] [P?] [Story] description (ACs)`.

## Phase 1: Setup
- [ ] T001 Add httpx, tenacity, lxml (ingest) and respx (dev); network-blocking fixture for unit tests (AC-002-14)
- [ ] T002 Record live fixtures once (PSE rce/csdac incl. 2025-03-30, 2025-10-26, 2026-03-29, empty day; Open-Meteo forecast + meta.json for PL-LOC-000001 and an out-of-domain 400) into `tests/fixtures/`, plus hand-written A44 XML fixtures; NOTICE files (AC-002-15)

## Phase 2: Foundational — mapping engine
- [ ] T003 [P] Unit tests for the engine: paths (dotted, pointer with dotted keys, `*`), columns zip + length check, time tz/labels, units, transforms, sign_split, null policies, quality map, schema rejection (AC-002-01/-02)
- [ ] T004 Implement `mapping/engine.py` and `mappings/_schema/mapping.schema.json`
- [ ] T005 [P] Unit tests for the HTTP helper: retry on 429/5xx/timeout, no retry on 400/401/403, redaction (AC-002-09/-10)
- [ ] T006 Implement `live/http.py`

## Phase 3: US1 PSE prices (P1)
- [ ] T007 [P] [US1] Tests: rce/csdac mappings on fixtures (92/100/96 slots), empty → none, incomplete → none, same publication → same id, new publication → new event (AC-002-03/-04/-05)
- [ ] T008 [US1] Implement `mappings/pse.{rce,csdac}.v2.yaml` and `live/pse.py` (poll schedule, paging, revisions)

## Phase 4: US2 Open-Meteo forecasts (P1)
- [ ] T009 [P] [US2] Tests: forecast mapping → 192-step event, model rule by longitude, issued_at from meta, unchanged run suppressed, customer API header (AC-002-06/-07)
- [ ] T010 [US2] Implement `mappings/openmeteo.forecast.v1.yaml` and `live/openmeteo.py`

## Phase 5: US3 record and replay (P1)
- [ ] T011 [P] [US3] Tests: recorder stores raw/events/manifest without secrets; replay keeps ids and marks `replay:` (AC-002-10/-11)
- [ ] T012 [US3] Implement `sources/recording.py`, CLI `record`, `replay`, `fixtures record`; Makefile targets
- [ ] T013 [US3] Integration test: replay a fixture recording into Kafka, validate, ids preserved (AC-002-11)

## Phase 6: US4 hybrid (P2)
- [ ] T014 [P] [US4] Tests: router modes per stream/location, one price source, live-site P/C follow live weather (AC-002-12)
- [ ] T015 [US4] Implement `sources/router.py`, `sources/live_runner.py`, hybrid weather override, CLI wiring, `make gen-live`

## Phase 7: US5 ENTSO-E and calibration (P3)
- [ ] T016 [P] [US5] Tests: A44 fixtures (A03 fill, A01 selection, PT60M, duplicates, conflicts → DLQ flag, 999 ack) (AC-002-08)
- [ ] T017 [US5] Implement `mappings/entsoe.a44.v1.yaml` and `live/entsoe.py`
- [ ] T018 [P] [US5] Tests: calibration on fixture history writes candidates with data window (AC-002-13)
- [ ] T019 [US5] Implement `jobs/calibrate.py`

## Phase 8: Polish
- [ ] T020 Docs (13, 06, 05, 09, README/CLAUDE commands); `make ci`, `make test-integration`, live smoke run
