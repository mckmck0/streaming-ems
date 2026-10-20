# Tasks: Scale and faults

## Phase 1: Foundational
- [ ] T001 [P] Property tests for the domain battery: SoC bounds, power limits, efficiency on charge and discharge, scalar = vector (AC-003-04)
- [ ] T002 Implement `src/ems/domain/battery.py`

## Phase 2: US2 background batteries (P1)
- [ ] T003 [P] [US2] Tests: catalog share of site-controlled batteries; BESS policy balance closes; SoC bounded over a year (AC-003-03/-04)
- [ ] T004 [US2] Implement `synthetic/bess.py`, catalog share, generator measurements (bess, SoC, grid incl. battery)

## Phase 3: US1 fleet (P1)
- [ ] T005 [P] [US1] Tests: chunked = unchunked; site in fleet = site alone (AC-003-01/-02)
- [ ] T006 [US1] Implement site chunking in the model and generator
- [ ] T007 [US1] Implement `jobs/benchmark.py`; run it and record results (AC-003-08)

## Phase 4: US3 faults and stats (P1)
- [ ] T008 [P] [US3] Tests: fault determinism, duplicate/late/missing/stuck/register/quality semantics (AC-003-05/-06)
- [ ] T009 [US3] Implement `faults.py`, registers, config, CLI wiring
- [ ] T010 [P] [US3] Tests: stats on hand-built records (AC-003-07)
- [ ] T011 [US3] Implement `tools/stats.py` and `ems-gen stats`; `make stats`
- [ ] T012 [US3] Integration test 4: faults on, stats rates within tolerance (AC-003-07)

## Phase 5: Polish
- [ ] T013 Docs (13, 12, 06, 09, 11, 00); `make ci`, `make test-integration`
