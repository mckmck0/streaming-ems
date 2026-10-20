# Tasks: Other assets, site types and a 3-broker profile

## Phase 1: Foundational
- [x] T001 [P] Tests for outage masks (deterministic, planned block length, forced rate)
- [x] T002 Implement `synthetic/outages.py` and asset kinds in `catalog/sites.py`

## Phase 2: US1 wind (P1)
- [x] T003 [P] [US1] Tests: curve bounds, density correction, capacity factor 22–35 % (AC-004-01/-02)
- [x] T004 [US1] Implement `synthetic/wind.py` with vendored curves

## Phase 3: US2 plants (P2)
- [x] T005 [P] [US2] Tests: CHP seasonality/min load/heat, biogas CF, biomass ramp, hydro seasonality (AC-004-03…-06)
- [x] T006 [US2] Implement `synthetic/plants.py`

## Phase 4: US3 EV and site types (P2)
- [x] T007 [P] [US3] Tests: EV window, power and need (AC-004-07); site mix shares and valid events (AC-004-08)
- [x] T008 [US3] Implement `synthetic/ev.py`, site-type generators, generator measurements

## Phase 5: US4 cluster (P3)
- [x] T009 [US4] Compose `cluster` profile; integration test (AC-004-09)

## Phase 6: Polish
- [ ] T010 Docs (13, 05, 09, 00), `make ci`, integration tests
