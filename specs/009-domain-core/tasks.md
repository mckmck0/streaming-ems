# Tasks: Domain core (settlement, actions, Policy, offline reference)

Spec: [spec.md](spec.md) · Plan: [plan.md](plan.md). Plan and tasks approved 2026-11-13.
- Tests come first (tasks marked [test]); each implementation task turns its tests green.
- Section numbers (§) refer to the plan's Design.
- T01 runs on unchanged code (`e39b4be`, main after spec 008).
- Implementation details the plan left open, decided here:
  - zone schemes sit once per operator (not per version), because no 2026 version changes zone hours;
  - a month without export creates no tranche;
  - the RCEm signal uses the newest month known at q, at its latest publication known then; a publication
    counts as known from the day after its date (spec amendment of 2026-11-13);
  - the national components are fixed for the year except the quality fee, which has two periods
    (`quality-2026-01`, `quality-2026-02`);
  - a used-up tranche leaves the state; `initial_state` checks the start, the SoC bounds and the opening tranches
    once for both paths; `site_model` requires the run window.

## Phase 1: Baseline and facts first

- [x] T01 [test] [US1] (AC-009-09) Pin the household catalog on unchanged code: a digest of 100 generated
  households (catalog seed 7, global seed 4242, BESS share 0.5, no overrides) with `net_billing` dropped, in
  `tests/unit/ingest/test_narrow_assets.py`.
- [x] T02 [docs] (SC-002, P1) Write every resolved VERIFY item into `docs/context/12-domain-model.md` with value,
  URL and date (plan, "Resolved VERIFY items"), before any net-billing code: the RES-act articles, cohort by first
  feed-in, hourly balancing, the deposit (1.23, m+1 assignment, energy-only offset, 12 months, cap before 1.23),
  RCEm publication and corrections, the zero-floor assumption, the distribution-fee basis, clock basis, zone
  tables, versions, excise and national components.

## Phase 2: Failing tests

- [x] T03 [test] [US1–US4] (AC-009-22, -23) `tests/unit/domain/test_purity.py` (import allow-list, no
  `now()`/`today()`, the only ÷ 1000 in `units.py`) and the schema-name check in
  `tests/unit/contracts/test_topics_schemas.py`.
- [x] T04 [test] [US1] (AC-009-03) `tests/unit/domain/test_calendar.py`: the cases of the plan's test table,
  including the 92/100-slot civil days, 21:30 CEST Enea G12w, PGE seasons, 2026-09-30T22:00Z, a synthetic Friday →
  Saturday scheme and the holiday list against `holidays.Poland(years=2026)`.
- [x] T05 [test] [US1] (AC-009-01, -02) `tests/unit/tariffs/test_config.py` with broken fixtures under
  `tests/fixtures/tariffs/`: the real config loads; missing `ref`, missing `source`, unknown component, unknown
  seller zone, overlap, gap, a version not on the 1st and a wrong unit each fail naming file and field; Stoen, C11,
  missing `net_billing`, net-metering cohort, a non-EMS site, a 1-phase site and a window outside 2026 fail naming
  the site.
- [x] T06 [test] [US1] (AC-009-04 … -06) `tests/unit/domain/test_tariff_prices.py` with the golden table
  `tests/fixtures/tariffs/golden_2026.csv`, transcribed from the source tables separately from the config; fixed
  charges by band; the two-site fixed-charge check; the VAT scaling check.
- [x] T07 [test] [US1] (AC-009-07 … -09) `tests/unit/domain/test_netbilling.py`: the negative slot, the
  net-export hour with negative quarters, charge and export netting in one hour, the proxy, latest-published
  lookups around a correction, real mode with a missing month, and the cohort table with its rejections.
- [x] T08 [test] [US1, US2] (AC-009-10 … -14) `tests/unit/domain/test_settlement.py`,
  `tests/unit/domain/test_signal.py` and `tests/property/test_ledger.py`: row invariants, the AC-009-12 scenarios,
  the signal for RCE and RCEm sites, hour-aligned splits and the non-hour border.
- [x] T09 [test] [US3] (AC-009-15, -16) `tests/unit/domain/test_actions.py` and `tests/property/test_actions.py`:
  each kind, the grid bounds, level and setpoint validation, policy determinism and the observation fields.
- [x] T10 [test] [US4] (AC-009-13, -17 … -20) `tests/unit/domain/test_reference.py` and
  `tests/property/test_reference.py`: output shape and P14 metadata, the ten-day `run` == `step` window with two
  opening tranches and an RCEm variant, `settle_quarter` rejecting an out-of-order `t`, chunk independence with a
  month-end border, and energy conservation.
- [x] T11 [test] [US1] (AC-009-09) Catalog: every generated household has the default cohort and every plant has
  None; the test site loads its cohort; `G12w` parses; the plant digest drops `net_billing`.

## Phase 3: Implementation

- [x] T12 [US1] (AC-009-23) `src/ems/domain/units.py`; remove the unused
  `ems.contracts.units.price_per_mwh_to_per_kwh` (§1).
- [x] T13 [US1] (AC-009-03) `src/ems/domain/calendar.py` (§2). T04 green.
- [x] T14 [US1] (AC-009-04 … -06) `src/ems/domain/tariff.py` (§4).
- [x] T15 [US1] (AC-009-07 … -09) `src/ems/domain/netbilling.py` (§5). T07 green.
- [x] T16 [US1, US2] (AC-009-10 … -14) `src/ems/domain/settlement.py` (§6–8, `settle_quarter` of §10). T08 green.
- [x] T17 [US3] (AC-009-15, -16) `src/ems/domain/actions.py` (§9). T09 green.
- [x] T18 [US1] (AC-009-01, -02, -08) Config and loader (§3): `config/tariffs/2026/{national,enea,energa,pge,
  tauron}.yaml` and `config/tariffs/rcem.yaml`, each value with its page and a header citing its documents;
  `src/ems/tariffs/` (models, loader, RCEm table, `site_model`). T05 and T06 green.
- [x] T19 [US1] (AC-009-09) Catalog (§11): `TariffGroup.G12W`, `NetBilling`, `DEFAULT_NET_BILLING` on every
  generated household, the test site's cohort, the catalog export. T01 and T11 green.
- [x] T20 [US4] (AC-009-17 … -20) `national_pv_index` factored out of `price_curve` (event digests unchanged),
  `src/ems/domain/reference.py` and `src/ems/domain/stepper.py` (§10). T10 green.

## Phase 4: Docs and gates

- [x] T21 [docs] (SC-003, P15) `docs/context/12` (tariff and zone model, two layers, signal and its gaps, actions
  and `Policy`, "additive" limited to the ledger), `11` (cost metric), `09` (D1 resolved, D2–D14), `00` (status and
  backlog row 009, decisions), `13` (`G12w`, `net_billing`), `08` (observation points to 12).
- [x] T22 (SC-001) `make format`, `make ci` green; architect and energy-domain review of the diff; PR via `ems-pr`
  on the user's go. Done 2026-11-13: `make ci` green, both reviews folded in; risks D15 (excise on
  deposit-settled import) and D16 (hourly balancing) accepted for now as open VERIFY items with the current model;
  PR opened.
