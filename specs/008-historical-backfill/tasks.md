# Tasks: Historical backfill on real data

Spec: [spec.md](spec.md) · Plan: [plan.md](plan.md).
- Tests come first and are written by `test-engineer` (tasks marked [test]).
- T01 runs on unchanged code (`48fafee`).
- Section numbers (§) refer to the plan.
- The fetch path comes first because ICON-D2 archives may vanish after 2026-11-30 (L1). Its tests (T02, T03)
  still precede its code (T04, T05).

## Phase 1: Regression baseline (before any code change)

- [x] T01 [test] [US2] Pin today's behaviour on `48fafee`:
  - `tests/unit/ingest/test_narrow_assets.py`: order-independent digests of the same 20 households and day,
    computed over rows and over ids sorted by `event_id`. They must stay green after T15, when the emission-order
    digests change (§4, "Existing tests").
  - `tests/unit/ingest/test_openmeteo.py`: the event JSON (minus `ingested_at`) that `build_event` gives for
    `forecast_PL-LOC-000001.json`, pinned. Live events must stay byte-identical after T16 (AC-008-05, §3).

## Phase 2: Fetch (deadline path)

- [x] T02 [test] [US1, US2] Fixtures and request tests:
  - Capture real fixtures into `tests/fixtures/openmeteo/`: one archived ICON-D2 run and one archived DMI run
    (`forecast_minutely_15=193`, Poznań, 2026-05-15T03), and a two-day ERA5 response. Extend the NOTICE with the
    Copernicus attribution and DOI (plan, "Fixtures").
  - `tests/unit/ingest/test_openmeteo.py`: `single_run_params` has `run`, `forecast_minutely_15=193` and no
    `start_minutely_15`; the customer host and header with a key (AC-008-01).
- [x] T03 [test] [US1] `tests/unit/ingest/test_history_fetch.py` (respx, a fake clock, an injected "today"):
  - AC-008-01: the layout of §1; meta files with SHA-256; manifest fields including grid cells and the
    `generator.yaml` digest; no secrets; no `.tmp` left behind.
  - AC-008-02: each refusal sends no request and exits 2. The refusals are:
    - a window before 2026-04-02, too recent, not whole days, or empty;
    - an unknown site, or a site-controlled battery at a real-data site;
    - an estimate above 9,000 calls;
    - ERA5 missing the hour at `end`;
    - a 401 from a customer host (no retry);
    - a rerun with a different window, site set or config.
  - AC-008-03: a second fetch sends nothing. A "run not available" 400 writes a marker and is retried next time;
    another 400 aborts. `complete` is set only when every request is accounted for.
  - AC-008-04: Open-Meteo requests ≥ 0.8 s apart; PSE requests ≥ 1 s apart; a final 429 → manifest written,
    exit 4, and a rerun resumes.
  - `config/generator.history.yaml`: both real-data sites resolve in the catalog (PL-LOC-000004 east of 20.34° E,
    DMI).
- [x] T04 [US1] (AC-008-01, -04) Small splits in existing code (§1, §2, §5):
  - `sources/recording.py`: `write_raw(root, fetched, secrets, name=None)`, atomic, with `sha256` in the meta.
    `Recorder.raw` calls it, so spec 002 recordings keep their layout.
  - `live/pse.py`: `fetch_pages(day)` split out of `fetch_rows`.
  - `live/openmeteo.py`: `single_run_params` and the Single Runs hosts (free and `customer-`).
- [x] T05 [US1] (AC-008-01 … -04) `src/ems/ingest/history/fetch.py` and `ems-gen history fetch` (§2, §9):
  - checks, requests in order, the `Pacer`, missing-run markers, resume;
  - ERA5 completeness;
  - the manifest derived from the raw folder;
  - exit codes 0, 2 and 4.

  Also `config/generator.history.yaml` (§9) and `make history` (fetch only until T18). T02 and T03 go green.
- [x] T06 [US1] (AC-008-01, SC-002) Run the real fetch for both sites:
  `make history NAME=spring-2026 START=2026-04-02 END=2026-07-01`.
  - Target 2026-11-20. If T05 is not done by 2026-11-25, run the code as it stands. As a last resort, a throwaway
    script writes the raw files in the layout of §1.
  - Check `complete: true` and record the counts, missing runs and durations for T23.
- [x] T07 [US2] (AC-008-08) Start the 7-day `meta.json` sample for both models on the free host (a throwaway
  script outside the repo, every 5 min, read-only; §4).
  - When it ends, compare each delay + 10 min with the table.
  - **If any exceeds 105 (ICON-D2) or 195 (DMI) min, stop and ask the user** (AC-008-08 values are approved).
  - Results go into 13 (T23).
  - Outcome 2026-11-13: the ICON-D2 12 UTC run took 106.7 min (> 95). The user raised ICON-D2 and synthetic to
    120 min and ended the sample early; T15's digests were refreshed again and `spring-2026` rebuilt.

## Phase 3: Failing tests for build, truth and replay

- [x] T08 [test] [US2] `tests/unit/contracts/test_availability.py` (AC-008-08):
  - the table values; each ≥ `MEASURED_MAX_MIN` + 10 min and a multiple of 15 min; an unknown model raises;
  - synthetic forecasts are emitted at `issued_at` + 120 min (105 until the T07 outcome);
  - `tests/unit/ingest/test_generator.py:59` changes from +90 min to `forecast_delay("synthetic")`.
- [x] T09 [test] [US2] Builders:
  - `tests/unit/ingest/test_openmeteo.py` (AC-008-05, -11), on the T02 fixtures:
    - archived ICON-D2 → 191 steps from +15 min;
    - archived DMI → first step ending at +45 min;
    - more than four leading gaps → `None`;
    - `source_system` and `source_record_ref` as given;
    - the T01 pin still equal.
  - `tests/unit/ingest/test_pse.py` (AC-008-09, -12): a fixture with two publications for some slots → the latest
    row per slot wins and `issued_at` is the latest; with one row per slot the event is unchanged.
- [x] T10 [test] [US3] `tests/unit/ingest/test_observed_truth.py`, on hand-written series:
  - AC-008-13: label positions; weights 2/3 and 1/3; a null 10-minute slot with zero clear-sky → 0, also in a
    sunrise quarter.
  - AC-008-14: ERA5 interpolation at interval ends, including the last interval.
  - AC-008-15: a constant `ObservedTruth` gives PV and heat pump equal to `pv_energy_kwh` and `heat_pump_kwh` on its
    arrays.
  - AC-008-16:
    - a daylight gap → ERA5 fill with an Erbs split;
    - at sunrise, the ERA5 label is `floor_hour(s) + 1 h` (a one-hour shift changes the result);
    - GHI present but DNI missing → Erbs;
    - a DNI that fails `check_split` → Erbs, counted;
    - ERA5 missing too → the fallback;
    - masks; no NaN in any output; a non-hour-aligned axis raises.
- [x] T11 [test] [US2, US3] `tests/unit/ingest/test_history_build.py`, with a helper that writes a raw recording in
  the API formats for any window:
  - Fixtures: satellite bodies are hand-written in code (`tests/unit/ingest/history_recording.py`; no real
    satellite data, L6) instead of a `tests/fixtures/satellite/` folder; `tests/fixtures/pse/history_rce-pln_2026-10-25.json`
    (real, L3; named outside the `rce-pln_20*.json` glob of the AC-002-13 price fit).
  - AC-008-05: event kinds and counts; a malformed run is counted.
  - AC-008-06:
    - two builds are byte-identical;
    - `build.json` holds the hash, digests and raw-set digest;
    - `ingested_at` equals the meta `fetched_at`;
    - an incomplete fetch, a changed raw body, or the same `run_id` with a changed digest is refused.
  - AC-008-07: forecast `issued_at` = the `run` of its raw file; price `issued_at` = the latest raw
    `publication_ts_utc`.
  - AC-008-09: day selection (the initial day sorts to `start`); flagged and incomplete days counted; a flagged
    day published after the next one sorts after it.
  - AC-008-10: window 2026-10-24 … 10-27: 100 slots; meters for every UTC quarter-hour; `build.json` lists 10-25
    and 10-26 as the complete local days.
  - AC-008-11: `source_system` values; non-null `run_id`; no `weather.actual` event; schema export unchanged.
  - AC-008-12: one PSE fixture through `PseSource.poll` and through build → equal events.
  - AC-008-16: `estimated` and `quality_raw` on PV, the heat pump and (non-managed sites) grid, only in filled
    intervals; the CLI prints the shares.
- [x] T12 [test] [US2] `tests/property/test_history_lookahead.py` (AC-008-07). Hypothesis with the fixed profile
  draws whole-day windows, gap positions and D−1 publication times. Checks:
  - availability − `issued_at` ≥ the stream's delay;
  - the file is sorted by `emission_key`;
  - per topic and key, `interval_start` never decreases;
  - `record_timestamp` follows the 06 rule.
- [x] T13 [test] [US4] `tests/unit/ingest/test_replay.py`:
  - AC-008-17: a stubbed admin with live-profile configs → `PreflightError`, nothing published; exit 1 through
    the CLI.
  - AC-008-18: `--background` into a list sink with `generator.history.yaml`:
    - a non-empty fleet, with every other site present;
    - no synthetic event for a real-data site, and no synthetic price;
    - merged emission order;
    - seed and `run_id` from `build.json`;
    - a run manifest written;
    - a changed calibration digest is refused.
  - History checks: a changed `events.jsonl` → refused; `--speed` → refused.
  - `tests/unit/ingest/test_recording_hybrid.py:131` passes `check_topics=False`.
- [x] T14 [test] [US4] `tests/integration/test_history_replay.py`:
  - AC-008-17: replay into `live`-profile topics → exit 1 and empty topics.
  - AC-008-19: a fixture recording replayed into `sim` topics → `validate_records` clean, and the topic ids equal
    the recording's.

## Phase 4: Implementation

- [x] T15 [US2] (AC-008-08, -11) Availability (§4, §3):
  - `src/ems/contracts/availability.py`;
  - `generator.emissions` uses `forecast_delay("synthetic")`;
  - `forecast.availability_delay_min` removed from `weather.yaml`, with a pointer comment;
  - `contracts.examples.forecast_event(first_step=0)`.

  Then refresh the emission-order digests in `test_narrow_assets.py` (comment: spec 008, forecasts at +120 min);
  T01's order-independent digests must stay equal. T08 goes green.
- [x] T16 [US2] (AC-008-05, -09, -12) Builders (§3, §5):
  - `build_event`: drop rows at or before the run time; skip up to four leading gaps; `source_system` parameter;
    today's `source_record_ref` format.
  - `build_day_event`: the latest row per slot.

  T09 and T01's live pin go green.
- [x] T17 [US3] (AC-008-13 … -16) `src/ems/ingest/history/observed.py` (§6): `quarter_means`,
  `instants_at_end`, `era5_fill_ghi`, `check_split`, `ObservedTruth`, `masks` and the diagnostics. No asset model
  is edited. T10 goes green.
- [x] T18 [US2, US3] (AC-008-05 … -12, -16) `src/ems/ingest/history/build.py` and `ems-gen history build` (§7):
  - a `meter_source` parameter through `emissions` and `_meter_builders`;
  - the quality helper with `quality_raw`;
  - `emission_key`;
  - atomic `events.jsonl`, then `build.json` with complete local days and diagnostics;
  - `--seed` and `--run-id`, with the G11 refusal.

  `make history` runs fetch, then build. T11 and T12 go green.
- [x] T19 [US4] (AC-008-17 … -19) Replay (§8):
  - `preflight_topics` split out of `preflight`, used by `run` and `replay`;
  - history hash and raw-set checks; `--speed` refused for history recordings;
  - `--background` with the digest check, `heapq.merge` on `emission_key`, and a run manifest;
  - CLI flags; `make replay … BACKGROUND=1`.

  T13 and T14 go green.
- [x] T20 [US4] (AC-008-19, SC-002) Build the real recording from T06's raw data
  (`ems-gen history build --name spring-2026`).
  - Replay it into `sim` topics with and without `--background`; `make validate`.
  - Record the counts, gap shares, diagnostics, complete local days, durations and the `events.jsonl` hash for
    T23 (SC-002).

## Phase 5: Forecast error (P2)

- [x] T21 [test] [US5] `tests/unit/ingest/test_calibration.py` (AC-008-20). Input: synthetic truth plus
  `forecast_run`, 60 days, two sites.
  - Temperature and wind sd within ±10 % of `weather.yaml`'s sd(L) per bin.
  - Consecutive-run correlation within ±0.05 of ρ².
  - The candidate holds the recording, hash, window, sites, attribution, the hourly-aggregate kt error and the
    upper-bound caveat.
  - `weather.yaml` is untouched.
- [x] T22 [US5] (AC-008-20) `fit_forecast_error` in `ingest/calibration.py` and
  `jobs/calibrate.py forecast-error` (§10).
  - Run it on `spring-2026` and leave the candidate for review (not adopted).
  - T21 goes green.

## Phase 6: Docs and gates

- [x] T23 (SC-002, SC-003) Docs (plan "Docs and ADRs"):
  - `00`: decisions, status row, backlog row 008 moved before Kafka ingestion.
  - `06`, `08` (availability rule per model, payload wording, `source_system` values, run scope, the
    consumer-side open question).
  - `09`: K1, L1, L2, L3, L5, G10, G11, new L6–L9.
  - `11`: the real-data window, its use and limits, the SC-002 numbers.
  - `12`: the cohort wording and the net-billing facts with sources, marked for re-check.
  - `13`: history, observed truth, quality marking, sources and attribution, resolved VERIFY items with dates, the
    T07 sample, determinism scope, the live leading-gap edge case, the SC-002 numbers.
  - README (lines 19, 35, 308, 334; the history section and attribution); `CLAUDE.md` commands table;
    `.claude/rules/ingest.md`; the generator docstring.
- [x] T24 (SC-001) Gates (2026-11-13: `make format`, `make ci` (370 passed) and `make test-integration` (13 passed)
  green; code checked against spec, plan and tasks; the PR waits for the user's go):
  - `make format`, `make ci`, then `make up` and `make test-integration`.
  - Check the code against spec, plan and tasks, and list anything unbuilt.
  - Then `ems-pr`.

## Coverage

| Criterion | Tasks |
|---|---|
| AC-008-01 | T02, T03, T04, T05, T06 |
| AC-008-02 | T03, T05 |
| AC-008-03 | T03, T05 |
| AC-008-04 | T03, T04, T05 |
| AC-008-05 | T01, T02, T09, T11, T16, T18 |
| AC-008-06 | T11, T18 |
| AC-008-07 | T11, T12, T18 |
| AC-008-08 | T07, T08, T15 |
| AC-008-09 | T09, T11, T16, T18 |
| AC-008-10 | T11, T18 |
| AC-008-11 | T01, T09, T11, T15, T18 |
| AC-008-12 | T09, T11, T16, T18 |
| AC-008-13 | T10, T17 |
| AC-008-14 | T10, T17 |
| AC-008-15 | T10, T17 |
| AC-008-16 | T10, T11, T17, T18 |
| AC-008-17 | T13, T14, T19 |
| AC-008-18 | T13, T19 |
| AC-008-19 | T14, T19, T20 |
| AC-008-20 | T21, T22 |
| SC-001 | T24 |
| SC-002 | T06, T20, T23 |
| SC-003 | T23 |
