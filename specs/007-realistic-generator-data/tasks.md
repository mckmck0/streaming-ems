# Tasks: Realistic generator statistics

Spec: [spec.md](spec.md) · Plan: [plan.md](plan.md).
- Tests come first and are written by `test-engineer` (T01–T06).
- T01 runs on unchanged code (`d3d9f30`).
- Section numbers (§) refer to the plan.

## Phase 1: Regression baseline (before any code change)

- [x] T01 [test] [US1–3] `tests/unit/ingest/test_narrow_assets.py`: add an id-only digest of the same 20
  households and day the event digests use.
  - SHA-256 over `event_id` in emission order.
  - Pinned and green on `d3d9f30`. It must still pass after T12 (AC-007-18).

## Phase 2: Failing tests for the new behaviour

- [x] T02 [test] [US2] `tests/property/test_forecast_errors.py`, one site, 60 days. The statistics use the total
  `kt_error` on daylight intervals.
  - AC-007-02: consecutive-run correlation ≥ 0.7 in every 3 h bin of the newer run's lead (3–45 h).
  - AC-007-03: kt sd at 42–48 h ≥ 1.1 × at 0–6 h; every 6 h bin within 0.17–0.30.
  - AC-007-04: temperature and wind meet AC-007-02, and their sd grows ≥ 1.3 ×.
  - AC-007-05:
    - |`phase_h`| ≤ δ_max;
    - sd at 42–48 h ≥ 2 × at 0–6 h;
    - in every run the warp `i0 + k + 4·δ_k` is non-decreasing and ≥ i0;
    - GHI = 0 where clear-sky GHI = 0.
  - AC-007-06, using hypothesis with the fixed CI profile:
    - a site's runs are bit-identical alone, in a batch of 4 and with `site_chunk` 1;
    - the runs issued in [a, c) equal those from [a, b) + [b, c) within the G1 tolerance;
    - the same seed gives the same values.
- [x] T03 [test] [US1, US4] `tests/unit/ingest/test_truth_source.py`:
  - AC-007-01: a stub `TruthSource` with constant arrays gives PV and heat pump equal to `pv_energy_kwh` and
    `heat_pump_kwh` applied to the stub.
  - AC-007-07: with mocked Open-Meteo (existing fixtures), the published forecast event equals `build_event` on
    the fetched payload.
  - AC-007-08: stub live events with GHI = 0.55·clear-sky over 30 days.
    - Truth GHI ≠ step GHI in every daylight interval.
    - Truth kt stays inside (0.03, 1.1).
    - The sd of to_x(step kt) − to_x(truth kt) is within ±20 % of `kt_sd(L)`.
    - Truth wind is the step × the log-normal factor at all three heights.
  - AC-007-09: no event, an event truncated before v, and v past the horizon each give the synthetic truth, and
    meters are still emitted.
  - AC-007-10:
    - `live_meters` for the same interval, event and seed gives identical values twice;
    - two live sites on different `issued_at` give the same values together as each alone.
- [x] T04 [test] [US3] `tests/unit/ingest/test_load_model.py`:
  - AC-007-11: one local year 2026; test site ±5 %; mean ratio of 50 households ±2 %.
  - AC-007-12: lag-1 of `log_noise` within ±0.03 of exp(−0.25/τ); the default within 0.75–0.92.
  - AC-007-13, via `impulses(…)`:
    - counts per kind within ±10 % of Σ_h rate·(A_h/A_ref)·days;
    - each impulse's energy per quarter-hour ≤ p·0.25 h;
    - G25 sites have none.
  - AC-007-14: hour-of-day means within ±10 % of H25.
  - AC-007-15: `daily_factor` is constant per local day.
    - The days have 92, 96 and 100 slots: 2026-03-28/29/30 and 2026-10-24/25/26.
    - It changes between 2026-03-28T22:45Z and 23:00Z.
    - It does not change between 2026-10-25T00:45Z and 01:00Z.
  - AC-007-16:
    - bit-identical alone, in a batch and under `site_chunk`;
    - impulse energy bit-identical for [a, c) and [a, b) + [b, c), including a spill across b;
    - AR noise within the G1 tolerance.
  - Guards: a temp `load.yaml` with s > 0.4, or with max q > 1 at 7,000 kWh, fails at parse.
- [x] T05 [test] [US3] `tests/unit/ingest/test_calibration.py`:
  - AC-007-17, CSV path: 20 households × 90 days from `base_load_kwh`, impulses on, known τ, σ_ar and σ_d,
    with a few linear runs inserted, written to a tmp CSV in the documented layout.
    - `fit_load` recovers τ ± 0.3 h, σ_ar ± 0.05 and σ_d ± 0.03, and reports the masked share.
    - The candidate holds the data window, household count, and dataset citation and licence.
  - AC-007-17, HDF5 path: `read_wpuq` on a tmp HDF5 with the WPuQ compound layout (`NO_PV/SFH1/HOUSEHOLD/table`,
    fields `index`, `P_TOT`) gives the same frame as the CSV.
  - No network.
- [x] T06 [test] Existing tests:
  - `tests/property/test_weather_pv.py`: the AC-001-21 threshold goes from 1.5 × to 1.1 ×.
  - `tests/unit/ingest/test_recording_hybrid.py`: AC-002-12 drops the `live_weather_override` import and asserts
    PV > 0 in daylight through the seam.

## Phase 3: Implementation

- [x] T07 [US2, US3] Config (§5):
  - `config/calibration/weather.yaml` `forecast`, all with source comments and VERIFY marks:
    - `kt_sd_base` 0.95 and `kt_sd_per_h` 0.002 (logit units);
    - `rho` 0.9, `rho_phase` 0.95;
    - `common_tau_h` 2.0, `step_correlation` 0.88;
    - `phase_sd_per_sqrt_h` 0.42, `phase_max_h` 6, `phase_knot_h` 12;
    - temperature 1.3 + 0.015·L;
    - `wind_ln_sd_*` 0.34 + 0.003·L.
  - New `config/calibration/load.yaml`:
    - A_ref 2,300 kWh;
    - kettle, oven and washing machine (p_kw, dur_min, rate_per_day);
    - H25 and G25 noise with initial VERIFY values;
    - `impulses` per profile.
  - Criteria: AC-007-02 … -05, -12, -13.
- [x] T08 [US2] `src/ems/ingest/synthetic/weather.py` (§2):
  - `forecast_errors`, `to_x`/`from_x`, phase knots and interpolation, the log-normal wind factor;
  - `ForecastRun.phase_h`, and a coverage check with a `ceil(4·δ_max)` margin;
  - in `generator.py`, `chunk()` extends the truth axis at its end only.
  - Criteria: AC-007-02 … -06.
- [x] T09 [US1, US4] Truth seam (§1):
  - New `src/ems/ingest/synthetic/truth.py`:
    - `TruthSource`, `SyntheticTruth`;
    - `LiveForecastTruth`: grouped by `issued_at`, `steps = FORECAST_STEPS`, the bounded kt space, additive
      temperature, the mirrored wind factor, fallback.
  - `SyntheticModel(truth_source=…)` and `chunk(…, truth_source=…)`, replacing `weather_override`; the slice
    restriction is removed.
  - `LiveRunner.live_meters` builds the source from a snapshot `dict(self.state.forecasts)`;
    `live_weather_override` is deleted.
  - Criteria: AC-007-01, -07 … -10.
- [x] T10 [US3] `src/ems/ingest/synthetic/load.py` (§3):
  - `daily_factor` on the int64 local day index;
  - `log_noise` (AR(1), with an assert that `t` is contiguous);
  - `impulses`: q with A/A_ref and the day count of the local year; spill window; fixed-order accumulation;
  - mean-preserving `base_load_kwh(…, params)`;
  - `load.yaml` parse with guards.
  - `generator.py` passes the parsed params.
  - Criteria: AC-007-11 … -16.
- [x] T11 [US3] Calibration (§4):
  - In `src/ems/ingest/calibration.py`:
    - `read_load_csv` and `read_wpuq` (h5py imported lazily; NO_PV houses; the authors' exclusions);
    - `fit_load`: mask interpolated runs, flag impulses, remove the household's own profile, AR-corrected σ_ar
      and σ_d, diagnostics;
    - `write_candidate` with the dataset block.
  - `jobs/calibrate.py load PATH… [--format csv|wpuq]`.
  - `pyproject.toml`: declare `h5py` in `ingest`; run `uv lock` (no upgrade).
  - Criteria: AC-007-17, P12.
- [x] T12 [US1–3] `tests/unit/ingest/test_narrow_assets.py`:
  - refresh `HOUSEHOLDS_DIGEST` and `PLANT_EVENTS_DIGEST` with a comment naming spec 007 (values change on
    purpose);
  - the T01 id digest and `PLANT_CATALOG_DIGEST` stay unchanged;
  - `uv run ems-gen schemas export` leaves `schemas/` unchanged.
  - Criteria: AC-007-18.

## Phase 4: Calibration on WPuQ (needs `data/external/wpuq/2019_data_15min.hdf5`)

- [x] T13 [US3] Resolve the WPuQ VERIFY items, then fit and adopt.
  - Check `index[0] == 1546300800` and `index[-1] == 1577835900`, which together mean the stamps label interval
    starts.
  - Check that the morning ramp moves 1 h earlier in UTC after 2019-03-31 and back after 2019-10-27. If it does
    not move, the stored "UTC" is local wall time.
  - Run `uv run python -I jobs/calibrate.py load data/external/wpuq/2019_data_15min.hdf5 --format wpuq`.
  - Review `config/calibration/candidates/load.yaml`. Copy τ_fast, the slow share, `interval_sd` and
    `daily_sd` into `load.yaml` for H25, with the full-load `calibration_targets`, a comment naming the run, the
    data window and the CC BY 4.0 attribution.
  - **If τ falls outside 1–2 h, stop and ask the user to revise AC-007-12.**
  - Re-run T04.
  - Criteria: AC-007-12, -17.

- [x] T16 [test] [US3] Revise the AC-007-12 and AC-007-17 tests for the two-timescale noise (test-engineer).
  - `noise_acf` matches the log-noise autocorrelation within ±0.03 at 15 min, 1 h and 2 h.
  - The full default H25 load (30 households, WPuQ-like energies 2,034–3,976 kWh, year 2019) measured with
    `household_stats` is within ±0.05 of `calibration_targets` in ACF and within ±0.03 in both sds.
  - The fit on generated households (noise and impulses) gives a fitted model whose full-load ACF is within ±0.03
    of theirs.
  - Criteria: AC-007-12, -17 (revised during implementation, 2026-11-10).
- [x] T17 [US3] Two-timescale noise and the joint full-load fit:
  - `log_noise` slow component and `noise_acf`;
  - `NoiseParams.tau_slow_h` / `slow_share` with parse checks;
  - `household_stats`, `model_stats`, `TAU_SLOW_H = 3.0`;
  - the fixed point on full-load statistics.
  - Criteria: AC-007-12, -17.

## Phase 5: Docs

- [x] T14 Docs (P15; SC-003):
  - `docs/context/13-subsystem-ingest.md`:
    - the forecast error model (formula, bounded kt space, phase knots, parameters, sources);
    - hybrid truth and the seam ("Router and hybrid" rewritten, including the jump at a run change);
    - the load model, the CSV calibration layout and the WPuQ attribution.
  - `docs/context/09-risks-and-tech-debt.md`: L5 narrowed; G1 extended; G9, G10 and G11 new.
  - `docs/context/00-overview.md`:
    - status and backlog row 007;
    - a "Historical backfill on real data" row before the experiment harness, with the spec's handoff facts;
    - the decisions: synthetic hybrid truth behind a seam, the shared error structure for temperature and wind,
      the WPuQ calibration, pvmonitor.pl not used without permission, the backfill split.
  - `README.md`: the calibration command, if listed. (Not listed, so the README is unchanged.)

## Phase 6: Verify

- [x] T15 Final checks (SC-001, SC-002; AC-007-18):
  - `make format`, then `make ci`.
  - `make up`, `make gen-backfill DAYS=1`, `make validate`.
  - `make bench`: the event count per simulated day equals the 2026-10-20 runs (174,562 for 1,000 sites).
  - Record the new arrays and events/s numbers in `11-experiments.md` and `13` beside the earlier runs.

## Coverage

| Criterion | Tasks |
|---|---|
| AC-007-01 | T03, T09 |
| AC-007-02 | T02, T07, T08 |
| AC-007-03 | T02, T06, T07, T08 |
| AC-007-04 | T02, T07, T08 |
| AC-007-05 | T02, T07, T08 |
| AC-007-06 | T02, T08 |
| AC-007-07 | T03, T09 |
| AC-007-08 | T03, T09 |
| AC-007-09 | T03, T09 |
| AC-007-10 | T03, T09 |
| AC-007-11 | T04, T10 |
| AC-007-12 | T04, T07, T10, T13, T16, T17 |
| AC-007-13 | T04, T07, T10 |
| AC-007-14 | T04, T10 |
| AC-007-15 | T04, T10 |
| AC-007-16 | T04, T10 |
| AC-007-17 | T05, T11, T13, T16, T17 |
| AC-007-18 | T01, T12, T15 |
| SC-001 | T12, T15 |
| SC-002 | T15 |
| SC-003 | T07, T14 |
