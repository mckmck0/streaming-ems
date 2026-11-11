# Feature Specification: Realistic generator statistics

**Feature Branch**: `007-realistic-generator-data` · **Created**: 2026-11-10 · **Status**: Approved 2026-11-10 (AC-007-03, -04, -08 and -13 revised with the plan, approved 2026-11-10)

**Input**: A review of the generator (2026-11) found the structure sound (contract v1, deterministic ids,
92/100-slot days, no simulation of EMS-managed batteries), but the statistics not yet fit for comparing
strategies. Findings 1–3 are in this spec. Finding 4 (historical backfill on real data) becomes its own spec.
User decisions 2026-11-10: hybrid truth is a synthetic deviation now, with satellite radiation in the
historical-backfill spec; the base-load calibration is fitted on WPuQ.

## Goal

Make the synthetic data honest enough that strategy and forecasting comparisons mean something. Only model
values change; wire contract v1 stays frozen (ADR 0016).

1. **Hybrid truth deviates from the live forecast (risk L5).** A live site's Open-Meteo forecasts stay exactly
   as fetched. The truth weather behind its synthetic meters becomes the latest live step minus a synthetic error
   from the forecast error model, so a forecast-driven strategy is no longer an oracle. Truth weather comes from
   one truth-source seam, so a later spec can plug in satellite or ERA5 data without touching the PV, heat-pump
   or load models.
2. **Synthetic forecast runs agree with each other.** The error at a valid time is a component shared by every
   run plus a small per-run component, scaled by a lead-dependent sd. A lead-dependent phase (timing) error acts
   on the clear-sky index. Runs agree in sign, and the timing error shrinks to zero as the lead shrinks. The
   amplitude error stays close to its short-lead size, as published NWP verification shows.
3. **Base load has persistence and appliance peaks.** Two-timescale noise in log space plus rare appliance impulses,
   mean-preserving, with the daily factor on the Europe/Warsaw delivery day, calibrated on measured household data.

### Models

**Forecast error** for variable x, valid time v and run r with lead L = v − issue(r):

    err_x(v, r) = σ_x(L) · [ ρ · Z_x(v) + √(1 − ρ²) · Y_x(r, v) ]

- `Z_x(v)`: unit-variance AR(1) over valid time, keyed by (seed, site, variable, v), shared by every run.
- `Y_x(r, v)`: unit-variance AR(1) along the steps of run r (today's per-run path), keyed by (seed, site,
  variable, run, step).
- `σ_x(L)` grows with lead (weakly for kt). In the error's own space, consecutive runs' errors for one valid time
  correlate at ρ².
- **Spaces:**
  - kt: the error is added in a bounded logit space, `x = logit((kt − 0.03) / 1.07)`, so kt stays inside
    (0.03, 1.1) without clipping and errors are smaller under clear or fully overcast skies;
  - temperature: additive;
  - wind speed: a mean-one log-normal factor.
- **Phase (kt only):** the forecast kt at v samples the truth kt at v + δ(v, r).
  - δ is drawn at knots (one at the issue time with δ = 0, then every 12 h of valid time), with
    sd σ_φ(L) = a·√L and the same shared-plus-per-run mix (ρ_φ).
  - It is clipped to [−min(L, δ_max), δ_max] and interpolated linearly. The time warp is therefore monotone and
    never reaches before the issue time.
  - Only cloudiness moves; the clear-sky GHI does not, so day and night stay in place.
- The parameters go in `config/calibration/weather.yaml`, with published sources cited (plan; anything
  unverified is marked VERIFY). kt must have this structure. The plan decides whether temperature and wind get it too.

**Hybrid truth:** truth_x(v) = F_x(v, r*) − err_x(v, r*) in the same spaces, where r* is the live run that is
latest when v's meters are computed. Amplitude only, no phase. GHI = truth kt × clear-sky GHI, and DNI and DHI
are re-split (Erbs) as for the synthetic truth.

**Base load** per site, quarter-hour t, local delivery day d(t):

    E(t) = A · (1 − s) · shape(t) · D(d) · exp(σ_ar · X(t) − σ_ar² / 2) + I(t)

- A is the annual energy. `shape` is the BDEW share (sums to 1 per local year). D(d) is a mean-one lognormal
  draw per site and local day.
- X = √(1 − w)·X_fast + √w·X_slow, both unit-variance AR(1) components: τ_fast is fitted, and τ_slow = 3 h is
  fixed (spec revision during implementation, AC-007-12).
- I(t) is the energy of appliance impulses that overlap t (H25 households only). Each impulse is (power,
  duration) at a uniform start within its quarter-hour, and the probability that one starts in a quarter-hour is
  proportional to shape(t). s = expected annual impulse energy / A, so the expected annual energy stays A.
  G25 sites (commercial, solar-farm auxiliary load) have s = 0 and I = 0.
- The parameters go in a new `config/calibration/load.yaml`.

## Scope

In scope:
- A truth-source seam in `src/ems/ingest/` that replaces `weather_override`. The synthetic source is today's
  `generate_truth`. The live-forecast source applies the deviation and falls back to synthetic truth.
- The forecast error model in `synthetic/weather.py` (`forecast_run`) and its parameters in `weather.yaml`.
- Hybrid live-site meters (`sources/live_runner.py`) going through the seam.
- The base-load model in `synthetic/load.py`, with the daily factor on the local delivery day via `localtime.py`.
- A load fit in `ingest/calibration.py` and `jobs/calibrate.py load` over WPuQ files the user downloads. It
  writes a candidate file with the CC BY 4.0 attribution.
- Event digests in `tests/unit/ingest/test_narrow_assets.py` refreshed once: values change on purpose, while the
  catalog digest and every event id stay the same.
- `make bench` rerun and recorded (P16).
- Docs: `13-subsystem-ingest.md` (forecast error model, load model, hybrid truth), `09` (L5 narrowed, new risks),
  `00-overview.md` (backlog rows, decisions), `11-experiments.md` (benchmark), and comments with sources in
  `weather.yaml` and `load.yaml`.

Out of scope:
- The historical backfill (PSE history, archived Open-Meteo runs, satellite and ERA5 truth): the
  historical-backfill spec (handoff below).
- Prices model, tariffs G11/G12/G12w, distribution fees, RCE/RCEm (domain-core spec).
- Anything in Restate; new site types or asset kinds; contract changes; publishing truth weather
  (`ems.weather.actual.v1` stays disabled).
- Phase errors in the hybrid (reverse) direction; per-site appliance ownership; changes to the PV, heat-pump, EV
  and battery models (the heat pump changes only through the truth temperature).

## Affected

- For a given seed, every synthetic forecast value and every base-load value changes; event ids do not (identity
  has no values). In hybrid mode, live sites' meter values change; live forecast events do not.
- No topic consumer exists yet. Topics filled by earlier runs with the same seed and no `run_id` hold events with
  the same ids but old values: run `make reset` or use a new `run_id`.
- The calibration job gains a `load` subcommand.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Honest forecasts in hybrid mode (Priority: P1)
A forecast-driven strategy at a live site sees the real Open-Meteo forecast. The meters it is settled on deviate
from that forecast by realistic errors. **Acceptance**: AC-007-01, -07, -08, -09, -10.

### User Story 2 - Consistent synthetic forecast runs (Priority: P1)
Consecutive synthetic runs agree for the same valid time and carry timing errors that shrink with lead, so
re-planning is worth what it is worth in reality. **Acceptance**: AC-007-02 … -06.

### User Story 3 - Persistent, peaky base load (Priority: P1)
Base load persists over hours (so a persistence forecast has skill) and has appliance peaks. It keeps its annual
energy, follows local days and is calibrated on measured households. **Acceptance**: AC-007-11 … -17.

### User Story 4 - Ready for real truth (Priority: P2)
The historical-backfill spec adds satellite or ERA5 truth by adding a truth source and nothing else.
**Acceptance**: AC-007-01.

### Edge Cases
- Night or clear-sky GHI 0: kt is undefined, so no deviation applies and truth and forecast GHI are 0.
- Low sun: the live kt (GHI ÷ our clear-sky GHI) can exceed 1.1. It is clipped into (0.03, 1.1) before the
  transform.
- A truncated live run (fewer than 192 steps): intervals past its last step use the fallback.
- ICON-D2 hourly runs after the 2026-11-30 cutover (L1): the lead comes from the step's `issued_at`, so a newer
  run gives a smaller deviation.
- A phase shift never samples the truth before its run's issue time (δ ≥ −L) and never past the truth window,
  which is extended by δ_max at its end (plan, section 2).
- An impulse that starts before a window and spills into it is still counted (window independence).
- 2026-03-29 (92 slots) and 2026-10-25 (100 slots): one daily factor per local day.

## Requirements *(mandatory)*

- **FR-001**: Truth weather MUST come from one truth source per site; the asset models MUST read weather only from
  it. → AC-007-01
- **FR-002**: Synthetic forecast errors MUST combine a component shared by valid time with a per-run component,
  scaled by a lead-dependent sd. → AC-007-02, -03, -04
- **FR-003**: The forecast kt MUST carry a bounded, lead-dependent phase error. → AC-007-05
- **FR-004**: Every new component MUST be a pure function of (seed, site, variable, t) and keep batch, chunk and
  window independence. → AC-007-06, -16
- **FR-005**: Live forecasts MUST stay as fetched; live-site truth MUST deviate from them, with a synthetic
  fallback. → AC-007-07 … -10
- **FR-006**: Base load MUST have two-timescale log noise and household appliance impulses and keep the annual energy and
  profile shape. → AC-007-11 … -14
- **FR-007**: The daily load factor MUST follow the Europe/Warsaw delivery day. → AC-007-15
- **FR-008**: The load parameters MUST be fittable from WPuQ by the calibration job. → AC-007-17
- **FR-009**: Wire contract v1 and event identity MUST NOT change. → AC-007-18

### Acceptance criteria (EARS)

Truth seam
- **AC-007-01**: THE SYSTEM SHALL take every site's truth weather for a chunk (kt, GHI, DNI, DHI, temperature,
  wind) from exactly one truth source chosen per site, and the PV, heat-pump and base-load models SHALL read
  weather only from that truth. (Test: a stub source with constant weather gives PV and heat-pump energy equal to
  those models applied to the stub's arrays.)

Synthetic forecast errors (finding 2)
- **AC-007-02**: WHEN synthetic forecast runs are generated for one site over at least 60 days, THE SYSTEM SHALL
  give the kt errors of consecutive runs for the same valid time a Pearson correlation of at least 0.7 in every
  3-hour bin of the newer run's lead from 3 h to 45 h.
- **AC-007-03**: WHEN the same runs are analysed, THE SYSTEM SHALL make the sd of the kt error at leads
  42–48 h at least 1.1 times its sd at leads 0–6 h. The sd SHALL lie within 0.17–0.30 in every 6-hour lead bin.
  (Revised in the plan phase: published NWP irradiance errors grow only weakly with lead; see plan, section 5.)
- **AC-007-04**: WHERE temperature or wind carries the shared-plus-per-run structure (decided in the plan), THE
  SYSTEM SHALL meet AC-007-02 for that variable, and its error sd at leads 42–48 h SHALL be at least 1.3 times
  its sd at leads 0–6 h.
- **AC-007-05**: THE SYSTEM SHALL shift the forecast kt in time by a phase error δ with |δ| ≤ δ_max at every step
  and an sd at leads 42–48 h at least twice the sd at leads 0–6 h, and SHALL keep forecast GHI at 0 wherever the
  clear-sky GHI is 0.
- **AC-007-06**: THE SYSTEM SHALL make every forecast error component a pure function of (seed, site, variable,
  valid time or run and step), so that:
  - a site's runs are bit-identical when generated alone, in any batch or under any `scale.site_chunk`;
  - the runs issued in [a, c) equal those from [a, b) followed by [b, c) within the burn-in residual accepted in
    risk G1;
  - the same seed reproduces every value.

Hybrid truth (finding 1)
- **AC-007-07**: WHILE hybrid mode runs, THE SYSTEM SHALL publish each live site's Open-Meteo forecast event
  unchanged (step values, `issued_at`, `source_system = open-meteo`, `event_id`), as built from the fetched payload.
- **AC-007-08**: WHEN a live site's meters for interval v are computed and its latest live run r* has a step for
  v, THE SYSTEM SHALL derive the truth weather as that step minus err(v, r*) from the error model (kt,
  temperature and wind; DNI and DHI re-split from the truth GHI). The truth GHI SHALL then differ from the step's
  GHI in every daylight interval, and the truth kt SHALL stay inside (0.03, 1.1). Over at least 30 days of
  mid-range steps, the sd of x(step kt) − x(truth kt) SHALL be within ±20 % of σ_kt at that lead, in logit units.
  (Revised in the plan phase: kt errors act in the bounded logit space.)
- **AC-007-09**: IF a live site has no live step for v (no run yet, a run truncated before v, or v beyond the
  latest run's horizon), THEN THE SYSTEM SHALL use the site's synthetic truth for v and still emit its meters.
- **AC-007-10**: WHEN a live site's meters for v are computed again from the same forecast event and seed, THE
  SYSTEM SHALL produce identical values.

Base load (finding 3)
- **AC-007-11**: WHEN one local year is generated, THE SYSTEM SHALL keep each site's base-load energy within
  ±5 % of its configured annual energy, and the mean ratio over at least 50 households within ±2 %.
- **AC-007-12**: THE SYSTEM SHALL generate the interval noise of the base load in log space as the sum of a fast
  and a slow unit AR(1) (time constants τ_fast and τ_slow = 3 h, fixed; the slow one with variance share w).
  - Over a year, the autocorrelation of the log noise at 15 min, 1 h and 2 h SHALL lie within ±0.03 of
    (1 − w)·φ_fast^L + w·φ_slow^L for the configured parameters.
  - WHEN the full default base load (noise and impulses) of H25 households with WPuQ-like annual energies is
    measured with the calibration estimator over a year, its autocorrelation at 15 min, 30 min, 1 h and 2 h SHALL
    lie within ±0.05 of the WPuQ values recorded in `load.yaml`, and its within-day and day-to-day sd within ±0.03.
  - (Revised during implementation, 2026-11-10: the WPuQ data are not AR(1); at τ ≈ 0.6 h an AR(1) matches
    only the 15-minute value. The user chose the model closest to the data.)
- **AC-007-13**: WHERE a site uses the H25 household profile, THE SYSTEM SHALL add appliance impulses of each
  configured kind (at least kettle, oven and washing machine), with power, duration and daily rate taken from
  `load.yaml`. The rate is for a reference household and scales with the site's annual energy.
  - Each impulse's energy SHALL be spread over the quarter-hours it overlaps, at most power × 0.25 h per impulse
    and quarter-hour.
  - Over a year and at least 50 households, the count per kind SHALL be within ±10 % of its expected count, the
    sum over households of rate × (A / A_ref) × days.
  - Sites on G25 SHALL get no impulses.
- **AC-007-14**: THE SYSTEM SHALL keep the expected profile shape: over a year and at least 50 households, the
  mean base load per local hour of day SHALL lie within ±10 % of the BDEW H25 value for that hour, scaled to the
  same annual energy.
- **AC-007-15**: THE SYSTEM SHALL draw the daily load factor once per site and Europe/Warsaw delivery day. It
  SHALL be constant over each local day (92, 96 or 100 quarter-hours) and change only at local midnight,
  including 2026-03-29 and 2026-10-25.
- **AC-007-16**: THE SYSTEM SHALL keep the base load a pure function of (seed, site, variable, t):
  - bit-identical when generated alone, in any batch or under any `scale.site_chunk`;
  - the impulse energy bit-identical for [a, c) and for [a, b) followed by [b, c), including impulses that start
    before a window;
  - the AR noise within the G1 residual.
- **AC-007-17**: WHEN `jobs/calibrate.py load` runs on per-household 15-minute base-load energy (WPuQ files, or
  a CSV in the layout documented in 13, which the test fixture uses), THE SYSTEM SHALL write
  `config/calibration/candidates/load.yaml` with:
  - the fitted τ_fast, slow share, σ_ar and daily sd, with τ_slow fixed;
  - the full-load statistics of the data and of the fitted model;
  - the peak-quarter-hour rate, the data window and the household count;
  - the dataset citation and licence.

  It SHALL use no network. On households generated by the model (noise and impulses), the fitted model's full-load
  autocorrelation SHALL match theirs within ±0.03 at 15 min, 30 min, 1 h and 2 h.

Contract
- **AC-007-18**: THE SYSTEM SHALL leave wire contract v1.0.0 unchanged (schema export equals `schemas/`, the
  baseline check passes) and keep the `event_id`s of a backfill day for a given seed and run as they were before
  this spec (an id-only digest pinned from `main`).

## Constraints (from the checklists)

- **P1**: spec, plan and tasks are each approved before code.
- **P2**: `src/ems/domain/` is untouched. The ingest models stay pure, with no I/O and no clock.
- **P3**: `ems-gen` stays the only writer of external inputs. Truth weather is internal and is not published.
- **P4–P6, P8, P13**: no actors, handlers or tracing are touched.
- **P7**: deviations are keyed by `t`, never by wall time. Hybrid pacing is unchanged.
- **P9**: no contract change (AC-007-18). If one turns out to be needed, work stops and the user is asked.
- **P10**: the load fit is a CLI over local files (`jobs/calibrate.py`).
- **P11**: no new settings; the WPuQ path is a CLI argument.
- **P12**: if an HDF5 reader is needed for WPuQ, it is pinned in `uv.lock` under `exclude-newer` (plan decides).
- **P14, P16**: no energy result exists yet. `make bench` is rerun because the array-generation time is a reported number.
- **P15**: docs land with the change. No ADR: decisions go to `00-overview.md` "Decisions so far".
- **Risks** (09):
  - L5 is narrowed: the oracle effect is removed, but the hybrid deviations are synthetic.
  - G1 extends to Z, Y, X and the phase process.
  - G2, S1 and G6 are unchanged.
  - New:
    - load calibration on German single-family houses with heat pumps, not Polish data;
    - constant ρ (the correlation does not decay with run separation) and phase on kt only;
    - same ids with new values for an unchanged seed and `run_id`.

## Success Criteria

- **SC-001**: `make ci` is green, every AC has at least one new unit or property test, and the refreshed digests
  carry a comment naming spec 007.
- **SC-002**: `make bench` is rerun and its numbers recorded in 11 and 13 beside the 2026-10-20 runs. Event counts
  per simulated day are unchanged.
- **SC-003** (review):
  - 13 documents the error model, the load model and hybrid truth.
  - 09 narrows L5 and adds the new risks.
  - 00 has backlog rows for 007 and the historical-backfill spec, plus the decisions.
  - `weather.yaml` and `load.yaml` cite sources, with VERIFY where a value is unchecked.

## Assumptions

- Published NWP and PV-forecast verification (lead growth, run-to-run consistency, timing errors) is
  representative of ICON-D2-class forecasts for Poland (sources and VERIFY marks in the plan).
- WPuQ households (Lower Saxony, 2018–2020, all with heat pumps) are an acceptable proxy for the dynamics of
  Polish base load. Annual energy and the BDEW shape still come from the catalog.
- No open Polish quarter-hour household dataset is known. pvmonitor.pl (a Polish community portal for PV,
  consumption and heat-pump installations) shows public dashboards, but it has no API or export, its site is
  "All Rights Reserved", and its privacy policy gives no reuse terms (checked 2026-11-10). It can be used only
  with the operator's written permission. An extract obtained that way would be fitted by the same job through
  the CSV layout, and Polish data then validates or replaces the WPuQ fit.
- Impulses model energy per quarter-hour, not instantaneous power: a 2 kW kettle running for 3 min adds 0.1 kWh
  to its quarter-hour.

## Open questions (resolved in the plan unless marked)

- Do temperature and wind get the shared-plus-per-run structure? (Leaning: yes, the same function, no phase.)
- Defaults and sources for σ_x(L), ρ, Z's time constant, σ_φ, δ_max and the appliance catalogue (VERIFY where unchecked).
- WPuQ file format (HDF5) and reader: a pinned `h5py` or `tables`, or a documented CSV export (P12).
- Resolved 2026-11-10: the WPuQ fit put a single-AR(1) τ at about 0.6 h. AC-007-12 was revised to the
  two-timescale model the user chose.
- For the user: where the historical-backfill spec sits in the backlog (proposed: before the experiment harness).

## Handoff to the historical-backfill spec (verified 2026-11-10 against open-meteo.com docs)

- **Single Runs API** (`https://single-runs-api.open-meteo.com/v1/forecast`, `run=YYYY-MM-DDTHH:MM`) serves
  archived full runs by initialisation time from 2026-04-02 for most models (ICON-D2 and DMI HARMONIE are
  listed), 15-minutely in Central Europe. Forecast events can therefore be runs of record built by the existing
  `build_event`, not reconstructions. VERIFY free-tier access, rate limits and the licence.
- **Previous Runs API**: hourly `_previous_dayN` values (N = 1–7, offset from valid time), archived from January
  2024. These are not full runs, so they are only a fallback for windows before 2026-04-02, and such events would
  need their own labelling.
- **Satellite Radiation API** (`https://satellite-api.open-meteo.com/v1/archive`,
  `models=satellite_radiation_seamless`, `temporal_resolution=native`): DWD MTG GHI, DNI and DHI every 10 min,
  about 20 min behind real time, archived since February 2026. A probe on 2026-11-10 returned 10-minute W/m²
  values without a key. It plugs in as a truth source and can also serve hybrid GHI.
- Temperature and wind truth: ERA5 (Historical Weather API) for the window.
- Suggested window: 2026-04-02 … 2026-06-30 (spring; 13.5 % negative RCE quarter-hours in April, per 12).
- Household meters stay synthetic: PV from real weather through the existing PV model, load synthetic.
