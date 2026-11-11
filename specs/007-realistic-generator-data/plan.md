# Implementation Plan: Realistic generator statistics

**Branch**: `007-realistic-generator-data` | **Date**: 2026-11-10 | **Spec**: [spec.md](spec.md)

## Summary

Three changes to model values, no wire change:
- a truth-source seam with a live-forecast source that subtracts the synthetic forecast error (finding 1);
- an error model shared by valid time, with per-run and phase components (finding 2);
- base load with AR(1) log noise, appliance impulses and a daily factor on the local day, plus a WPuQ fit in the
  calibration job (finding 3).

All randomness stays counter-based (`ems.ingest.rng`) and every loop runs over time. The PV, heat-pump, EV,
battery and price models are not touched.

## Principles Check (PASS)

| # | Touched? | How it is satisfied | Shown by |
|---|---|---|---|
| P1 | yes | spec approved 2026-11-10; this plan and the tasks each get their own approval; every change cites a task | review |
| P2 | no | `src/ems/domain/` untouched; the ingest models stay pure (no I/O, clock or shared Generator) | hook, review |
| P3 | yes | `ems-gen` stays the only writer of external inputs; truth weather is internal and never published | review |
| P4–P6, P8 | no | no actor, handler, ingress or battery code | — |
| P7 | yes | deviations are keyed by `t` and `issued_at`, never wall time; hybrid pacing is unchanged | AC-007-10 test |
| P9 | yes (guard) | no schema, topic, key or identity change; refreshed value digests, pinned id digest | AC-007-18 test, `make ci` |
| P10 | yes | `jobs/calibrate.py load` is a CLI over local files | review |
| P11 | no | no settings; the input path is a CLI argument | — |
| P12 | yes | `h5py` 3.16.0 is already locked (pulled in by pvlib, upload 2026-03-06, before `exclude-newer`; it depends only on numpy). It is declared explicitly in the `ingest` group, then `uv lock` (no upgrade) refreshes the project metadata and the lock is committed. It is imported lazily in `read_wpuq` | `make lint` (`uv lock --check`) |
| P13 | no | no tracing | — |
| P14 | yes | the benchmark rerun stores config, seed, versions, hardware and commit as before; the candidate file stores data window and source | review |
| P15 | yes | 00, 09, 11, 13, `weather.yaml`, new `load.yaml`; no ADR (decisions in 00) | review |
| P16 | yes | `make bench` rerun: array generation time is a reported systems number | SC-002 |

## Contract changes (`08-contracts.md`)

None. Topics, keys, payload schemas, `schema_version` and the identity rule are unchanged. Event ids do not
depend on values, so every id stays the same for a given seed, run and window (AC-007-18). Only values change:
synthetic forecasts, base load and the meters of hybrid live sites.

## Design

### 1. Truth-source seam (`synthetic/truth.py`, new; AC-007-01, -07 … -10)

```python
class TruthSource(Protocol):
    def __call__(self, sites: SiteArrays, axis: TimeAxis) -> Truth: ...
```

- `SyntheticTruth(seed, params)` wraps today's `generate_truth`.
- `LiveForecastTruth(location_by_key, forecasts, fallback, seed, params)` does the following:
  - It computes the fallback truth for all rows.
  - For each row whose site has a live event with a step at an axis interval, it replaces kt, GHI, DNI, DHI,
    cloud, temperature and wind with step − err(v, r*).
  - It uses the same `forecast_errors` function as the synthetic forecasts (section 2), with
    `issue_t = quarter_index(event.issued_at)`, step index `k = v − issue_t`, and always
    `steps = FORECAST_STEPS`. The error therefore never depends on how long a truncated event is.
  - Rows are grouped by `issued_at`, and `forecast_errors` is called once per group, so there is no loop over
    sites. Sites can be on different runs, for example ICON-D2 and DMI HARMONIE.
  - **kt.** The step kt is the step GHI ÷ the truth's clear-sky GHI. The error acts in the bounded space of
    section 2: `truth kt = from_x(to_x(step kt) − err)`. No clip binds, so the truth stays inside (0.03, 1.1) and
    never sits at 110 % of clear sky for hours. Where clear-sky GHI is 0, GHI, DNI and DHI are 0.
  - DNI and DHI are re-split from the truth GHI with the existing `decompose`.
  - **Temperature** is additive: `truth = step − err`.
  - **Wind** uses the mean-one log-normal factor `truth = step · exp(−err − σ²/2)`, the mirror of the forward
    factor. A division by `(1 + err)` would bias wind high by 15–30 % and sometimes multiply it by 5. The factor
    applies to the 10, 100 and 120 m speeds alike.
  - Rows and intervals without a step keep the fallback (AC-007-09).
- `SyntheticModel.__init__(…, truth_source: TruthSource | None = None)`, defaulting to `SyntheticTruth`.
  `_subset` calls it once per site slice. `chunk()` loses its `weather_override` parameter, and
  `chunk(…, truth_source=…)` overrides the model's source for one call. The "overrides need all sites in one
  slice" error goes, because the live source maps rows by location key.
- `LiveRunner.live_meters` builds the source per call from a snapshot, `dict(self.state.forecasts)`, as today's
  override does. `chunk()` thus stays a function of its arguments, and the same event and seed always give the
  same truth (AC-007-10). `live_weather_override` is removed.
- In hybrid mode the truth kt jumps by about σ·√(1−ρ²)·√2 ≈ 0.12 when the latest live run changes, on top of the
  real forecast revision, because Y is per run. This is recorded with L5 in 09 and 13.
- PV (`pv_energy_kwh(truth, …)`) and the heat pump (`heat_pump_kwh(truth.temp_c, …)`) already read only the
  truth arrays. They stay unchanged, and so does base load, which reads no weather.

### 2. Forecast error model (`synthetic/weather.py`; AC-007-02 … -06)

`forecast_errors(seed, keys, issue_t, steps, var, sd, rho, tau_common_h, step_corr) -> [n_loc, steps]` computes
`sd · (ρ·Z + √(1−ρ²)·Y)`, where:
- `Z = rng.ar1(seed, keys, f"{var}.common", issue_t, steps, phi_for(tau_common_h))` runs over the run's own
  valid-time window. A run's errors therefore never depend on the chunk, and two runs share Z for a valid time up
  to the G1 burn-in residual.
- `Y` is today's per-run recursion (keys `issue_t·256 + step`, `step_correlation`).

Uses:
- **kt, in a bounded logit space.** `to_x(kt) = logit((kt − 0.03) / 1.07)` and `from_x` is its inverse, so kt
  stays inside (0.03, 1.1), the existing truth bounds. The forecast kt is `from_x(to_x(kt_shifted) + err)`, with
  `err` in logit units (`kt_sd_*`).
  - The same logit error is larger in kt terms at mid kt and smaller near clear or fully overcast skies. That
    matches the published pattern of errors smallest under clear and overcast skies (Lorenz et al. 2009 model the
    error by kt and sun elevation; not opened, VERIFY).
  - It replaces the clip, which in the hybrid direction held the truth at kt 1.1 for about 31 % of clear-sky
    quarter-hours and biased overcast truth up by about 11 %.
- **Temperature** (additive) and **wind speed** (log-normal factor `exp(err − σ²/2)`, mean one) use the same
  function (decision below). AC-007-04 therefore applies to both.
- **Wind direction** keeps today's per-run path, since no model reads it.

**Lead convention.**
- The amplitude sd uses `L_k = (k + 1)·0.25 h`, the interval end, as today, in both synthetic forecasts and
  hybrid truth.
- Phase positions use axis indices of interval starts: step k of a run issued at axis index i0 sits at `i0 + k`.

**Phase on kt.** The forecast kt at step k is the truth kt at axis position `pos_k = i0 + k + 4·δ_k`, by linear
interpolation along the truth axis.
- δ is computed only at knots, then interpolated linearly with no rescaling:
  - one knot at the issue time with δ = 0;
  - knots at absolute valid times `j·12 h` (`phase_knot_h`) after the issue, covering the horizon;
  - `δ_j = clip(σ_φ(L_j)·(ρ_φ·Zφ_j + √(1−ρ_φ²)·Yφ_{r,j}), −min(L_j, δ_max), δ_max)`, with
    `σ_φ(L) = phase_sd_per_sqrt_h · √L` and L_j the knot time minus the issue time;
  - `Zφ_j ~ N(0, 1)` is keyed by the knot index and shared by every run, and `Yφ_{r,j}` by (run, knot).
- **Guarantees.**
  - δ(issue) = 0 and |δ| ≤ δ_max.
  - Knots are spaced at least 2·δ_max apart, so the slope is ≥ −1 between knots, and also from the issue knot
    because δ_j ≥ −L_j. The warp `pos_k` is therefore non-decreasing and `pos_k ≥ i0`: a forecast never samples the
    truth before its own issue time.
  - Linear interpolation lowers the variance mid-segment (by up to half at the midpoint); this is recorded in G10.
- **Truth window.**
  - `chunk()` extends the truth axis by `ceil(4·δ_max)` quarter-hours at its end only. AR noise and the regime
    chain are causal, so earlier values do not change.
  - The coverage check in `forecast_run` (`weather.py:264`) gains the same margin.
  - The start needs no margin because `pos_k ≥ i0` (spec edge case updated).
- **Numeric check (2026-11-10).** Throwaway prototypes, not repo code, ran 64 days at 3 seeds × 4 season starts.
  They measured the total kt error (amplitude plus phase) on daylight intervals (clear-sky GHI > 0), the same
  quantity the AC tests measure.
  - With an additive kt error and a clip (the first draft):
    - kt error sd 0.19–0.28 per 6 h bin, growth 1.20–1.42 ×, consecutive-run correlation ≥ 0.75;
    - the total sat up to 20 % above the hourly literature value, because the published errors already include
      timing errors;
    - with ρ_φ = 0.9 the correlation dipped to 0.73, hence ρ_φ = 0.95.
  - In logit space with the chosen defaults (0.95 + 0.002·L, phase below):
    - kt error sd 0.18–0.20 at 0–6 h and 0.24–0.27 at 42–48 h, against the hourly literature curve of
      0.20 → 0.23; at 15 minutes it may be somewhat higher;
    - every bin within 0.179–0.272, growth ≥ 1.19 ×, consecutive-run correlation ≥ 0.75;
    - hybrid truth above kt 1.05 in 3–12 % of quarter-hours, the most in summer. That counts the
      prototype's own forecasts, which are themselves near 1.1 under clear skies.
  - A constant 0.9 was 0.005 short of the 0.17 floor in one case, hence 0.95.
  - The implementation task reruns the check, ported to the property test.
  - The warp was monotone in every run. With the phase off, growth was only 1.06 ×: once timing is modelled, it
    carries most of the growth, and the amplitude can stay nearly flat, as NWP verification shows.

**Night.** GHI = clear-sky × kt stays 0 where the clear-sky GHI is 0 (AC-007-05).

`ForecastRun` gains `phase_h` (diagnostic, like `kt_error`).

**Decision: temperature and wind get the shared-plus-per-run structure, without phase.**
- Their consecutive-run errors are also dominated by the shared synoptic error (sources below).
- The heat pump (temperature) and PV cell temperature (wind) consume them, so oscillating temperature runs would
  overstate re-planning for heat-pump-aware strategies.
- Reusing one function adds no code.
- Phase stays on kt only, where timing matters most for PV ramps.

### 3. Base load (`synthetic/load.py`, `config/calibration/load.yaml` new; AC-007-11 … -16)

```text
E(t) = A·(1−s)·shape(t)·D(d(t))·exp(σ_ar·X(t) − σ_ar²/2) + I(t)
```

- **Daily factor D.** The local day index is `d = floor(local_unix(t·900) / 86400)`, cast to int64
  (`localtime.local_unix`; `t` is the interval start, never the midpoint or end). It replaces `t // 96`. `rng.normal(seed, keys, "load.daily", unique(d))` gives one draw per site and local
  delivery day, and D = exp(σ_d·z − σ_d²/2).
- **Noise X** (revised during implementation, AC-007-12): `X = √(1−w)·X_fast + √w·X_slow`.
  - `X_fast = rng.ar1(…, "load.ar", t0, n, phi_for(tau_h))` and `X_slow = rng.ar1(…, "load.ar.slow", …,
    phi_for(tau_slow_h))`, with `t0 = t[0]`; `w` is `slow_share`. X has unit variance, and its ACF is
    `noise_acf(L) = (1−w)·φ_fast^L + w·φ_slow^L`.
  - `slow_share: 0` gives the single AR(1), which G25 keeps.
  - `base_load_kwh` asserts that `t` is contiguous.
  - **Why two time scales.** WPuQ household load is not AR(1). Its residual autocorrelation is about 0.67 at
    15 min, 0.31 at 1 h and 0.19 at 2 h. An AR(1) matching 15 min (τ ≈ 0.6 h) gives 0.21 and 0.03 at 1 h and 2 h.
    The user chose the model that resembles the data best.
- **Impulses I** (profiles with `impulses: true`, by default H25 only). For each kind (power `p_kw`, duration
  `dur_min`, `rate_per_day`):
  - The start probability is `q(t) = rate_per_day · (A/A_ref) · N_days(local year) · shape(t)`, with 365 or 366
    days. Because q ∝ shape, about 15 % of oven starts fall between 00:00 and 06:00. This is accepted and
    documented: the H25 night share is small, and per-kind time-of-use curves would add parameters without a
    source.
  - An impulse starts in quarter-hour t if `U(seed, key, "load.imp.<kind>", t) < q(t)`.
  - The start offset is `15 min · U(…, "load.imp.<kind>.offset", t)`.
  - The energy `p_kw · overlap_min / 60` goes into each quarter-hour the impulse overlaps.
  - Starts are evaluated from `t0 − ceil(dur_max/15)` so that impulses spilling into the window are counted,
    which makes window independence exact (AC-007-16).
  - Contributions to each target quarter-hour are added in a fixed order (kind, then start lag 0, 1, 2), never
    with a cumsum or convolution over the window, whose rounding would depend on the window start.
  - q(t) must stay ≤ 1 for the mean to hold (guard below).
- **Mean preservation.** Rates scale with A / A_ref (section 5), so `s = Σ_k rate_k·365.25·p_k·dur_k/60 / A_ref`
  is the same for every household. The smooth part is scaled by (1 − s).
  - Because q ∝ shape, the expected profile is A·shape(t), up to the spill of impulse energy into the next
    quarter-hour (AC-007-14 tolerance).
- **Guards.** When `load.yaml` is parsed, it fails fast unless s ≤ 0.4 and max q(t) ≤ 1 at the catalog's largest
  household energy (7,000 kWh/yr). This protects against an edited rate pushing (1 − s) to 0 or below, or
  breaking the mean. Unit-tested.
- **Exposed helpers.** `daily_factor(…)`, `log_noise(…)` and `impulses(…)` are exposed for the AC-007-12, -13 and
  -15 tests. `impulses` returns the starts, offsets and per-quarter energy for each kind. `base_load_kwh(…, params)`
  takes the parsed `load.yaml`.
- **Profiles.** H25 values come from the WPuQ fit (section 4). G25 keeps separate VERIFY defaults with no
  impulses: τ 1.5 h, σ_ar 0.20, σ_d 0.10 (there is no commercial data).
- **Not modelled.** A synthetic household has no persistent individual deviation from the H25 shape (for example
  dinner one hour early every day). The fit reports its size as a diagnostic (section 4), and G9 records it.

### 4. Load calibration (`ingest/calibration.py`, `jobs/calibrate.py load`; AC-007-17)

- **Input layout** (documented in 13): CSV `timestamp_utc,household_id,kwh`. Each row is the base-load energy of
  one quarter-hour starting at `timestamp_utc` (UTC, `Z`), excluding heat pump and EV.
- **WPuQ reader.** `read_wpuq(path)` reads `NO_PV/SFH*/HOUSEHOLD/table` (fields `index`, `P_TOT`) with `h5py`
  and converts W to kWh per quarter-hour (× 0.25 / 1000) into the same frame (facts in section 5). The test checks
  it against a small HDF5 file with the same compound layout, built in `tmp_path`.
- **Fit `fit_load(frame)`** (revised during implementation). It measures the full load, noise and impulses
  alike, with one estimator, `household_stats`, applied to data and model.
  1. **Clean.**
     - Drop rows with E ≤ 0 or NaN, and days that are holidays in DE-NI or PL.
     - Mask runs of ≥ 4 points with a zero second difference (the authors' linear gap filling). The masked share
       is reported.
     - `r = log(E / (Â·shape_H25))`.
  2. **Remove what the model does not represent.**
     - Each household's own profile, its mean of r per local quarter of day × weekday/weekend
       (`profile_deviation_sd` is reported).
     - Each local day's median.
  3. **Statistics** (medians over households):
     - the within-day autocorrelation at 15 min, 30 min, 1 h and 2 h;
     - the within-day sd;
     - the day-to-day sd of the day medians around a 31-day running mean.
  4. **Joint fit with the impulses.** The noise is fitted with the impulse model held at its literature rates.
     Model households have the same timestamps, gaps and annual energy as the data, 3 replicas each.
     - The raw statistics are biased by the day-median and profile removal and by the impulses. A fixed point on
       the statistics removes that bias: `target ← target + (observed − model(params(target)))`, 6 steps.
     - `params()` maps the four ACF values to (τ_fast, w) by least squares with τ_slow fixed, and the two sds to
       σ_ar and σ_d.
     - Fitting the noise to the noise alone and then adding impulses pulled the full-load ACF about 0.1 below the
       data at 15–30 min (0.51/0.24/0.13/0.07 against 0.60/0.35/0.18/0.09). Hence the joint fit.
  5. **τ_slow fixed at 3 h** (`TAU_SLOW_H`, documented in `load.yaml`). Left free, the fit runs to any upper bound
     it is given, and a longer τ_slow overstates day-to-day variability with an equally good ACF:

     | τ_slow | model's day-to-day sd | data |
     |---|---|---|
     | 6 h | 0.192 | 0.150 |
     | 3 h | 0.165 | 0.150 |
     | 2 h | 0.158 | 0.150 |

     The noise-only estimator gave 0.136 against 0.091 at 6 h, and 0.095 at 3 h. 3 h is the user's choice
     (2026-11-10); 2 h fits the day level slightly better and is recorded as an alternative.
  6. **Report.** The candidate lists:
     - the fitted values;
     - the data's and the model's full-load statistics side by side;
     - `peak_rate_per_day` (data and model, diagnostic only);
     - the IQRs, the data window, the household count and the dataset licence.
- **Impulse rates stay at the Polish literature values.** They are not scaled by the WPuQ peak rate, because
  that statistic counts every large appliance (hob, dishwasher, dryer) in the German mix.
- **Adopting.** Copying τ, `interval_sd` and `daily_sd` into `load.yaml` is a reviewed task. The user (or Claude,
  with the user's OK) downloads `2019_data_15min.hdf5` to `data/external/wpuq/` (gitignored). Before fitting, that
  task resolves the timestamp VERIFY items:
  - `index[0] == 1546300800` (2019-01-01T00:00Z) and `index[-1] == 1577835900` means the stamps label interval
    starts;
  - the morning ramp moves 1 h earlier in UTC after 2019-03-31 and back after 2019-10-27; if it does not, the
    stored "UTC" is local wall time.

  If τ falls outside 1–2 h, AC-007-12 is revised with the user before adoption.
- `h5py` is imported lazily inside `read_wpuq`.

### 5. Parameters (`config/calibration/weather.yaml`, `load.yaml`)

Research 2026-11-10. **[O]** = source opened and read; **[S]** = seen in search results only; VERIFY = no direct
published value.

**Forecast errors (`weather.yaml` → `forecast`)**

| Key | Old | New | Basis |
|---|---|---|---|
| `kt_sd_base`, `kt_sd_per_h` (logit units; replace `kt_sd_per_sqrt_h`) | 0.05 + 0.045·√L (kt units) | 0.95 + 0.002·L | Calibrated by simulation so that the daylight total kt-error sd, timing included, matches the hourly literature curve of about 0.20 (0–6 h) → 0.23 (48 h). That curve comes from:<br>• day-1/2/3 hourly GHI rRMSE in Germany: 40/42/45 % (Perez et al. 2013, Table 9) and 37 % → 46 % (Lorenz et al. 2008);<br>• flat NWP error over 0–5 h (Lorenz 2016);<br>• ICON-D2 noon RMSE ≈ 175 W/m² (DWD COSMO GM 2019; August 2019 only, pre-operational; seasonality VERIFY).<br>All three were opened by the research agent; the reviewer could not render them. The conversion uses a clear-sky-weighted mean daytime kt of about 0.5 (the generator's regimes give 0.43 in winter and 0.73 in summer; VERIFY) |
| `rho` (new) | — | 0.9 (ρ² = 0.81) | No published run-to-run error correlation exists for irradiance, temperature or wind (VERIFY). ECMWF: medium-range synoptic jumpiness correlates with error at ≤ 0.5 [O, confirmed by the reviewer]. For efficient forecasts the martingale bound is ≈ 0.99 at a 3 h cadence, so ρ here acts as a jumpiness parameter. Range 0.7–0.95 |
| `rho_phase` (new) | — | 0.95 | No source (VERIFY). Chosen by the numeric check: 0.9 put AC-007-02 at 0.73 at long leads. Tunable inside 0.9–0.98 together with `rho` |
| `common_tau_h` (new) | — | 2.0 | Lag-1 h correlation of HRRR GHI errors is 0.58 (τ ≈ 1.8 h, arXiv 2603.04132 [O]); COSMO-DE-EPS wind errors are 0.6–0.8 at 1 h (arXiv 1511.05877 [O]) |
| `step_correlation` | 0.97 | 0.88 (τ ≈ 2 h) | Same sources. 0.97 implied errors lasting about 8 h |
| `phase_sd_per_sqrt_h` (new) | — | 0.42 (0.7 h at 3 h, 2.1 h at 24 h, 2.9 h at 48 h) | No timing-error sd is published (VERIFY). Proxy: ECMWF EPS cyclone position error ≈ 1° at day 1 and 1.5° at day 2 [O]. At 111 km per degree, with the along-track part taken as total/√2 and a system speed of 30–50 km/h (VERIFY), that is about 1.5–2.6 h at day 1 and 2.3–3.9 h at day 2. Without the √2 it would be 2.2–3.7 h and 3.3–5.6 h; the chosen value sits at the low end. NWP misses wind-ramp timing by 1–3 h (AMS 2016 [S]) |
| `phase_max_h`, `phase_knot_h` (new) | — | 6, 12 | Cap. Knots spaced at least 2·δ_max apart keep the time warp monotone |
| `temp_sd_base_c`, `temp_sd_per_h_c` | 0.4 + 0.045·L | 1.3 + 0.015·L (1.7 K at 24 h, 2.0 K at 48 h) | ICON-D2 T2m RMSE 1.35 K (0–6 h) and 1.4–1.75 K (15–27 h) (DWD 2019 [O]; August 2019, pre-operational; winter errors are larger, VERIFY). ECMWF error sd at European SYNOP stations 2.0–2.4 K at 60–72 h (TM 911 [O]) |
| `wind_ln_sd_base`, `wind_ln_sd_per_h` (replace `wind_rel_*`) | 0.08 + 0.004·L (relative) | 0.34 + 0.0035·L (log-normal sd) | ICON-D2 10 m wind RMSE 1.25–1.65 m/s (DWD 2019 [O]; August 2019); ECMWF error sd 1.9–2.4 m/s at 60–72 h [O]. Taken as absolute values of 1.2 + 0.0135·L m/s (the line from ICON-D2 at 0–6 h to ECMWF at 60–72 h), divided by a typical inland 10 m speed of 3.5 m/s (VERIFY, IMGW), and converted to a log sd as √ln(1 + cv²). Implementation (T07): the slope was raised from 0.003 because the first draft's 0.012 m/s per hour gave a nominal growth ratio of only 1.36 against the 1.3 × of AC-007-04 |

URLs (for the `weather.yaml` comments and 13):
- https://research.asrc.albany.edu/people/faculty/perez/2013/forecast-se.pdf
- https://52.iea-shc.org/Data/Sites/1/publications/23rd_EU_PVSEC_5AO.8.6_lorenz.pdf
- https://www.ises.org/sites/default/files/webinars/ISES_Webinar_2016_01_Lorenz_0.pdf
- https://www.cosmo-model.org/content/consortium/generalMeetings/general2019/plenary/nwp_zangl.pdf
- https://confluence.ecmwf.int/x/A62HEw
- https://www.ecmwf.int/sites/default/files/elibrary/2012/9466-predictability-extratropical-cyclones.pdf
- https://www.ecmwf.int/sites/default/files/elibrary/092023/81389-evaluation-of-ecmwf-forecasts-including-the-2023-upgrade.pdf
- https://arxiv.org/pdf/2603.04132
- https://arxiv.org/pdf/1511.05877

Consequences:
- **Growth with lead.** Published NWP irradiance errors grow only weakly with lead and do not shrink at
  intraday leads (NWP alone; satellite nowcasting is what improves them). The spec's "≥ 1.5 ×" growth for kt
  contradicts this, so **AC-007-03 is revised** (spec updated, flagged for approval): the kt error sd grows by
  ≥ 10 % from 0–6 h to 42–48 h and lies within 0.17–0.30 in every 6 h bin. Temperature and wind keep a ≥ 1.3 ×
  growth (AC-007-04). The old run-difference clause is dropped, because AC-007-02's correlation already bounds
  how much runs differ (sd of the difference ≤ σ·√(2(1 − 0.7)) ≈ 0.77 σ). AC-001-21 ("spread grows with lead")
  still holds; its test threshold drops from 1.5 × to 1.1 ×.
- **No double counting of timing.**
  - Published errors already include timing, so the amplitude is calibrated against the total, phase included,
    not added on top of it.
  - In hybrid mode the latest run is 1.5–4.5 h old, where phase is near zero. The amplitude alone then equals the
    short-lead literature value, and both directions use one σ curve.
  - AC-007-08 checks the hybrid deviation in logit units against `kt_sd(L)`.
- **Accepted simplification.** Every pair of runs correlates at ρ², however far apart they were issued. Risk G10
  accepts this; a decay with the issue gap would add a parameter nobody has published.

**Base load (`load.yaml`)**

Appliance impulses. Rates are for the reference household (A_ref = 2,300 kWh/yr, the catalog median) and include
ownership. A site's rate is scaled by A / A_ref, so the impulse share s = E_imp,ref / A_ref ≈ 401 / 2,300 ≈ 0.17
is the same for every household. The guards in section 3 still validate s and q.

| Kind | p_kw | dur_min | Rate per day (ref.) | kWh/yr (ref.) | Basis |
|---|---|---|---|---|---|
| kettle | 2.0 | 3 | 4.0 | 146 | 2.0 kW, 3 min, 0.10 kWh, 4.2/day (CREST via richardsonpy [O]); 65 % of cycles < 0.1 kWh (UK HES, Intertek R66141 [O]). Polish rate VERIFY |
| oven | 2.1 | 27 | 0.45 (0.6/day per owner × 75 % electric-oven ownership) | 155 | 2.125 kW, 27 min, 0.60/day per owner (CREST CSV [O, confirmed by the reviewer]). The rate is UK-based, in a gas-hob country: the Polish rate is VERIFY. Ownership is derived from GUS 2018: 56.9 % gas-electric cookers plus electric cookers with an oven (VERIFY). PV prosumers in single-family houses probably own more ovens and induction hobs than the national average (VERIFY) |
| washing_machine | 2.0 | 20 | 0.41 (0.45/day × 92.1 % ownership) | 100 | 2.056 kW heater for 21 min, from the CREST appliance code (richardsonpy `appliance.py` l. 245–287 [O]); the CSV row is the 406 W cycle mean. Cycles: EEA 0.48/day in 2020, falling toward 0.34 by 2030 [O, confirmed by the reviewer], so about 0.45 in 2026. Ownership from GUS 2018 (research agent; the reviewer could not render it, VERIFY). Only the heating phase is an impulse; the rest of the cycle stays in the smooth part |

Not modelled as impulses: the hob (electric in only about 20 % of households; GUS 2024 calls electricity "rather a
supplementary carrier" for cooking [O]), the dishwasher (31.8 %) and the tumble dryer (3 %). Their energy stays in
the smooth BDEW part, because (1 − s) removes only the modelled impulses.

Noise:
- **H25, fitted on WPuQ 2019** (27 houses; T13, 2026-11-10): `tau_h` 0.66, `tau_slow_h` 3.0 (fixed),
  `slow_share` 0.44, `interval_sd` 0.45, `daily_sd` 0.01. The slow component carries almost all of the day-to-day
  variation; the daily factor is kept, and G25 uses it. Full-load statistics:

  | | 15 min | 30 min | 1 h | 2 h | within-day sd | day-to-day sd |
  |---|---|---|---|---|---|---|
  | WPuQ data | 0.603 | 0.345 | 0.184 | 0.094 | 0.541 | 0.150 |
  | fitted model | 0.587 | 0.343 | 0.196 | 0.082 | 0.540 | 0.165 |

  - The model's day-to-day sd is 0.015 high, with `daily_sd` already near 0: the slow component and the
    day-to-day spread of impulse counts give a little more day-level variation than the data.
  - `load.yaml` records the data row as `calibration_targets` (AC-007-12).
  - The source is German households (2019); Polish applicability is VERIFY.
- **G25**: `tau_h` 1.5, `interval_sd` 0.20, `daily_sd` 0.10 (today's magnitudes), a single AR(1) with no
  impulses. It stays VERIFY, as there is no commercial data.
- **WPuQ timestamps** (T13 checks):
  - `index[0]` = 1546300800 and `index[-1]` = 1577835900, so the stamps label interval starts.
  - The PV inverter's daily centroid falls at 11:35, 11:22 and 11:14 UTC in January, June and September,
    matching solar noon at Hamelin in UTC. So the stamps are true UTC.
  - The morning ramp moves about 45 min earlier in UTC after 2019-03-31.
  - Both VERIFY items are resolved.
- The impulse rates stay at the values above. WPuQ's peak rate is a diagnostic only (section 4). German ownership
  (Destatis EVS 2018: electric cooker 94 %, dishwasher 72 %, tumble dryer 42 %) differs too much from Polish
  ownership to transfer.

Sources:
- CREST via richardsonpy: https://raw.githubusercontent.com/RWTH-EBC/richardsonpy/master/richardsonpy/inputs/Appliances.csv
- UK HES (Intertek R66141): https://assets.publishing.service.gov.uk/media/5a7c2fd940f0b67d0b11f6df/10043_R66141HouseholdElectricitySurveyFinalReportissue4.pdf
- EEA: https://www.eea.europa.eu/en/circularity/sectoral-modules/product-lifespans/evolution-in-the-average-number-of-washing-cycles-per-washing-machine-in-eu-intensity-of-use
- GUS 2018: https://stat.gov.pl/download/gfx/portalinformacyjny/pl/defaultaktualnosci/5485/2/4/1/zuzycie_energii_w_gospodarstwach_domowych_w_2018.pdf
- GUS 2024: https://stat.gov.pl/download/gfx/portalinformacyjny/pl/defaultaktualnosci/5485/12/3/1/zuzycie_energii_w_gospodarstwach_domowych_w_2024_r..pdf
  (press release: https://stat.gov.pl/dla-mediow/informacje-prasowe/gus-pokazuje-zuzycie-energii-w-gospodarstwach-domowych,49,1.html)
- Destatis EVS 2018 (German ownership): https://www.destatis.de/DE/Themen/Gesellschaft-Umwelt/Einkommen-Konsum-Lebensbedingungen/Ausstattung-Gebrauchsgueter/Tabellen/a-evs-haushaltsgeraete-d.html
- CREST washing-machine code: https://github.com/RWTH-EBC/richardsonpy/blob/master/richardsonpy/classes/appliance.py

**WPuQ facts (Zenodo 5642902 v2.0, paper, authors' code; checked 2026-11-10 with h5py reads)**

- **Files and format.**
  - 15-minute files `2018_data_15min.hdf5` (471 MB), `2019_…` (459 MB) and `2020_…` (448 MB), at
    `https://zenodo.org/api/records/5642902/files/<name>/content`. There is no CSV variant.
  - They are pandas/PyTables `frame_table` files. `h5py` reads them as compound datasets at
    `NO_PV|WITH_PV/SFH<n>/HOUSEHOLD/table`, so the `tables` package is not needed.
- **Values.**
  - `index` is int64 "Seconds since 01.01.1970 00:00:00 UTC" (paper Table 4, confirmed by the reviewer). A
    PV-noon spot check agrees; the authors' plot code assumes Europe/London. Whether it labels the interval start
    is not stated (VERIFY; the checks are in section 4).
  - `P_TOT` is the interval-mean active power in W, so kWh = `P_TOT · 0.25 / 1000`.
  - The household meter excludes the heat pump and its 6 kW backup heater, which are on a separate meter.
- **Fit set.**
  - The 2019 file only: 35,040 rows, each a full 15-minute step.
  - `NO_PV` houses only, because the measured `WITH_PV` values are household consumption net of PV feed-in and
    can be negative.
  - Houses with ≥ 90 % non-NaN data, minus those the authors list as missing at least a full month in 2019 (6,
    13, 17, 24, 25, 31, 37, 40).
  - NaN marks gaps longer than one day. Gaps of up to one day were interpolated linearly by the authors; the fit
    masks those runs (section 4).
- **Attribution:** "WPuQ dataset, Schlemminger et al. 2022, Sci. Data 9:56, CC BY 4.0" in the candidate file and in 13.

## Risks (`09-risks-and-tech-debt.md`)

| Id | Change |
|---|---|
| L5 | Narrowed: hybrid live-site truth = live step − synthetic error, so the oracle effect is gone. Accepted until the historical-backfill spec brings satellite truth:<br>• the deviation statistics are assumed (amplitude only, no phase);<br>• truth kt jumps by ≈ 0.12 when the latest run changes, because the per-run component changes;<br>• the logit space makes the truth regress mildly toward mid kt (prototype: −0.05 under clear-sky forecasts, +0.07 to +0.15 under overcast ones; VERIFY against Lorenz et al. 2009's kt-dependent bias) |
| G1 | Extends to Z, the load AR noise and the truth window; impulses and phase knots are exact |
| G9 (new) | Load calibration on German single-family houses with heat pumps (WPuQ 2019), not on Polish households. Accepted; refit through the CSV layout when Polish data exist:<br>• the timestamp label is VERIFY until the adoption checks run;<br>• synthetic households have no persistent individual deviation from H25 (reported as `profile_deviation_sd`);<br>• G25 noise is uncalibrated;<br>• the Polish appliance rates and ownership are VERIFY (UK CREST rates, GUS national averages) |
| G10 (new) | Error-model simplifications, accepted with their parameters cited and VERIFY marks:<br>• constant ρ (the correlation does not decay with run separation);<br>• phase on kt only;<br>• 12 h phase knots, with lower variance mid-segment;<br>• oven impulses follow the H25 shape (about 15 % at night) |
| G11 (new) | Same ids with new values: topics filled before spec 007 with the same seed and no `run_id` keep old values under the ids new runs produce. Accepted (no consumer yet): `make reset` or a new `run_id` |
| S1, G2, G6 | Unchanged |

## Experiment impact

- No energy result exists yet, so no reported energy number changes.
- The benchmark in `11-experiments.md` and `13` reports array generation time and events/s.
  - Array time can grow: one more AR path per variable and run, phase interpolation, and impulse draws.
  - Events per day are unchanged.
- `make bench` is rerun (it needs `make up` for the produce step), and the new run is recorded beside the
  2026-10-20 runs (P16, SC-002).

## Test strategy

Unit and property tiers, written first by `test-engineer`. No network: AC-002-14 still blocks it.

| File | Covers |
|---|---|
| `tests/unit/ingest/test_narrow_assets.py` | AC-007-18: an id-only digest of a backfill day, pinned on this branch **before** any code change; event digests refreshed (comment: spec 007); catalog digest unchanged |
| `tests/unit/ingest/test_truth_source.py` | **AC-007-01**: a stub `TruthSource` with constant weather gives PV and heat pump equal to the models applied to its arrays.<br>**AC-007-07**: the published live event equals the fetched one.<br>**AC-007-08**: stub live events with mid-range sky (GHI = 0.55·clear-sky) over 30 days. Truth GHI ≠ step GHI in every daylight interval; sd of to_x(step kt) − to_x(truth kt) within ±20 % of `kt_sd(L)`; truth kt inside (0.03, 1.1).<br>**AC-007-09**: no event, a truncated event, and an interval past the horizon all fall back to synthetic truth and still emit meters.<br>**AC-007-10**: the same event and seed give identical meters. Two live sites on different `issued_at` give the same values as each alone |
| `tests/property/test_forecast_errors.py` | Every statistic is the total kt error (`kt_error`, phase included) on daylight intervals, 60 days.<br>**AC-007-02/-04**: consecutive-run correlation ≥ 0.7 per 3 h lead bin (3–45 h) for kt, temperature and wind.<br>**AC-007-03**: kt sd at 42–48 h ≥ 1.1 × at 0–6 h, and within 0.17–0.30 in every 6 h bin.<br>**AC-007-04**: temperature and wind sd growth ≥ 1.3 ×.<br>**AC-007-05**: \|δ\| ≤ δ_max; sd(δ) at 42–48 h ≥ 2 × at 0–6 h; the warp `pos_k` is non-decreasing and ≥ i0 in every run; GHI = 0 where clear-sky = 0.<br>**AC-007-06**: alone vs batch vs `site_chunk` bit-identical; [a, c) vs [a, b) + [b, c) within G1 tolerance; same seed, same values (hypothesis, fixed profile) |
| `tests/unit/ingest/test_load_model.py` | **AC-007-11**: one local year; test site ±5 %; mean of 50 households ±2 %.<br>**AC-007-12**: lag-1 of `log_noise` within ±0.03 of exp(−0.25/τ); the default in 0.75–0.92.<br>**AC-007-13**: via `impulses(…)`, counts per kind within ±10 % of Σ_h rate·(A_h/A_ref)·days over 50 households; each impulse's energy per quarter-hour ≤ p·0.25 h; G25 none.<br>**AC-007-14**: hour-of-day means within ±10 % of H25.<br>**AC-007-15**: `daily_factor` is constant over 92/96/100 slots and changes at local midnight. It is checked on 2026-03-28/29/30 and 2026-10-24/25/26, including 2026-03-28T22:45Z/23:00Z and 2026-10-25T00:45Z/01:00Z (the repeated local hour, the same day).<br>**AC-007-16**: alone vs batch vs `site_chunk` bit-identical; impulses bit-identical across window splits, spill included.<br>**Guards**: `load.yaml` with s > 0.4 or max q > 1 fails at parse |
| `tests/unit/ingest/test_calibration.py` | **AC-007-17**: households generated by the model with impulses on, with known τ, σ_ar and σ_d, written to a tmp CSV with a few linearly interpolated gaps. The fit recovers τ ± 0.3 h, σ_ar ± 0.05 and σ_d ± 0.03, and reports the masked share. The candidate holds the window, count and dataset licence. `read_wpuq` on a tmp HDF5 in the WPuQ compound layout gives the same frame |

Existing tests:
- AC-001-21 (error grows with lead): its test threshold drops from late > 1.5 × early to > 1.1 × (section 5).
- AC-001-23 (annual base load ±5 %) and the fleet chunking tests stay and must pass.
- AC-002-12 is updated: live meters follow the live weather through the seam, so PV is > 0 in daylight, and the
  `live_weather_override` import goes.

## Docs and ADRs

- No ADR. These go to `00-overview.md` "Decisions so far":
  - hybrid truth is a synthetic deviation behind a truth-source seam;
  - temperature and wind share the error structure;
  - load is calibrated on WPuQ;
  - pvmonitor.pl is not used without permission;
  - the historical backfill is its own spec.
- `00`: status row and backlog row 007 (built); a "Historical backfill on real data" row before the experiment
  harness, with the spec's handoff facts.
- `13`:
  - the forecast error model (formula, parameters, sources);
  - the phase model;
  - the hybrid truth and the seam ("Router and hybrid" rewritten);
  - the load model and the CSV calibration layout;
  - the WPuQ attribution;
  - the new benchmark row.
- `09`: the table above. `11`: benchmark rerun line.
- `config/calibration/weather.yaml` (new `forecast` keys with source comments), `config/calibration/load.yaml`
  (new, with source comments and VERIFY marks).
- `README.md`: the calibration command list gains `load`, if the README lists the commands.

## Review (2026-11-10)

Both reviews were run on the draft plan. `architect-reviewer` found no blockers and 17 findings.
`energy-domain-reviewer` found 1 blocker and 14 findings. Every finding is fixed above, except where the table says
otherwise.

| Finding | Resolution |
|---|---|
| Phase sd written as linear in one place and √L in another; 6 h vs 12 h knots (energy blocker; architect 1) | `phase_sd_per_sqrt_h · √L` and 12 h knots everywhere |
| Warp not monotone; −L bound binds; lead and index convention off by one (architect 2, 3; energy 4) | δ only at knots, plus a δ = 0 knot at issue, linear interpolation with no rescale; positions on interval-start indices; property test that the warp is non-decreasing and ≥ i0; coverage check extended |
| ρ_φ missing (architect 4) | `rho_phase` 0.95, chosen by the numeric check |
| Timing counted twice; phase effect on AC-007-02/-03 not quantified (architect 5; energy 3) | Amplitude calibrated against the literature total, phase included; numeric check recorded; tests use the total daylight kt error |
| Clipping makes hybrid truth implausible: hours at kt 1.1, overcast bias (energy 5; architect 11) | kt error in a bounded logit space, no clip; AC-007-08 reworded to logit units on mid-range fixtures |
| Wind inverse biased (architect 10; energy 6) | Mean-one log-normal factor in both directions, the same for all heights |
| No guards on s and q (architect 6; energy 12) | Fail-fast checks when `load.yaml` is parsed, plus a test |
| `impulse_rate_scale` imports the German appliance mix (architect 7; energy 12) | Dropped. The peak rate is a diagnostic only |
| `daily_sd` and `interval_sd` estimators biased (architect 8) | Analytic AR corrections; m_d over unflagged slots; the test also recovers σ_ar |
| τ and σ estimators not robust: kettles, H25 shape mismatch, interpolated gaps (architect 9; energy 10, 11) | 0.05 kWh impulse flag; the household's own profile removed (reported as a diagnostic); linear runs masked; the test generates impulses and gaps |
| AC-007-13 count and per-quarter bound ill-defined (architect 12) | AC reworded (expected count with A/A_ref; bound per impulse); `impulses(…)` helper |
| Accumulation order, contiguity, mutable mapping, loop over sites, lock refresh, run-change jump (architect 13–17) | Fixed order; assert; snapshot per call; grouped by `issued_at`, steps = 192; `uv lock` committed with a lazy import; noted in L5 |
| DST day index (energy 1) | Confirmed; int64 cast and four boundary test points added |
| ICON-D2 2019 basis, phase proxy conversion, martingale bound, mean kt (energy 7, 8) | Labelled August 2019, pre-operational, VERIFY; conversion stated; 0.99; regime means noted |
| CREST washing machine, EEA trend, oven rate, prosumer bias, days per year, oven night starts (energy 13) | Code cited; 0.45 cycles/day; VERIFY marks; local-year day count; accepted in G10 |
| WITH_PV wording, WPuQ start label and UTC (energy 9, 14) | Reworded; index and DST-ramp checks in the adoption task |
| Polish 24 December holiday (energy 15) | Checked: `holidays` 0.105 includes 2025-12-24 and 2026-12-24. Nothing to do |
| Sources the reviewer could not render (GUS, UK HES, Perez, Lorenz, TM 911) | Kept as opened by the research agent; numbers that rest only on them stay VERIFY where marked |

## Complexity Tracking

None. The seam replaces `weather_override` rather than adding a second path.
