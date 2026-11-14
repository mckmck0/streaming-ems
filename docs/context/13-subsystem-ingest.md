# 13 · Subsystem: ingest (data generators → Kafka)

## Current consensus

`ems-gen` (`src/ems/ingest/`) streams contract events (`08-contracts.md`) for Polish sites to Kafka. External
data reaches the EMS only through these topics (ADR 0011). Built: synthetic data for EMS-managed households (spec 001) and
live sources, record/replay and hybrid mode (spec 002), scale, background batteries and faults (spec
003), EV chargers and site types (spec 004), narrowed to PV prosumer sites (spec 005). Kafka runs as one broker
(spec 006 removed spec 004's 3-broker profile). Spec 007 made the statistics fit for comparing strategies: run-
consistent forecast errors with timing errors, persistent base load with appliance peaks, and hybrid truth that
deviates from the live forecasts. Spec 008 adds one real-data window: archived forecast runs, PSE prices
and observed truth weather (satellite, ERA5) fetched once into a recording and replayed many times.

```mermaid
flowchart LR
  cfg[config/: generator.yaml, sites.yaml, calibration/, topics.yaml] --> cat[catalog: sites]
  cat --> model[SyntheticModel: truth weather, PV, base load, heat pump, prices]
  model --> sched[emissions: emission-time order]
  sched --> sink[KafkaSink: validate, route, headers, event time]
  sink --> topics[(ems.*.v1 topics)]
  sink -.invalid.-> dlq[(ems.gen.dlq.v1)]
  pre[preflight: topic time rules] --> sink
  tools[tail / validate] --> topics
```

### Layout

| Module | Role |
|---|---|
| `contracts/` | envelope and payload models, ids, time, topic plans and routing, schema export (importable by actors) |
| `ingest/catalog/` | site models, voivodeship table, deterministic catalog, `config/sites.yaml` overrides |
| `ingest/rng.py` | counter-based randomness keyed by (seed, location, variable, step) |
| `ingest/synthetic/` | `solar` (pvlib SPA numpy + Ineichen), `weather` (truth and forecasts), `truth` (truth-source seam), `pv`, `load` (BDEW H25/G25, noise, impulses), `heat_pump`, `ev`, `bess`, `balance`, `prices` |
| `ingest/generator.py` | chunks, event builders, emission schedule |
| `ingest/runner.py` | preflight, pacing per clock mode, run manifest |
| `ingest/kafka/` | client config, admin (ensure), sink, preflight |
| `ingest/tools/` | bounded topic reader, validate |

### Synthetic models (calibration in `config/calibration/*.yaml`)

- **Determinism**: every value is a function of (seed, location, variable, quarter index). Hash-based uniforms
  make a location's output independent of batch size and order (AC-001-26, bit-exact); AR(1) noise is a per-row
  recursion with a 6-time-constant burn-in, so values barely depend on the window start.
- **Weather truth**: a national hourly regime chain (clear / broken / overcast, monthly probabilities) shared by
  all sites, plus regional and local AR noise, gives the clear-sky index; GHI = Ineichen clear-sky × index
  (≤ 1.1 × clear-sky); Erbs split into DNI/DHI. Temperature: seasonal and diurnal harmonics, continentality,
  lapse rate, national synoptic and local anomalies. Wind: Gaussian AR copula (national share 0.8) → Weibull at
  10 m, power-law shear to 100/120 m, direction as a mean-reverting walk.
- **Truth source** (`synthetic/truth.py`, spec 007): the one place that decides where a site's truth weather
  comes from. `SyntheticModel(truth_source=…)` calls a `TruthSource(sites, axis) -> Truth`; PV, heat pump and base
  load read weather only from that `Truth`. `SyntheticTruth` is the synthetic weather above; `LiveForecastTruth`
  serves hybrid live sites (below). A historical backfill adds a source (satellite or reanalysis arrays) without
  touching the asset models.
- **Forecasts** (spec 007): runs every 3 h, 192 steps, emitted when available (`ems.contracts.availability`: 120 min
  after issue since spec 008; was 90). For variable x, valid time v and
  run r with lead L = (k + 1)·0.25 h, `err_x = sd_x(L)·(ρ·Z_x(v) + √(1−ρ²)·Y_x(r, v))`: Z is an AR(1) over valid
  time shared by every run (τ 2 h), Y an AR(1) along one run's steps, ρ = 0.9, so consecutive runs' errors for one
  valid time correlate at about ρ² and agree in sign. The clear-sky index error acts in a bounded logit space
  (`logit((kt − 0.03)/1.07)`, sd 0.95 + 0.002·L), so kt stays inside (0.03, 1.1) without clipping and errors are
  smaller under clear or overcast skies; temperature is additive (1.3 + 0.015·L K), wind speed a mean-one
  log-normal factor (0.34 + 0.0035·L, all heights). A timing error moves the clear-sky index only (clear-sky GHI
  stays, so day and night stay in place): δ is drawn at knots (0 at the issue time, then every 12 h of valid time,
  sd 0.42·√L h, the same shared-plus-per-run mix with ρ_φ 0.95), clipped to [−min(L, 6 h), 6 h] and interpolated
  linearly, so the time warp is monotone and never samples before the issue time. The truth axis extends past the
  last run by the 6 h cap at its end only (causal processes: meters do not change). Checked on 64 days × 3 seeds
  × 4 seasons: daylight kt error sd about 0.19 at 0–6 h → 0.24–0.27 at 42–48 h (hourly NWP verification: about
  0.20 → 0.23, timing included), consecutive-run correlation ≥ 0.75. Parameters and sources (VERIFY where no
  value is published): `config/calibration/weather.yaml`, `specs/007-realistic-generator-data/plan.md` §5.
- **PV**: pvlib Hay-Davies POA, Faiman cell temperature, PVWatts DC (`effective_irradiance=`) and inverter
  clipping at the AC rating, seasonal soiling/snow derate. One simulated year for the test site: 932 kWh/kWp
  (PVGIS central Poland about 930; measured PL rooftops 869–1076).
- **Base load**: demandlib 0.2.2 BDEW 2025 H25 (G11/G12) and G25 (C11/C21) on a tz-aware Europe/Warsaw index
  with Polish holidays, renormalised to an exact annual sum. Official Polish DSO profiles exist only as yearly PDF
  annexes in each DSO's IRiESD with no reuse terms, so BDEW is the default. Spec 007
  (`config/calibration/load.yaml`): `E = A·(1−s)·shape·D(day)·exp(σ·X − σ²/2) + I`, with D a mean-one lognormal
  per site and Europe/Warsaw delivery day (92/100-slot days included), X two-timescale noise
  (`√(1−w)·X_fast + √w·X_slow`, unit AR(1)s; H25: τ_fast 0.66 h, τ_slow 3 h fixed, w 0.44, σ 0.45, daily sd 0.01,
  fitted on WPuQ 2019; G25 a single AR(1), τ 1.5 h, VERIFY) and I appliance impulses for H25 households: kettle 2.0 kW × 3 min, oven
  2.1 kW × 27 min, washing-machine heater 2.0 kW × 20 min (CREST, UK HES, EEA, GUS ownership; Polish rates VERIFY),
  spread over the quarter-hours they overlap (a kettle adds 0.1 kWh to its quarter). Rates are per reference
  household (2,300 kWh/yr) and scale with the site's energy; starts follow the profile share, so the expected
  annual energy stays A and s ≈ 0.17. G25 sites get noise but no impulses. Parsing fails fast if s > 0.4 or a
  start probability exceeds 1. Impulses that start before a window are drawn too, so window splits are exact.
- **Heat pump**: degree-hour space heating (base 15 °C, heating curve) and hot water over a Carnot-fraction COP;
  the test site uses about 3.7 MWh/yr.
- **Prices (PL_RCE-like)**: monthly base, daily shape with morning and evening peaks, weekend/holiday factor,
  national PV index dip (negative middays in spring/summer), wind index, AR noise and rare evening spikes; one
  simulated year gives 3.3 % negative quarter-hours. Published D−1 13:00–16:15 local (77 % 13–14 h).
- **Batteries**: sites declare `bess.control: ems | site`. EMS-managed sites (incl. the test site) emit no
  battery or grid measurements (ADR 0014); unmanaged sites emit grid import/export closing the balance.

### Clock modes

`backfill` (as fast as possible; simulated time only), `accelerated` (`--speed`, default 96), `realtime` (start
now, wall-clock pacing). Wall time is used only for pacing and `ingested_at`.

### Facts owned (single writer)

`ems-gen` is the only writer of the external inputs: weather forecasts, metered production/consumption of
uncontrolled assets, and prices. The single-writer table is in `01-architecture.md`.

### Live sources, routing, record and replay (spec 002)

- **Mapping engine** (`ingest/mapping/engine.py`, `mappings/*.yaml`, schema `mappings/_schema/`): raw payload →
  canonical rows (rows or column layouts, dotted paths or JSON pointers whose segments may contain dots,
  time zones and labels, units, transforms, sign split, null policy, quality map). Adapters build events from rows.
- **PSE** (`live/pse.py`; `pse.rce.v2`, `pse.csdac.v2`): one event per report and business day once every
  local-day slot is there; `issued_at` = PSE publication time (revisions → new events); `nextLink` paging;
  polls D+1 13:00–18:00 local every 15 min, then hourly, and re-polls D for revisions.
- **Open-Meteo** (`live/openmeteo.py`; `openmeteo.forecast.v1`): explicit model per site (ICON-D2 inside
  43.18–58.08 N / 3.94 W–20.34 E, DMI HARMONIE elsewhere), run time from `/data/<model>/static/meta.json`
  (used ≥ 10 min after availability), run-anchored 192-step window, truncated before the first step with a
  missing value; free API by default, customer API with the key in an `X-Api-Key` header.
- **ENTSO-E A44** (`live/entsoe.py`; off until `EMS_ENTSOE_TOKEN` and `live.entsoe.enabled`): hardened lxml
  parsing by local name, contract type A01, A03 forward-fill to the period end, native resolution (PT60M before
  2025-10-01), identical duplicate series collapse, conflicting ones are logged and not emitted, token in the
  `SECURITY_TOKEN` header, 10-minute back-off on 429. Run against the real API on 2026-10-29
  (<https://web-api.tp.entsoe.eu/api>, document type A44, zone `10YPL-AREA-----S`): token and header accepted;
  2025-09-30 returned `PT1H` × 24 slots in EUR, 2025-10-26 returned `PT15M` × 100 slots, and the current and next
  delivery day returned `PT15M` × 96 slots (tomorrow was already published in the early afternoon). A recording of
  these responses (`make record`, raw XML in `data/recordings/<name>/raw/entsoe/`, never commit it) resolved the
  VERIFY items: schema version 7:3, a single `TimeSeries` with the same elements and order as the hand-written
  fixtures, `curveType` A03 with all 96 positions present (so the forward-fill path is exercised by fixtures
  only), `createdDateTime` equal to the request time (no publication time, as risk L4 assumed). The EUR prices
  match PSE's `csdac-pln` slot for slot with a constant PLN/EUR ratio (≈ 4.4 on 2026-10-29 and 2026-10-30).
- **HTTP** (`live/http.py`): retries with jitter on 429/5xx/timeouts only; secrets scrubbed from URLs, errors
  and recordings.
- **Router and hybrid** (`sources/router.py`, `sources/live_runner.py`, `runner.run_hybrid`): per-stream modes
  from the generator config; live weather for `live.locations`, synthetic for the rest, one price source. A live
  site's synthetic PV and heat pump use `LiveForecastTruth` (spec 007, risk L5): the latest live step minus the
  synthetic forecast error at that step's lead, in the same spaces as the synthetic forecasts (bounded logit kt,
  additive temperature, mean-one log-normal wind at every height), with DNI/DHI re-split from the truth GHI. The
  published forecasts stay exactly as fetched, so a forecast-driven strategy is no longer an oracle. Intervals
  without a step (no run yet, a truncated run, past the horizon) use the synthetic truth. `live_meters` builds the
  source from a snapshot of the latest events, so the same event and seed always give the same meters. The truth
  kt jumps by about 0.12 when a new run arrives (the per-run part changes), and in logit space it regresses mildly
  toward mid kt. `config/generator.live.yaml` + `make gen-live` run it.
- **Record and replay** (`sources/recording.py`): `ems-gen record --name N` stores raw responses (redacted,
  atomic writes, SHA-256 in each `.meta.json` since spec 008), events (`events.jsonl`) and a manifest in
  `data/recordings/N/`; `ems-gen replay --name N` re-publishes them with their ids, marked `replay:<source>`.
  Since spec 008 replay first runs the topic preflight of `ems-gen run` over the recording's record times and
  refuses (exit 1) topics that would reject or expire them (K1), for every recording.
- **Calibration** (`ingest/calibration.py`, `jobs/calibrate.py`): price and temperature fits → candidate files in
  `config/calibration/candidates/` (review before adopting). Example: ERA5 2024 at Poznań gives an annual mean of
  11.5 °C (a record-warm year; the default 8.7 °C is the 1991–2020 normal).
- **Forecast-error calibration** (spec 008, `jobs/calibrate.py forecast-error NAME`, offline): on a built history
  recording, forecast-vs-truth error sd by forecast model and 3-hour lead (kt plain and in the bounded logit space
  where both kt are inside the bounds, temperature, 10 m wind log-ratio, hourly kt) and the correlation of
  consecutive runs' errors, with linear fits beside `weather.yaml`'s values, written to
  `candidates/forecast-error.yaml` with the recording, hashes and attribution. The truth is the satellite and ERA5,
  so every sd is an upper bound (caveats in the file). On `spring-2026` (2026-11-13; review only, not adopted):
  ICON-D2 at Poznań kt sd 0.20 (0–3 h) → 0.27 (45–48 h), consecutive-run correlation 0.77–0.81 (spec 007 assumes
  ρ² = 0.81 and gives totals of 0.19 → 0.24–0.27); temperature sd 1.25 → 1.60 K (assumed 1.3 → 2.0 K); 10 m wind
  log sd 0.44–0.49 (assumed 0.34 → 0.51; ERA5 inflates it). DMI at Radom: kt sd 0.25 → 0.28, temperature 1.16 →
  1.68 K, wind 0.35–0.40.
- **Load calibration** (spec 007, `jobs/calibrate.py load PATH… --format csv|wpuq`): fits the H25 noise
  (τ_fast, slow share w, σ, daily sd; τ_slow fixed at 3 h). Input is per-household quarter-hour base-load energy
  (heat pump and EV excluded): either a CSV with header `timestamp_utc,household_id,kwh` (`timestamp_utc` = interval
  start, RFC 3339 UTC `Z`; any source, for example a permitted Polish extract) or the WPuQ 15-minute HDF5 files
  (read with h5py: `NO_PV/SFH<n>/HOUSEHOLD/table`, `P_TOT` W → kWh × 0.25/1000; stamps verified as UTC interval
  starts; PV houses and the authors' list of houses missing a month skipped).
  - One estimator, `household_stats`, measures the **full load** (noise and appliance peaks alike) in data and
    model: drop holidays (PL, DE-NI) and linearly interpolated runs; r = log(E/(A·shape)) minus the household's own
    profile (quarter of day × weekday/weekend, not modelled; its sd is reported) and each local day's median; then
    the within-day ACF at 15 min, 30 min, 1 h, 2 h, the within-day sd and the day-to-day sd.
  - The noise is fitted jointly with the impulses held at their literature rates (fitting the noise alone and then
    adding impulses left the full-load ACF about 0.1 below the data at 15–30 min). A fixed point on the statistics
    removes the estimator's bias: model households with the same timestamps, gaps and energies, iterated until
    their statistics match the data's.
  - τ_slow is fixed, not fitted: free, it runs to any bound and overstates day-to-day variability (full-load day sd
    0.192 at 6 h, 0.165 at 3 h, 0.158 at 2 h vs the data's 0.150).
  - WPuQ 2019 result (27 houses): full-load ACF 0.603 / 0.345 / 0.184 / 0.094 (data) vs 0.587 / 0.343 / 0.196 /
    0.082 (model), within-day sd 0.541 vs 0.540, day-to-day sd 0.150 vs 0.165; recorded in `load.yaml`
    (`calibration_targets`). Impulse rates are not fitted (the WPuQ peak rate counts the German appliance mix: 2.1
    peaks/day vs the model's 1.7); it is a diagnostic.
  - Data: WPuQ dataset, Schlemminger et al. 2022, Sci. Data 9:56 (Zenodo 5642902), CC BY 4.0, German single-family
    houses (2019); Polish applicability VERIFY. Downloaded to `data/external/wpuq/` (gitignored), never committed.
    pvmonitor.pl (Polish PV, consumption and heat-pump dashboards) has no API, export or reuse terms (checked
    2026-11-10) and is not used without the operator's written permission.

### Historical backfill on real data (spec 008)

One real-data window for evaluating strategies, **fetched once and replayed many times**. Real: PSE RCE prices,
archived forecast runs of record, and the truth weather the meters respond to. **Household meters stay synthetic**:
PV and heat pump are the models above driven by observed weather, base load and EV are the spec 007 and spec 004
models. No real meter data is used.

- **Real-data sites** = `live.locations` of the config (`config/generator.history.yaml`: PL-LOC-000001, Poznań,
  ICON-D2; PL-LOC-000004, a generated household in Radom, 51.14 N 21.25 E, DMI HARMONIE). Sites with a
  site-controlled battery are refused (it would dispatch on synthetic prices). Every other site stays synthetic.
- **Fetch** (`history/fetch.py`, `ems-gen history fetch --name N --start YYYY-MM-DD --end YYYY-MM-DD`, whole UTC
  days `[start, end)`): per site ERA5 (temperature, 10 m wind, GHI), the satellite's 10-minute GHI/DNI/DHI and every
  archived run issued in the window (3-hourly; `model_for` as live); every PSE `rce-pln` business day
  `local_date(start)` … `local_date(end − 1 s)`. Layout `data/recordings/N/`: `generator.yaml` (copy),
  `raw/<source>/…` with `.meta.json` (URL without secrets, fetch time, SHA-256) or `.missing.json` for a run the
  archive lacks, `manifest.json` (window, sites and models, grid cells, sources with attribution and licence,
  `complete`, ERA5T flag, git commit, versions, hardware). Refused before any request: a start before 2026-04-02 (the
  Single Runs archive), an end later than today − 6 days (ERA5 lag), an unknown site, more than 9,000 weighted
  Open-Meteo calls (split the window), or a rerun with another window, site set or config. Requests are ≥ 0.8 s
  apart (Open-Meteo) and ≥ 1 s (PSE); an ERA5 response without temperature or wind at `end` is refused, a 401/403
  on a customer host too. A final 429 stops with `complete: false` and exit 4; rerunning resumes (stored responses
  are never requested again). With `EMS_OPEN_METEO_API_KEY` the `customer-` hosts are used (exist, 401 without a
  key; checked 2026-11-13).
- **Archived runs**: Single Runs API, `run=YYYY-MM-DDTHH:MM`, `forecast_minutely_15=193` from the run time
  (`start_minutely_15` is rejected). `build_event` drops the step labelled at the run time and skips up to four
  leading incomplete steps (archived DMI runs lack radiation at +15 and +30 min: DMI's `dswrf` is "not present on
  first model step"), so an event may start after +15 min (`forecast_event(first_step=…)`; v1 only needs contiguous
  steps). Archived ICON-D2 runs end at 191 steps (cloud cover missing at +48 h). Live responses have no leading gap,
  so live events are byte-identical (pinned); a live response with up to four leading gaps now gives an event instead
  of none. Only 3-hourly runs are archived, for both models (hourly runs: "The requested model run is not
  available", checked 2026-11-13). Archived values differ slightly from what the live API served for the same run
  (GHI by a few W/m², temperature by 0.1–0.2 K), hence `source_system` `open-meteo-single-runs`.
- **Observed truth** (`history/observed.py`, pure; `ObservedTruth` is a `TruthSource`, no asset model changed):
  satellite 10-minute backward means labelled at their end (Open-Meteo docs; a clear-day check on the six clearest
  days of 2026-04-02 … 06-30 fits the end label within −2 … +5 min and the start label clearly worse, 2026-11-13)
  become quarter-hour means (weights 2/3 and 1/3); a null slot whose clear-sky GHI is 0 is night and counts as 0.
  ERA5 hourly instants are interpolated to each quarter's end. Fallbacks, in order: satellite GHI with missing or
  implausible DNI/DHI (DHI > GHI, DNI > extraterrestrial, DNI·cos z > GHI + 20 W/m²) → Erbs split; satellite GHI
  missing in daylight → ERA5's hourly clear-sky index (label `floor_hour(s) + 1 h`, clipped to 1.1) × the quarter's
  clear-sky GHI, Erbs split; ERA5 missing too → the synthetic truth; ERA5 temperature or wind missing → the synthetic
  truth. Fields no asset model reads at these sites (kt, cloud, 100/120 m wind, direction) keep the synthetic values.
  Whether the satellite's DNI/DHI are DWD retrievals or an Open-Meteo separation: VERIFY.
- **Build** (`history/build.py`, `ems-gen history build --name N [--seed S] [--run-id ID]`, offline): forecasts,
  prices (`build_day_event`, which keeps the latest row per slot since spec 008) and meters
  (`source_system` `synthetic-history`) of the real-data sites into `events.jsonl`, sorted by `emission_key`
  (availability, never before `start`, then stream, subject, interval, id), written atomically; then `build.json`
  (seed, `run_id` (default: the name), raw-set digest, input digests, events hash, counts, gap shares, flagged and
  incomplete price days, empty and malformed runs, the complete local delivery days, diagnostics, hardware).
  Measurements that depend on a fallback carry quality `estimated` and `quality_raw` `truth:erbs|era5|synthetic`
  (PV: any fallback; heat pump: temperature; grid flows at unmanaged sites: whenever PV or heat pump are). Refused:
  an incomplete fetch, a raw body whose SHA-256 changed, a coordinate change, or a rebuild under the same `run_id`
  with other inputs (G11: pass a new `--run-id`, no refetch needed). `ingested_at` is the recorded fetch time, so a
  rebuild is byte-identical on one machine with the same `uv.lock`, tzdata and config (meter values carry full float
  precision; `build.json` records the hardware). Price days are flagged when they carry more than one publication
  time or the latest is not on D−1; a flagged day published after the next day's publication sorts after it, the one
  per-key order exception (06).
- **Replay** (`ems-gen replay --name N [--background]`, `make replay NAME=N [BACKGROUND=1]`): a history recording
  must still match `build.json` (events hash, raw-set digest); `--speed` is refused (it paces by `ingested_at`, a
  fetch time). `--background` generates every other catalog site synthetically over the window with the recording's
  seed and `run_id`, synthetic prices off, merged by `emission_key`; refused if calibration or catalog changed since
  the build. A run manifest goes to `data/runs/`.
- **The default window** (`make history NAME=spring-2026 START=2026-04-02 END=2026-07-01`, fetched 2026-11-13;
  `data/recordings/spring-2026`, not committed): 1,535 expected requests, fetched in 24 min (1,458 s); 1,435 archived
  runs stored and 5 missing in the archive (Poznań 2026-06-10T15, 06-11T12; Radom 2026-06-10T21, 06-11T00,
  06-11T06); 7 stored ICON-D2 runs give no event (2026-04-12T03 and 05-13T06 have no 100/120 m wind; five runs on
  2026-06-10/11 are all null), so 1,428 forecast events; 91 price days, none flagged or incomplete; 17,280
  production and 17,280 consumption events; 36,079 events in all, 104 MB, built in 5 s,
  `events.jsonl` SHA-256 `75fc631fc194…` (after the 120 min change). Daylight quarters filled from ERA5: 2.07 % (Poznań), 2.08 % (Radom); Erbs
  split of satellite GHI: 0.04 %, 0.05 %; synthetic: 0. ERA5 fills clipped at 1.1: 21 % and 17 % (the satellite
  and ERA5 run above pvlib's Ineichen clear-sky on clear days, L9). Satellite zeros at low sun: 0.002 % of clear-sky
  irradiation. Complete local delivery days 2026-04-03 … 06-30. Replayed into `sim` topics with `--background` (18
  synthetic sites): 308,239 events in 64 s, `validate` clean.
- **Forecast availability** (`src/ems/contracts/availability.py`): Open-Meteo's `last_run_availability_time` minus
  the run time, measured 2026-10-20 … 11-13 on `api.open-meteo.com`: ICON-D2 85.2–106.7 min (6 runs; the 12 UTC
  run of 2026-11-13 took 106.7), DMI HARMONIE 168.9–170.4 min (3 runs); plus Open-Meteo's recommended 10-minute
  wait, rounded up to a quarter-hour: 120 and 195 min (synthetic: 120). The short sample of 2026-11-13 (spec
  008/T07) found the 106.7 min run and raised ICON-D2 from 105; a planned 7-day sample was ended early by the
  user. Spring 2026 delays cannot be measured afterwards (assumed alike, L8).

Attribution: Open-Meteo data is CC BY 4.0 (NOTICE next to fixtures; "Weather data by Open-Meteo.com"); ERA5 is CC BY
4.0 on the CDS since 2025-07-02 ("Contains modified Copernicus Climate Change Service information 2026", DOI
10.24381/cds.adbb2d47); the satellite product's terms (DWD, EUMETSAT MTG via Open-Meteo) are VERIFY, so only
hand-written satellite bodies appear in tests and raw data is never committed (L6); ENTSO-E
day-ahead prices are not open data, so only hand-written fixtures are committed; PSE data is kept only as a few
test fixtures.

### Scale, background batteries, faults, stats (spec 003)

- **Fleets**: `scale.synthetic_locations` sites, processed in slices of `scale.site_chunk` (default 2,000);
  slicing never changes a value (row-wise maths). Batteries and cumulative registers carry their state
  across consecutive chunks of a run.
- **Background batteries**: `scale.site_controlled_bess_share` of generated batteries are `control: site`
  (a separate hash decides, so the share never changes other catalog draws). Their rule policy (PV surplus,
  cheapest 4 night slots, evening 17–21 local and top-10 % prices) drives the pure domain battery
  (`src/ems/domain/battery.py`); events carry `bess` charge (consumption), discharge (production), an instant
  SoC reading (quantity `soc`, direction `charge`) and grid flows that close the balance.
- **Faults** (`ingest/faults.py`, `faults:` in `generator.yaml`, `ems-gen run --faults`): missing, duplicate
  (identical event twice), late (up to `late_max`, same event time and id), stuck values, register resets
  (with `meters.emit_registers`), estimated/substituted flags; each decision is a keyed uniform of (seed,
  event id, kind), so runs are reproducible.
- **Stats** (`ems-gen stats`, `make stats`): per topic and key: count, partitions, event-time range, gaps
  (interior missing quarter-hours), duplicate ids, late arrivals (after a newer interval of the same key).
- **Benchmark** (`make bench`, `jobs/benchmark.py`; results in `experiments/results/benchmark-*/results.json`).
  2026-10-20 on the author's Mac (arm64, 10 CPUs), one simulated day:

  | Sites | Arrays | Events built + serialised | Payload | Kafka produce (1,000 sites) |
  |---|---|---|---|---|
  | 1,000 | 0.8 s | 174,562 in 14 s (12k/s) | 623 MB | 171,202 events in 35 s (4.9k/s), 0 failures |
  | 10,000 | 3.0 s | 1.74 M in 199 s (8.7k/s) | 6.2 GB | — |

  Per-event object building and validation dominate, not the models; forecasts are ~70 % of the bytes.
  Rerun 2026-10-20 after spec 004 (households with EVs; `benchmark-20261020T204019Z`): 1,000 sites 174,562
  events in 15.0 s, 627 MB; 10,000 sites 1.74 M in 210 s, 6.3 GB; produce 171,202 in 33 s (5.2k/s), 0 failures.
  Rerun 2026-11-11 after spec 007 (`benchmark-20261111T005219Z`): same event counts; arrays 0.77 s (1,000 sites)
  and 4.4 s (10,000 sites; was 3.0 s: the shared and phase forecast errors and the impulse draws); 1,000 sites
  174,562 events in 14.9 s (11.7k/s), 627 MB; 10,000 sites 1,736,578 in 194 s (8.9k/s), 6.3 GB; produce 171,202 in
  33 s (5.1k/s), 0 failures. Event building still dominates.

### Site types and assets (specs 004, 005; frozen, ADR 0016)

The generator models PV prosumer sites only; PV is the only generation source. Spec 005 removed wind farms,
industrial sites with CHP, biogas and biomass plants, run-of-river hydro, the turbine curves and the outage model,
and froze the range: a new site type or asset kind needs its own spec and ADR.

| Site type (`scale.site_mix`) | Assets | Tariff | Region weight |
|---|---|---|---|
| `household` (the only type when `site_mix` is empty) | PV 70 % (2.5–20 kWp), heat pump 30 %, EV 20 % (overnight, 3.7–11 kW), battery 30 % (`ems` or `site`) | G11 / G12 | population |
| `commercial` | PV 60 % (20–150 kWp), EV 50 % (daytime, 22 kW); G25 base load 15–150 MWh/yr | C11 or C21 (the 40 kW / 63 A rule) | population |
| `solar_farm` | PV 1–20 MWp (tilt 25°, DC/AC 1.25); small auxiliary load | C21 | PV weight (south) |

- The type comes from its own hash of (catalog seed, location) and household EVs from another, so households keep
  all earlier draws. Commercial sites and solar farms are background sites (never EMS-managed).
- Spec 009 (config, not wire contract): `TariffGroup.G12W` (`G12w`, profile H25) for hand-defined sites only;
  `Site.net_billing: {first_feed_in, rce_from}`, set to the constant `DEFAULT_NET_BILLING` (first feed-in
  2024-07-01, RCE) on every generated household and None on plants, so no existing value changes (a pinned
  household digest checks it). The test site has first feed-in 2024-09-01. The catalog parquet gains a
  `net_billing` column (JSON).
- An unknown name in `scale.site_mix` (for example `wind_farm`) fails at config load and in `build_catalog`; an
  unknown asset `kind` or site type in `config/sites.yaml` fails at catalog load.
- **EV** (`synthetic/ev.py`): one session per site and local day (overnight 18:30 → 07:00, daytime 08:30 →
  16:30, normal jitter), charging at the charger limit from arrival until the lognormal need is met or the car
  leaves.
- Production events carry `pv` (and `bess` discharge at site-controlled batteries); consumption events `load`,
  `heat_pump`, `ev_charger`, `bess` charge and SoC, and grid flows at unmanaged sites.

### How to add an asset model

The range is frozen (ADR 0016), so this needs a spec and an ADR. Add the `asset_type` value to the contract first
(additive, minor version bump, refresh `schemas/` but not `schemas/baseline/`), then parameters in
`catalog/sites.py` (new `kind`), a vectorised function in `synthetic/` returning `[n_loc, n_steps]` kWh from the
truth arrays, the array in `Chunk.generation()` or `Chunk.consumption()` so grid flows include it, a builder line
in `generator._meter_builders` (production or consumption by direction), and property tests for bounds and
balance.

## What to build

- Stages 2 and 3 (`01-architecture.md`): consume these topics into Restate through the Kafka ingress
  integration, then join and order them per household (ADR 0017). Stage 3 adds the schedule constants (forecast
  run cadence, price publication window) to `ems.contracts.availability` with a test of the generator against
  them; stage 4 adds pacing for history replays.
- Later, if needed: weather-cell keyed forecasts (G6), DSO standard profiles for C-tariff shapes.

## Prerequisites

- `make up` (Kafka stack).

## Open questions

- VERIFY: voivodeship bounding boxes are approximate (0.1°); refine from the GUGiK PRG register if spatial
  precision matters.
- Faker output can change with its version (pinned 40.40.0; golden catalog test).
