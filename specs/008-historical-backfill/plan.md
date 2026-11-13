# Implementation Plan: Historical backfill on real data

**Branch**: `008-historical-backfill` | **Date**: 2026-11-13 | **Spec**: [spec.md](spec.md)

## Summary

Three steps, one new package `src/ems/ingest/history/`, no new dependency or infrastructure:
- **fetch** (`history/fetch.py`, network): archived forecast runs, satellite and ERA5 series and PSE days for the
  real-data sites go into `data/recordings/<name>/raw/` under deterministic names. Fetch is paced within the free
  tier, crash-safe and resumable;
- **build** (`history/build.py`, offline): forecast events through the existing `build_event`, price events through
  `build_day_event`, meters through the existing generator driven by a new `ObservedTruth` source
  (`history/observed.py`, pure). Everything goes into `events.jsonl` in availability order;
- **replay** (existing): the topic preflight is added, plus a hash check for history recordings and
  `--background` for the synthetic fleet.

Small edits elsewhere:
- a per-model availability table in `src/ems/contracts/availability.py` (new, no schema);
- an optional first-step offset on `contracts.examples.forecast_event`;
- a leading-gap skip and a `source_system` parameter in `live/openmeteo.build_event`;
- a page-level fetch in `live/pse.py`;
- a `meter_source` parameter in the generator's meter builders;
- atomic, named raw files in `sources/recording.py`;
- `config/generator.history.yaml` (new);
- a P2 fit in the calibration job.

## Principles Check (PASS)

| # | Touched? | How it is satisfied | Shown by |
|---|---|---|---|
| P1 | yes | spec approved 2026-11-13; this plan and the tasks each get their own approval; every change cites a task | review |
| P2 | no | `src/ems/domain/` untouched. `history/observed.py` is pure (arrays in, arrays out); I/O lives in `history/fetch.py` only | hook, review |
| P3 | yes | `ems-gen` stays the only writer of external inputs (replay publishes the recording); observed truth is internal and never published. Within a recording, fetch alone writes `manifest.json` and build alone writes `build.json` and `events.jsonl` | AC-008-11 test, review |
| P4, P5, P8, P13 | no | no actor, handler, battery or tracing code; R1–R7 do not apply | — |
| P6 | yes | ids follow the 08 rule from source data and a non-null `run_id`; replay keeps them; build is byte-deterministic on one machine (section 7) | AC-008-06, -11, -12, -19 tests |
| P7 | yes | build and replay use event time only. The wall clock appears only in fetch (pacing, fetch times), a batch job outside experiments. `ingested_at` comes from recorded fetch times. `replay --speed` (paced by `ingested_at`) is refused for history recordings | AC-008-06 test, replay test |
| P9 | yes (guard) | no schema, topic, key or identity change. New: a consumer-side availability table in `contracts/` (no field) and two `source_system` values within the existing pattern | schema export and baseline checks in `make ci` |
| P10 | yes | `ems-gen history fetch` and `build`, and `jobs/calibrate.py forecast-error`, are CLIs over artefacts | review |
| P11 | yes | no new setting. `EMS_OPEN_METEO_API_KEY` (optional) switches to the `customer-` hosts with the `X-Api-Key` header, as the live adapter does; the three customer hosts exist (401 without a key, checked 2026-11-13) | unit test |
| P12 | no | httpx, tenacity, numpy, pvlib, ruamel.yaml are already locked | `uv lock --check` |
| P14 | yes | the recording holds its generator config, raw responses with SHA-256, missing-run markers, a raw-set digest, seed, config digests, the `events.jsonl` hash, git commit, versions and hardware; replay refuses a recording whose hashes no longer match | AC-008-01, -06 tests, replay test |
| P15 | yes | 00, 06, 08, 09, 11, 12, 13, README, `CLAUDE.md` (commands table), `.claude/rules/ingest.md`, `weather.yaml`, the generator docstring; no ADR (decisions in 00) | review |
| P16 | yes | no experiment runs; fetch and build durations and request counts go into the manifests | SC-002 |

## Contract changes (`08-contracts.md`)

None on the wire. Schema export and the baseline check stay green (AC-008-11). Additions, all non-schema:
- **`src/ems/contracts/availability.py`** (new, standard library only):
  - the forecast availability delay per `forecast_model`;
  - the measured maxima it rests on;
  - `available_at(event, meter_delay=timedelta(0))`.

  Producers raise on an unknown model; the ingestion spec chooses the consumer behaviour (for example the most
  conservative delay plus a counter). The 08 rule becomes "forecasts: `issued_at` + the delay of
  `payload.forecast_model`" (was a flat 90 min).
- **`contracts.examples.forecast_event(..., first_step=0)`**: an optional offset so an event can start after
  +15 min. v1 already allows it: contiguous steps, an envelope spanning them, `lead_time = valid_end − issued_at`
  (`envelope.py:326-340`). The 08 payload summary changes from "192 PT15M steps" to "up to 192 contiguous PT15M
  steps from the first complete one; `interval_start` may lie after `issued_at`".
- **`source_system` values**: `open-meteo-single-runs` (archived forecasts) and `synthetic-history` (meters at
  real-data sites). Prices keep `pse-rce`.

## Design

### 1. Recording layout (AC-008-01, -03, -06)

```
data/recordings/<name>/                      gitignored
  generator.yaml                             copy of the config fetch used (build and replay --background read it)
  raw/open-meteo-single-runs/<location>/<YYYYMMDDTHHMM>.json + .meta.json | .missing.json
  raw/satellite/<location>.json + .meta.json
  raw/era5/<location>.json + .meta.json
  raw/pse-rce/<YYYY-MM-DD>-p<NN>.json + .meta.json      pages zero-padded
  manifest.json                              written only by fetch
  build.json                                 written only by build, last
  events.jsonl                               written only by build
```

- **Atomic writes**: every file is written to `<file>.tmp` and renamed, so a crash leaves either the old file or
  the new one.
- **Raw files**: `Recorder.raw` moves into `write_raw(root, fetched, secrets, name=None)`. `Recorder` calls it with
  its sequence number, so spec 002 recordings keep their layout. The meta file gains `sha256` of the stored
  (redacted) body.
- **`manifest.json`** (fetch):
  - contents: window; market (`rce-pln`); real-data sites (id, lat, lon, elevation, model); the `generator.yaml`
    digest; per source the endpoint, parameters (no secrets), attribution and licence; counts; missing runs;
    `complete`; git commit; versions; hardware; durations;
  - it is derived from the raw folder (meta files and `.missing.json` markers), so a crashed fetch loses nothing;
  - `complete` is true only when every expected request has a stored response or a missing marker.
- **`build.json`** (build):
  - seed and `run_id`;
  - the raw-set digest: SHA-256 over sorted raw names and their meta `sha256`, missing markers included;
  - digests of `generator.yaml`, `config/calibration/*.yaml`, `config/sites.yaml` and the mappings used;
  - the `events.jsonl` SHA-256, counts per stream, gap shares, flagged and incomplete price days, malformed runs;
  - durations, git commit, versions, hardware.
- **Refusals**: a fetch rerun under an existing name with a different window, site set or `generator.yaml` digest.
  A build of an incomplete fetch. A replay of a history recording whose `events.jsonl` or raw set no longer matches
  `build.json`.

### 2. Fetch (`history/fetch.py`; AC-008-01 … -04)

**Checks before any request (AC-008-02).** Each failure means exit 2 with the reason:
- `start` and `end` are whole UTC days, with `start < end`;
- `start ≥ 2026-04-02` (`SINGLE_RUNS_ARCHIVE_START`);
- `end ≤ today_utc − 6 days`, where "today" is injected for tests;
- every id in `live.locations` is in the catalog (`load_sites`), and none has a site-controlled battery (it would
  dispatch on the synthetic price curve while the recording carries real RCE);
- the estimated weighted Open-Meteo calls still needed are at most 9,000; above that the message says to split the
  window.

**Requests**, cheap first, so a bad window fails fast:

| Source (raw folder) | Request | Per site and 90-day window |
|---|---|---|
| `era5` | `archive-api.open-meteo.com/v1/archive`: lat, lon, elevation, `start_date`/`end_date` = the dates of `start` and `end` (covers the hour at `end`), `hourly=temperature_2m,wind_speed_10m,shortwave_radiation`, `models=era5`, `wind_speed_unit=ms`, `timezone=UTC`, `timeformat=unixtime` | 1 request, ≈ 6.5 calls |
| `satellite` | `satellite-api.open-meteo.com/v1/archive`: lat, lon, the same dates, `hourly=shortwave_radiation,direct_normal_irradiance,diffuse_radiation`, `models=satellite_radiation_seamless`, `temporal_resolution=native`, `timezone=UTC`, `timeformat=unixtime` | 1 request, ≈ 6.5 calls |
| `pse-rce` | `PseSource.fetch_pages(day)` (new, split out of `fetch_rows`) for business dates `local_date(start)` … `local_date(end − 1 s)` (for whole-UTC-day windows the next day is always published after `end`) | 1 request per day and page |
| `open-meteo-single-runs` | `single-runs-api.open-meteo.com/v1/forecast` with `single_run_params(site, model, issued_at)` (new in `openmeteo.py`): the live parameters without `start_/end_minutely_15`, plus `run=YYYY-MM-DDTHH:MM` and `forecast_minutely_15=193`. Runs every 3 h (00, 03, … UTC) in `[start, end)`; `model_for(site)` picks the model | 8 a day, 1 call each |

- **Satellite model**: the seamless selection served regular 10-minute DWD MTG steps for the whole probe window.
  Build checks that every step is 600 s and fails naming the site otherwise. The explicit DWD MTG model name is
  VERIFY.
- **Run cadence**: only 3-hourly runs are archived, for both models. Hourly runs return "The requested model run is
  not available" (`icon_d2` 2026-06-15T01 and 2026-11-10T01; `dmi_harmonie_arome_europe` 2026-06-15T01 and T04;
  probed 2026-11-13). Both models' `meta.json` gives `update_interval_seconds` 10800.
- **Missing runs (AC-008-03)**: an HTTP 400 whose `reason` starts with "The requested model run is not available"
  writes a `.missing.json` marker, and fetch continues. Any other 4xx aborts with the redacted error. Missing runs
  are retried on the next fetch.
- **Resume (AC-008-03)**: a request is skipped when its raw file has a meta file with status 200.
- **Pacing (AC-008-04)**:
  - Open-Meteo requests are at least 0.8 s apart (≤ 75 a minute, ≤ 4,500 an hour). Calls are weighted
    max(1, variables/10) × max(1, days/14), as published.
  - The up-front estimate (≤ 9,000 per fetch) keeps one fetch below the daily limit.
  - PSE requests are at least 1.0 s apart (no published limit, VERIFY).
  - A `Pacer(min_interval_s, clock, sleep)` is injected for tests.
- **Server limits**: if `Fetcher` still gets 429 after its retries (other traffic from the same IP, or two
  fetches on one day), fetch writes the manifest (`complete: false`), prints that a rerun resumes, and exits 4.
  A 401 or 403 (for example a paid key whose plan excludes the Historical or Satellite API) is not retried; fetch
  exits 2 naming the host. The monthly free-tier cap (300,000) is far away.
- **ERA5 completeness**: after the ERA5 response is stored, fetch checks that temperature and wind exist for every
  hour in `[start, end]`. If one is missing (the daily ERA5 update has not reached `end` yet), fetch exits 2 naming
  the hour, rather than letting build fill it with synthetic values.
- **Grid cells**: the manifest records each response's `latitude`, `longitude` and `elevation`, so a fetch after the
  ICON-D2 native-grid change (L1) is visible.
- **ERA5T**: when `end` lies within three months of the fetch, the manifest notes that ERA5 values are preliminary
  (ERA5T, possibly revised 2–3 months later). The default window, fetched in November, is final data.
- A 90-day window for one site is about 735 weighted calls: about 12 minutes of fetching.

### 3. Forecast events from archived runs (`live/openmeteo.py`, `contracts/examples.py`; AC-008-05, -11)

`build_event(site, model, issued_at, payload, mappings_dir, ingested_at, run_id=None, source_system="open-meteo")`:
1. drop rows with `valid_end ≤ issued_at` (the archive's step labelled at the run time);
2. skip leading rows with any missing value, at most `LEADING_GAP_MAX = 4` (one hour); with more, there is no event;
3. keep the complete prefix (as today), checking `valid_end = issued_at + (first + k + 1)·15 min`;
4. `forecast_event(..., first_step=first, source_system=source_system,
   source_record_ref=f"{source_system}:{model.request}:{issued_at.isoformat()}")`. This is today's format with the
   source as a parameter.

Live behaviour:
- A live response starts at +15 min with no leading gap, so its event is byte-identical to before. A test pins the
  live fixture's event JSON (minus `ingested_at`) from the base commit before the code changes.
- One edge case changes: a live response with up to four leading gaps now gives an event instead of `None`. This is
  harmless and documented in 13.

Archived ICON-D2 runs give 191 steps from +15 min. Archived DMI runs start with the step ending at +45 min
(190 steps).

**Why not DMI everywhere.** DMI covers all of Poland, so one model for every site would be simpler and would not
depend on the ICON-D2 archive. A probe on 2026-11-13 compared 40 archived runs per model at Poznań (runs 00, 06, 12
and 18 UTC on ten days in May 2026), with satellite GHI and ERA5 temperature and wind as truth:

| Lead | GHI 15-min RMSE (bias), W/m², ICON-D2 · DMI | T RMSE vs ERA5, K, ICON-D2 · DMI | 10 m wind RMSE vs ERA5, m/s, ICON-D2 · DMI |
|---|---|---|---|
| 0–6 h | 92 (−39) · 103 (−28) | 1.50 · 2.33 | 0.99 · 0.71 |
| 6–24 h | 115 (−47) · 136 (−61) | 1.73 · 2.62 | 1.07 · 0.84 |
| 24–48 h | 131 (−48) · 149 (−52) | 1.66 · 2.43 | 1.21 · 0.96 |

- ICON-D2's GHI error is 11–19 % lower; on hourly means at 6–24 h it is 103 vs 129 W/m².
- ICON-D2's 15-minute GHI varies within the hour as much as the satellite does (mean within-hour sd 41 vs 42 W/m²).
  DMI's is smoothed (29 W/m²), because its 15-minute values are interpolated from hourly output (F7a).
- ICON-D2 runs are usable 75 min earlier (120 vs 195 min).
- Temperature and wind are compared with ERA5, itself a smooth 25 km reanalysis, so those columns rank the models
  against ERA5, not against reality.

The live adapter uses ICON-D2 inside its domain, and the thesis evaluates strategies on what the live system would
see. So the history uses `model_for`, as live. A DMI-only build would make forecast-driven strategies look worse at
western sites than they would be live. The ICON-D2 deadline (L1) only means fetching before 2026-11-30. Raw data
on disk do not depend on the archive afterwards. DMI matters only for windows fetched later (a model override is
later work).

Caveats: one site, one month, 40 runs per model, and truth that is itself an estimate (both models sit 30–60 W/m²
below the satellite, consistent with L9).

### 4. Availability (`contracts/availability.py`, new; AC-008-07, -08)

```python
MEASURED_MAX_MIN = {"icon_d2": 106.7, "dmi_harmonie_arome_europe": 170.4}  # F6 + T07, meta.json, 2026-10-20 … 11-13
FORECAST_DELAY_MIN = {"synthetic": 120, "icon_d2": 120, "dmi_harmonie_arome_europe": 195}
def forecast_delay(model: str) -> timedelta          # unknown model -> ValueError naming it
def available_at(event, meter_delay=timedelta(0)) -> datetime
    # meters: interval_end + meter_delay; forecasts: issued_at + forecast_delay; prices: issued_at
```

- **What the rule measures**: when a run was publicly usable. That is Open-Meteo's `last_run_availability_time`
  plus the 10-minute wait Open-Meteo itself recommends (docs/model-updates), rounded up to a quarter-hour. 08
  states it so.
  - The live adapter also polls `meta.json` every 300 s, so its own arrival can trail by up to 5 more minutes
    (ICON-D2 worst case 106.9 min).
  - That is the adapter's lateness, not information a consumer could not have had, so it is not look-ahead.
    `meta_poll_s` stays 300.
- **The delay sample (T07)**: a read-only sample of both models' `meta.json` on the free host the live adapter
  uses, every 5 min, from a throwaway script outside the repo. Outcome (2026-11-13): within hours it caught the
  ICON-D2 12 UTC run at 106.7 min (DMI 170.4 min). The user raised ICON-D2 and synthetic to 120 min and ended the
  sample early instead of running it for 7 days. Recorded in 13.
- **One definition**: `generator.emissions` uses `forecast_delay("synthetic")`, and `forecast.availability_delay_min`
  leaves `weather.yaml` (a comment points to the table).
- **Synthetic forecasts** keep their values and ids but are emitted 15 min later, which reorders events within a
  backfill (see "Existing tests").

### 5. Prices (`live/pse.py`, `history/build.py`; AC-008-09, -10, -12)

- `PseSource.fetch_pages(day) -> list[Fetched]` holds today's paging loop. `fetch_rows` maps its pages, so live
  behaviour is unchanged.
- **`build_day_event`** keeps, per slot, the row with the latest `published`. Today it keeps whichever row comes
  last in API order while stamping the event with the latest time, so a day with two versions could carry the
  earlier prices. The fix applies to live and history alike and changes nothing when each slot has one row.
  A merged day (unrevised slots plus revised ones) is what was known at the latest publication, so it is complete.
- **Build** reads the pages in numeric order, maps them with `pse.rce.v2`, groups rows by `business_date` and calls
  `build_day_event` (`issued_at` = the latest `publication_ts_utc`, as live).
  - A day is written if it overlaps `[start, end)` or its `issued_at < end`. Days published before `start` sort to
    `start`.
  - `IncompleteDay`: counted, no event.
  - **Flagged day**: more than one publication time, or the latest not on D−1 local. It is counted and keeps the
    latest `issued_at`.
    - If that publication comes after the next day's, the day sorts after it, so `interval_start` on `PL:RCE` goes
      back once. Live revisions do the same.
    - This is the one documented exception to per-key order (06), and `validate` reports it.
    - The default window has no flagged day (F13).
- **Identity (AC-008-12)**: the same fixture through the live `PseSource.poll` path and through build, with the
  same `run_id`, gives equal events.
- **DST (AC-008-10)**: `local_day_slots` already gives 100 slots on 2026-10-25. A real PSE fixture for that day is
  added (few fixtures, L3).

### 6. Observed truth (`history/observed.py`, pure; AC-008-13 … -16)

Each site's raw responses are parsed into two series:
- satellite: 10-minute label times with GHI, DNI and DHI (NaN for null);
- ERA5: hour times with temperature, 10 m wind and GHI.

| Function | Rule |
|---|---|
| `quarter_means(labels, values, cs10, t)` | The value labelled L is the mean over `[L − 10 min, L)` (Open-Meteo docs, and the clear-day check of 2026-11-13: on the six clearest days of the probe the best-fitting offset lies within −2 … +5 min of the end label, and the start label fits clearly worse). A null 10-minute value whose own clear-sky GHI (`cs10`, at that slot's midpoint) is 0 counts as 0, so sunrise and sunset quarters are not gaps. The quarter `[s, s + 15 min)` is the overlap-weighted mean of the two 10-minute intervals it touches (weights 2/3 and 1/3). NaN if either is still null |
| `instants_at_end(hours, values, t)` | Linear interpolation of hourly instants at `s + 15 min`; NaN if a neighbour is missing |
| `era5_fill_ghi(hours, ghi_h, cs_q, t)` | The ERA5 value labelled H is the mean over `[H − 1 h, H)`. The quarter `[s, s + 15 min)` uses the label `floor_hour(s) + 1 h`, and "that hour's four quarters" are those starting at H − 60, − 45, − 30 and − 15 min. The hour's clear-sky index = GHI(H) / the mean clear-sky GHI of those four quarters (0 when that mean is below 1 W/m²), clipped to [0, 1.1]. Fill = index × the quarter's clear-sky GHI. Needs hour-aligned axes |
| `check_split(ghi, dni, dhi, zenith, dni_extra)` | Per quarter: DHI ≤ GHI, DNI ≤ the extraterrestrial DNI, and DNI·cos z ≤ GHI + 20 W/m². A quarter that fails keeps the satellite GHI and gets an Erbs split. Satellite DNI/DHI may be Open-Meteo's own separation (provenance VERIFY), and 15-minute averaging with midpoint geometry can break closure at low sun |

`ObservedTruth(location_by_key, series, fallback, params)` is a `TruthSource` in the pattern of
`LiveForecastTruth`. It computes `truth = fallback(sites, axis)`. Then, for each row with a series, it overlays
per quarter only what the asset models read (`pv.py`: geometry, `ghi`, `dni`, `dhi`, `temp_c`, `wind10`;
`heat_pump_kwh`: `temp_c`):
- **radiation**:
  - satellite GHI, DNI and DHI as given, if they pass `check_split`;
  - satellite GHI present but DNI or DHI missing, or failing `check_split`: Erbs split of the satellite GHI
    (`decompose`);
  - satellite GHI missing while the clear-sky GHI > 0: GHI from ERA5 (`era5_fill_ghi`) and an Erbs split, or the
    fallback's radiation if ERA5 is missing too;
  - missing with clear-sky GHI 0: 0, not a gap;
- **temperature and 10 m wind**: ERA5 at the interval end, else the fallback's;
- **untouched fields**: `kt`, `cloud_pct`, `wind100`, `wind120`, `wind_dir` and the national arrays keep the
  fallback's values (no asset model reads them at real-data sites).

Further rules:
- The axis must start and end on whole hours. That holds for whole-day windows and day chunks; otherwise it raises.
- The output contains no NaN; this is asserted, because `pv.py` would turn a NaN into 0 kWh.
- `ObservedTruth.masks(sites, axis)` returns, per site, boolean arrays `radiation_erbs`, `radiation_era5`,
  `radiation_synthetic`, `temperature_synthetic` and `wind_synthetic`, from the same function. Build uses them for
  quality and shares without touching the asset models.
- Diagnostics for `build.json`:
  - the share of ERA5 fills whose index hit the 1.1 clip (the satellite runs 11–13 % above Ineichen on clear days,
    L9);
  - the share of clear-sky irradiation in quarters where the satellite reports 0 while the clear-sky GHI > 0 (low
    sun).
- The PV, heat-pump, base-load and EV modules are not edited (AC-008-15).

### 7. Build (`history/build.py`; AC-008-05 … -12, -16)

1. **Checks**. Read `manifest.json` (refuse unless `complete`) and the recording's `generator.yaml`, then rebuild
   the catalog. Refuse (exit 2) if a real-data site's coordinates differ from the manifest, or if a raw body's
   SHA-256 differs from its meta.
2. **Forecasts**: every stored run → `build_event(..., source_system="open-meteo-single-runs",
   ingested_at=<meta fetched_at>)`. A run giving `None`, or a malformed one (`ValueError`), is counted, not fatal.
3. **Prices**: section 5. `ingested_at` = the latest fetch time of the day's pages.
4. **Meters**:
   - the model is `SyntheticModel(real_sites, seed, config_dir, cfg.site_chunk, truth_source=ObservedTruth(...))`,
     with the `SyntheticTruth` fallback;
   - events come from `emissions(model, meters_cfg, start, end, run_id, meter_source="synthetic-history")`.
     `meters_cfg` turns forecasts and prices off, and `meter_source` is a new parameter (default `"synthetic"`)
     threaded through `_meter_builders` to `measurement_event`;
   - `ingested_at` = the latest fetch time of the site's satellite and ERA5 responses;
   - **quality**, through a `model_copy` helper (ids do not change):
     - PV gets `estimated` where any radiation, temperature or wind mask is set;
     - the heat pump gets `estimated` where the temperature mask is set;
     - at sites that emit them, grid import/export and site-controlled battery measurements get `estimated`
       whenever PV or the heat pump does;
     - `quality_raw` names the fallback: `truth:erbs`, `truth:era5` or `truth:synthetic`, the strongest one used;
     - base load and EV are unaffected.
5. **Order**: `emission_key(event, start, meter_delay)` = (max(`available_at(event, meter_delay)`, `start`),
   `STREAM_ORDER`, subject, `interval_start`). It is a total order, independent of file listing or set iteration,
   and replay imports the same function.
6. **Write**: `events.jsonl` (temp file plus rename, `to_json_bytes` per line), then `build.json` last.
   `build.json` also records the range of complete local delivery days. A whole-UTC-day window starts and ends at
   01:00 or 02:00 local, so its first and last local days are partial (22 h and 2 h in summer). 11 states that
   per-day scoring uses only complete days.

Options and scope:
- `--seed` overrides the config seed. `--run-id` (default: the recording name) is a build option, so a new
  `run_id` needs no refetch. Build refuses to rewrite an existing `events.jsonl` under the same `run_id` when any
  digest changed (risk G11) and asks for a new `--run-id`.
- **Determinism scope**: byte-identical on one machine with the same `uv.lock`, tzdata and config. Meter values
  carry full float precision, and numpy or pvlib may differ across CPUs, so `build.json` records the hardware and
  13 states the scope.

### 8. Replay (`runner.replay`; AC-008-17 … -19)

- **Preflight**: `runner.preflight` is split. `preflight_topics(settings, lo, hi, now)` holds the topic part, and
  `run` keeps calling it through `preflight`.
  - `replay` computes `lo` and `hi` from `record_timestamp(event)` over the recording, plus `record_time_bounds` of
    the background run.
  - It calls `preflight_topics` and raises `PreflightError` (CLI exit 1) before any publish.
  - `check_topics=False` exists for unit tests, as in `run`. `tests/unit/ingest/test_recording_hybrid.py:131`
    passes it.
  - This covers spec 002 recordings too.
- **History recordings** (`kind: ems-gen-history`):
  - replay checks the `events.jsonl` SHA-256 and the raw-set digest against `build.json`;
  - it refuses `--speed`, which paces by `ingested_at`, here a fetch time.
- **`--background`** (history recordings only):
  - config from the recording's `generator.yaml`; seed and `run_id` from `build.json`;
  - refused if the current `config/calibration/*.yaml` or `config/sites.yaml` digests differ from `build.json`,
    since the background is generated now (G11);
  - sites = the catalog minus the real-data sites;
  - `mask_synthetic(cfg, price_live=True)` turns synthetic prices off and keeps synthetic forecasts;
  - `emissions(...)` over the window is merged with the recording's events by `emission_key` through
    `heapq.merge`, so the stream interleaves as in hybrid mode;
  - a run manifest is written with `runner.write_manifest` (for every history replay, with or without
    `--background`).
- Make: `make replay NAME=… [BACKGROUND=1]`.

### 9. CLI, config and make

- `ems-gen history fetch --name N --start YYYY-MM-DD --end YYYY-MM-DD [--config config/generator.history.yaml]`:
  exit 0 complete, 2 refused, 4 incomplete after a server 429 (rerun later). The market is RCE only (one market,
  no option).
- `ems-gen history build --name N [--seed S] [--run-id ID]`: exit 0, or 2 on an incomplete fetch, a mismatch, or
  the same `run_id` with changed digests.
- `ems-gen replay --name N [--background]`: exit 1 if the preflight refuses; exit 2 on a hash mismatch or `--speed`
  for a history recording.
- **`config/generator.history.yaml`** (new; `generator.yaml`'s values otherwise):
  - `live.locations: [PL-LOC-000001, PL-LOC-000004]`: the Poznań test site (ICON-D2) and a generated household in
    Radom (51.14 N, 21.25 E; PV 13.7 kWp; EMS-managed battery; DMI), from the default catalog (seed 7);
  - `synthetic_locations: 20`, so the background fleet has 18 sites;
  - `site_controlled_bess_share: 0.0` (as the default), so no background battery dispatches on synthetic prices;
  - a test checks that both sites resolve and the background is non-empty.
- Limits stated in 11: background sites use synthetic weather and forecasts, uncorrelated with the real prices and
  weather of the window. They exist for scale and systems metrics, not for energy results.
- `make history NAME=… START=… END=…` runs fetch, then build, with that config.
- README: a short section with the attribution. `CLAUDE.md`: a row for `make history` and `BACKGROUND=1` in the
  commands table.

### 10. Forecast error (P2; `ingest/calibration.py`, `jobs/calibrate.py forecast-error`; AC-008-20)

- **`fit_forecast_error(forecasts, truth, weather_params)`** is pure; `truth` maps each site to a `TruthSeries`
  (`t0`, `ghi`, `cs_ghi`, `temp_c`, `wind10` on the quarter-hour grid, NaN where not observed). It pairs values per
  (model, site, run, step), with lead L = `valid_end − issued_at` and the truth at the quarter of `valid_start`:
  - kt: daylight only (clear-sky ≥ 50 W/m²), kt clipped to [0.03, 1.1]; the logit-space error (`to_x`) only where
    both kt lie strictly inside the bounds (the logit explodes at them, and the satellite often exceeds Ineichen);
  - temperature: forecast − truth;
  - wind: log(forecast / truth) at 10 m, where truth ≥ 0.5 m/s;
  - 3-hour lead bins with their mean lead; the correlation of the errors of runs issued 3 h apart for the same
    valid time; hourly kt error over four complete daylight quarters of one clock hour;
  - fit sd = base + per_h·L by count-weighted least squares on each bin's mean lead.
- **The CLI** reads the forecasts from `events.jsonl` and the observed truth through
  `history.build.observed_truth_series` (quarters filled by a fallback are NaN; no network). It writes
  `candidates/forecast-error.yaml` through `write_candidate`, with `weather.yaml`'s values beside the fit, the
  recording name, window, sites, events and `build.json` hashes, `run_id` and attribution. It never writes
  `weather.yaml`.
- **What else the measured "error" contains**, stated in the candidate's caveats. Its sds are upper bounds to
  review, not values to adopt as they are:
  - satellite retrieval error (spec assumption);
  - ERA5 error for temperature and wind (a 25 km reanalysis, linearly interpolated, with a damped diurnal range),
    which inflates the wind log-ratio sd most;
  - for DMI, the smoothing of 15-minute radiation interpolated from hourly output (F7a), hence the hourly kt error;
  - the timing (phase) error inside the measured kt total, while `weather.yaml`'s kt sd sets only the amplitude.
- Two tasks (test, implementation), placed after SC-002 so they never block the deadline.

## Task order (for tasks.md)

ICON-D2 archives may disappear after 2026-11-30 (L1), so the order is:
1. fetch first: `write_raw`, `single_run_params`, `fetch_pages`, `Pacer`, `history fetch` and its tests;
2. run the real fetch of the default window for both sites, target 2026-11-20. If the fetch code is not ready by
   2026-11-25, the raw responses are fetched by a one-off run of the code as it stands. As a last resort a
   throwaway script writes them in the layout of section 1, so build can still use them;
3. then the availability table, the build pieces, replay, docs, and US5 last.

## Risks (`09-risks-and-tech-debt.md`)

| Id | Change |
|---|---|
| K1 | Extended to replay: `ems-gen replay` now runs the preflight (AC-008-17) |
| L1 | Open-Meteo marks ICON-D2 "Discontinued 2026-11-30"; whether the archive persists is VERIFY. The default window is fetched before then (SC-002, task order); once on disk, its raw data no longer depend on the archive. Windows fetched later, if ICON-D2 archives vanish, can use DMI HARMONIE, which covers all of Poland but forecasts GHI 11–19 % worse at Poznań (section 3). A model override is later work |
| L2 | Unchanged. The free tier covers "public research conducted at public institutions": the thesis is university work, not work for an employer |
| L3 | PSE history shows one publication per day (F13, F15), so an earlier revision would be invisible. RES act art. 4b defines no RCE correction procedure (energy review 2026-11-13). Accepted; flagged days are counted per build (none in the default window) and are the one documented per-key order exception. `build_day_event` now keeps the latest row per slot |
| L5 | Resolved for history recordings (observed truth); unchanged for live hybrid mode |
| G10 | Measurable through the forecast-error candidate (US5) |
| G11 | Build refuses to rewrite under the same `run_id` with changed digests; replay `--background` refuses changed calibration or sites |
| L6 (new) | Satellite data terms (DWD MTG via Open-Meteo) unverified: hand-written satellite fixtures only; raw data never committed |
| L7 (new) | Archived run values differ slightly from what the live API served for the same run (F4), hence `open-meteo-single-runs` |
| L8 (new) | Availability delays rest on six ICON-D2 and three DMI autumn samples (the short T07 sample included); spring 2026 is assumed alike. The rule is public availability + Open-Meteo's recommended 10-minute wait; the live adapter's 5-minute poll can add lateness, not information. DMI forecasts reach strategies 90 min later than ICON-D2 ones, as in reality. DMI 15-minute radiation is interpolated from hourly output (F7a, confirmed by the DMI API docs; DMI's `dswrf` is "not present on first model step", which explains F3's leading gap) |
| L9 (new) | Observed truth is a satellite (2.5 km pixel) and reanalysis (25 km) estimate, with gaps (1.7 % of daylight at Poznań, longest 7 h) and low-sun zeros.<br>No published accuracy exists for the DWD MTG 10-minute GHI (SARAH-3 vs BSRN, a proxy: about 5 W/m² monthly and 11 W/m² daily mean absolute difference, Pfeifroth et al. 2024). Whether satellite DNI/DHI are DWD retrievals or an Open-Meteo separation is VERIFY.<br>On the clearest spring days the satellite exceeds pvlib's Ineichen clear-sky by about 11–13 %, so real-data PV may sit above the synthetic yield; a comparison at Lindenberg (BSRN) with DWD station data would settle it (not in scope).<br>Resampling uses data up to 5 min (satellite) and 45 min (ERA5) after an interval's end. That concerns how truth is reconstructed, not when events become available (FR-003).<br>Runs issued before `start` are not fetched, so the first forecast is usable 120 min (DMI 195 min) after `start`. The first and last local delivery days of a whole-UTC-day window are partial.<br>ERA5 within about three months of real time is preliminary (ERA5T); the manifest notes it.<br>The default window has no winter months |

## Experiment impact

- No energy result exists yet, so no reported energy number changes.
- Synthetic forecasts are emitted 15 min later. Event values, ids and counts are unchanged; only the emission order
  within a backfill shifts. The benchmark's array timings and event counts are unaffected, so `make bench` is not
  rerun.
- SC-002 records the real window's counts, gap shares, durations and hash in 11 and 13. It is a dataset
  description, not a strategy result.

## Test strategy

Written first by `test-engineer`. Unit and property tests use no network (AC-002-14 blocks it); fetch tests use
respx, a fake clock and an injected "today".

| File | Covers |
|---|---|
| `tests/unit/contracts/test_availability.py` | **AC-008-08**: table values; each ≥ `MEASURED_MAX_MIN` + 10 min and a multiple of 15 min; unknown model raises; synthetic forecasts are emitted at `issued_at` + 120 min |
| `tests/unit/ingest/test_pse.py` | **AC-008-09, -12**: a fixture with two publications for some slots → the latest row per slot wins and `issued_at` is the latest time; with one row per slot the event is unchanged |
| `tests/unit/ingest/test_openmeteo.py` | **AC-008-05**: an archived ICON-D2 fixture gives 191 steps from +15 min; an archived DMI fixture gives a first step ending at +45 min; more than four leading gaps → `None`; the live fixture's event JSON equals the one pinned from the base commit; `single_run_params` has `run`, `forecast_minutely_15=193` and no `start_minutely_15` |
| `tests/unit/ingest/test_history_fetch.py` | **AC-008-01**: layout, meta with SHA-256, manifest fields, no secrets, atomic writes (no `.tmp` left).<br>**AC-008-02**: each refusal (before 2026-04-02, too recent against an injected today, not whole days, empty, unknown site, a site-controlled battery at a real-data site, estimate above 9,000) sends no request; an ERA5 response missing the hour at `end` → exit 2 naming it; a 401 from a customer host → exit 2, no retry.<br>**AC-008-03**: a second fetch sends nothing; a "run not available" 400 writes a marker and is retried next time; another 400 aborts; `complete` is set only when every request is accounted for.<br>**AC-008-04**: with a fake clock, Open-Meteo requests are ≥ 0.8 s apart, so no sliding minute or hour exceeds its limit; a final 429 → manifest written, exit 4, and a rerun resumes; PSE requests ≥ 1 s apart |
| `tests/unit/ingest/test_observed_truth.py` | **AC-008-13**: hand-written satellite series (label positions, weights 2/3 and 1/3, a null 10-minute slot with zero clear-sky → 0 even in a sunrise quarter).<br>**AC-008-14**: ERA5 interpolation at interval ends, including the last interval.<br>**AC-008-15**: a constant `ObservedTruth` gives PV and heat-pump energy equal to `pv_energy_kwh` and `heat_pump_kwh` applied to its arrays.<br>**AC-008-16**: a daylight gap → ERA5 fill with Erbs split; at sunrise the fill uses the ERA5 label `floor_hour(s) + 1 h` (a one-hour shift changes the result); GHI present, DNI missing → Erbs; a DNI that breaks `check_split` → Erbs, counted; ERA5 missing too → fallback; masks; no NaN in any output; a non-hour-aligned axis raises |
| `tests/unit/ingest/test_history_build.py` | Uses a helper that writes a raw recording in the API formats for any window (synthetic values; real fixtures only where noted).<br>**AC-008-05**: event kinds and counts; a malformed run is counted.<br>**AC-008-06**: two builds are byte-identical; hash, digests and raw-set digest in `build.json`; `ingested_at` equals the meta `fetched_at`; an incomplete fetch or a changed raw body is refused; the same `run_id` with a changed digest is refused.<br>**AC-008-07**: every forecast `issued_at` equals the `run` of its raw file; every price `issued_at` equals the latest `publication_ts_utc` of that day's raw records.<br>**AC-008-09**: day selection (the initial day sorts to `start`); flagged and incomplete days counted; a flagged day published after the next day sorts after it.<br>**AC-008-10**: window 2026-10-24 … 10-27 with the real 2026-10-25 PSE fixture: 100 slots; meters for every UTC quarter-hour, no gap or duplicate; `build.json` lists the complete local delivery days (10-25 and 10-26, not the partial edge days).<br>**AC-008-11**: `source_system` values, non-null `run_id`, no `weather.actual` event; schema export unchanged.<br>**AC-008-12**: one PSE fixture through `PseSource.poll` and through build → equal events.<br>**AC-008-16**: `estimated` and `quality_raw` on PV, heat pump and (non-managed sites) grid in filled intervals only; the CLI output prints the shares |
| `tests/property/test_history_lookahead.py` | **AC-008-07** (hypothesis, fixed profile; whole-day window start and length, gap positions, D−1 publication times): availability − `issued_at` ≥ the stream's delay for every event; the file is sorted by `emission_key`; per topic and key `interval_start` never decreases; `record_timestamp` equals the 06 rule |
| `tests/unit/ingest/test_replay.py` | **AC-008-17**: a stubbed admin with live-profile configs → `PreflightError`, nothing published; through the CLI, exit 1.<br>**AC-008-18**: `--background` into a list sink with `generator.history.yaml`: a non-empty fleet, no synthetic event for a real-data site, no synthetic price, every other site present, merged emission order, seed and `run_id` taken from `build.json`, a run manifest written; a changed calibration digest is refused.<br>History hash check: a changed `events.jsonl` → refused; `--speed` → refused |
| `tests/integration/test_history_replay.py` | **AC-008-17**: replay into `live`-profile topics → exit 1, topics empty.<br>**AC-008-19**: a fixture recording replayed into `sim` topics → `validate_records` clean; the topic ids equal the recording's |
| `tests/unit/ingest/test_calibration.py` | **AC-008-20**: synthetic truth plus `forecast_run` over 60 days and two sites. The fit recovers temperature and wind sd within ±10 % of `weather.yaml`'s sd(L) per bin and the consecutive-run correlation within ±0.05 of ρ². The candidate holds recording, hash, window, sites, attribution, the hourly-aggregate kt error and the upper-bound caveat; `weather.yaml` is untouched |

Fixtures (each folder keeps a NOTICE, AC-002-15):
- `tests/fixtures/openmeteo/`: one archived ICON-D2 run, one archived DMI run (CC BY 4.0, real) and a two-day
  ERA5 response. The NOTICE is extended with "Contains modified Copernicus Climate Change Service information
  2026" (ERA5 is CC BY 4.0 on the CDS since 2025-07-02) and the ERA5 DOI 10.24381/cds.adbb2d47.
- Satellite bodies: hand-written in the API format in test code (`history_recording.py`, `test_observed_truth.py`),
  no fixture folder and no real satellite data (L6).
- `tests/fixtures/pse/history_rce-pln_2026-10-25.json` (real, L3).

Existing tests:
- `tests/unit/ingest/test_generator.py:59` expects forecasts at +90 min; it moves to `forecast_delay("synthetic")`.
- `tests/unit/ingest/test_narrow_assets.py` hashes events in emission order, so the 15-minute shift changes its
  value and id digests.
  - Before any code change, order-independent digests (rows and ids sorted by `event_id`) are pinned from this
    branch's base, and they must stay equal.
  - Then the emission-order digests are refreshed, with a comment naming spec 008.
- `tests/unit/ingest/test_recording_hybrid.py:131` passes `check_topics=False` to `replay`.
- AC-002-06 (live forecast fixture), the PSE tests and AC-002-11 (replay keeps ids) stay and must pass.

## Docs and ADRs

- **No ADR.** These go to `00-overview.md` "Decisions so far":
  - fetch once, replay many, with meters stored in the recording;
  - the per-model availability table (synthetic 90 → 120 min), defined as public availability plus Open-Meteo's
    recommended 10-minute wait;
  - DMI support for eastern sites;
  - the background replay option;
  - the `source_system` values;
  - `run_id` chosen at build;
  - real-data sites without site-controlled batteries;
  - the backlog reordered (008 before Kafka ingestion).
- **`00`**: status row and backlog row 008.
- **`13`**:
  - history fetch, build and layout; `ObservedTruth`, gap filling and quality marking;
  - each source's attribution;
  - the measured delays and the T07 sample;
  - the Copernicus attribution and licence (CC BY 4.0), ERA5T;
  - the resolved VERIFY items with value, method and date (satellite end label, hourly runs absent, customer
    hosts);
  - the live leading-gap edge case; the determinism scope; the SC-002 numbers;
  - line 53 ("emitted 90 min after issue").
- **`08`**: the availability rule per model (line 66); the forecast payload wording (line 31); the
  `source_system` values; the history `run_id` scope; the consumer behaviour for unknown models as an open
  question for the ingestion spec.
- **`06`**: forecast emission per model (line 16); the replay preflight; the per-key order exception for flagged
  price days (line 19); history recordings.
- **`09`**: the table above.
- **`11`**: the real-data window, how experiments replay it (with or without `--background`), its limits (spring,
  synthetic households, warm-up, partial edge days, background sites not correlated with real weather and prices),
  the SC-002 numbers.
- **`12`**: two facts from the energy review, each with its source and "re-check against Dz.U. 2026 poz. 68" for
  the settlement spec:
  - the cohort wording "first generated and fed in from 2024-07-01";
  - the net-billing items it resolved (negative RCE as 0 for individual prosumers, 12-month deposit, refund caps,
    the 1.23 factor).
- **Other files**:
  - `config/calibration/weather.yaml`: `availability_delay_min` removed, with a pointer comment;
  - `src/ems/ingest/generator.py` module docstring ("emitted 1.5 h later");
  - `README.md`: lines 19, 35, 308 ("The 18:00 run is usable from 19:30"), 334, plus the history section and
    attribution;
  - `.claude/rules/ingest.md`: preflight also for replay;
  - `CLAUDE.md`: commands table.

## Review (2026-11-13)

Both reviews were run on the draft plan. Every finding is fixed above, except where the tables say otherwise.

`architect-reviewer` found no blockers, 12 should-fix findings and 11 notes.

| Finding | Resolution |
|---|---|
| 1 Live arrival includes the 5-minute meta poll: ICON-D2 worst case 106.9 > 105 min | The rule is defined as public availability + Open-Meteo's recommended 10-minute wait (energy review 9), so the adapter's poll adds lateness, not information. 105/195 and `meta_poll_s` stay. A 7-day sample above the table stops work for a user decision (section 4). Later (T07, 2026-11-13) a 106.7 min ICON-D2 run raised the value to 120 min |
| 2 Fetch and build both write the manifest; config overwrite on rerun | `manifest.json` (fetch) and `build.json` (build) are separate; raw-set and `generator.yaml` digests; fetch refuses a changed config; replay refuses a changed raw set |
| 3 Replay does not check the `events.jsonl` hash; in-place truncation | Temp file and rename, `build.json` last; replay checks the hash |
| 4 Build cannot tell "never requested" from "missing" | `.missing.json` markers; `complete` flag; build refuses incomplete fetches |
| 5 Fetch log lost on a crash; a final 429 aborts | No fetch log. The manifest is derived from the raw folder; a final 429 → exit 4, resumable |
| 6 `replay --speed` paces by fetch time | Refused for history recordings |
| 7 `--background` depends on the current config; no manifest; merge key undefined | Digest check against `build.json`; run manifest; shared `emission_key` |
| 8 The look-ahead property is tautological | Tests compare `issued_at` with the raw `run` and the raw `publication_ts_utc`, and `ingested_at` with the meta `fetched_at` |
| 9 Flagged price day can break per-key order; page order p1, p10, p2 | Documented exception; pages zero-padded and read in order; rows sorted by `published`; the property test draws D−1 publications only; an explicit flagged-day test |
| 10 `run_id` fixed at fetch forces a refetch | `--run-id` is a build option; a rebuild with changed digests needs a new `run_id` |
| 11 No eastern site or config for SC-002 | `config/generator.history.yaml` with PL-LOC-000001 and PL-LOC-000004 (Radom), 20 sites; non-empty background test |
| 12 Stale 90-minute and payload wording in docs | Listed under "Docs and ADRs" |
| 13 `source_record_ref` format; live leading-gap edge case | Today's format kept; live bytes pinned from the base; edge case documented |
| 14 `test_recording_hybrid` would hit Kafka admin | `check_topics=False` added |
| 15 Grid and site-battery flows stay `synthetic`; no NaN check | Grid flows are marked too; real-data sites with a site-controlled battery are refused (energy 13); `quality_raw` names the fallback; no-NaN assertion |
| 16 `wind100`, `wind120`, `kt`, `cloud_pct` overlays unused | Dropped; ERA5 request has three variables |
| 17 Valid GHI discarded when DNI/DHI missing; partial ERA5 hours | Erbs split of the satellite GHI; hour-aligned axes required |
| 18 Determinism scope | Hardware in `build.json`; scope stated in 13 |
| 19 Resampling uses data after the interval end | Stated in L9 |
| 20 Daily-budget machinery is overkill | Replaced by an up-front estimate (≤ 9,000) plus exit 4 on a final 429; AC-008-04's stop-and-resume holds through the 429 path |
| 21 `--market`, the +1 PSE day, the AC-008-10 fixture, US5 size | `--market` and +1 dropped. The 2026-10-25 fixture is kept, because the AC names that date. US5 stays at two tasks, placed last |
| 22 Availability table refinements | `meter_delay` parameter, `MEASURED_MAX_MIN` constants, consumer behaviour left to the ingestion spec |
| 23 Task order, warm-up, malformed runs, atomic writes, test gaps | Task-order section with a fallback; warm-up in L9 and 11; malformed runs counted; atomic writes; tests added for each gap listed |
| Open question 6: ERA5T | Answered by the energy review: ERA5T may be revised 2–3 months later; the manifest notes it; the default window is final data |

`energy-domain-reviewer` found no blockers, 8 should-fix findings and 12 notes. It confirmed with sources:
- RCE as the net-billing price (RES act art. 4b), quarter-hourly from 2025-10-01;
- `dtime_utc` labels the interval end; 2026-10-25 has 100 slots; the DST dates;
- the Single Runs facts and Open-Meteo's recommended 10-minute wait;
- the satellite end label;
- ERA5's delay and labelling.

| Finding | Resolution |
|---|---|
| 1 `build_day_event` keeps the last row per slot in API order, not the latest publication | Latest row per slot, live and history; two-publication fixture test (section 5) |
| 2 Whole-UTC-day windows give partial first and last local delivery days | Complete local days recorded in `build.json`; 11 scores only those (section 7) |
| 3 ERA5 hour for a quarter ambiguous | Mapping written out (`floor_hour(s) + 1 h`); sunrise fixture test |
| 4 Satellite DNI/DHI used without physical bounds | `check_split` per quarter, Erbs on failure, counted |
| 5 ERA5 hour at `end` may be missing and get filled silently | Fetch checks ERA5 completeness and exits 2 naming the hour |
| 6 Copernicus licence now CC BY 4.0; attribution wording; ERA5T | Manifest, NOTICE, README and 13 use CC BY 4.0, "Contains modified Copernicus Climate Change Service information 2026" and the DOI; ERA5T noted. Spec F11 corrected |
| 7 Forecast-error candidate mixes ERA5 error and DMI interpolation into "error" | Both named; hourly-aggregate kt error; sds labelled upper bounds (section 10) |
| 8 Resolvable VERIFY marks | Spec F7a and F8 updated with the sources; new VERIFY on satellite DNI/DHI provenance (L9, 13) |
| 9 Delay margin vs the 5-minute poll; thin sample | Rule defined as public availability + recommended wait; a sample on the free host (T07), which raised ICON-D2 to 120 min |
| 10 1.1 clip on ERA5 fills; clear-sky reference | Clip share and low-sun share reported in `build.json`; Lindenberg comparison noted in L9, out of scope |
| 11 Night rule per 10-minute slot; low-sun share | `quarter_means` uses each slot's own clear-sky; share reported |
| 12 120 m wind unused | Already dropped (architect 16) |
| 13 Site-controlled batteries dispatch on synthetic prices; background uncorrelated | Real-data sites with one refused; history config has none; limits stated in 11 |
| 14 Archive persistence; grid cells | Grid cell per response in the manifest. A Zenodo deposit of the CC BY forecast responses is possible later, not in scope |
| 15 Monthly cap; paid plans exclude Historical and Satellite; L2 | 401/403 fails clearly; L2 note; spec F12 adds the monthly cap |
| 16 RCE vs csdac | Consistent with 12; no change |
| 17 Net-billing VERIFY items in 12 (out of scope) | Recorded in 12 with sources, marked for re-check against Dz.U. 2026 poz. 68 |

## Complexity Tracking

One new package with three modules, each with one job: network (fetch), pure conversion (observed) and offline
assembly (build). Everything else is a parameter or a small split in existing code.
