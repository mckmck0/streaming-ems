# Feature Specification: Historical backfill on real data

**Feature Branch**: `008-historical-backfill` · **Created**: 2026-11-12 · **Status**: Approved 2026-11-13

**Input**: A review of the generator (2026-11) found the generated data structurally sound but every window fully
synthetic (finding 4). Spec 007 fixed findings 1–3 and handed this one over (section "Handoff to the
historical-backfill spec" in `specs/007-realistic-generator-data/spec.md`). User request 2026-11-12: plan it next,
ahead of the Kafka-ingestion spec (00 listed it as "later"). User decisions 2026-11-12/13: Q1–Q3 under "Open
questions".

## Goal

One real-data window for evaluating strategies, fetched once and replayed many times.

What is real and what is not:

| Data | Source | Real? |
|---|---|---|
| Prices | PSE RCE (`rce-pln`), one market | Real, with PSE's publication times |
| Weather forecasts | Archived runs of record from the Open-Meteo Single Runs API: ICON-D2 inside its domain, DMI HARMONIE AROME Europe east of 20.34° E (the live adapter's `model_for`) | Real, with their initialisation times |
| Truth weather (feeds the meters only) | Satellite GHI/DNI/DHI (DWD MTG via Open-Meteo); ERA5 temperature and wind | Real, as satellite and reanalysis estimates, not ground measurements |
| Household meters | PV and heat pump: the existing models driven by the real truth weather. Base load: the spec 007 model. EV: the existing model | **Synthetic** |

Results on this window are therefore real in prices, forecasts and the weather the households respond to.
Household behaviour (base load, appliances, EV) stays modelled. No real household meter data is used.

**Fetch once, replay many.**
1. **fetch** (network, batch CLI, P10): raw responses for a window `[start, end)` go into
   `data/recordings/<name>/raw/` (the existing recording layout), with a manifest. Fetch is resumable.
2. **build** (offline, deterministic): canonical v1 events for the real-data sites and the price market go into
   `events.jsonl` in availability order, meters included.
3. **publish**: the existing `ems-gen replay` (ids kept, `replay:<source>`), now behind the topic preflight,
   optionally with the synthetic background sites (Q2).

**Meters are stored in the recording, not recomputed at replay.** Replay stays a plain re-publisher with no
models, config or truth source. Experiments read exactly the bytes whose hash the manifest records (P14), so a
later model change cannot silently change a "real-data" window. The build needs the models anyway, and a rebuild
from raw with another seed is offline and cheap. Cost: 17,280 meter events per site for 90 days, small next to the
raw forecasts.

**Real-data sites** are the generator config's `live.locations` (default: the EMS-managed test site
PL-LOC-000001, Poznań), as in hybrid mode. Every other site stays fully synthetic.

## Verified facts (checked 2026-11-12 and 2026-11-13)

| # | Fact | Evidence | Status |
|---|---|---|---|
| F1 | Single Runs API `https://single-runs-api.open-meteo.com/v1/forecast`, `run=YYYY-MM-DDTHH:MM` (UTC init); "Archival runs are available from 2nd of April 2026 for most models" | open-meteo.com/en/docs/single-runs-api; probes of `icon_d2` and `dmi_harmonie_arome_europe` runs 2026-04-02T00, 2026-05-15T00/03/12 and 2026-06-30T21 at Poznań returned 15-minute data | Confirmed |
| F2 | `start_minutely_15` is rejected ("must not be set"), so the live `forecast_params` cannot be reused as is. `forecast_minutely_15=193` from the run time works; step 0 (labelled at the run time) has no radiation and is dropped | probe | Confirmed |
| F3 | Archived ICON-D2 runs have cloud cover missing at +48 h, so `build_event` keeps 191 complete steps. Archived DMI runs lack GHI and DHI for the steps ending +15 and +30 min, so today's `build_event` would emit nothing. The live API serves DMI runs without that gap | probes, 3 runs per model (2026-05-15); live DMI probe 2026-11-12 | Confirmed |
| F3a | Wire contract v1 lets a forecast event start after the first quarter-hour: steps must be contiguous and `lead_time` must equal `valid_end − issued_at`, nothing more. A DMI event can therefore start at its first complete step (+45 min) without a contract change. The helper `contracts.examples.forecast_event` always starts at +15 min and needs an optional offset | `src/ems/contracts/envelope.py` (`_forecast`), `examples.py` | Confirmed |
| F4 | Archived values differ slightly from what the live API served for the same run and grid cell: 37–71 of 192 GHI steps differ by a few W/m², temperature by 0.1–0.2 K, 10 m wind by up to about 0.3 m/s | runs 2026-10-20T18Z and 2026-10-29T15Z (recordings `smoke`, `entsoe-smoke`) vs the archive | Confirmed; cause unknown |
| F5 | ICON-D2 is marked "Discontinued 2026-11-30"; afterwards `dwd_icon_d2` uses DWD's native grid. Whether archived runs stay retrievable is not documented | open-meteo.com/en/docs/dwd-api | **VERIFY**; fetch the default window before 2026-11-30 |
| F6 | Availability after init: docs say "1–3 hours for regional models". `meta.json` (`last_run_availability_time − last_run_initialisation_time`): ICON-D2 85.2, 91.9, 85.3, 88.5 min (runs 2026-10-20T18Z, 10-29T15Z, 11-12T15Z, 11-13T09Z); DMI HARMONIE 168.9, 170.2 min (11-12T15Z, 11-13T06Z). The live adapter fetches ≥ 10 min after availability | docs, `meta.json`, recordings | Measured, n = 4 and 2. A short follow-up sample (008/T07, 2026-11-13; the user ended it early) found the ICON-D2 12 UTC run of 2026-11-13 at 106.7 min and DMI at 170.4 min. Spring 2026 delays cannot be measured afterwards |
| F7 | Live forecasts therefore reach Kafka 95–102 min (ICON-D2) and about 180 min (DMI) after issue, while 06, 08 and `weather.yaml` assume 90 min for every forecast | F6 | Finding |
| F7a | DMI HARMONIE output is hourly per its `meta.json` (`temporal_resolution_seconds` 3600), so its 15-minute radiation is probably interpolated by Open-Meteo, as on the live API | `meta.json`; open-meteo.com/en/docs/dmi-api (temporal resolution "Hourly"); DMI documents `dswrf` as "not present on first model step" | Confirmed 2026-11-13 (energy review); same for live and archive. The DMI note also explains F3's leading gap |
| F8 | Satellite Radiation API `https://satellite-api.open-meteo.com/v1/archive`, `models=satellite_radiation_seamless`, `temporal_resolution=native`: DWD MTG every 10 min, 0.025°, archived since Feb 2026, 20 min delay; "backward averages will use the same 10/15/30-minute intervals" | docs; probe for Poznań 2026-04-02 … 06-30: 12,960 regular 10-min steps, pixel 52.400 N 16.925 E | Confirmed; end label confirmed 2026-11-13 by the docs and a clear-day check (plan section 6). Whether DNI/DHI are DWD retrievals or an Open-Meteo separation is **VERIFY** |
| F9 | Satellite gaps in that probe: 3.0 % of values null, mostly at 00:00–00:10 UTC (night). While the sun is up (04–18 UTC) 1.7 %, on 9 days; longest gap 43 steps (7 h 10 min, 2026-04-14). Night values are otherwise 0. At very low sun the satellite gives 0 where a clear-sky model gives a few W/m² | probe | Measured at one site |
| F10 | Satellite data terms: Open-Meteo's licence page lists DWD, ECMWF, Copernicus and DMI, but neither EUMETSAT nor the satellite product | open-meteo.com/en/licence | **VERIFY**; until then only hand-written satellite fixtures |
| F11 | ERA5: `https://archive-api.open-meteo.com/v1/archive`, `models=era5`, 0.25°, "Daily with 5 days delay"; temperature and wind are hourly instants, radiation the preceding-hour mean; `wind_speed_unit=ms`. Probe: 2,160 hourly values for the window at Poznań, no nulls; on 2026-11-12 the last value was 2026-11-06T23:00 | docs, probes | Confirmed. Licence corrected 2026-11-13 (energy review): ERA5 on the CDS is CC BY 4.0 since 2025-07-02; attribution "Contains modified Copernicus Climate Change Service information 2026", DOI 10.24381/cds.adbb2d47. ERA5 within about three months of real time is preliminary (ERA5T) |
| F12 | Open-Meteo free tier: under 10,000 calls a day, 5,000 an hour and 600 a minute; non-commercial (public research qualifies); CC BY 4.0. A request counts as several calls above 10 variables or 2 weeks per location (fractional counts) | open-meteo.com/en/terms, /en/pricing | Confirmed; also 300,000 calls a month (not enforced yet), and paid plans below Professional exclude the Historical and Satellite APIs. A 90-day window needs about 720 run requests per site (1 call each), plus about 7 calls each for satellite and ERA5 |
| F13 | PSE: a past `business_date` query returns one record per slot and one `publication_ts_utc` per day. Business dates 2026-04-01 … 07-01: 92 days, all with 96 slots and one publication, all on D−1 between 13:00 and 15:00 local. Negative RCE: 13.5 % (Apr), 8.3 % (May), 4.4 % (Jun) | `api.raporty.pse.pl/api/rce-pln` | Confirmed |
| F14 | Four days recorded live in Oct 2026 (10-20, 10-21, 10-29, 10-30) return identical prices and publication times when queried later, so `build_day_event` gives the same `event_id` under the same `run_id` | recordings vs a fresh query | Confirmed |
| F15 | Whether PSE keeps superseded revisions in its history: no documentation found. The history shows one publication per day, so an earlier version, if there was one, is invisible (risk L3) | — | **VERIFY** |
| F16 | `ems-gen replay` runs no topic preflight; only `ems-gen run` does. Replaying months-old records into a `live`-profile topic would be accepted and then deleted by retention | `src/ems/ingest/runner.py` (`replay`) | Finding |

## Scope

In scope:
- A history **fetch** and **build** (CLI shape in the plan; leaning: `ems-gen history fetch|build` plus a `make`
  target). They reuse `live/http.py` (`Fetcher`), `sources/recording.py` (`Recorder` layout),
  `live/openmeteo.py` (`model_for`, the `openmeteo.forecast.v1` mapping, `build_event`), `live/pse.py`
  (`fetch_rows`, `build_day_event`) and the generator's meter emissions.
- Both forecast models of the live adapter: ICON-D2 inside its domain and DMI HARMONIE elsewhere in Poland. A
  forecast event starts at its run's first complete step (F3, F3a).
- A real truth source behind the spec 007 seam (`TruthSource`, `synthetic/truth.py`): satellite radiation, ERA5
  temperature and wind, and a defined gap fallback.
- One forecast availability delay per forecast model, defined once (Q1): 120 min for ICON-D2 and synthetic
  forecasts, 195 min for DMI HARMONIE.
- The topic preflight for `ems-gen replay`.
- A replay option that generates the synthetic background sites (Q2).
- (P2) A forecast-error candidate from the calibration job (`jobs/calibrate.py`).
- Docs: 13, 08, 06, 09, 11, 00, README; `weather.yaml` where it holds the forecast availability delay.

Out of scope:
- Real household meter data; tariffs, purchase prices, RCE/RCEm settlement (domain-core spec); anything in
  Restate; wire contract changes (v1 frozen, ADR 0016); publishing truth weather (`ems.weather.actual.v1` stays
  disabled).
- The Previous Runs API (windows start on or after 2026-04-02). A model override (for example DMI for every
  site, should ICON-D2 archives vanish, F5). The csdac market or several markets in one recording. Satellite truth
  in live hybrid mode.
- Changes to the PV, heat-pump, base-load or EV models.

## Affected

- New recordings of kind `ems-gen-history` under `data/recordings/<name>/` (gitignored, never committed).
- Synthetic forecasts are emitted 120 min after issue instead of 90 (Q1). Their values and ids do not change; only
  the emission order in a backfill shifts.
- The 08 availability rule for forecasts becomes `issued_at` + the delay of the event's `forecast_model`. It also
  corrects replays of live recordings (F7).
- `build_event` skips a short leading block of incomplete steps. Live responses have none (F3), so live events
  do not change.
- `ems-gen replay` refuses topics that would reject or expire its records.
- New `source_system` values `open-meteo-single-runs` and `synthetic-history`.
- No consumer exists yet. No reported number changes: there is no energy result yet, and the benchmark's event
  counts and array timings are unaffected.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Fetch a real window once (Priority: P1)
The author fetches the raw data for a window and the real-data sites once, within the free-tier limits and with
full provenance, and can resume an interrupted fetch. **Acceptance**: AC-008-01 … -04.

### User Story 2 - Canonical events without look-ahead (Priority: P1)
Building the recording gives v1 events whose issue and availability times are those a live run would have seen,
byte-identically on every rebuild. **Acceptance**: AC-008-05 … -12.

### User Story 3 - Meters under real weather (Priority: P1)
The real-data sites' meters come from the existing models driven by satellite and ERA5 truth, with defined,
counted gap filling. **Acceptance**: AC-008-13 … -16.

### User Story 4 - Publish the window for experiments (Priority: P1)
The recording is replayed into sim topics as often as needed, safely, optionally with the synthetic fleet around
it. **Acceptance**: AC-008-17 … -19.

### User Story 5 - Measure real forecast error (Priority: P2)
The window yields forecast-vs-truth error statistics by lead, written as a candidate next to the assumed spec 007
parameters, for review. Dropped if the plan shows it needs more than about two tasks. **Acceptance**: AC-008-20.

### Edge Cases
- A window with a 100-slot day (2026-10-25): a 100-slot price event; meters are continuous in UTC. 92-slot days
  (2026-03-29) lie before the archive start and keep their existing PSE tests.
- A run missing from the archive: no event, counted; the previous run stays the latest.
- An ICON-D2 run truncated at 191 steps (F3): an event with the complete prefix, as live.
- A DMI run without radiation in its first 30 minutes (F3): the event starts at +30 min (first step ends at
  +45 min, `lead_time` PT45M). Those steps lie well before the run's 195-minute availability, so no strategy
  loses usable information.
- A site near 20.34° E: `model_for` decides, as live.
- A price day published before the window start: available from the start, like the synthetic backfill's
  initial day.
- Satellite nulls at night: 0, not a gap. Low sun with satellite 0 and clear-sky above 0: kept as real data.
- The last interval of the window needs the ERA5 value at `end`, so the fetch covers that hour too.
- An interrupted fetch, or one stopped at the daily budget: rerun the same command.
- A rebuild after a model or config change gives new meter values under the same ids (as risk G11): build into a
  new recording name with a new `run_id`.

## Requirements *(mandatory)*

- **FR-001**: Raw real data for a window MUST be fetched once and stored with provenance. → AC-008-01 … -04
- **FR-002**: Events MUST be built offline and deterministically from the stored raw data. → AC-008-05, -06
- **FR-003**: No event MUST become available earlier than it was in reality. → AC-008-07 … -10
- **FR-004**: Identity MUST follow the 08 rule; wire contract v1 MUST NOT change. → AC-008-11, -12
- **FR-005**: Real truth weather MUST enter through the truth-source seam only, with a defined gap fallback.
  → AC-008-13 … -16
- **FR-006**: Recordings MUST be published only into topics that keep their records. → AC-008-17 … -19
- **FR-007** (P2): Real forecast error MUST be measurable as a reviewable candidate. → AC-008-20

### Acceptance criteria (EARS)

Fetch (US1)
- **AC-008-01**: WHEN `fetch` runs for a window `[start, end)` and a recording name, THE SYSTEM SHALL store under
  `data/recordings/<name>/raw/`:
  - for each real-data site, every run of its model (`model_for`: ICON-D2 inside its domain, DMI HARMONIE
    elsewhere) initialised in `[start, end)` from the Single Runs API (the nine live variables, 15-minute, from
    the run time);
  - for each real-data site, the satellite 10-minute GHI, DNI and DHI and the ERA5 hourly variables the truth
    needs (temperature, wind, GHI for gap filling), covering `[start, end]`;
  - for the price market, every PSE business day of AC-008-09.

  Each raw response SHALL have a meta file (URL without secrets, status, content type, fetch time, SHA-256 of the
  body). `manifest.json` SHALL record the window, sites, market, `run_id`, endpoint and parameters per source,
  model per site, fetch start and end, counts, missing runs, attribution and licence per source, git commit and
  versions.
- **AC-008-02**: IF the window starts before 2026-04-02, ends later than six days before the current UTC date
  (ERA5's documented five-day delay plus one day), is empty or is not whole UTC days, or IF a real-data site is
  not in the catalog, THEN THE SYSTEM SHALL refuse before sending any request, naming the limit or the site.
- **AC-008-03**: WHEN `fetch` runs again for the same recording, THE SYSTEM SHALL request only the responses not
  yet stored and leave stored ones untouched. IF a run is not in the archive, THEN THE SYSTEM SHALL record it as
  missing in the manifest and continue with the next run.
- **AC-008-04**: THE SYSTEM SHALL send every request through the existing `Fetcher` (jittered back-off on 429,
  5xx and timeouts only; secrets scrubbed), keep Open-Meteo requests within the free-tier limits (600 a minute,
  5,000 an hour, 10,000 a day, weighted as published) and pace PSE requests (PSE publishes no limit; VERIFY).
  (Test with a fake clock: no sliding minute, hour or day exceeds a limit; when the day budget is spent the fetch
  stops cleanly and a rerun resumes.)

Build and look-ahead (US2)
- **AC-008-05**: WHEN `build` runs on a fetched recording, THE SYSTEM SHALL use no network and write
  `events.jsonl` with:
  - one forecast event per stored run and site, mapped by the existing `openmeteo.forecast.v1` mapping and
    `build_event`. It starts at the first step after the run time whose values are all present, skipping at most
    the first hour, and is truncated before the next step with a missing value. (Fixture tests: an archived
    ICON-D2 run gives 191 steps from +15 min; an archived DMI run gives an event whose first step ends at
    +45 min.)
  - one price event per complete business day, built by the existing `build_day_event`;
  - production and consumption events for every real-data site and every quarter-hour in `[start, end)`, from
    the existing generator models driven by the real truth source (US3), with the configured seed.
- **AC-008-06**: WHEN `build` runs twice on the same raw responses, config and seed, THE SYSTEM SHALL write
  byte-identical `events.jsonl` files (`ingested_at` comes from the recorded fetch times, never the build time),
  and the manifest SHALL record the seed, the SHA-256 of `events.jsonl` and digests of the generator config,
  `config/calibration/*.yaml`, `config/sites.yaml` and the mappings used.
- **AC-008-07**: THE SYSTEM SHALL take `issued_at` from the source: a forecast's is its run's initialisation time
  (the requested `run`), a price day's is the latest `publication_ts_utc` of its records. It SHALL write
  `events.jsonl` in availability order (meters: interval end + `meters.availability_delay_s`; forecasts:
  `issued_at` + the forecast availability delay; prices: `issued_at`; ties in a fixed order). Every record
  timestamp SHALL follow `06-data-flows.md` (meters `interval_end`; forecasts and prices `issued_at`).
  (Property test over built recordings: for every event, availability − `issued_at` ≥ its stream's delay; per
  topic and key, `interval_start` never decreases; the sink's record timestamp equals the 06 rule.)
- **AC-008-08**: THE SYSTEM SHALL define the forecast availability delay once per `forecast_model` and use that
  one definition for synthetic forecast emission, the history build and the availability rule in
  `08-contracts.md`: 120 min for ICON-D2 and synthetic forecasts, 195 min for DMI HARMONIE. Each delay SHALL be at
  least the largest measured Open-Meteo availability delay for its model plus the live adapter's 10-minute grace,
  rounded up to a quarter-hour (F6 and T07: ICON-D2 106.7 + 10 → 120; DMI 170.4 + 10 → 195). (Revised by the user
  on 2026-11-13 after the T07 sample; was 105 min for ICON-D2 and synthetic.) A strategy ticking on
  quarter-hours sees a forecast at the same tick either way.
- **AC-008-09**: THE SYSTEM SHALL write a price event for every local delivery day that overlaps `[start, end)`
  and for every later day published before `end`, as the synthetic backfill does; a day published before `start`
  is available from `start`.
  - IF a day's records carry more than one publication time, or the latest is not on D−1 local, THEN THE SYSTEM
    SHALL use the latest as `issued_at`, never an earlier one, and count the day in the manifest.
  - IF a day is incomplete, THEN THE SYSTEM SHALL write no event for it and count it.
- **AC-008-10**: WHEN the window contains a 100-slot delivery day (2026-10-25), THE SYSTEM SHALL build its price
  event with 100 slots and meters for every UTC quarter-hour, without gaps or duplicates. (Fixture test.)
- **AC-008-11**: THE SYSTEM SHALL set `source_system` to:
  - `open-meteo-single-runs` on archived forecast events (archived values differ from the live API's, F4, so
    they are not the same fact as a live `open-meteo` event);
  - `pse-rce` on price events;
  - `synthetic-history` on the real-data sites' meter events.

  Every event SHALL carry a non-null `run_id` (default: the recording name). Wire contract v1.0.0 SHALL stay
  unchanged (schema export equals `schemas/`, the baseline check passes), and no event SHALL go to
  `ems.weather.actual.v1`.
- **AC-008-12**: WHEN a PSE business day is built from a history response and from a live response of the same
  publication with the same `run_id`, THE SYSTEM SHALL produce equal events (`event_id` and payload). (Fixture
  test; F14 shows the real responses agree.)

Truth and meters (US3)
- **AC-008-13**: THE SYSTEM SHALL turn the satellite's 10-minute backward means into quarter-hour means: the
  time-weighted average over the half-open interval `[s, s + 15 min)` (weights 2/3 and 1/3 of the two overlapping
  10-minute intervals), stored at the interval's `t` in UTC. A missing value while the clear-sky GHI is 0 SHALL
  count as 0, not as a gap. (Fixture test with hand-written values: labels, weights, night.)
- **AC-008-14**: THE SYSTEM SHALL interpolate ERA5's hourly instants (temperature, wind) linearly to each
  quarter-hour's end instant, the label `Truth` uses for temperature and wind, for every interval in the window
  including the last. (Fixture test.)
- **AC-008-15**: THE SYSTEM SHALL provide the real truth weather as a new `TruthSource` and change nothing in the
  PV, heat-pump, base-load or EV models: a real-data site's PV and heat-pump energy SHALL equal those models
  applied to the source's arrays. (Unit test as for AC-007-01; the diff leaves `synthetic/pv.py`,
  `heat_pump.py`, `load.py` and `ev.py` untouched.)
- **AC-008-16**: IF a quarter-hour has a missing satellite value while the sun is up, THEN THE SYSTEM SHALL take
  its GHI from ERA5 (that hour's clear-sky index times the quarter-hour's clear-sky GHI; DNI and DHI re-split as
  in the synthetic truth), or from the synthetic truth if ERA5 is missing too. IF ERA5 temperature or wind is
  missing, THEN THE SYSTEM SHALL take it from the synthetic truth.
  - The weather-driven measurements of such an interval (PV; the heat pump when temperature was filled) SHALL
    carry quality `estimated`.
  - The manifest and the CLI output SHALL report, per site and variable, the share of daylight intervals filled
    from each fallback. (Fixture test with a gap.)

Publish (US4)
- **AC-008-17**: WHEN a recording is replayed, THE SYSTEM SHALL first check, with the preflight `ems-gen run`
  uses, that every target topic accepts and keeps the recording's record times. IF one would not (for example the
  `live` profile), THEN THE SYSTEM SHALL refuse with exit code 1 before publishing. Otherwise every event SHALL
  keep its `event_id` and be marked `replay:<source>`. (This covers every recording, including spec 002's.)
- **AC-008-18**: WHERE background sites are requested on replay (Q2), THE SYSTEM SHALL generate every catalog
  site except the real-data sites fully synthetically over the recording's window, with the config, seed and
  `run_id` stored in its manifest and synthetic prices off (one price source), and SHALL emit no synthetic event
  for a real-data site.
- **AC-008-19**: WHEN a recording built from fixtures is replayed into the `sim` profile, THE SYSTEM SHALL pass
  `make validate` with no invalid record, and the topics SHALL hold exactly the recording's event ids.
  (Integration test against `make up`.)

Forecast error (US5, P2)
- **AC-008-20**: WHEN `jobs/calibrate.py forecast-error` runs on a built recording, THE SYSTEM SHALL use no
  network and write `config/calibration/candidates/forecast-error.yaml` with:
  - per forecast model and 3-hour lead bin from 0 to 48 h: the daylight kt error sd (plain and in spec 007's
    logit space), the temperature error sd, the wind log-ratio sd, the correlation of consecutive runs' errors
    for the same valid time, and the sample counts;
  - base and per-hour fits of the sds next to the values in `weather.yaml`;
  - the recording name, manifest hash, window, sites and attributions.

  It SHALL never write `weather.yaml`. On synthetic input whose forecasts are a known truth plus the spec 007
  error model, it SHALL recover the temperature and wind error sd within ±10 % of `weather.yaml`'s sd_x(L) and the
  consecutive-run correlation within ±0.05 of ρ².

## Constraints (from the checklists)

- **P1**: spec, plan and tasks are each approved before code.
- **P2**: `src/ems/domain/` untouched. Truth conversion functions are pure; I/O lives only in fetch.
- **P3**: `ems-gen` stays the only writer of external inputs; real truth is internal and not published.
- **P4, P5, P8, P13**: no actors, handlers or tracing touched.
- **P6**: ids are deterministic from source data and `run_id`; replay keeps them; every history event is
  run-scoped.
- **P7**: build and replay use simulated time only. Fetch uses the wall clock for pacing and fetch times (a batch
  job, not an experiment).
- **P9**: no contract change. `source_system` is free-form within its pattern; the availability rule is a
  documented consumer rule in 08, not a schema field.
- **P10**: fetch, build and the forecast-error fit are CLIs over artefacts.
- **P11**: no new setting is required. The optional `EMS_OPEN_METEO_API_KEY` is reused if the Single Runs API
  has a customer host (VERIFY); the free tier is the default.
- **P12**: no new dependency expected (httpx, tenacity, numpy, pvlib are locked). Any new one is pinned under
  `exclude-newer`.
- **P14**: the manifest holds window, sites, seed, config digests, sources, fetch times, content hashes, git
  commit, versions and hardware.
- **P15**: docs land with the change. No ADR expected: decisions go to `00-overview.md` "Decisions so far".
- **P16**: no experiment runs. Fetch and build durations go into the manifest.
- **Risks** (09):
  - K1: fixed for replay (AC-008-17).
  - L1: fetch the default window before 2026-11-30; archive persistence VERIFY. If ICON-D2 archives vanish,
    DMI HARMONIE covers all of Poland (a model override is a later, small change).
  - L2: accepted (public research on the free tier).
  - L3: PSE history shows only the final publication (F15); accepted, late or revised days counted per run.
  - L5: resolved for history recordings (observed truth); unchanged for live hybrid mode.
  - G10: measurable through US5.
  - New:
    - L6: satellite data terms unverified (F10), so hand-written satellite fixtures only and raw data never
      committed;
    - L7: archived run values differ from live ones (F4), hence their own `source_system`;
    - L8: the availability delays rest on four (ICON-D2) and two (DMI) autumn samples; spring 2026 is assumed
      alike, with 120 and 195 min as margin. DMI forecasts reach strategies about 90 min later than ICON-D2 ones,
      as in reality, and their 15-minute radiation is probably interpolated from hourly output (F7a);
    - L9: truth is a satellite (2.5 km) and reanalysis (25 km) estimate with gaps (F9) and low-sun zeros; the
      default window has no winter months.

## Success Criteria

- **SC-001**: `make ci` is green, and every AC has at least one new unit, property or integration test, written
  before its implementation.
- **SC-002**: The default window (2026-04-02 … 2026-06-30, i.e. `--start 2026-04-02 --end 2026-07-01`) is fetched
  before 2026-11-30 for the test site (ICON-D2) and at least one site east of 20.34° E (DMI), and built. Its
  counts, missing runs, gap shares, fetch and build durations and `events.jsonl` hash are recorded in 13 and 11;
  the data itself is not committed.
- **SC-003** (review):
  - 13 documents the history fetch, build and the real truth source;
  - 08 lists the new `source_system` values, the per-model availability rule and the run scope;
  - 06 has the per-model forecast emission and the replay preflight;
  - 09 updates L1, L3 and L5 and adds L6–L9;
  - 11 describes the real-data window and how experiments use it;
  - 00 has backlog row 008 and the decisions;
  - README documents the make target.

## Assumptions

- Spring 2026 Open-Meteo availability delays were like those measured in Oct–Nov 2026 (L8).
- Satellite GHI for a 2.5 km pixel and ERA5 for a 25 km cell are acceptable truth for a point site. Satellite
  retrieval error (VERIFY: published MTG GHI accuracy) adds to the "forecast error" measured in US5.
- The archived run of record stands in for what a live consumer saw (F4: small differences).
- PSE's history shows the final publication. With one publication per day in the default window (F13),
  revisions do not affect it.
- The default window has no winter months: results hold for spring (13.5 % negative RCE quarter-hours in April,
  F13), not for the heating season.

## Open questions

Resolved by the user on 2026-11-12, revised 2026-11-13:
- **Q1** Forecast availability: one delay per forecast model, defined once: 120 min for ICON-D2 and synthetic
  forecasts (synthetic was 90 min; 105 until the T07 sample, 2026-11-13), 195 min for DMI HARMONIE.
- **Q2** Background sites: a replay option in this spec generates the synthetic fleet around the real-data sites.
- **Q3** Sites east of 20.34° E: supported through archived DMI HARMONIE runs (event from the first complete
  step, 195-minute delay), not refused.

For the plan:
- The CLI shape (subcommands, make target, `--seed`, `--run-id`, market option).
- The ERA5 margin past `end`; 120 m wind from the 10 and 100 m shear or the site's shear exponent.
- The clear-day check of the satellite end label (F8); a one-day `meta.json` sample for F6, both models.
- Where the per-model delay table lives so consumers can import it (leaning: `src/ems/contracts/`, replacing
  `forecast.availability_delay_min` in `weather.yaml`), and the optional first-step offset on
  `contracts.examples.forecast_event` (F3a).
- The throttle and daily-budget mechanics; how missing runs are told apart from bad parameters.
- Where the per-interval fallback mask lives, so quality can be set without touching the asset models.
- Task order: fetch first, so SC-002 lands before 2026-11-30.

Sources: [Single Runs API](https://open-meteo.com/en/docs/single-runs-api) ·
[Satellite Radiation API](https://open-meteo.com/en/docs/satellite-radiation-api) ·
[Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api) ·
[DWD API](https://open-meteo.com/en/docs/dwd-api) · [Terms](https://open-meteo.com/en/terms) ·
[Pricing](https://open-meteo.com/en/pricing) · [Licence](https://open-meteo.com/en/licence) ·
[PSE rce-pln](https://api.raporty.pse.pl/api/rce-pln) (all checked 2026-11-12; `meta.json` samples also
2026-11-13).
