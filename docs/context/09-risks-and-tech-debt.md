# 09 · Risks and tech debt

## Current consensus

| Id | Risk / known issue | Status and fix hint |
|---|---|---|
| K1 | Kafka 4.x rejects CreateTime records > 1 h in the future and deletes old-timestamped records within minutes (retention by record time) | Mitigated: topic profiles (`live`/`sim`) + preflight in `ems-gen run` and, since spec 008, `ems-gen replay`; integration tests cover both |
| K2 | Env-configured `apache/kafka` drops single-node defaults (internal-topic RFs incl. share-group state) and does not persist without `KAFKA_LOG_DIRS` | Fixed in compose; restart test |
| K3 | Every `KAFKA_*` env var in the kafka container becomes a broker property | Fixed: no `env_file` on kafka; generator uses `EMS_*` |
| K4 | librdkafka on macOS tries `::1` for `localhost` | Fixed: `broker.address.family=v4` in all clients |
| K5 | kafka-ui v1.5.0 shows the Kafka 4.3 broker version as "Unknown" | Accepted for now (read-only UI); fallback broker 4.2.2 |
| K6 | Restate 1.7.13 Kafka subscriptions do not forward record headers and cannot set idempotency keys from records | Accepted: JSON is self-contained; dedup by `event_id` in object state (ingestion spec) |
| K7 | Two measurement topics need a per-interval join downstream (ADR 0013) | Accepted: ingestion spec designs the join |
| G1 | AR noise uses a 6-time-constant burn-in: values depend on the window start by < exp(−6) | Accepted: exact batch independence matters more (AC-001-26). Spec 007 adds the shared forecast-error process Z (two runs share it up to the residual) and the base-load AR noise; impulses and phase knots are exact |
| G2 | Realtime/accelerated runs generate in day chunks; truth is regenerated per chunk (burn-in makes seams negligible) | Accepted |
| G3 | Faker locale data changes between versions | Mitigated: exact pin 40.40.0, golden catalog test |
| G4 | Voivodeship bounding boxes are approximate | Accepted for sampling; VERIFY with GUGiK PRG if needed |
| G5 | windpowerlib is unmaintained (last release 2024-02-20; 1-D density correction) | Obsolete (spec 005): wind farms and the vendored curves were removed |
| G7 | Synthetic 10 m wind is weak inland: old turbine types reach 10–18 % capacity factor outside the north | Obsolete (spec 005): no wind farms; the 10 m wind only feeds PV cell temperature and the price model |
| G8 | Households now get EVs (20 %), so 003-era catalogs emit one more measurement per such site | Accepted: separate hash, no other draw changes; benchmark rerun 2026-10-20 |
| K8 | With combined KRaft nodes, two of three down loses the quorum and writes time out instead of NOT_ENOUGH_REPLICAS | Obsolete (spec 006): the 3-broker profile was removed |
| K9 | One broker, RF 1: a broker outage pauses producers and consumers until it restarts; data survives restarts on the named volume (restart test in `tests/integration/test_stack.py`) | Accepted (spec 006): broker replication and failover are out of thesis scope |
| G6 | Forecast volume at scale: 10k sites ≈ 6.2 GB/day of JSON (benchmark 2026-10-20), ~70 % forecasts | Accepted; a weather-cell key (shared forecasts) can be added additively if Plan 2 needs it |
| S1 | Event building and strict validation (~8–12k events/s) bound throughput, not the models | Accepted; validation can be sampled for very large runs |
| S2 | Battery and register state continue across chunks, but chunk borders still carry the AR burn-in residual (G1) | Accepted |
| D1 | Brief §4 net-billing wording is outdated (RCE default from 2024-07-01; quarter-hourly since 2025-10-01) | Recorded in 12; settlement spec must VERIFY art. 4b |
| L1 | Open-Meteo ICON-D2 RUC (hourly runs) and the 2026-11-30 native-grid cutover can change run cadence and domain edges; Open-Meteo marks ICON-D2 "Discontinued 2026-11-30" and does not say whether archived runs stay retrievable (VERIFY) | Accepted: `issued_at` comes from meta.json; re-probe coverage and re-record fixtures after the cutover. The spec 008 window was fetched 2026-11-13 and its raw data no longer depend on the archive; the manifest records each response's grid cell. Windows fetched later can use DMI HARMONIE, which covers all of Poland but forecast GHI 11–19 % worse at Poznań (spec 008 plan §3); a model override is later work |
| L2 | Free Open-Meteo API is non-commercial only | Accepted (public research at a public institution: the thesis is university work, not work for an employer); set `EMS_OPEN_METEO_API_KEY` for any commercial context (the Historical and Satellite APIs need a Professional plan) |
| L3 | PSE API reuse terms are unpublished; same-day re-publications were observed. A past `business_date` query returns one publication per day, so an earlier version would be invisible in history; the RES act (art. 4b) defines no RCE correction procedure | Accepted: only a few fixtures committed; revisions become new events; `build_day_event` keeps the latest row per slot (spec 008). History builds count flagged days (more than one publication time, or not on D−1); none in 2026-04-01 … 07-01 |
| L4 | ENTSO-E A44 has no publication time; `issued_at` = first sight of a content version (stored in recordings). Confirmed on live responses 2026-10-29: `createdDateTime` equals the request time, so ENTSO-E prices look available only from the first fetch | Accepted: replay keeps ids; live restarts may re-emit once; PSE prices carry the real publication time |
| L5 | Hybrid live-site meters used forecast steps as "truth" weather, so forecast-driven strategies were oracles | Narrowed (spec 007): truth = latest live step − synthetic forecast error (`LiveForecastTruth`). Accepted until the historical-backfill spec brings satellite truth: the deviation statistics are assumed (amplitude only, no timing error), truth kt jumps by ≈ 0.12 when a new run arrives, and the logit space regresses truth mildly toward mid kt (−0.05 under clear-sky forecasts, +0.07…+0.15 under overcast ones in a prototype; VERIFY against published kt-dependent biases). Resolved for history recordings (spec 008): their meters use observed truth (satellite, ERA5); unchanged for live hybrid mode |
| G9 | Base-load noise calibrated on German single-family houses with heat pumps (WPuQ 2019), not Polish households; τ_slow fixed at 3 h (2 h fits the day level slightly better); the model's day-to-day sd is 0.015 above the data's; synthetic households have no persistent individual profile (reported as `profile_deviation_sd`); G25 noise uncalibrated; Polish appliance rates and ownership VERIFY (UK CREST rates, GUS national averages; PV prosumers may own more ovens and dishwashers) | Accepted (spec 007): refit through the CSV layout when Polish data become available |
| G10 | Forecast-error simplifications: constant ρ (run-to-run correlation does not decay with run separation), timing error on kt only, 12 h phase knots (lower variance mid-segment), oven impulses follow the H25 shape (about 15 % at night); ρ, ρ_φ and the timing sd have no published value | Accepted (spec 007): parameters cited with VERIFY marks; the historical-backfill spec can measure them (Single Runs vs satellite). Measurable since spec 008: `jobs/calibrate.py forecast-error` on a history recording writes a candidate (upper bounds; review before adopting) |
| G11 | Same ids with new values: topics filled before spec 007 with the same seed and no `run_id` keep old values under the ids new runs produce (identity has no values) | Accepted (no consumer yet): `make reset` or a new `run_id`. History builds refuse a rebuild under the same `run_id` with other inputs, and `replay --background` refuses changed calibration or catalog (spec 008) |
| H1 | `Bash(make *)` is allow-listed, so `make reset` (`down -v`) bypasses the `ask` rule | Mitigated: target asks for confirmation; user may add `Bash(make reset*)` to `ask` |
| L6 | Satellite data terms (DWD MTG radiation via Open-Meteo, from EUMETSAT MTG) are not published on Open-Meteo's licence page (checked 2026-11-13) | Accepted: only hand-written satellite bodies in tests; raw data never committed; VERIFY before any redistribution |
| L7 | Archived forecast runs (Single Runs API) differ slightly from what the live API served for the same run and grid cell (GHI a few W/m², temperature 0.1–0.2 K, 10 m wind ≤ 0.3 m/s; 2 runs, 2026-10) | Accepted: archived events use `source_system` `open-meteo-single-runs`, so they are never confused with live ones |
| L8 | Forecast availability delays rest on 6 ICON-D2 and 3 DMI runs (2026-10-20 … 11-13; the 12 UTC ICON-D2 run of 2026-11-13 took 106.7 min); spring 2026 delays cannot be measured afterwards. DMI forecasts are usable 75 min later than ICON-D2 ones and their 15-minute radiation is interpolated from hourly output | Accepted: 120 / 195 min include Open-Meteo's 10-minute wait and a rounding margin; the rule describes public availability, a live poller (5-minute meta poll) can only be later. A slower run would still be usable early: re-sample before a new window |
| L9 | Observed truth is a satellite (2.5 km pixel) and reanalysis (25 km) estimate: gaps (2.1 % of daylight quarters at the two sites in 2026-04-02 … 06-30, filled from ERA5), low-sun zeros, no published accuracy for DWD MTG 10-minute GHI (SARAH-3 vs BSRN as a proxy: about 5 W/m² monthly, 11 W/m² daily mean absolute difference, Pfeifroth et al. 2024), satellite DNI/DHI provenance VERIFY; on clear spring days satellite and ERA5 run 11–13 % above pvlib's Ineichen clear-sky (21 % and 17 % of ERA5 fills clip at 1.1). Truth resampling uses data up to 5 min (satellite) and 45 min (ERA5) after an interval's end (reconstruction, not availability). Runs issued before the window are not fetched (first forecast usable 120/195 min after `start`); the first and last local delivery days are partial; recent ERA5 is preliminary (ERA5T); the default window has no winter months | Accepted (spec 008): fallbacks marked `estimated` and counted in `build.json`; experiments score complete local days; a Lindenberg (BSRN) comparison with DWD station data would settle the bias (not in scope) |

Brief §13 risks (post-restart timer delay, Restate licence and maturity, `ty` pre-1.0, single-household load
noise, sandbox scale numbers) still apply to later specs.

## What to build

- Nothing on its own; each spec updates this table.

## Prerequisites

- None.

## Open questions

- None.
