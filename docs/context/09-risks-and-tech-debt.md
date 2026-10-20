# 09 · Risks and tech debt

## Current consensus

| Id | Risk / known issue | Status and fix hint |
|---|---|---|
| K1 | Kafka 4.x rejects CreateTime records > 1 h in the future and deletes old-timestamped records within minutes (retention by record time) | Mitigated: topic profiles (`live`/`sim`) + `ems-gen run` preflight; integration tests cover both |
| K2 | Env-configured `apache/kafka` drops single-node defaults (internal-topic RFs incl. share-group state) and does not persist without `KAFKA_LOG_DIRS` | Fixed in compose; restart test |
| K3 | Every `KAFKA_*` env var in the kafka container becomes a broker property | Fixed: no `env_file` on kafka; generator uses `EMS_*` |
| K4 | librdkafka on macOS tries `::1` for `localhost` | Fixed: `broker.address.family=v4` in all clients |
| K5 | kafka-ui v1.5.0 shows the Kafka 4.3 broker version as "Unknown" | Accepted for now (read-only UI); fallback broker 4.2.2 |
| K6 | Restate 1.7.13 Kafka subscriptions do not forward record headers and cannot set idempotency keys from records | Accepted: JSON is self-contained; dedup by `event_id` in object state (ingestion spec) |
| K7 | Two measurement topics need a per-interval join downstream (ADR 0013) | Accepted: ingestion spec designs the join |
| G1 | AR noise uses a 6-time-constant burn-in: values depend on the window start by < exp(−6) | Accepted: exact batch independence matters more (AC-001-26) |
| G2 | Realtime/accelerated runs generate in day chunks; truth is regenerated per chunk (burn-in makes seams negligible) | Accepted |
| G3 | Faker locale data changes between versions | Mitigated: exact pin 40.40.0, golden catalog test |
| G4 | Voivodeship bounding boxes are approximate | Accepted for sampling; VERIFY with GUGiK PRG if needed |
| G5 | windpowerlib is unmaintained (last release 2024-02-20; 1-D density correction) | Spec 004: vendor curves, numpy power curve |
| G6 | Forecast volume at 100k sites (tens of GB/day, 7-day retention) | Spec 003: benchmark; weather-cell key option |
| D1 | Brief §4 net-billing wording is outdated (RCE default from 2024-07-01; quarter-hourly since 2025-10-01) | Recorded in 12; settlement spec must VERIFY art. 4b |
| L1 | Open-Meteo ICON-D2 RUC (hourly runs) and the 2026-11-30 native-grid cutover can change run cadence and domain edges | Accepted: `issued_at` comes from meta.json; re-probe coverage and re-record fixtures after the cutover |
| L2 | Free Open-Meteo API is non-commercial only | Accepted (public research); set `EMS_OPEN_METEO_API_KEY` for any commercial context |
| L3 | PSE API reuse terms are unpublished; same-day re-publications were observed | Accepted: only a few fixtures committed; revisions become new events |
| L4 | ENTSO-E A44 has no publication time; `issued_at` = first sight of a content version (stored in recordings) | Accepted: replay keeps ids; live restarts may re-emit once |
| L5 | Hybrid live-site meters use forecast steps as "truth" weather (no observations) | Accepted: documented; observation sources are a later option |
| H1 | `Bash(make *)` is allow-listed, so `make reset` (`down -v`) bypasses the `ask` rule | Mitigated: target asks for confirmation; user may add `Bash(make reset*)` to `ask` |

Brief §13 risks (post-restart timer delay, Restate licence and maturity, `ty` pre-1.0, single-household load
noise, sandbox scale numbers) still apply to later specs.

## What to build

- Nothing on its own; each spec updates this table.

## Prerequisites

- None.

## Open questions

- None.
