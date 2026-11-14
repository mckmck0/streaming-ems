# 08 · Contracts

## Current consensus

### Versioning rules

- Every message and Kafka record carries `schema_version`; within a major version changes are additive only.
- Renaming or removing a field or an enum value means a new major version and a new topic, an ADR, coordinated
  updates of all producers and consumers, and refreshed golden examples. Additions bump the minor version.
- v1.0.0 was amended in place once, by spec 005, before any consumer existed (ADR 0016): the plant asset types
  and their quantities were removed. It is frozen since: `schemas/baseline/1.0.0/` holds the frozen schemas and a
  contract test fails on any removal against them.
- Producers validate strictly (unknown fields rejected); consumers ignore unknown fields. Exported schemas
  (`schemas/*.v1.json`, JSON Schema 2020-12) are open and checked FULL-compatible
  (`ems.contracts.schema_export.incompatibilities`); golden examples live in `schemas/examples/`.

### Wire contract v1.0.0 (Kafka, spec 001; ADR 0015)

Source of truth: `src/ems/contracts/` (pydantic). Full field tables:
`specs/001-data-pipeline-core/data-model.md`. Summary:

- **Envelope**: `schema` (`ems.measurement` | `ems.weather_forecast` | `ems.price`), `schema_version`,
  `event_id`, `stream`, `source_system`, `source_record_ref`, `location_id` (null for prices), `metering_point_id`
  (18-digit GSRN), `algorithm_id` (reserved, null), `run_id`, `interval_start`/`interval_end` (UTC, half-open),
  `resolution`, **`t`** (UTC quarter-hour index of `interval_start`, R7), `event_time_kind`, `issued_at`,
  `ingested_at` (wall clock, never part of identity), `synthetic`, `seed`, `payload`.
- **Identity**: `event_id = uuid5(EMS_NAMESPACE, "stream|subject|start|issued_at|origin_source|run_id|major")`
  with datetimes as RFC 3339 UTC, milliseconds, `Z`; subject = location id or market key; origin source =
  `source_system` without `replay:`. A replay inside its run scope keeps the id; a new `run_id` gives new ids.
- **Payloads**: measurements (one event per location and interval; asset, quantity, direction, aggregation,
  value ≥ 0, unit, quality), weather forecast (one event per location and run; up to 192 contiguous PT15M steps
  from the first complete one, so `interval_start` and `t` may lie after `issued_at` (archived DMI runs start at
  +30 min, spec 008); radiation is the preceding-interval mean labelled at `valid_end`, temperature/wind/cloud are
  instants at `valid_end`), price
  (one event per market and local delivery day; 92/96/100 slots; native currency per MWh; negatives allowed).
- **Measurement values** (spec 005): `asset_type` is `pv`, `bess`, `heat_pump`, `ev_charger`, `load` or
  `grid_connection`; `quantity` is `active_energy`, `soc` (both emitted), `active_power` or `reactive_energy`
  (reserved).
- **EMS-managed batteries** (ADR 0014): sites whose battery the EMS controls emit no `bess` and no
  `grid_connection` measurements.
- **`source_system` values in use** (pattern `^(replay:)?[a-z0-9-]+$`, no enum): `synthetic`, `open-meteo`
  (live forecasts), `pse-rce`, `pse-csdac`, `entsoe`, and since spec 008 `open-meteo-single-runs` (archived runs of
  record; their values differ slightly from the live API's, so they are not the same fact) and `synthetic-history`
  (meters of real-data sites, driven by observed weather). A PSE day built from history equals the live event of the
  same publication under the same `run_id`. History recordings always carry a non-null `run_id` (default: the
  recording name), so they never collide with live topics.

### Topics, keys, headers (`config/topics.yaml`)

| Topic | Key | Partitions live/sim · scale | Retention (live) |
|---|---|---|---|
| `ems.weather.forecast.v1` | `[run_id:]location_id` | 3 · 48 | 7 d |
| `ems.weather.actual.v1` (disabled) | `[run_id:]location_id` | 3 · 48 | 7 d |
| `ems.production.v1` | `[run_id:]location_id` | 3 · 48 | 7 d |
| `ems.consumption.v1` | `[run_id:]location_id` | 3 · 48 | 7 d |
| `ems.price.v1` | market key `PL:RCE`, `PL:SDAC`, `ENTSOE:PL`, `PL:TGE` (ADR 0012) | 1 · 3 | 30 d |
| `ems.gen.dlq.v1` | original key | 1 · 3 | 14 d, LogAppendTime |

- Partition = Java-compatible murmur2 of the key (`ems.contracts.topics.expected_partition`, no client import).
- Record timestamp = event time (`interval_end`; `issued_at` for forecasts and prices).
- Headers (`schema`, `schema_version`, `source_system`, `content-type`, `synthetic`; DLQ adds `error`) are for
  tooling only; the JSON value is self-contained.
- Values are UTF-8 JSON, no registry prefix, < 900,000 bytes with key and headers.
- Two measurement topics (ADR 0013): production and consumption arrive as two records per location and `t`.

### Handoff to the actor stages (`01-architecture.md`)

- Kafka reaches Restate only through the standalone Kafka ingress integration (Restate ≥ 1.8.1;
  <https://docs.restate.dev/services/integrations/kafka>, checked 2026-11-14): record key → Virtual Object key
  (must be UTF-8; never null on these topics), value → JSON handler input, one container per target handler (it
  may consume several topics), a stable consumer group id. The handler sees `kafka.topic`, `kafka.partition`,
  `kafka.offset` and `kafka.timestamp`; forwarding of record headers is not documented, so the JSON value stays
  self-contained (K6). Restate deduplicates a redelivered record by consumer group, topic, partition and offset.
  Restate's in-server Kafka subscriptions are documented as old and due for removal and are not used.
- Dedup by slot (stream, `t`) in object state: an identical `event_id` is ignored, different content for the same
  slot is a revision. No idempotency key is taken from a record.
- Join production and consumption per `(location, t)` by the watermark rules of ADR 0017; there is no cross-topic
  ordering. Actors order by the event's own `t` and `available_at`, never by the Kafka record timestamp, so
  CreateTime = event time stays the timestamp policy (this closes the former open question).
- Availability rule: a strategy at step `t` sees only data whose `available_at ≤ t` (meters: interval end;
  forecasts: `issued_at` + the delay of `payload.forecast_model`; prices: publication time). One table,
  `ems.contracts.availability` (spec 008), used by the generator's emission, the history build and consumers:
  `icon_d2` and `synthetic` 120 min, `dmi_harmonie_arome_europe` 195 min. Each delay is the largest measured
  Open-Meteo availability delay plus Open-Meteo's recommended 10-minute wait, rounded up to a quarter-hour: when a
  run was publicly usable (a live poller can only be later). It was a flat 90 min before spec 008, up to 27 min early
  for real ICON-D2 runs.
- Schedule constants for the watermarks of the sparse streams (ADR 0017) join the same module in stage 3: the run
  cadence per `forecast_model` and the opening of the price publication window (13:00 local on D−1). They are
  code, not wire contract, and the generator is tested against them.
- One pure function maps measurement events to the domain inputs (per quarter-hour PV and load energy and RCE; the
  policy's `Observation` is a domain type defined in `12-domain-model.md`, not a wire message) and is shared with
  the offline reference. Prices convert PLN/MWh → PLN/kWh (÷ 1000) in exactly one place,
  `ems.domain.units.pln_per_mwh_to_pln_per_kwh`. Tariff, RCEm and catalog config (`config/tariffs/`,
  `Site.net_billing`) are not part of the wire contract (spec 009).
- Run scoping: with `run_id` set, keys are `run_id:location_id` (fresh Virtual Object state per run). Actors read
  `run_id` from the event, never from the key (market keys contain `:`).
- Messages between actors are internal Restate messages, not wire contract: versioned models that carry
  `schema_version`, `t` and the sim timestamp (R7). Any new Kafka topic or event is wire contract (ADR 0016).

## What to build

- The internal actor messages of `01-architecture.md` (`Step`, `PriceUpdate`, `ForecastUpdate`, `Subscribe`,
  `Apply`, `Ack`, `Flows`) with stage 3; the result Parquet and run summary with stage 4; ranking and selection
  with stage 6.
- Results leave Restate through an export CLI that reads Virtual Object state after a run and writes the result
  Parquet and run summary (spec 006 decision). There is no Kafka result (egress) topic in v1.

## Prerequisites

- None.

## Open questions

- OPEN (spec 003): forecast volume at 100k sites (tens of GB/day); a weather-cell key may be added additively.
- OPEN (stage 3): consumer behaviour for a `forecast_model` missing from the availability table (producers
  refuse it); for example the most conservative delay plus a counter.
