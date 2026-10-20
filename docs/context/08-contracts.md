# 08 · Contracts

## Current consensus

### Versioning rules

- Every message and Kafka record carries `schema_version`; within a major version changes are additive only.
- Renaming or removing a field means a new major version and a new topic, an ADR, coordinated updates of all
  producers and consumers, and refreshed golden examples.
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
  value ≥ 0, unit, quality), weather forecast (one event per location and run; 192 PT15M steps; radiation is the
  preceding-interval mean labelled at `valid_end`, temperature/wind/cloud are instants at `valid_end`), price
  (one event per market and local delivery day; 92/96/100 slots; native currency per MWh; negatives allowed).
- **EMS-managed batteries** (ADR 0014): sites whose battery the EMS controls emit no `bess` and no
  `grid_connection` measurements.

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

### Handoff to the Kafka-ingestion spec (Plan 2)

- Restate 1.7.13 legacy Kafka subscriptions: record key → Virtual Object key (must be UTF-8; never null on these
  topics), value bytes → handler input. Record headers are **not** forwarded (only `kafka.timestamp`,
  `kafka.offset`, `kafka.partition`, `kafka.key`, …). Offsets are deduplicated, but no idempotency key can be set
  from a record: dedup by `event_id` in object state. Create subscriptions with `auto.offset.reset: earliest` and a
  stable `group.id`. The standalone ingress (`ingress-integration-kafka`) needs Restate ≥ 1.8.1.
- Join production and consumption per `(location, t)`; there is no cross-topic ordering.
- Availability rule: a strategy at step `t` sees only data whose `available_at ≤ t` (meters: interval end;
  forecasts: `issued_at` + 90 min; prices: publication time).
- One pure function maps measurement events to the domain observation `{t, pv_kw, load_kw}` and is shared with the
  offline reference. Prices convert PLN/MWh → PLN/kWh (÷ 1000) in exactly one place.
- Run scoping: with `run_id` set, keys are `run_id:location_id` (fresh Virtual Object state per run).

## What to build

- Initial actor contracts (tick, step result, ranking, selection, command, SoC ack, energy flows, result Parquet,
  run summary, strategy registry entry) with the actor-skeleton spec.

## Prerequisites

- None.

## Open questions

- OPEN (ingestion spec): Kafka record timestamp policy if handlers need wall-clock order (CreateTime = event time
  is the default).
- OPEN (spec 003): forecast volume at 100k sites (tens of GB/day); a weather-cell key may be added additively.
