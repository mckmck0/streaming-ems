# Contract: topics, keys, headers, record time (v1)

Single source: `config/topics.yaml`, loaded by `ems.contracts.topics` (shared with the ingestion spec).

| Topic | Class | Key | Partitions live/sim · scale | Retention (live) |
|---|---|---|---|---|
| `ems.weather.forecast.v1` | location | `[run_id:]location_id` | 3 · 48 | 7 d |
| `ems.weather.actual.v1` (disabled) | location | `[run_id:]location_id` | 3 · 48 | 7 d |
| `ems.production.v1` | location | `[run_id:]location_id` | 3 · 48 | 7 d |
| `ems.consumption.v1` | location | `[run_id:]location_id` | 3 · 48 | 7 d |
| `ems.price.v1` | global | market key `PL:RCE`, `PL:SDAC`, `ENTSOE:PL` (`[run_id:]` prefix when set) | 1 · 3 | 30 d |
| `ems.gen.dlq.v1` | global | original key | 1 · 3 | 14 d, LogAppendTime |

## Profiles (`EMS_TOPICS_PROFILE`)

| Profile | Overrides |
|---|---|
| `live` (default) | `message.timestamp.type=CreateTime`, `message.timestamp.after.max.ms=3600000`, `message.timestamp.before.max.ms` = `retention.ms` |
| `sim` | `retention.ms=-1`, `message.timestamp.after.max.ms=9223372036854775807`, before.max unlimited |
| `scale` | partitions 48 / 3, `retention.bytes` cap per partition |
| `cluster` | RF 3, `min.insync.replicas=2` (used in spec 004) |

Defaults: RF 1, `cleanup.policy=delete`, `compression.type=producer`, `min.insync.replicas=1`.

## Profile switching and isolation

- A profile is applied by `ems-gen topics ensure --profile P --apply-config`; `make topics PROFILE=sim` and
  `make gen-backfill` do this, so switching profiles never leaves stale time rules (preflight would refuse).
- `EMS_TOPIC_PREFIX` (default empty) prefixes every topic name; integration tests use a per-run prefix
  (`it-<run>.`) on the shared broker, so leftover data from earlier runs never affects results.

## Rules

- Partition = Java-compatible murmur2 of the UTF-8 key (`(murmur2(key) & 0x7fffffff) % n`). Keys are never null
  on location or price topics.
- Record timestamp = `interval_end` (measurements), `issued_at` (forecasts, prices); DLQ = append time.
- Headers (strings): `schema`, `schema_version`, `source_system`, `content-type: application/json`,
  `synthetic: true|false`; DLQ adds `error`. Headers are for tooling only: Restate does not forward them, so the
  JSON value is self-contained.
- Value = UTF-8 JSON envelope, no registry prefix, < 900,000 bytes with key and headers.
- Changing partition counts of an existing topic breaks per-key ordering; ensure refuses unless explicitly allowed.

## Downstream notes (ingestion spec)

- Production and consumption arrive as two records per location and `t`, with no cross-topic ordering: join per
  `(location, t)`.
- Dedup by `event_id` in object state (Restate 1.7.13 Kafka subscriptions dedupe by offset only).
- Price values are per MWh in native currency; the domain converts PLN/MWh → PLN/kWh (÷ 1000) in one place.
