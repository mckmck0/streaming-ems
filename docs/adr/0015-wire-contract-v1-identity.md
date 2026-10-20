# 0015. Wire contract v1: quarter-hour index, run scope and event identity

- Status: accepted
- Date: 2026-10-20
- Spec: specs/001-data-pipeline-core

## Context
R7 requires `t` on every message and Kafka record; R4 requires idempotent ingress scoped by run; Plan 1's
`event_id` hashed `source_system`, so replays (`replay:<orig>`) would have changed ids despite its own promise.

## Decision
Envelope v1.0.0 adds `t` (`floor(unix(interval_start)/900)`) and an optional `run_id`. `event_id` = UUIDv5 over
stream, location or market key, canonical `interval_start` and `issued_at` (RFC 3339 UTC, ms, `Z`), the origin
source (without `replay:`), `run_id` and the major version. With `run_id` set, Kafka keys become
`run_id:location_id`. `issued_at` comes from the source (model run time, publication time), never the poll
time. Producers are strict, consumers tolerant; schemas are open and checked FULL.

## Consequences
Retries and in-scope replays keep ids; re-running a window as a new experiment run gets fresh ids and fresh
actor state. Restate 1.7.13 cannot take an idempotency key from a record, so the ingestion spec dedups by
`event_id` in state.

## Alternatives considered
Idempotency key `<household>:<t>` only (does not fit forecasts and prices); per-run household-id prefixes inside
`location_id` (breaks the id format).
