---
paths:
  - "src/ems/actors/**"
---

# Actor adapters (`src/ems/actors/`)

Actors are thin Restate Virtual Objects (Python SDK, pinned in stage 2) over the domain core. The actors, their
edges and the time and ordering model are in `docs/context/01-architecture.md` (ADR 0017). Before changing a
handler, message, timer or state ownership, invoke the `ems-actor-contract` skill and update
`docs/context/08-contracts.md`.

- Handler shape: validate the message → call the domain → write only this actor's own state → send messages.
  No business logic in handlers.
- Single writer per fact (ownership table in `docs/context/01-architecture.md`). Copies of facts owned by
  other actors are views: tag them with their source and `t`, and never write them back.
- R1/R2: request-response (`ctx.object_call`) only to shared (read-only) handlers (`kind="shared"`); every
  edge that can close a cycle through exclusive handlers is a one-way `ctx.object_send`.
- R3: everything non-deterministic or external goes through the journal: `ctx.run_typed` / `ctx.run` for
  HTTP, model inference and solver calls, `ctx.time()`, `ctx.random()` and `ctx.uuid()` for time and randomness.
- Async-first: no blocking call on the event loop. Offload CPU-bound or blocking work inside `ctx.run` to a
  bounded executor. Never `time.sleep` or `ctx.sleep` inside an exclusive handler (R5). In sim mode, wait by
  buffering in state until the input watermarks pass; a scheduled action is data fired at its step. Wall-clock
  deadlines with `send_delay` exist only in live mode (principle 7).
- Ordering: release a step only when the watermarks of all its input streams cover it. Judge lateness and
  duplicates inside the record's own stream, never by arrival time or the Kafka record timestamp.
- R4: idempotent ingress. Restate deduplicates redelivered Kafka records and a handler's own sends; the slot
  table in state drops an event published twice (`event_id` per stream and `t`). Kafka record key = household
  key. A handler fed from Kafka must not raise on bad data, because a retried record would block its key: it
  counts the record and returns. Whether that needs raw-bytes input is VERIFY until stage 2 (V2, V3 in 01).
- R6: self-rescheduling jobs carry a generation number and stale chains stop themselves.
- R7: every message carries `schema_version`, `t` and a sim timestamp; receivers apply messages in `t` or
  sequence order, hold back early ones, and count every reordering instead of failing.
- Messages are versioned and additive within a major version; breaking changes need a new version, an ADR
  and refreshed golden replay examples.
