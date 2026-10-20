---
paths:
  - "src/ems/actors/**"
---

# Actor adapters (`src/ems/actors/`)

Actors are thin Restate Virtual Objects (Python SDK 1.0.x) over the domain core. Before changing a handler,
message, timer or state ownership, invoke the `ems-actor-contract` skill and update `docs/context/08-contracts.md`.

- Handler shape: validate the message → call the domain → write only this actor's own state → send messages.
  No business logic in handlers.
- Single writer per fact (ownership table in `docs/context/01-architecture.md`). Copies of facts owned by
  other actors are views: tag them with their source and `t`, and never write them back.
- R1/R2: request-response (`ctx.object_call`) only to shared (read-only) handlers (`kind="shared"`); every
  edge that can close a cycle through exclusive handlers is a one-way `ctx.object_send`.
- R3: everything non-deterministic or external goes through the journal: `ctx.run_typed` / `ctx.run` for
  HTTP, model inference and solver calls, `ctx.time()`, `ctx.random()` and `ctx.uuid()` for time and randomness.
- Async-first: no blocking call on the event loop. Offload CPU-bound or blocking work inside `ctx.run` to a
  bounded executor. Never `time.sleep` or `ctx.sleep` inside an exclusive handler (R5); schedule with
  `send_delay` (live mode) or a virtual timer in state (sim mode, principle 7).
- R4: idempotent ingress, keyed by household; Kafka record key = household id.
- R6: self-rescheduling jobs carry a generation number and stale chains stop themselves.
- R7: every message carries `schema_version`, `t` and a sim timestamp; receivers check monotonicity and
  count violations instead of failing.
- Messages are versioned and additive within a major version; breaking changes need a new version, an ADR
  and refreshed golden replay examples.
