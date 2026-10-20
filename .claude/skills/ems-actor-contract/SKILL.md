---
name: ems-actor-contract
description: Rules and template for changing Restate actors in the EMS system. Covers connection rules R1–R7, single-writer ownership, exclusive vs shared handlers, message envelopes and versioning, timers in sim and live mode, and failure modes. Use for any change to handlers, message schemas, timers or state ownership, when writing the Actor contract section of a plan.md, and when reviewing such a change.
---

# ems-actor-contract

Sources: `docs/context/01-architecture.md` (actors, ownership), `docs/context/02-subsystem-actors.md`
(handlers, registration), `docs/context/08-contracts.md` (schemas, versioning). If they disagree with this
skill, they win; say so.

## Connection rules

- **R1 / R2, no blocking cycles.** Exclusive handlers are reached only by one-way messages
  (`ctx.object_send`, optionally with `send_delay`). Request-response (`ctx.object_call`) targets shared
  (`kind="shared"`, read-only) handlers only, and shared handlers call only shared handlers. A wait-for
  cycle through an exclusive handler is then impossible. The allowed request-response targets today are
  `Tariff.prices`, `Weather.forecast`, `*Forecaster.forecast` / forecast quality, and `Strategy.decide`.
  (This handler-level wording clarifies the brief's "every edge that can be part of a cycle"; see 07.)
- **R3, journal everything non-deterministic.** HTTP calls, model inference, solver calls: `ctx.run_typed`
  or `ctx.run`. Time, randomness, ids: `ctx.time()`, `ctx.random()`, `ctx.uuid()`.
- **R4, idempotent ingress.** Idempotency key per household and interval, scoped by run (format in 08);
  Kafka record key = household id.
- **R5, never wait inside an exclusive handler.** No `time.sleep`, no `ctx.sleep`, no long awaits: they
  block the object's queue. Use delayed messages (live) or virtual timers in state (sim).
- **R6, generations.** Self-rescheduling jobs carry a generation number; a tick from an old generation
  stops its chain.
- **R7, envelope.** Every message carries `schema_version`, `t` and a sim timestamp. Receivers check
  monotonicity per sender and count violations instead of failing.

## Ownership

Every fact has exactly one writer (table in 01). Another actor may keep a view of it, tagged with the
source and `t`, but never writes it back. A new fact needs a row in the ownership table.

## Handler checklist

- Kind: exclusive (writes state; one invocation per key at a time) or shared (read-only, concurrent).
- Thin: validate → domain call → write own state → send. Business logic lives in `src/ems/domain/`.
- Inputs and outputs are versioned contract types from 08 (additive within a major version).
- Timers: sim mode stores virtual timers in state and fires them when an event with `t ≥ due` arrives;
  live mode uses `send_delay`. Same domain code in both.
- Failure modes covered: SDK service `kill -9`, server `kill -9`, duplicate delivery, out-of-order or late
  messages, stale views, registration of a new deployment while traffic flows.

## Actor contract section for plan.md

Every plan that touches actors fills in [actor-contract-template.md](actor-contract-template.md).
