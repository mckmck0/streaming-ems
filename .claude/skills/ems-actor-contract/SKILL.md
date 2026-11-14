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
  cycle through an exclusive handler is then impossible. No step path uses request-response today: shared
  handlers (`status`, `summary`, `state`, `bill`) are called only from outside the actors (tests, the export
  CLI). `Strategy.decide` is not used, because `Ems` evaluates the live policy itself (01). How forecast
  payloads reach a policy is decided in stage 5.
  (This handler-level wording clarifies the brief's "every edge that can be part of a cycle"; see 07.)
- **R3, journal everything non-deterministic.** HTTP calls, model inference, solver calls: `ctx.run_typed`
  or `ctx.run`. Time, randomness, ids: `ctx.time()`, `ctx.random()`, `ctx.uuid()`.
- **R4, idempotent ingress.** Each kind of duplicate is caught where it arises. A Kafka record redelivered
  after a restart: Restate deduplicates by consumer group, topic, partition and offset. Messages a handler
  sends: Restate deduplicates them across retries of that execution, so they need no idempotency key. The
  same event published twice: the slot table in object state drops it by `event_id` per (stream, `t`); no
  idempotency key is taken from a record. Control calls through HTTP ingress carry an `Idempotency-Key`.
  Kafka record key = household key, scoped by run (08).
- **R5, never wait inside an exclusive handler.** No `time.sleep`, no `ctx.sleep`, no long awaits: they
  block the object's queue. Waiting means buffering in state until the input watermarks pass (01); live
  deadlines are delayed messages.
- **R6, generations.** Self-rescheduling jobs carry a generation number; a tick from an old generation
  stops its chain.
- **R7, envelope.** Every message carries `schema_version`, `t` and a sim timestamp. Order between actors
  is not assumed: receivers apply messages in `t` or sequence order, hold back early ones, and count every
  reordering instead of failing.

## Time and ordering (ADR 0017, full model in 01)

A household releases step `q` only when the watermarks of all its input streams cover it. Watermarks come
from the stream's content and schedule in sim, and also from a wall-clock timer in live mode. Lateness and
dedup are judged inside a record's own stream. A handler never orders by arrival time or by the Kafka record
timestamp.

## Ownership

Every fact has exactly one writer (table in 01). Another actor may keep a view of it, tagged with the
source and `t`, but never writes it back. A new fact needs a row in the ownership table.

## Handler checklist

- Kind: exclusive (writes state; one invocation per key at a time) or shared (read-only, concurrent).
- Thin: validate → domain call → write own state → send. Business logic lives in `src/ems/domain/`.
- Inputs and outputs are versioned contract types from 08 (additive within a major version).
- Timers: sim mode has no wall-clock timers. A scheduled action is data in state, fired when its step is
  processed, and a step is processed when the watermarks cover it. Live mode adds durable deadlines with
  `send_delay` and a generation number. Same domain code in both.
- Failure modes covered: SDK service `kill -9`, server `kill -9`, duplicate delivery, out-of-order or late
  messages, stale views, registration of a new deployment while traffic flows.

## Actor contract section for plan.md

Every plan that touches actors fills in [actor-contract-template.md](actor-contract-template.md).
