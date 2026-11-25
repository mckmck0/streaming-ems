# 0022. How experiments measure the system

- Status: accepted
- Date: 2026-11-25
- Spec: specs/012-clocks-systems-experiments

## Context
P16 asks for systems metrics next to energy metrics: decision latency p50/p99, events/s, sustained speed,
recovery latency by phase, an exactly-once check. Object state holds no timings. Measured on the pinned server
(2026-11-22, run `demo1`, 2 managed households × 192 steps): completed invocations keep their journal 24 h, and the
journal holds a copy of the object's state for every read and write, 43.4 MB of journals against 130.8 kB of
state. Spec 006 decided that results are read from Restate state, not a Kafka egress topic.

## Decision
- **A timing log** written by the SDK service, outside state, messages and journal: one JSON line per handler
  commit (wall ns, service, handler, key, invocation id, the `t` handled and the `t` of each message sent), on
  only when `EMS_TIMING_LOG` is set, written by a `QueueListener` thread and flushed at exit and on SIGTERM. Per
  invocation the first line is kept; more lines count as retries. Decision latency is the `Battery` commit that
  acknowledges `t` minus the `Household` commit that releases it.
- **The runner's own events**: publication start (the generator's `paced_from_unix`), kills, restarts and health,
  plus sampled container memory and CPU, the restate cgroup's anonymous memory, the SDK process group's CPU and
  RSS, and the ingress groups' lag. End and stall detection read the timing log, so the measured server is polled
  at most every 5 s.
- **Journal retention** `0s` in experiments and live, `1d` in development (`EMS_RESTATE_JOURNAL_RETENTION`);
  idempotency retention stays 24 h.
- **A fresh stack per run**: the runner wipes the Kafka and Restate volumes and starts Kafka and Restate again
  (no `kafka-ui`), with its own ingress project `ems-exp-*`.
- **Export from state**: the export reads object state through SQL on the `state` table, decoded with the actors'
  serde, into Parquet (`households`, `strategies`, `months`, `timings`). Per-step series (`steps`) come from the
  offline fold, written only after the fold's final state equals the actors' with `==`. Live runs, which have no
  twin, are checked by invariants.

## Consequences
- Exact per-step latency at any scale with no new container; the timing log costs one queued record per
  invocation, and a `kill -9` loses the queued lines (counted as missing).
- At `0s` a completed invocation without an idempotency key leaves `sys_invocation` at once (measured
  2026-11-25), so a message that failed for good is caught by stall detection, not by the failure poll.
- Systems numbers of different runs are comparable: each starts from an empty server.
- Docs changed: `02-subsystem-actors.md`, `05-deploy-and-infra.md`, `09-risks-and-tech-debt.md` (A6, A14),
  `11-experiments.md`.

## Alternatives considered
- **Restate's `sys_invocation`.** No actor code and Restate's own clock, but only millisecond timestamps, steps
  matched to invocations through caller links, and polling the server being measured.
- **Runner sampling alone.** No actor code, but 1 s resolution: no latency percentiles.
- **OpenTelemetry tracing.** The standard tool, but a new pinned container, all-or-nothing tracing (P13) and
  about a million spans per scale run.
- **Journal retention of 10 minutes or 1 day.** At 12.9 kB per invocation, 10 minutes at 400 households holds
  about 2.6 GB of journals; a day distorts every memory and speed number.
- **A shared stack with run-scoped keys.** Nothing wiped, but later runs measure a fuller server.
- **Per-hour rows in write-once keys.** Series from the actors themselves, at the cost of three changed actors;
  the offline series are proved equal instead.
