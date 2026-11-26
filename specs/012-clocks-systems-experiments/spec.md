# Feature Specification: Clocks and systems experiments

**Feature Branch**: `012-clocks-systems-experiments` · **Created**: 2026-11-22 · **Status**: Approved 2026-11-22;
revised with the plan after the architecture and energy-domain reviews (AC-012-01, -06, -07, -09, -12, -16 … -19,
-24 … -30, -32, -34, -36 reworded; see `plan.md`, Review); amended 2026-11-23 (review gaps: FR-010,
AC-012-38 … -42 added, AC-012-28 and -32 extended, two facts corrected; then the second review round:
AC-012-07, -26 … -29, -32, -34, -36, -38, -41 tightened), awaiting approval

**Input**: The stage 4 brief in `docs/context/01-architecture.md`. User decisions of 2026-11-22, each taken
after comparing alternatives (table below; the alternatives go into ADRs 0020–0022).

## Goal

Finish how the actors handle time, and measure the system:

1. a live wall-clock deadline per household, so that a missing reading or a late publication cannot stall a
   live decision;
2. strategies added and removed while a run is going, from a step `t`, without a restart;
3. an experiment runner (`make exp`) that publishes paced data, kills processes, changes strategies, exports
   object state to Parquet and reports systems metrics next to the energy results (P16);
4. results that stay identical to the offline side under `kill -9` of the SDK service and of the server.

The runs of this stage are **validation runs** on the author's laptop: they show that the measurement works.
Thesis-scale evaluation follows once all stages are built and tested, at growing scale and in a cluster (user,
2026-11-22). The runner is built so that scaling up is a config change.

## Decisions

| Item | Decision |
|---|---|
| Source of systems metrics | A timing log written by the SDK service, outside state, messages and journal: one line per handler invocation of a configured object, with the wall time, the service, handler and key, the invocation id, and the `t` of the message handled and of each message sent. Together with the runner's own events (publish start, kills, restarts, health) and sampled memory and CPU. *Not chosen: Restate's `sys_invocation` (millisecond timestamps only, steps matched to invocations through caller links, and polling the server being measured; its timestamps need no journal, since `completion_retention` is kept apart from `journal_retention`, and T03 records what stays at journal retention 0), runner sampling alone (1 s resolution, no latency percentiles), OpenTelemetry (new container, all-or-nothing tracing, span volume)* |
| Invocation retention (risk A6) | Journal retention 0 for experiments and live, 1 day for development. Compose reads it from `EMS_RESTATE_JOURNAL_RETENTION` (default `1d`); the runner starts the stack with `0`. Idempotency retention stays 24 h. *Measured 2026-11-22 on the pinned server, run `demo1` (2 managed households, 3 `Household` objects, 192 steps from 2026-10-31T00:00Z): completed invocations keep their journal 24 h; 3,371 invocations held 43.4 MB of journals (GetEagerState 24.4 MB, SetState 14.1 MB) against 130.8 kB of live state, about 11 MB per household and day of data* |
| Export and per-step data (risk A9) | The export reads what object state holds through SQL on the `state` table: per household and strategy the closed months, the open month's rows, SoC and counters. Per-step and hourly series come from the offline fold, used only after its final state equals the actors' with `==`. No split of the state value; its size is measured |
| Isolation between runs | A fresh stack per run: the runner wipes the Kafka and Restate volumes and starts compose again; `make exp` asks for confirmation once |
| Live clock | One grace `G` per run (`grace_s`, default 120 s) for all four streams. The chain starts once the household holds its price snapshot. A deadline that fires late releases every step that is due in one firing, then lines up with the next quarter boundary plus `G` |
| Strategy changes | `Household.set_strategies(t, strategies)`: from step `t`, which starts a UTC hour, the household feeds exactly that set. An added strategy starts at `t` on a fresh virtual battery with the run's initial SoC; a removed one receives nothing from `t` and keeps its state. A change that cannot take effect exactly at `t` is refused, recorded and counted, never silent. Live: `t` is the first full UTC hour after the request. Sim: the runner sends the change while the publication is still before `t`. *Hour boundary: settlement nets per hour and `RunParams.first_step` must start an hour* |
| Live evaluation | Tests, and one recorded realtime run of about 2 h with 10 households and faults. Experiments stay on simulated time (P7) |
| Proxy RCEm table (risk D4) | Built by the runner for synthetic data, for every civil month the run's price signal and settlement need: from two months before the window's first month through the month of its last step. History recordings keep the PSE values. The table, the entries used and their source go into the manifest. Live runs take RCE-basis sites only |
| History pacing (risk A1) | `ems-gen replay --speed` paces a history recording by emission time, with the pacing helper `ems-gen run --mode accelerated` uses |
| Strategy dimension of scale runs | 1 and 2 strategies (the policies that exist); strategies are a config field, so stage 5 reruns the configs |
| Experiment volume | A minimal validation set (about 3 h plus the 2 h live run) |
| SDK service | One hypercorn worker, as in every test tier |

## How each item the brief leaves to this stage is resolved

| Item (01 stage 4 brief, 11) | Resolved by |
|---|---|
| The grace period and catch-up of the live timer | AC-012-02, -07; ADR 0020 |
| A strategy change that arrives after `t0` | AC-012-12; ADR 0021 |
| The experiment runner, how the export reads state, where systems metrics come from | AC-012-25 … -32; ADR 0022 |
| Pacing a history replay | AC-012-22 |
| The builder of the proxy RCEm table | AC-012-24 |
| Whether to split an object's `state` value (A9) | Not split; sizes measured (AC-012-29) |
| The invocation retention for scale runs (A6) | AC-012-18; ADR 0022 |
| V10 re-run at scale | AC-012-33 and the server kills of `sys-recovery` (AC-012-36) |

## Scope

In scope:
- `src/ems/assembler/`: `RunParams` 1.1.0 (`mode`, `grace_s`), the clock rule (`meter_stream`, `join`), the
  strategy schedule helpers, the offline fold with a change history.
- `src/ems/actors/`: `Household.on_deadline` and `Household.set_strategies`, the messages `Deadline` and
  `StrategyChange`, a delayed send in `shell`, the timing log (`timing.py`), `app.py`.
- `src/ems/experiments/`: `config.py`, `run.py`, `export.py`, `metrics.py`, `rcem.py`; `stack.py` (fresh stack,
  paced publishing, ends with strategy changes, host-process sampling); `runs.py` (RCEm table as an argument).
- `src/ems/ingest/`: `ems-gen replay --speed` for history recordings, a shared pacing helper, the pacing start
  time in the generator's run manifest.
- `src/ems/settings.py`: `EMS_TIMING_LOG` (optional).
- `infra/docker-compose.yml` (journal retention variable), `Makefile` (`exp`), `experiments/configs/`.
- Tests in all five tiers; the five validation runs.
- Fixes to code that exists on `main` and that the measurements rely on (amendment 2026-11-23): the harness's
  failure polling (`stack.py`), Restate's memory pools and a memory limit for `kafka-ui` (compose), the serde's
  refusal of values that do not survive JSON (`serde.py`), stored-state fixtures of version 1.0.0 with the
  additive rule for stored state (08), and the procedure for paused invocations (02).
- Docs: 00, 01, 02, 05, 06, 08, 09, 11; ADRs 0020–0022; `CLAUDE.md`; the `ems-experiment` skill.

Out of scope:
- Thesis-scale experiment matrices and the cluster deployment (the evaluation comes later; where it runs is
  recorded as OPEN in `00-overview.md`).
- Switching the live strategy and the Selector (stage 6); new policies (stage 5).
- Several SDK workers; splitting object state; Prometheus or OpenTelemetry.
- Exact replay of a live run (risk A4 stays accepted).
- Any change to wire contract v1, the topics or the generator's data.

## Affected

- Anyone running the stack: compose gains `EMS_RESTATE_JOURNAL_RETENTION` (unset keeps today's 1 day).
  `make exp` wipes the Kafka and Restate volumes after a confirmation.
- `RunParams` gains two fields with defaults; parameters stored by stage 3 decode unchanged.
- The harness (`run_ends`, `assert_run`) follows strategy changes; existing tiers keep their meaning.
- `ems-gen replay --speed` now works for history recordings (it was refused).
- Restate gets explicit memory pools (invoker, query engine, Bifrost record cache) beside RocksDB, and
  `kafka-ui` a memory limit. The harness stops waiting on a query the admin API rejects, instead of running to
  its timeout. The serde refuses a NaN or an infinity before it is stored or sent (none occurs today: the
  domain guards its divisions).
- No wire contract, topic or key change. No reported thesis number changes: the stage produces validation runs
  only, and the results in `11-experiments.md` stay as they are.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - A live household never stalls on missing data (Priority: P1)
In live mode, each household's deadline declares quarter-hours that ended more than `G` ago complete; a missing
reading becomes an estimate and a later reading a correction, exactly as ADR 0018 describes for sim.
**Acceptance**: AC-012-01 … -11.

### User Story 2 - Strategies join and leave a running fleet (Priority: P1)
The operator adds or removes strategies from an hour `t`; every household that can apply the change exactly at
`t` does, the others refuse it visibly, and the offline twin reproduces every strategy.
**Acceptance**: AC-012-12 … -17, -34.

### User Story 3 - One command runs and measures an experiment (Priority: P1)
`make exp NAME=<id>` runs a config end to end on a fresh stack and writes data, metrics and a manifest that
make the run reproducible (P14) and comparable (P16).
**Acceptance**: AC-012-18 … -32.

### User Story 4 - Results survive crashes (Priority: P1)
A `kill -9` of the SDK service or of the Restate server during a run leaves results equal to the offline side,
and the recovery is measured by phase.
**Acceptance**: AC-012-30, -33, -36.

### User Story 5 - The measured system is not distorted by its harness or its configuration (Priority: P1, amendment 2026-11-23)
The harness polls without loading the server it measures, Restate's memory fits its container, a value that
cannot be stored is refused where it is produced, state stored before this stage still reads, and a paused
invocation can be found and resumed.
**Acceptance**: AC-012-38 … -42.

### Edge Cases
- A stream that never delivers a reading: the clock closes its slots on the estimate 0; a first reading that
  arrives later is a correction or `too_old`, never lost without a count.
- A deadline that fires while the household lags far behind (after an outage): every due step is released in
  one firing.
- A deadline that fires early (the host and the Docker VM clocks differ): it reschedules and is not stale.
- A window that starts in the future: the first deadline releases nothing and waits for the first step's end
  plus `G`.
- A correction that would attach to a step at or after `end_step`: counted, not applied, on both sides.
- One release batch that spans a change's `t`: each step goes to the set in force at its own `t`.
- A step after `t` that carries a correction for a slot before `t`: the added strategy drops and counts it.
- Re-adding a strategy removed earlier, removing the live strategy, a `t` off the hour or already released:
  refused and counted.
- A removed strategy's open month never closes: its result covers `[start, stop)` and is compared per step.
- An added strategy pays a month's fixed charges for a partial month, and starts at half capacity while the others
  carry their own SoC: it is compared per hour of ledger cost from `t`, with its start and end SoC, and its bill
  is flagged `partial`.
- A window that closes no month: bill and deposit are null, not 0.
- A curve published before a quarter starts but delivered more than `G` after it ends (Kafka lag, a kill) is
  replaced in live where sim would use it: counted `price_replaced` (risk A12 widened to delivery delay).
- A killed SDK service loses the timing lines still in its queue: the metrics count the steps without a line.
- A message lost to a terminal failure (an object not configured, an input that cannot be read) leaves its
  receiver waiting for its number: the run is marked `stalled` with a diagnosis instead of waiting forever.
- A NaN or an infinity in a value to store or send: the invocation fails with a retryable error and pauses where
  the value was produced; nothing is stored.

## Requirements *(mandatory)*

- **FR-001**: The assembler MUST apply a wall-clock deadline as a pure rule that changes nothing in sim.
  → AC-012-01 … -06
- **FR-002**: `Household` MUST run one durable deadline chain per live household and none in sim.
  → AC-012-07 … -11
- **FR-003**: Strategies MUST be added and removed from an hour `t`, exactly or not at all, and the offline twin
  MUST reproduce them. → AC-012-12 … -17
- **FR-004**: Journal retention MUST be configurable per environment. → AC-012-18
- **FR-005**: The SDK service MUST be able to log handler timings without changing any journaled operation.
  → AC-012-19, -20
- **FR-006**: Publishing MUST be pacable for synthetic and history data without changing results.
  → AC-012-21 … -23
- **FR-007**: Synthetic runs MUST settle RCEm-basis sites with a proxy table from their own prices.
  → AC-012-24
- **FR-008**: The runner MUST run, export and measure experiments reproducibly. → AC-012-25 … -32
- **FR-009**: Results MUST stay equal to the offline side under kills and strategy changes, and the validation
  runs MUST be recorded. → AC-012-33 … -37
- **FR-010** (amendment 2026-11-23): The harness, Restate's configuration, the serde and the stored-state
  contract MUST neither distort the measurements nor leave a failure silent, and paused invocations MUST be
  recoverable by a documented procedure. → AC-012-38 … -42

### Acceptance criteria (EARS)

`L` is `late_slots`, `G` is `grace_s`. "Clock step" is `⌊(now − G) / 900 s⌋`, capped at `end_step`: the steps
below it ended at least `G` ago. "Equal" means `==` on the decoded dataclasses.

Clock rule (assembler, pure)
- **AC-012-01**: WHERE a run is in sim mode, THE SYSTEM SHALL release the same steps, flags, corrections and
  counters as in spec 011 for any input, with one exception: a correction that could only attach at or after
  `end_step` is counted in the join's `after_end_late` (a subset of the stream's `late`) and dropped, instead of
  being kept unapplied. The spec 011 tests SHALL pass unchanged except that end-step case in
  `tests/unit/assembler/test_join.py`.
- **AC-012-02**: WHEN the clock step reaches `c`, THE SYSTEM SHALL close every needed meter stream's slots below
  `c` (with the reading when one is held, else with the estimate of ADR 0018, flagged `estimated`), raise the
  price and forecast watermarks to at least `c`, and release every step the release rule of ADR 0017 then
  allows.
- **AC-012-03**: WHEN a reading arrives for a slot that closed on an estimate, THE SYSTEM SHALL judge it against
  the stream's horizon before the reading (the higher of its newest reading and its last closed slot): below
  `2L` behind the horizon it SHALL become one correction attached to the step after the horizon; otherwise it
  SHALL be counted `too_old`. This SHALL hold also when the stream had no reading before.
- **AC-012-04**: WHEN a curve or a forecast run reaches the household's join, THE SYSTEM SHALL make it visible
  from the later of its own visibility and the household's watermark for that stream before the update; in sim
  this SHALL change no visibility.
- **AC-012-05**: THE SYSTEM SHALL release no step at or after `end_step` and give no step a curve or forecast run
  visible after the step's start, with or without clock events; a correction due at or after `end_step` SHALL be
  counted and not applied, on the actors and the offline side alike.
- **AC-012-06**: WHEN records, updates and non-decreasing clock steps are applied in any interleaving that keeps
  each stream's own order, THE SYSTEM SHALL account for every accepted reading once (as its slot's value, as a
  correction applied at a released step, or in `too_old` or `after_end_late`), release steps strictly in order,
  and no transition SHALL raise.

Deadline (actor)
- **AC-012-07**: WHEN a household in live mode has applied its price snapshot from `Tariff`, THE SYSTEM SHALL
  send it one `Deadline` for `first_step` without delay; WHEN the `Deadline` it expects arrives, THE SYSTEM SHALL
  read the time once through the journaled `ctx.time()`, apply AC-012-02, count the firing and, unless the clock
  step has reached `end_step`, send exactly one next `Deadline`, delayed to the moment the clock step next
  increases plus a margin of 1 s, and store the step it now expects. A firing whose clock step has not advanced
  (it came early: the Docker VM clock leads the host) SHALL be counted as early and reschedule the same step with a
  delay of at least 1 s. *(Starting after the snapshot, not at `configure`, keeps a first firing from
  raising the price watermark before the household holds the curves.)*
- **AC-012-08**: IF a `Deadline` arrives for another step than the one expected, or after the chain stopped,
  THEN THE SYSTEM SHALL count it and send nothing.
- **AC-012-09**: WHERE a run is in sim mode, THE SYSTEM SHALL schedule no `Deadline` and read no clock; a test
  SHALL allow `ctx.time` and `send_delay` in `src/ems/actors/` only on the deadline path, and the wall clock only
  in `timing.py`.
- **AC-012-10**: WHEN the SDK service is killed with SIGKILL while a live household's deadline is pending, and
  started again, THE SYSTEM SHALL fire that deadline and keep exactly one pending deadline per live household.
- **AC-012-11**: WHEN a deadline invocation is retried, THE SYSTEM SHALL write the same state bytes and send the
  same messages, the delay being derived only from the journaled time.

Strategy changes
- **AC-012-12**: WHEN `set_strategies(t, strategies)` reaches a configured household, THE SYSTEM SHALL accept it
  only if `t` starts a UTC hour, lies at or after the next unreleased step and before `end_step`, lies after
  every pending change, names only known policies, keeps the live strategy, and re-adds no strategy removed
  earlier in the run; an accepted change SHALL apply when step `t` is released, before `t` is routed. IF any
  condition fails, THEN THE SYSTEM SHALL store the refusal with its reason, count it, and change nothing else.
  The call SHALL carry an `Idempotency-Key` made of the key, `t` and the sorted strategy names.
- **AC-012-13**: THE SYSTEM SHALL route every released step to the strategy set in force at that step's own `t`,
  also when one release batch spans a change.
- **AC-012-14**: WHEN a strategy joins at `t`, THE SYSTEM SHALL run it with the parameters of
  `strategy_params(params, change)` (`first_step = t`, the run's initial SoC); WHEN a step it receives carries a
  correction for a slot before its first step, THE SYSTEM SHALL count and drop that correction, on the actors
  and the offline side alike.
- **AC-012-15**: WHEN a strategy is removed at `t`, THE SYSTEM SHALL send it no step at or after `t` and SHALL
  keep its state.
- **AC-012-16**: WHEN the offline fold is given a household's change history, THE SYSTEM SHALL give every
  strategy a result equal to that strategy object's state. WHEN all transitions run as an in-memory network in
  sim with changes and an arbitrary delivery order, THE SYSTEM SHALL end equal to the offline twin. WHEN they run
  in live mode with a fake clock, THE SYSTEM SHALL keep the invariants of AC-012-05 and -06, give every strategy
  a result equal to `step_strategy` folded over the steps its household sent it, keep `Battery` and `Meter` equal
  to the live strategy's shadow, and hold at most one pending `Deadline` per household. *(A live run has no
  offline twin: where the clock closes a slot depends on when it fires, risk A4.)*
- **AC-012-17**: WHEN the runner adds strategies, THE SYSTEM SHALL configure every added object, wait until each
  stores parameters equal to those sent, and only then send `set_strategies`; in sim it SHALL send the change
  while the publication schedule is still at least one hour of data before `t`; in live it SHALL choose `t` as
  the first full UTC hour after the request.

Retention and timing log
- **AC-012-18**: THE SYSTEM SHALL start Restate with the journal retention given by
  `EMS_RESTATE_JOURNAL_RETENTION`, `1d` when it is unset, and the runner SHALL start it with `0`; a test SHALL
  show the journal retention Restate reports for a completed invocation in both settings, SHALL record what a
  completed invocation without an idempotency key still shows at `0`, and SHALL restore the default afterwards.
- **AC-012-19**: WHERE `EMS_TIMING_LOG` is set, THE SYSTEM SHALL write one line per handler invocation (wall time
  in ns, service, handler, key, invocation id, the `t` of the message handled and of each message sent), except
  for an unconfigured `Household` or `Weather` (an unmanaged site), through a queue, so that no file is written
  on the event loop; WHERE it is unset, THE SYSTEM SHALL write nothing.
- **AC-012-20**: THE SYSTEM SHALL perform the same journaled operations, write the same state bytes and send the
  same messages with the timing log on and off.

Pacing and the RCEm table
- **AC-012-21**: WHEN the harness publishes a synthetic run with a speed, THE SYSTEM SHALL start
  `ems-gen run --mode accelerated --speed S` without blocking the caller, and the generator's run manifest SHALL
  record the Unix time at which pacing started.
- **AC-012-22**: WHEN `ems-gen replay --speed S` replays a history recording, THE SYSTEM SHALL publish each event
  no earlier than the pacing start plus its emission time's offset from the window start divided by `S`, in the
  recording's order with the background fleet merged as today, and SHALL publish the same events as without
  `--speed`.
- **AC-012-23**: WHEN a run is published paced, THE SYSTEM SHALL end with states equal to the offline side's, as
  for an unpaced run.
- **AC-012-24**: WHEN the parameters of a synthetic run are built, THE SYSTEM SHALL compute the proxy RCEm table
  for every civil month from two months before `civil_month(first_step)` through `civil_month(end_step − 1)`
  (the price signal of a day reads the newest month published before it, and a proxy month counts as published
  on the 11th of the next month, as PSE publishes), from the run's synthetic prices and national PV index, equal
  to the table the spec 009 test helper builds for the same months and seed; a history run SHALL use the PSE
  table; the manifest SHALL record the table built and the entries actually used, with their source. IF a live
  run includes an RCEm-basis site, THEN THE SYSTEM SHALL refuse it before anything is sent (no RCEm of the
  running month exists until the 11th of the next one).

Runner, export and metrics
- **AC-012-25**: WHEN `make exp NAME=<id>` runs, THE SYSTEM SHALL validate `experiments/configs/<id>.yaml`
  before touching the stack, ask once for confirmation that the Kafka and Restate volumes will be wiped, and for
  each run of the config's matrix start a fresh stack with journal retention 0, configure the run, publish it
  (paced in sim; in real time from `first_step` in live), carry out the configured kills and strategy changes,
  wait for its end, export, compute the metrics and write the run's result directory.
- **AC-012-26**: THE SYSTEM SHALL write for every run `manifest.json` (config, seed, data window, hardware,
  software and image versions, git commit, the RCEm table and the entries used with their source, the
  recording's hash if any, the timing-log format version, the measured offset between the host and the Docker VM
  clocks), `config.yaml`, `households.parquet`,
  `strategies.parquet`, `months.parquet`, `steps.parquet` and `lags.parquet` (sim runs), `timings.parquet` and `summary.json`; every
  table, the manifest and the summary SHALL carry a `schema_version`, and no file SHALL contain a NaN or an
  infinity; IF any of them cannot be written, THEN the run SHALL be marked failed in `summary.json`.
- **AC-012-27**: WHEN the export runs, THE SYSTEM SHALL read object state only through SQL on the `state` table,
  decode it with the actors' serde, and give per household and strategy window, and for the real `Meter`: the
  start and end step and SoC; `bill_pln` over the closed months and `deposit_closing_pln` as the remaining value
  of the tranches held, both null while no month has closed; `ledger_net_cost_pln` over the closed months and
  every row still held, null when there is neither; a `partial` flag for a strategy that did not run whole
  months; and, for a removed strategy, the last step up to which its hours are comparable,
  `4 · ⌊(stop − 2L) / 4⌋` (corrections for later slots ride on steps it no longer receives). The steps table
  SHALL come from the offline fold and SHALL be written only in sim, and only when the fold's final state equals
  the actors'.
- **AC-012-28**: THE SYSTEM SHALL record for every sim run the exactly-once check: the number of household,
  strategy, `Battery` and `Meter` states that differ from the offline twin; for a live run, the invariant checks
  of AC-012-16 and AC-012-36. In both modes it SHALL check that no inbox of any object holds a message, once the
  end is confirmed and `sys_invocation` shows no pending invocation of the run. IF a check fails, THEN the run
  SHALL be marked failed.
- **AC-012-29**: THE SYSTEM SHALL compute from the timing log, keeping the first line per invocation (an
  invocation with more than one line counts as retried, and steps inside a recovery window of AC-012-30 are
  reported with the recovery, not in the percentiles), and from the runner's events: records/s and
  invocations/s; decision latency per household-step (the `Battery` commit that sends `Ack t` minus the
  `Household` commit that sends `Step t` to `Ems`) with p50, p99, max and the number of steps without both
  lines; release lag (the release minus the publication time of the record that made the step releasable,
  amended 2026-11-25), beside the publisher's own lateness behind its schedule,
  the consumer-group lag and each container's CPU, so that a speed that is not sustained shows where it stops;
  retries; the forecast age at each step; peak memory per container and the CPU and RSS of the SDK service's
  whole process group; state bytes per service at the end.
- **AC-012-30**: WHEN a run has kills, each placed at a fraction of the window in data time on the publication
  schedule, THE SYSTEM SHALL record per kill the time of the kill, of the process being healthy again, of the
  first commit after it, and of the release lag returning under its pre-kill p99, and report the three phase
  durations.
- **AC-012-31**: WHEN a config sweeps speeds, THE SYSTEM SHALL mark a speed `S` sustained when 99 % of steps
  have a release lag of at most 900 s / `S` and the last step is released within 2 × 900 s / `S` after the last
  record that releases a step is published, and SHALL report the highest sustained speed.
- **AC-012-32**: WHILE a run is in progress, THE SYSTEM SHALL detect its end from the timing log (every managed
  `Meter` has handled the last step the offline join releases; in live, the step before `end_step`) and read the
  `state` table at most once every 5 s; after the end it SHALL wait, through one SQL check, until every
  strategy, `Ems`, `Battery` and `Meter` has reached the end the offline join and the recorded strategy schedules
  give, before any comparison. IF no managed `Meter` line advances for `stall_s` (a config field; default
  `max(300, 10 × 900 / S)` s in sim, `1800 + G` s in live), THEN THE SYSTEM SHALL mark the run failed as
  `stalled` and record the invocations that are paused, backing off or retried at least once, every object whose
  inbox holds messages (its expected number and the numbers held), and the last `t` per service. IF the host
  slept during the run (its wall clock ran more than 1 s ahead of its monotonic clock), THEN THE SYSTEM SHALL
  record `host_slept_s` and mark the run failed as `the host slept N s` (added during validation: a closed lid
  stalled a run for 124 s).

Done-when and validation runs
- **AC-012-33**: WHEN the Restate server is killed with SIGKILL during a configured backfill, after the first
  and before the last step, and started again, THE SYSTEM SHALL end with states equal to the offline side's and
  AC-011-36 true (fault tier).
- **AC-012-34**: WHEN strategies are added and later removed with `set_strategies` during a replay of
  10 households through Kafka (the accepted changes sent before publishing; one more change, a removal at an hour
  already released, sent after the run), THE SYSTEM SHALL end with every strategy equal to the offline twin given the recorded change
  history, every household feeding the added strategy from its `t` and none feeding the removed one after its
  `t`, and the late change refused and counted (e2e).
- **AC-012-35**: WHEN a live household is configured with a window that starts in the past, THE SYSTEM SHALL
  release in the first deadline every step that ended at least `G` ago, schedule the next deadline at the next
  quarter boundary plus `G` (Restate's `scheduled_start_at` within 2 s of it), count a deadline sent for another
  step, and stop the chain at `end_step` (integration).
- **AC-012-36**: WHEN the validation configs run, THE SYSTEM SHALL produce:
  `sys-recovery` (100 managed households, 1 day, two SDK and two server kills) with 0 mismatches and the
  recovery phases of every kill; `sys-strategy-change` (from 2026-06-29T04:00Z for 2 days, so June closes; one
  strategy added on day 1, removed on day 2) with 0 mismatches; `sys-scale` (100 and 400 households with 1 and
  2 strategies, speed 96) and `sys-speed` (100 households, speed doubling from 24) with the metrics of AC-012-29
  and the highest sustained speed; `sys-live` (synthetic data in real time, RCE-basis sites, 2 h,
  10 households, faults) with every step of the window released by its end plus `G` plus 5 s and the
  distribution of that delay reported, `Battery` equal to the SelfConsumption shadow, and the deadline firings
  (early ones apart), estimates and `price_replaced` steps reported.
- **AC-012-37**: THE SYSTEM SHALL keep the harness's end detection and state comparison (`run_ends`,
  `assert_run`) correct with strategy changes, and the integration, e2e and fault tiers of specs 010 and 011
  green.

Fixes to existing code (amendment 2026-11-23)
- **AC-012-38**: WHILE the harness waits for a run, THE SYSTEM SHALL select failed, paused and retrying
  invocations in SQL instead of reading every invocation of the run, and SHALL stop waiting with the error when
  the admin API answers with a status other than 503; a connection error or a 503 SHALL be retried, and so SHALL
  any 5xx within 60 s after a kill the harness itself caused (the fault tier records the statuses it sees there).
- **AC-012-39**: THE SYSTEM SHALL start Restate with its RocksDB, invoker, query-engine and Bifrost record-cache
  memory set so that their sum stays below the container's memory limit, SHALL give every long-running
  container of the stack (the services `make up` and `make register` start) a memory limit, and the validation
  runs SHALL report each container's peak memory and whether it was OOM-killed.
- **AC-012-40**: IF a stored or sent value does not decode back equal to itself (a NaN or an infinity), THEN THE
  SYSTEM SHALL fail the invocation with a retryable error before the value is stored or sent.
- **AC-012-41**: WHEN this stage changes a stored state type, THE SYSTEM SHALL still decode every stored type as
  stage 3 encoded it (state, parameters and the messages that can wait in Restate's queues), and the value
  re-encoded SHALL keep every key and value of the stored line (so a removed, renamed or changed field fails); the
  fixtures SHALL contain every dataclass reachable from those types.
- **AC-012-42**: THE SYSTEM SHALL document how to list and resume paused invocations on the stack (risk A7).

## Constraints (from the checklists)

- **P1**: spec, plan and tasks approved before code; each commit cites a task.
- **P2**: the clock rule, the strategy schedule and `strategy_params` are pure (`src/ems/assembler/`, the actors'
  transitions); the timing log runs outside the transitions and makes no `ctx.*` call.
- **P3**: owners as in 01. New facts, all written by `Household`: the strategy schedule with its history and
  refusals, and the step its next deadline is expected for. No second writer.
- **P4, R1, R2**: both new handlers are reached by one-way messages (a delayed self-send; an HTTP ingress control
  call). No request-response is added.
- **P5, R3**: `ctx.time()` only in `on_deadline`, journaled; the timing log writes through a queue.
- **P6, R4**: `set_strategies` carries an `Idempotency-Key`; every run and the fault tier compare with `==`.
- **P7, R5, R6**: no clock and no timer in sim; the deadline chain is the only self-rescheduling chain, and the
  expected step serves as its generation.
- **P8**: `Battery` unchanged (clamps every command).
- **P9, R7**: `RunParams` 1.1.0 (additive); `Deadline` and `StrategyChange` 1.0.0 carry `schema_version` and
  `t`; the result schemas are new and versioned. No wire contract change.
- **P10**: `ems.experiments.run` and `ems.experiments.export` are CLIs over the stack and artefacts.
- **P11**: `EMS_TIMING_LOG` and `EMS_RESTATE_JOURNAL_RETENTION` are optional, not secrets.
- **P12**: no new dependency (`pyarrow` is already locked); no version bump.
- **P13**: no tracing.
- **P14**: every result directory holds its manifest (AC-012-26).
- **P15**: docs and ADRs 0020–0022 land with the change.
- **P16**: systems metrics next to the energy tables in every run (AC-012-29 … -31).
- **Stop rule**: IF AC-012-33 or AC-012-34 cannot be met on the pinned versions, the stage stops and the cause is
  recorded before any rule is changed.
- **Risks** (`09`): A1 gets pacing for history; A6 gets its retention, which also removes the cost of keeping a
  journal entry for every whole-state rewrite; A9 is measured and its memory limit revisited (K15); K14
  retries are counted; A4 stays accepted (live runs are checked by invariants); A7 gets its procedure; A12 is
  widened to delivery delay. New rows: H5 (harness polling, fixed), K15 (Restate memory pools, fixed), A14 (a
  lost message stalls its receiver; mitigated by the stored-state rule, the 1.0.0 fixtures and stall
  detection), A15 (NaN and infinity in the serde, fixed), A16 (`Tariff` fan-out, accepted and measured); the
  timing log's observer cost; queued timing lines lost at a `kill -9`; at retention 0 completed invocations
  cannot be inspected in experiments; where the thesis evaluation runs is OPEN.

## Success Criteria

- **SC-001** (review): every doc listed under Scope matches the built state; ADRs 0020–0022 are accepted and
  indexed; the items of the table above are closed in 01, 08, 09 and 11; the stage 5 brief is refreshed.
- **SC-002**: `make ci` is green locally and in GitHub Actions; `make test-integration`, `make test-e2e` and
  `make test-fault` are green on the author's machine, with their output in the pull request.
- **SC-003**: the validation results are in `11-experiments.md`, each naming its result directory, labelled as
  validation runs; the stage 4 row of the stage table in `00-overview.md` is marked done.

## Assumptions

- A delayed one-way send survives a restart of the SDK service and of the server (Restate documents delayed
  sends as durable); AC-012-10 tests it.
- `ctx.request().id` gives the invocation id in `restate-sdk` 1.5.2 (read in the installed source). The SDK's replay
  flag is an internal context variable, so it is not used.
- The existing proxy functions give a valid table for every 2026 month (tariffs exist for 2026 only, risk D9).
- The laptop runs 400 managed households at speed 96; if it does not, the run's metrics show where it stops, and
  that is a valid validation result.

## Open questions

Decided in `plan.md`:
- the field lists of `Deadline`, `StrategyChange`, the new `Household` state and the result tables;
- the YAML layout of an experiment config and the validation configs' exact windows and seeds;
- the timing line format and where the queue listener starts and stops;
- how the in-memory network gets a fake clock and delayed sends;
- the hypothesis sizes of the new properties;
- the file names of ADRs 0020–0022.
