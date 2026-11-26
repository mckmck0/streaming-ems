# Tasks: Clocks and systems experiments

Spec: [spec.md](spec.md) · Plan: [plan.md](plan.md). Spec approved 2026-11-22 and amended 2026-11-23 (review
gaps); the amendment, the plan and this list await approval together.
- Tests come first (tasks marked [test], written by `test-engineer`); each implementation task turns its tests
  green. Every phase ends with `make format` and `make ci` green.
- Section numbers (§) refer to the plan's Design.
- Phases 1–3 change no released step in sim. The spec 011 tests stay green after each task, except the end-step
  case in `test_join.py` that T06 rewrites (AC-012-01).
- Numbering: T01–T52 keep their numbers (the reviews cite them). The tasks added on 2026-11-23 for code that
  exists on `main` are T53–T64, in Phase 0b. Each fix lands as its own commit `fix(<scope>): … (012/Tnn)`, also
  when it touches only tests or docs (user decision, 2026-11-23); a fix's [test] task lands in the same commit as
  its implementation, and the commit line sits on the implementation task. Phase 0b comes before Phase 1,
  because T53 must capture the 1.0.0 encodings before T08, T09, T11, T15 and T22 change the types, and all of it
  before the tiers (Phase 5) and the validation runs (Phase 6).

## Phase 0: Retention on the pinned stack

- [ ] T01 [test] (AC-012-18) `tests/unit/infra/test_compose.py`: the restate service sets
  `RESTATE_DEFAULT_JOURNAL_RETENTION` from `EMS_RESTATE_JOURNAL_RETENTION` with default `1d`.
- [ ] T02 (AC-012-18) `infra/docker-compose.yml`: the variable (§4).
- [ ] T03 [test] (AC-012-18) `tests/integration/test_retention.py`:
  - with the default, a completed invocation reports `journal_retention` of 1 day;
  - with restate restarted at `0`, it reports 0, and the test records what `sys_invocation` still shows for a
    completed invocation with and without a key;
  - a new invocation of a service registered before the restart also gets 0 (or the test records that it needs
    a new registration; the runner registers after every reset either way);
  - the default is restored in `finally`.

## Phase 0b: Fixes to existing code

- [ ] T53 [test] (AC-012-41) First in this phase, before any type changes (plan §9, F5).
  - A generator writes every type of `STORED_AND_SENT` as today's `main` encodes it, to
    `tests/fixtures/state/1.0.0/<Type>.jsonl`, one value per line. Its values:
    - the `values()` fixture in `tests/unit/actors/test_serde.py:67`;
    - the network with faults at `L` = 8 and `L` = 2 (`FAULT_RUN_ID`);
    - hand-built settlement values (an opening tranche used, a tranche expiring with a refund, a tranche created,
      `SettleAcc` with an open hour and two months of rows), `Apply` with EXPORT and SETPOINT, and a `Cohort` with
      `rce_from`.
  - The generator asserts that every dataclass reachable from `STORED_AND_SENT` occurs in some line.
  - `tests/unit/actors/test_state_compat.py`: every line decodes with the current serde, and re-encoding the
    decoded value keeps every key and value of the line, recursively.
- [ ] T64 [docs] (AC-012-41) The stored-state rule in `08-contracts.md` ("Internal actor messages") and row A14 in
  09, written by hand. Commit, with T53: `fix(contracts): stored state stays additive, with 1.0.0 state fixtures
  (012/T64)`.
- [ ] T54 [test] (AC-012-38) `tests/unit/test_stack_harness.py` (F1):
  - the fake `invocations` (line 352) takes `*, suspect_only`;
  - `failures()` sends exactly `starts_with(target_service_key, '<run>:') AND (status = 'paused' OR
    completion_result = 'failure' OR retry_count >= 4)`;
  - the stall query (`min_retries=1`) adds `status = 'backing-off'` and uses `retry_count >= 1`;
  - `wait_run` and `wait_handled` raise on an `HTTPStatusError` 500, and keep polling after an
    `httpx.ConnectError`, after a 503, and after any 5xx within 60 s of a kill the harness caused.
- [ ] T55 (AC-012-38) `stack.py`: `invocations(run_id, *, suspect_only=False, min_retries=STRICT_ATTEMPTS)`,
  `failures()` through it, `Restate.kill_window` (set by `kill_restate` and `SdkService.kill9`), and `_transient`
  in `wait_run` and `wait_handled`; the fact in plan.md is corrected; row H5 in 09 is set to fixed.
  Commit: `fix(harness): filter failed invocations in SQL; stop on query errors (012/T55)`.
- [ ] T56 [test] (AC-012-39) `tests/unit/infra/test_compose_restate.py`:
  - the restate service sets the RocksDB, invoker, query-engine and record-cache pools, and their sum stays
    below its `mem_limit`;
  - every service `make up` and `make register` start has a `mem_limit` (fails for `kafka-ui` until T59).
- [ ] T57 [test] (AC-012-39) `tests/integration/test_restate_config.py`: `restate-server --dump-config` in the
  running container shows the four pools with the values from compose (the `"512 MiB"` format read for the new
  variables).
- [ ] T58 (AC-012-39) Compose (F2): the three pools beside RocksDB, after `ems-deploy-checklist`. Docs: 05 (the
  restate row and its configuration list), 02 ("Runtime and SDK service"), 09 (row K15 fixed, A9's memory
  promise kept). Commit: `fix(infra): fit Restate memory pools in its container (012/T58)`.
- [ ] T59 (AC-012-39) Compose (F3): `kafka-ui` gets `mem_limit: 512m`; the 05 row. Commit:
  `fix(infra): cap kafka-ui memory (012/T59)`.
- [ ] T60 [test] (AC-012-40) `tests/unit/actors/test_serde.py`: NaN and ±inf in a `float` field and in a
  `float | None` field are refused with a `ValueError`; the `test_ac_011_33_*` tests stay unchanged.
- [ ] T61 (AC-012-40) `serde.py` (F4): decode-and-compare after `dump_json`; `shell._commit` encodes every send
  before `ctx.set(STATE)`. Docs: the "Serialisation" sentence in
  02; row A15 in 09. Commit: `fix(actors): refuse values that do not survive JSON (012/T61)`.
- [ ] T62 [docs] (AC-012-42) `02-subsystem-actors.md`, "Operations: paused invocations": the SQL, the CLI in the
  server image, the HTTP resume, the `-t` caveat, and the RT0016 caveat for a resume on a new deployment; a
  pointer in row A7 of 09 (F6). Commit:
  `fix(docs): list and resume paused invocations (012/T62)`.
- [ ] T63 (AC-012-38, -39) After T55, T58 and T61, `make measure MODE=skew`, compared with
  `ingress-skew-20261121T005630Z` (records/s, `server_s`, the restate peak, OOM kills). If the numbers move,
  `11-experiments.md` and `docs/thesis/kafka-ingress.md` get the new result directory; otherwise 11 notes that
  they hold. Commit: `fix(docs): skew measurement after the harness, memory and serde fixes (012/T63)`.

## Phase 1: Assembler and domain (pure)

- [ ] T04 [test] (AC-012-02, -03) `tests/unit/assembler/test_meter_stream.py` extended:
  - `meter_close` with and without held readings, with `until` ≤ `closed`, and on a stream with no reading ever;
  - a first reading for a clock-closed slot becomes a correction, or `too_old`;
  - the clock more than `2L` ahead of the readings.
- [ ] T05 [test] (AC-012-01, -06) `tests/property/test_meter_stream.py` extended:
  - `horizon` equals `newest` after every `meter_apply`;
  - with non-decreasing `meter_close` calls interleaved, every accepted reading is accounted for once and slots
    close in order.
- [ ] T06 [test] (AC-012-01, -02, -04, -05) `tests/unit/assembler/test_join.py`:
  - the end-step case (lines 258-267) is rewritten: the `Late` is dropped and counted in `after_end_late`;
  - `on_clock` closes, raises both watermarks, releases, and caps at `end_step`;
  - a window in the future releases nothing;
  - the clamp moves a curve or run that arrives behind the watermark, and counts it.
- [ ] T07 [test] (AC-012-01, -05, -06) `tests/property/test_join.py` extended:
  - the clamp is a no-op without clock events (prices and forecasts, snapshot and updates);
  - records, updates and non-decreasing clock steps in any interleaving give steps in order, nothing at or past
    `end_step`, no record visible after its step's start, and no raise.
- [ ] T08 (AC-012-02, -03, -05) `meter_stream.py`: `horizon`, `meter_close`, the guard removed (§2).
- [ ] T09 (AC-012-02, -04, -05) `join.py`: `on_clock`, the clamp in `on_curves` and `on_run`, `after_end_late` in
  `_add`, the clamp counters (§2).
- [ ] T10 [test] (AC-012-07, -24) `tests/unit/assembler/test_params.py`:
  - `mode` and `grace_s`: defaults and validation;
  - a 1.0.0 `RunParams` JSON decodes with the defaults;
  - the field pin is updated.
- [ ] T11 (AC-012-07) `params.py`: `mode`, `grace_s`, `schema_version` 1.1.0.
- [ ] T12 [test] (AC-012-12 … -15) `tests/unit/assembler/test_schedule.py`:
  - each refusal reason of `accept`, and their order;
  - normalisation of the strategy names;
  - `strategies_at` across several changes;
  - `windows` for initial, added and removed strategies;
  - `strategy_params` validates.
- [ ] T13 (AC-012-12 … -15) `src/ems/assembler/schedule.py` (§3).
- [ ] T14 [test] (AC-012-14, -16) `tests/unit/assembler/test_offline.py`:
  - `step_strategy` drops and counts a correction before the strategy's `first_step`;
  - `fold(..., schedule)` gives an added strategy the steps from `t` and a removed one the steps before `t`;
  - `trace` per step equals the stepper's `StepOut`.
- [ ] T15 (AC-012-14, -16) `offline.py`: `dropped_corrections`, `fold(..., schedule=())`, `trace`.
- [ ] T16 [test] (AC-012-27) `tests/unit/domain/test_costs.py`. `costs` on:
  - no closed month (bill and deposit `None`), and neither months nor rows (ledger `None`);
  - a refund month, an export month and a month without export;
  - two months held during the lag;
  - opening tranches;
  - an open hour excluded;
  - the factor taken from `rules`.
- [ ] T17 (AC-012-27) `ems.domain.settlement.Costs` and `costs` (§5).

## Phase 2: Actors

- [ ] T18 [test] (AC-012-07 … -09, -11) `tests/unit/actors/test_household.py`, deadline:
  - the chain starts on the price snapshot in live mode, never at `configure`, never in sim;
  - a stale step is counted;
  - an early firing reschedules the same step after at least 1 s and counts `early`;
  - catch-up releases every due step in one firing;
  - every non-stale firing is counted;
  - the chain stops at `end_step`;
  - the delay is always > 0;
  - a window in the future fires next at the first step's end plus `G`.
- [ ] T19 [test] (AC-012-12, -13) `tests/unit/actors/test_household.py`:
  - `set_strategies` accepted and refused;
  - one release batch across `t` routes each step to its own set.
- [ ] T20 [test] (AC-012-09, -19, -20) `tests/unit/actors/test_app.py`:
  - the handler table and edges (`Household → Household`), with a live household in the fixture;
  - the forbidden-pattern scan: `ctx.time` only in `shell.handle_timed`, `send_delay` only in `shell._commit`,
    `time.time`/`time.time_ns` only in `timing.py`;
  - the field pins of `Deadline` and `StrategyChange`.

  `tests/unit/actors/test_timing.py`:
  - the line format;
  - `mark` returns at once when the setting is unset;
  - the file is written on the listener thread;
  - unmanaged `Household` and `Weather` are not marked; `Tariff` is.

  `tests/integration/test_timing_log.py` (runs with the integration tier): an `SdkService` started with
  `EMS_TIMING_LOG`, N invocations, then `stop()` (SIGTERM), gives N lines, both with coverage (as the tier runs
  it) and without (as the runner runs it). After `kill9()` lines may be missing, and the metrics count them as
  missing.
- [ ] T21 [test] (AC-012-11, -16, -20) `tests/network.py`:
  - a fake clock as a delivery source;
  - delayed sends delivered once the clock passes their due time;
  - `set_strategies` as a control delivery.

  `tests/property/test_network.py`:
  - in sim, with changes and any delivery order, the network equals the offline twin;
  - in live, with the fake clock, the invariants of AC-012-16 hold;
  - commits are identical with the timing log on and off;
  - the byte-determinism trace covers the new state types under two hash seeds.
- [ ] T22 (AC-012-07 … -15) `messages.py`, `shell.py` and `household.py` (§3).
- [ ] T23 (AC-012-19, -20) `timing.py`; `settings.py` (`timing_log`); `timing.start()` in `app.py`; the hook in
  `shell` (§3). The listener stops through `atexit` and, unconditionally, from a SIGTERM hook chained the way the
  SDK chains its own (coverage's SIGTERM handler would otherwise skip `atexit`).

## Phase 3: Ingest pacing and the RCEm table

- [ ] T24 [test] (AC-012-21, -22, -29) `tests/unit/ingest/test_pacing.py`:
  - `pace` with a fake monotonic clock;
  - history `replay --speed` publishes the same events in the same order as without, each no earlier than its
    due time;
  - `paced_from_unix` and the publisher's lateness behind its schedule (p99, max) are in the manifests;
  - spec 002 replay is unchanged.
- [ ] T25 (AC-012-21, -22, -29) `ems.ingest.runner`: `pace` (recording its lateness), history `--speed`,
  `paced_from_unix`; the `--speed` help
  text in `cli.py` (§6).
- [ ] T26 [test] (AC-012-24) `tests/unit/experiments/test_rcem.py`:
  - `proxy_table` equals `ref_window._daily` plus `proxy_rcem` for the same months and seed;
  - it covers M−2 … `civil_month(end_step − 1)`;
  - `managed_params` with it passes the window check for a June 2026 window;
  - in live mode an RCEm-basis site is refused;
  - `rcem_used` is empty for an all-RCE fleet.
- [ ] T27 (AC-012-24) `src/ems/experiments/rcem.py`; `runs.managed_params(..., rcem, mode, grace_s)`,
  `runs.pse_table`, `runs.rcem_used`; the call sites in `tests/stack_fixtures.py`, `tests/network.py`,
  `jobs/ingress_measure.py` (§6).

## Phase 4: Harness, runner, export, metrics

- [ ] T28 [test] (AC-012-25) `tests/unit/experiments/test_config.py`:
  - every validation config loads;
  - `extra="forbid"`;
  - full-product matrix, sweep and repetitions;
  - `households` larger than the catalog's managed count is refused (1400 locations hold at least 400);
  - the live config's next-hour window;
  - `stall_s`, with default `max(300, 10 × 900 / S)` s in sim and `1800 + G` s in live;
  - a kill that overlaps a strategy change's sending is refused.
- [ ] T29 (AC-012-25) `src/ems/experiments/config.py`; `experiments/configs/` with the five configs (§7, §8).
- [ ] T30 [test] (AC-012-29 … -32) `tests/unit/experiments/test_metrics.py`, on synthetic logs and events:
  - latency pairs and missing lines;
  - first-line deduplication and retries;
  - release lag;
  - the sustained rule at its boundary;
  - the three recovery phases and recovery-window exclusion;
  - end detection from Meter lines;
  - stall detection: no managed `Meter` line advancing for `stall_s` gives `stalled`;
  - the publisher's lateness, the consumer-group lag and the container CPU reported beside the release lag;
  - early deadline firings counted apart; the live release delay as a distribution against `G` + 5 s;
  - the live metrics.
- [ ] T31 (AC-012-29 … -32) `src/ems/experiments/metrics.py` (§7).
- [ ] T32 [test] (AC-012-26 … -28) `tests/unit/experiments/test_export.py`:
  - states encoded with the actors' serde → rows as SQL returns them → Parquet → read back equal;
  - the cost columns, null bills and a null ledger for a window that never started, start and end SoC,
    `partial`, `compare_until_step` for a removed strategy;
  - `summary.json` written with `allow_nan=False`;
  - `schema_version` in every table, the manifest and the summary;
  - `steps` refused while the check fails, and absent in live;
  - a missing file marks the run failed.
- [ ] T33 (AC-012-26 … -28) `src/ems/experiments/export.py` (CLI `--run-id`, `--out`) (§7).
- [ ] T34 [test] (AC-012-17, -21, -29, -37) `tests/unit/test_stack_harness.py` extended:
  - `Publisher` command lines (backfill, accelerated, realtime) and a non-blocking start;
  - `reset_stack` compose calls with the retention variable, starting `kafka`, `kafka-init` and `restate` only;
  - `run_ends` with schedules (removed, added, never reached);
  - the `ems-exp-` ingress project;
  - `HostSampler` over the whole process group, re-reading the group after a restart;
  - `MemorySampler` CPU and `restate` anonymous memory parsing; the lag sampler; the host–VM clock offset.
- [ ] T35 (AC-012-17, -21, -29, -37) `stack.py`: `Publisher` (with `--end` in realtime), `reset_stack`,
  `StackEnv(ingress_project_prefix)`, `run_ends` and the comparison helpers with schedules, `HostSampler`, the
  `MemorySampler` additions, the lag sampler, the clock offset; `tests/stack_fixtures.py` (`assert_run`
  with schedules) (§7).
- [ ] T36 [test] (AC-012-17, -25, -26, -28, -32) `tests/unit/experiments/test_run.py`, against a fake stack:
  - kills placed at their data-time fraction;
  - changes configured → stored params equal → sent, at least 1 h of data before `t`; a transport error retried
    with the same `Idempotency-Key`;
  - the run order: join fold, timing-log end, schedules, `wait_run` with the schedule-aware ends, strategy fold,
    comparison;
  - the live path (wait for `first_step`, realtime, invariants, no `steps`);
  - on a stall, `status: failed` with reason `stalled` and the diagnosis: invocations paused, backing off or
    retried at least once (`min_retries=1`), every object whose inbox holds messages (expected number and the
    numbers held), the last `t` per service;
  - a non-empty inbox, once the end is confirmed and no invocation is pending, fails the run (AC-012-28);
  - the live path passes `--end` and stops the publisher before the checks;
  - `status: failed` on any failure;
  - the manifest fields;
  - mismatches mark the run failed.
- [ ] T37 (AC-012-25, -26, -28, -32) `src/ems/experiments/run.py` (§7).
- [ ] T38 [test] (AC-012-25) `tests/unit/infra/test_makefile_targets.py`: `exp` is a phony target with the
  confirmation prompt and `CONFIRM=yes`.
- [ ] T39 (AC-012-25) `Makefile`: enable `exp`.

## Phase 5: Tiers against the stack

- [ ] T40 [test] (AC-012-10, -11, -35) `tests/integration/test_deadline.py`. A live household with a window that
  started two hours ago, prices published first:
  - the first deadline (after the snapshot) releases every step that ended at least `G` ago;
  - the next one is `scheduled` with `scheduled_start_at` within 2 s of the next boundary plus `G`;
  - a deadline for another step is counted;
  - after an SDK `kill -9` and restart the pending deadline fires, exactly one stays pending, and no invocation
    fails with RT0016 (the VERIFY item; the stop rule applies);
  - a short window stops the chain at `end_step`.
- [ ] T41 [test] (AC-012-12) `tests/integration/test_set_strategies.py`: through HTTP ingress, the same key twice
  stores one change; another key with the same `t` is refused `not_after_pending`.
- [ ] T42 [test] (AC-012-34, -23) `tests/e2e/test_strategy_changes.py`:
  - 10 households, paced publication;
  - the accepted changes (add NoBattery at an hour, remove it later) sent before publishing;
  - one more change, a removal at an hour already released, sent after the run, is refused `released`;
  - states equal the twin with the recorded schedules;
  - AC-011-36 holds.
- [ ] T43 [test] (AC-012-33, -38) `tests/fault/test_actor_kills.py` extended: `kill -9` of the server with
  households configured, after the first and before the last step; states equal offline, and AC-011-36 holds.
  The test records every HTTP status `/query` gives from the kill until the server is healthy, so the 60 s
  window of AC-012-38 rests on what the pinned server does.
- [ ] T44 (AC-012-10, -11, -33 … -35, -37) Whatever T40–T43 show missing. `make test-integration`,
  `make test-e2e`, `make test-fault` green, with the output kept for the pull request.

## Phase 6: Validation runs

- [ ] T45 (AC-012-36, -39) `make exp NAME=sys-recovery`, then `sys-strategy-change`:
  - 0 mismatches;
  - recovery phases for every kill;
  - June closed in `sys-strategy-change`;
  - each container's peak memory and OOM flag reported.
- [ ] T46 (AC-012-36, -39) `make exp NAME=sys-scale`, then `sys-speed`:
  - summaries with every metric and the highest sustained speed;
  - peaks and OOM flags reported, and the `Tariff` state size (A16);
  - if `restate` is OOM-killed, or its peak anonymous memory exceeds about 1.8 GiB, `mem_limit` goes to `2560m`
    (plan §9) and the run is repeated;
  - (added during validation) a run during which the host slept fails with `host_slept_s` (AC-012-32), and
    `make exp` runs under `caffeinate -i` where it exists; `sys-speed` starts at 24.
- [ ] T47 (AC-012-36, -39) `make exp NAME=sys-live` (up to about 3 h of wall time, including the wait for the
  next full hour):
  - every step released by its end plus `G` plus 5 s, with the distribution of the delay;
  - peaks and OOM flags reported;
  - `Battery` equals the SelfConsumption shadow;
  - firings, estimates and `price_replaced` reported;
  - (added during validation) the realtime preflight checks only the records due at the start, and a
    publisher that exits with an error fails the run at once (the first `sys-live` ran 2 h without data).

## Phase 7: Docs and pull request

- [ ] T48 [docs] (SC-001) ADRs 0020, 0021 and 0022, and the index in `docs/adr/README.md`.
- [ ] T49 [docs] (SC-001) `docs/context/01`, `02`, `06`, `08` and `12`, as listed in the plan's Docs and ADRs.
- [ ] T50 [docs] (SC-001, SC-003) `docs/context/00` (stage 4 done; OPEN: where the evaluation runs), `05`, `09`
  (A1, A4, A6, A8, A12, K14, the new A16, and the final statuses of H5, K15, A14 and A15), and `11` (with the
  validation results and their result directories).
- [ ] T51 [docs] (SC-001) `CLAUDE.md` (the `make exp` row), `.claude/skills/ems-experiment/SKILL.md` (the
  prerequisites), and the stage 5 brief in 01.
- [ ] T52 (SC-002) `make format`, `make ci`; the `ems-pr` skill opens the pull request with the tier outputs and
  the validation results.

## Coverage check

| Criteria | Tasks |
|---|---|
| AC-012-01 | T05, T06, T07, T21 |
| AC-012-02 … -06 | T04–T09 |
| AC-012-07 … -11 | T10, T11, T18, T20–T22, T40 |
| AC-012-12 … -17 | T12–T15, T19, T21, T22, T34–T37, T41, T42 |
| AC-012-18 | T01–T03 |
| AC-012-19, -20 | T20, T21, T23 (SIGTERM case amended 2026-11-23) |
| AC-012-21 … -23 | T24, T25, T34, T35, T42 |
| AC-012-24 | T10, T26, T27 |
| AC-012-25 … -32 | T16, T17, T28–T39 (AC-012-28, -32 amended 2026-11-23: T30, T31, T36, T37) |
| AC-012-33 … -35 | T40, T42, T43, T44 |
| AC-012-36 | T45–T47 |
| AC-012-38 | T54, T55, T63, T43 |
| AC-012-39 | T56–T59, T63, T45, T46 |
| AC-012-40 | T60, T61 |
| AC-012-41 | T53, T64 |
| AC-012-42 | T62 |
| AC-012-37 | T34, T35, T44 |
| SC-001 … -003 | T48–T52 |

Every task cites at least one criterion, and every criterion has a test task before its implementation task.
