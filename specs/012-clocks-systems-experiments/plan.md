# Implementation Plan: Clocks and systems experiments

**Branch**: `012-clocks-systems-experiments` · **Date**: 2026-11-23 · **Spec**: [spec.md](spec.md) ·
**Status**: Draft, revised after the architecture and energy-domain reviews, amended 2026-11-23 with the gaps of
an independent review (Phase 0b, §9); awaiting approval together with the spec amendment and `tasks.md`

## Summary

The change adds three pieces to the actors and one runner around them:

1. **Live clock.** A pure `join.on_clock` closes meter slots and raises the sparse watermarks up to "now minus
   `G`". `Household.on_deadline` calls it once per quarter-hour in live mode, through a journaled timer chain
   that starts once the household holds its price snapshot. Sim does not change.
2. **Strategy changes.** `Household.set_strategies(t, strategies)` keeps a schedule of accepted changes, each on
   a whole UTC hour, together with its refusals. Each released step goes to the set in force at its own `t`. The
   offline twin replays the schedule.
3. **Timing log.** The SDK service writes one line per handler commit through a logging queue. None of it is
   journaled.
4. **Experiment runner.** `make exp NAME=<id>` runs each configured run on a fresh stack with journal retention
   0. In sim it publishes paced; in live it publishes in real time. It kills processes, changes strategies,
   detects the end from the timing log, exports state to Parquet, computes the metrics and writes a manifest.

## Facts checked for this plan

| Fact | Where | Consequence |
|---|---|---|
| Completed invocations keep their journal 24 h. Kafka and actor invocations carry no idempotency key. Run `demo1` (2 managed households, 3 `Household` objects, 192 steps from 2026-10-31T00:00Z, 3,371 invocations) held 43.4 MB of journals (GetEagerState 24.4 MB, SetState 14.1 MB) against 130.8 kB of state: about 11 MB per household-day, 12.9 kB per invocation | Read-only SQL on the pinned server, 2026-11-22 and 2026-11-23 | Journal retention 0 for experiments (spec Decisions) |
| `sys_invocation` keeps `completion_retention` apart from `journal_retention` (both `PT86400S` on the stack, also for `invoked_by = service`) | Read-only SQL, 2026-11-22 | Invocation timestamps need no journal; T03 records what stays at journal retention 0 |
| `quarter_index(dt) = ⌊unix s / 900⌋`, so `s_t = 900·t` seconds after the epoch. The clock step has no DST pitfall | `src/ems/contracts/time.py:37` | The clock step comes straight from `ctx.time()` |
| `ctx.time()` is `run_typed("timestamp", time.time)`, so it is journaled | SDK `server_context.py:889` | A retried deadline sees the same `now` |
| A delayed send passes `delay` in ms to the shared core (`sys_send(..., delay=ms)`), and a zero delay counts as none | SDK `server_context.py:1137-1144` | `delay_s` is always > 0. Whether the core's replay check compares the delay or an absolute time is not documented: VERIFY in T40, under the stop rule |
| `ctx.request().id` is the invocation id. The replay flag is only an internal context variable (`restate_context_is_replaying`), not public API | SDK `context.py:142`, `server_context.py:451` | The timing log keeps the first line per invocation and does not use the flag |
| `RunParams.first_step` must start a UTC hour, and `initial_state` starts settlement on an hour. Warsaw offsets, G12/G12w zone windows (winter time) and civil month starts all fall on whole UTC hours | `params.py:41`, `reference.py:114`, energy-domain review | Strategy changes happen on hours only, and no settlement or zone hour mixes two batteries |
| The meter closing loop keeps `closed − 1 ≤ newest` after every reading | `meter_stream.py:87-95` | `horizon = max(newest, closed − 1)` equals `newest` in sim |
| `_covered` and `_release` do not check `end_step` | `join.py:108-146` | `on_clock` caps at `end_step` |
| A spec 011 test keeps a `Late` correction that falls on `end_step` | `tests/unit/assembler/test_join.py:258-267` | The only spec 011 test that changes |
| `stepper.correct` raises for a slot it no longer keeps | `settlement.py:296-298` | An added strategy drops corrections for slots before its first step |
| The signal reads the newest RCEm month published before the day (strict `<`). PSE publishes RCEm on the 11th of the next month (every publication 2025-10 … 2026-10, including 11.11.2026); the legal deadline is delegated to IRiESP (Prawo energetyczne art. 11y ust. 4), which nobody here has read | `netbilling.py:118-158`, pse.pl/oire (read 2026-11-23) | Proxy months run from M−2 to the month of the last step; the legal deadline stays VERIFY |
| A quarter's export price is fixed when its step runs; `close_month` and `correct` read no RCEm | `reference.py:150-152` | The proxy table's upper bound is `civil_month(end_step − 1)` |
| RES act art. 4b ust. 10 applies only when the price "cannot be determined" | Dz.U. 2021 poz. 2376 | A curve delivered late in live is replaced although it was published: risk A12 widens to delivery delay |
| `acc.month_rows` holds two months during the lag; `acc.tranches` holds the deposit, opening tranches included | `settlement.py:246-251` | The cost helper sums every held row and every held tranche (§5) |
| Deposit factor: `rules.deposit_factor` (1.23) on `SiteModel.rules` | `netbilling.py:74`, `reference.py:56` | No literal 1.23 |
| `publish_backfill` blocks, captures all output and times out after 3600 s | `stack.py:696-741` | A non-blocking `Publisher` beside it |
| Each `wait_run` poll (every 0.5 s) reads every `sys_invocation` row of the run (`failures()` filters in Python) plus the `n` keys; full `state` values are read only once every record is handled. `query` raises on any HTTP status, and `wait_run` and `wait_handled` swallow every `httpx.HTTPError`, so a 500 runs to the timeout | `stack.py:349`, `:378-384`, `:400-437` | End detection from the timing log (SQL at most every 5 s), and fix F1 (§9) for the tiers, `make measure` and the runner's final check |
| Compose sets only RocksDB (512 MiB) in a 2 GiB container. The invoker (1.5 GiB), query engine (1 GiB) and Bifrost record cache (250 MiB) keep their defaults: about 3.3 GiB of budgets. Peak 1,833 MiB (`ingress-skew-20261121T005630Z`) | `infra/docker-compose.yml:139-147`, <https://docs.restate.dev/server/memory> (read 2026-11-22) | Fix F2 (§9), before `sys-recovery` and `sys-scale` |
| `kafka-ui` has no `mem_limit` (peak 405 MiB). `ems-gen` (profile `gen`, started by hand) has none either | compose | Fix F3 for `kafka-ui`; `ems-gen` keeps no limit (user decision, 2026-11-24) |
| `DataclassSerde.serialize` writes NaN and ±inf as `null`: a `float` field then fails to read (the key retries until paused, the bad value already stored); a `float \| None` field reads `None`. `TypeAdapter` refuses `ser_json_inf_nan` for a stdlib dataclass. `ctx.set` calls `serialize` before `sys_set_state`; a plain exception takes the retry path | `serde.py:22`; pydantic 2.13; SDK `server_context.py:830-835`, `:584-607` | Fix F4 (§9) |
| Nothing checks that the state types still read state stored by stage 3 | tests | Fix F5 (§9), first in Phase 0b |
| `restate.app` has no ASGI lifespan (`LifeSpanNotImplemented`); the SDK chains its own SIGTERM hook (`install_shutdown_hook(..., chain=True)`); coverage runs with `sigterm = true` | SDK `server.py:360`; `pyproject.toml:98` | Whether `atexit` stops the timing listener after SIGTERM is tested (T20), with a chained SIGTERM hook as the fix if not |
| The server image ships `restate-cli` 1.8.1-hotfix.2; paused invocations resume with `restate invocations resume <id> [--deployment latest]` or `PATCH /invocations/<id>/resume[?deployment=<id>]` | checked 2026-11-22; <https://docs.restate.dev/services/invocation/managing-invocations> | Fix F6 (§9) |
| `MemorySampler` sees only containers | `stack.py:925-952` | The SDK process on the host is sampled with `ps` |
| No `bill_pln`, `deposit_closing_pln` or `ledger_net_cost_pln` helper exists in `src/` | grep | A pure domain helper (§5) |
| `stack.py` already needs the `ingest` group, which holds `pyarrow` | `stack.py:6`, `pyproject.toml:44-58` | `pyarrow` stays where it is |
| `make reset` asks for `yes` unless `CONFIRM=yes` | `Makefile:109-115` | `make exp` prompts the same way |

## Plan decisions (the spec's open questions, and the review revisions)

- **The `end_step` correction.** `meter_apply` still returns the `Late` and counts it in the stream's `late`.
  `join._add` drops a `Late` whose `at_step ≥ end_step` and counts it in `Join.after_end_late`, a subset of
  `late`. Only `test_join.py:258-267` changes.
- **The deadline chain starts after the price snapshot.** It starts in `on_prices` when the snapshot is applied,
  not at `configure`. A first firing therefore cannot raise the price watermark (and clamp the curves) before
  the household holds them.
- **Live has no offline twin (A4).** In sim, the network with changes equals the twin under any delivery order.
  In live, with a fake clock, the network is checked for invariants: AC-012-05 and -06 hold; each strategy
  equals `step_strategy` folded over the steps it was sent; `Battery` and `Meter` equal the live shadow; at most
  one deadline is pending.
- **The timing log keeps the first line per invocation.** An invocation with more lines counts as retried.
  Steps inside a recovery window are reported with the recovery. A line lost at a `kill -9` counts as missing.
  `Tariff` (never configured) is marked; an unconfigured `Household` or `Weather` (an unmanaged site) is not.
- **The `Idempotency-Key` of `set_strategies`** is `{key}:set_strategies:{t}:{sorted names joined by +}`.
- **Kills sit at a fraction of the window in data time**, located on the publication schedule (Popen start plus
  data offset divided by speed).
- **Live runner path.** Wait until `first_step`, then start `ems-gen run --mode realtime`. End at `end_step`
  plus `G` plus 1 s. Run the invariant checks instead of the mismatch count. Write no `steps` table.
- **RCEm months:** from two before `civil_month(first_step)` through `civil_month(end_step − 1)`. The manifest
  records the table built and the entries used. Live runs refuse an RCEm-basis site in `managed_params`.
- **Costs (§5):** the ledger over the closed months and every held row; the deposit as the remaining value of the
  held tranches; bill and deposit null while no month has closed; start and end SoC per strategy window; a
  `partial` flag for a strategy window shorter than the months it touches.
- **Gaps of the independent review (2026-11-23):** six fixes to code on `main` become Phase 0b (§9, T53–T64,
  commits `fix(<scope>): … (012/Tnn)`); two gaps amend planned work (stall detection in AC-012-32; the timing
  listener and SIGTERM in T20/T23); two facts are corrected (the `demo1` measurement, the reason
  `sys_invocation` was not chosen). Task numbers T01–T52 stay as they are; new tasks start at T53 (the prompt
  assumed 50 tasks; the revised list already has 52).
- **Stall detection (AC-012-32):** `stall_s` defaults to `max(300, 10 × 900 / S)` s. On a stall the runner
  records the paused and retrying invocations (one SQL query), every object whose inbox holds messages, and the
  last `t` per service, and marks the run `stalled`. After a confirmed end, every inbox must be empty
  (AC-012-28), which catches a lost message without the journal.
- **Timing listener and SIGTERM:** `QueueListener.stop()` through `atexit`; T20's integration case shows that a
  SIGTERM to the SDK service still writes every line. If it does not, the listener is stopped from a SIGTERM
  hook chained the way the SDK chains its own.
- **Already covered, no new task (so nobody duplicates it):** an unknown forecast model or missing runs in live
  (the deadline raises the forecast watermark, AC-012-02, T04–T09, T18; in sim and replay the household stalls,
  as ADR 0017 intends; `sys-live` uses synthetic forecasts, so the ICON-D2 cutover of 2026-11-30 does not touch
  it); backpressure and pacing (FR-006, AC-012-21 … -23, -31); one SDK worker (a spec decision); journal
  retention (T01–T03).
- **Versions:** `manifest.json` and `summary.json` carry `schema_version` 1.0.0; the timing lines carry no
  version, and the manifest records `timing_log_version` 1.
- **Field lists:** in the Actor contract.
- **Experiment config:** in §7.
- **Network:** a fake clock, plus delayed sends delivered once the clock passes them (Test strategy).
- **Hypothesis sizes:** as in spec 011.
- **ADR files:** `0020-live-deadline-clock.md`, `0021-strategy-changes-at-run-time.md`,
  `0022-experiment-measurement.md`.

## Principles Check (PASS)

| # | How it holds |
|---|---|
| P1 | The spec was approved 2026-11-22. The plan and tasks are approved before any code, and every commit cites `012/Tnn` |
| P2 | `on_clock`, `meter_close`, `ems.assembler.schedule`, `strategy_params` and `settlement.costs` are pure (allow-list test, post-edit hook). Handlers stay "read, transition, write, send", and `now` reaches the transition as an argument |
| P3 | New facts, all written by `Household`: the strategy schedule with its refusals, and the deadline chain (expected step and counters). Rows are added to the ownership table in 01 |
| P4 | Both new handlers are reached by one-way sends: a delayed self-send, and an HTTP ingress send. No request-response is added, and the scan test still forbids `object_call`, `service_call` and `generic_call` |
| P5 | `ctx.time()` is journaled. The timing log builds a small dict on the event loop and writes the file on a `QueueListener` thread. The serde guard (F4) adds one decode and one `==` per write (A9: about 0.4 ms for 160 kB), visible in the validation metrics |
| P6 | `set_strategies` carries an `Idempotency-Key`. Sim runs compare with `==` against the offline twin in the e2e and fault tiers and in every run. The clock is live-only, so no sim result depends on arrival order |
| P7 | In `mode = sim` nothing is scheduled and no clock is read (AC-012-09). Every experiment runs in sim except `sys-live` |
| P8 | `Battery` is unchanged |
| P9 | `RunParams` goes to 1.1.0, and the state types gain fields with defaults (additive). `Deadline` and `StrategyChange` start at 1.0.0, and so do the result schemas. No wire change |
| P10 | `python -m ems.experiments.run` and `python -m ems.experiments.export` are CLIs with no HTTP surface |
| P11 | `EMS_TIMING_LOG` is an optional path in `settings.py`. `EMS_RESTATE_JOURNAL_RETENTION` exists only in compose, with default `1d`. No secrets |
| P12 | No new dependency and no version bump; `pyarrow` stays in the `ingest` group |
| P13 | No tracing. The timing log has no spans and no context propagation; it is one local line per commit, so it is not partial tracing |
| P14 | One `manifest.json` per run (AC-012-26), built on `stack.manifest` |
| P15 | Docs and ADRs land in the same change (listed below) |
| P16 | `summary.json` holds the systems metrics beside the energy tables |

## Contract changes (`08-contracts.md`)

- `RunParams` 1.0.0 → **1.1.0**: adds `mode: "sim" | "live" = "sim"` and `grace_s: int = 120` (0 ≤ `grace_s` ≤
  3600). Parameters stored by stage 3 decode with these defaults.
- New internal messages at 1.0.0: `Deadline(t)` and `StrategyChange(t, strategies)`.
- State fields added with defaults:
  - `Join`: `after_end_late`, `clamped_price`, `clamped_forecast`;
  - `StrategyResult`: `dropped_corrections`;
  - `HouseholdState`: `schedule`, `clock`.
- New result schemas at 1.0.0: the Parquet tables `households`, `strategies`, `months`, `steps` and `timings`
  (columns in §7), plus `manifest.json` and `summary.json`.
- New rule in 08, "Internal actor messages" (F5): stored state changes within a run only by adding fields with
  defaults, and anything else needs a new `run_id`. A stored value that cannot be read fails every invocation
  of its object until it pauses. A message that cannot be read, or that reaches an object that is not
  configured, fails terminally and is lost, and its receiver waits for its number from then on (A14). Fixtures
  of every stored and sent type at 1.0.0 guard the rule.
- The serde refuses a value that does not decode back equal to itself (F4); no valid value changes encoding.
- No wire contract, topic or key change.

## Design

### 1. Layout

```
src/ems/assembler/
  meter_stream.py   horizon, meter_close, the late branch without the newest guard
  join.py           on_clock, visibility clamp, after_end_late in _add, clamp counters
  schedule.py       NEW: Change, Refusal, Schedule, accept(), strategies_at(), windows(), strategy_params()
  offline.py        fold(..., schedule=()), dropped corrections, trace() for the steps table
  params.py         mode, grace_s
src/ems/domain/settlement.py   Costs, costs(rules, months, acc)
src/ems/actors/
  messages.py       Deadline, StrategyChange
  shell.py          Send.delay_s, handle_timed, (service, handler) on every entry point, timing hook
  timing.py         NEW: queue logger; the only wall-clock read in src/ems/actors/
  household.py      schedule, clock, on_deadline, set_strategies, chain start on the snapshot, per-step routing
  app.py            timing.start() at import
src/ems/experiments/
  stack.py          reset_stack, Publisher (Popen; backfill, accelerated, realtime), run_ends with schedules,
                    HostSampler, StackEnv(ingress_project_prefix)
  runs.py           managed_params(..., rcem=..., mode=...), pse_table()
  rcem.py           NEW: proxy_table()
  config.py         NEW: experiment config model
  run.py            NEW: runner CLI
  export.py         NEW: state → Parquet CLI
  metrics.py        NEW: pure metric functions
src/ems/ingest/runner.py      pace() helper, replay --speed for history, paced_from_unix
experiments/configs/          sys-scale, sys-speed, sys-recovery, sys-strategy-change, sys-live
```

### 2. Assembler: the clock rule (AC-012-01 … -06)

```
horizon(s)        = max(s.newest, s.closed - 1)          # newest None -> closed - 1
meter_apply(s, r):
  before = horizon(s)
  too_old   if r.t <= before - 2L                        # was: newest - 2L
  late      if r.t < s.closed and r.t not stored         # the guard "newest is not None" goes
            -> Late(t, kwh, at_step = before + 1); late += 1   (unchanged)
  close loop as today (reading present, or newest >= closed + L)
  keep slots with t > horizon(after) - 2L
meter_close(s, until):                                   # until = min(clock_step, end_step)
  while s.closed < until or s.closed in slots: close with the reading, else the estimate
  prune with horizon(after)
join._add: a Late with at_step >= end_step is dropped and counted in after_end_late
on_clock(params, j, clock_step):
  c = min(clock_step, params.end_step)
  each side <- meter_close(side.stream, c); ready += closed
  price_covered = max(price_covered, c); forecast_covered = max(forecast_covered, c)
  return _release(j)
on_curves / on_run:                                      # clamp against the watermark before the update
  floor = j.price_covered (j.forecast_covered)
  each curve/run: visible = max(visible, floor); count clamped_price / clamped_forecast if it moved
```

**Why sim is unchanged.**
- After every `meter_apply`, `closed − 1 ≤ newest`, so `horizon == newest`.
- Removing the guard changes nothing, because `t < closed` implies that `newest` is set.
- No `on_clock` runs in sim.
- `price_apply` and `forecast_apply` stamp `visible ≥ covered`, and the household applies updates in `seq`
  order, so the clamp never moves anything.

The property tests check each of these.

### 3. Actors (AC-012-07 … -15, -19, -20)

**Shell.**
- `Send` gains `delay_s: float | None = None`. `_commit` passes `send_delay=timedelta(seconds=s.delay_s)` when it
  is set.
- `handle_timed(ctx, names, serde, transition)` reads `params` and `state`, then `now = await ctx.time()`, then
  commits `transition(params, state, now)`. It is the only `ctx.time` call in `src/ems/actors/`.
- Every entry point takes `names = (service, handler)` and calls
  `timing.mark(names, key, ctx.request().id, in_t, sends)` after its last `ctx.set`. `ingest` marks only when
  the transition ran.

**Timing log (`timing.py`).**
- `start()` runs when `app.py` is imported. If `EmsSettings().timing_log` is set, it attaches a `QueueHandler`
  to the logger `ems.timing` (`propagate=False`, level INFO) and starts a `QueueListener` with a `FileHandler`,
  stopped through `atexit` and, unconditionally, from a SIGTERM hook chained the way the SDK chains its own
  (under coverage, coverage's handler re-raises SIGTERM with the default action, so `atexit` alone would not run).
  Otherwise `mark` returns at once.
- A line is one JSON object: `{"ns": time_ns, "svc", "h", "key", "inv", "in_t": t|null, "sent": [[service, t],
  …]}`.
- It makes no `ctx.*` call. `time.time_ns` is read only in this module, and the scan test pins that.

**`Household`.**

| Transition | What it does |
|---|---|
| `start` | as today, plus `schedule = Schedule()` and `clock = Clock()` |
| `on_prices` | as today. In live mode, when the snapshot is applied and `clock.expected is None and not clock.stopped`, it also sends `Household.on_deadline(Deadline(first_step))` with no delay and sets `expected = first_step` |
| `on_deadline(params, state, d, now_s)` | If `d.t != expected`, count `stale` and send nothing. Otherwise count `firings`, compute `c = ⌊(now_s − G)/900⌋` and `k = min(c, end_step)`, run `join.on_clock(k)` and route the released steps. If `k ≥ end_step`, set `expected = None` and `stopped = True`. Otherwise compute `nxt = max(k, first_step) + 1`, send itself `Deadline(nxt)` delayed by `max(900·nxt + G + 1 − now_s, 1)` seconds, and set `expected = nxt`. A firing with `nxt == d.t` came early (the VM clock leads the host): it counts `early`, and the 1 s floor keeps a large lead from looping |
| `on_change(params, state, change)` | `schedule.accept(params, state.schedule, state.join.next, change)` returns either the new schedule or the schedule with a `Refusal` appended. No sends |
| `_route(params, schedule, steps)` | each step `s` goes to `strategies_at(params, schedule, s.t)` and to `Ems` |

- New handlers:
  - `on_deadline` (`serde_for(Deadline)`, through `shell.handle_timed`);
  - `set_strategies` (`serde_for(StrategyChange)`, through `shell.handle`).
- `tests/unit/actors/test_app.py`:
  - the handler table gains both handlers;
  - the edges gain `Household → Household`;
  - the network fixture gets a live household, so that edge is exercised;
  - the forbidden-pattern scan allows `ctx.time` only in `shell.handle_timed`, `send_delay` only in
    `shell._commit`, and `time.time_ns`/`time.time` only in `timing.py`.

**Schedule (`ems.assembler.schedule`, pure).**
- Types: `Change(t, strategies)`, `Refusal(t, strategies, reason)`, and `Schedule(changes, refused)`, both tuples
  ascending by `t`.
- `accept` sorts and deduplicates `strategies`, then checks the reasons below in order:

  | Reason | Refused when |
  |---|---|
  | `not_on_hour` | `t % 4 != 0` |
  | `released` | `t < next` |
  | `after_end` | `t ≥ end_step` |
  | `not_after_pending` | `t ≤` the last change's `t` |
  | `unknown_policy` | a name is not in `POLICIES` |
  | `live_removed` | `live ∉ strategies` |
  | `re_added` | a name was dropped by an earlier change |

- `strategies_at(params, schedule, t)` is the set of the last change with `t′ ≤ t`, else `params.strategies`.
- `windows(params, schedule)` maps each name to `(start, stop | None)`.
- `strategy_params(params, change)` is `replace(params, first_step=change.t, strategies=change.strategies)`.

**Strategies.** `offline.step_strategy` drops corrections with `c.t < params.first_step` and counts them in
`StrategyResult.dropped_corrections`. This never happens for a strategy that starts at the run's `first_step`.

### 4. Retention and compose (AC-012-18)

- `infra/docker-compose.yml`, service `restate`:
  `RESTATE_DEFAULT_JOURNAL_RETENTION: ${EMS_RESTATE_JOURNAL_RETENTION:-1d}`.
- T01 starts `restate` with the value `0`, records what a completed key-less invocation and a keyed one still
  show, and restores the default in `finally` (`compose up -d --wait restate` without the variable).

### 5. Domain cost helper (AC-012-27)

`ems.domain.settlement.costs(rules, months, acc) -> Costs`:
- `bill_pln` = Σ `payable_pln` − Σ `refund_pln` over `months`, or `None` when `months` is empty.
  - `payable_pln` already nets the offset.
  - A refund comes only from what remains after the offset.
  - The extinguished part and the tranches stay out of the bill.
- `deposit_closing_pln` = Σ `remaining_pln` over `acc.tranches` (opening tranches included), or `None` when
  `months` is empty. It equals the last month's `closing` once a month has closed.
- `ledger_net_cost_pln` = Σ (`energy_cost_pln` + `dist_cost_pln` − `rules.deposit_factor` ·
  `export_value_pln`). The sum runs over `months` and over every row in `acc.month_rows`, which during the lag
  can hold two months; `None` when there is neither (a strategy window that never started). The open hour (fewer
  than four quarters) has no row, and the export notes that.
- The deposit is the one after the last closed month: it leaves out the offsets and the tranche of a month whose
  rows are still held (11 says so).
- A removed strategy no longer receives the corrections for slots in `[stop − 2L, stop)`, which ride on later
  steps: its hours are comparable only before `4 · ⌊(stop − 2L)/4⌋`, which the export records
  (`compare_until_step`) and ADR 0021 explains. A correction dropped as `after_end_late` touches only held rows,
  never `bill_pln`, in windows of whole months plus `2L` (12 and 11 say so).

Energy-domain review 2026-11-23: these match `11-experiments.md`, `12-domain-model.md` and the `MonthSettlement`
fields, with no double counting.

### 6. Pacing and the RCEm table (AC-012-21 … -24)

- `ems.ingest.runner.pace(due_monotonic, sink)` is the sleep-in-slices loop taken out of `paced_publish`.
  `paced_publish`, spec 002 replay (unchanged) and history replay all use it.
- `replay --speed` for history: `due = wall0 + (emission_key(e)[0] − hist.start)/speed`, for both recording and
  background events. The refusal in `_history` goes.
- `runner.run` and `replay` write `paced_from_unix` (the `time.time()` taken together with `wall0`) into their
  manifests.
- `stack.Publisher(mode, speed, ...)`:
  - starts `ems-gen run --mode backfill|accelerated|realtime` with `Popen`, logging to a file;
  - offers `wait()`, `kill()`, `running`, `started_unix` and `manifest()`;
  - `publish_backfill` stays as a thin blocking wrapper.
- `ems.experiments.rcem.proxy_table(cfg, config_dir, first_step, end_step)`:
  - covers the civil months from two before `civil_month(first_step)` through `civil_month(end_step − 1)`;
  - builds the daily RCE (`day_prices`) and the PV-index weights (`national_pv_index`) from the run's
    `SyntheticModel` with its seed, as `tests/unit/domain/ref_window.py:_daily` does, then calls `proxy_rcem`.
- `managed_params(..., rcem: RcemTable, mode="sim", grace_s=120)`:
  - the RCEm table is required;
  - in live mode an RCEm-basis site raises (no RCEm of the running month exists before the 11th of the next one;
    OPEN in 01).
  - `runs.pse_table(config_dir)` serves the tests and history runs.
  - Call sites: `tests/stack_fixtures.py`, `jobs/ingress_measure.py`, `tests/network.py`.
- `runs.rcem_used(params)` lists the entries the run's RCEm-basis sites read, for the manifest; it is empty for
  an all-RCE fleet.

### 7. Runner, export, metrics (AC-012-25 … -32)

**Config (`ems.experiments.config.Experiment`, pydantic, `extra="forbid"`):**

```yaml
id: sys-recovery
hypothesis: "Results stay equal to the offline side under kill -9 of the SDK service and the server."
data: {kind: synthetic, start: "2026-06-03T00:00:00Z", days: 1, locations: 350, households: 100, faults: false}
# or: data: {kind: history, recording: spring-2026}
run: {late_slots: 8, strategies: [NoBattery, SelfConsumption], live: SelfConsumption,
      mode: sim, grace_s: 120, speed: 96}
kills: [{target: sdk, at: 0.3, downtime_s: 0}, {target: server, at: 0.7, downtime_s: 0}]
changes: []                       # e.g. [{at_hour: 12, strategies: [NoBattery, SelfConsumption]}]
matrix: {}                        # e.g. {households: [100, 400], strategies: [[SelfConsumption], [NoBattery, SelfConsumption]]}
sweep: null                       # e.g. {speed: {start: 96, factor: 2, max: 1536}}
repetitions: 1
```

- `households` keeps the first N managed keys (sorted). If the catalog of `locations` sites holds fewer, the
  config is refused before any stack call; T28 checks this against the real catalog.
- `at` is a fraction of the window in data time.
- `at_hour` counts hours after `first_step`.
- The matrix is the full product.
- In live mode, `start` and `days` are replaced by "the next full UTC hour" and `hours`.

**Stack additions (`stack.py`).**
- `reset_stack(retention="0")`:
  - `compose --profile ingress down -v` for the project `ems`;
  - remove the `ems.role=ingress` containers;
  - `compose up -d --wait kafka kafka-init restate` with `EMS_RESTATE_JOURNAL_RETENTION` set (no `kafka-ui`, a
    JVM polling Kafka inside the measured VM).
- `StackEnv(prefix, ingress_project_prefix="ems-exp-")`.
- `run_ends(params, offline, schedules)`: a removed strategy ends at `min(stop, next)`; an added one ends at
  `max(t, next)`, which is `t` if its step was never reached.
- `HostSampler(service)`: every 2 s, `pgrep -g <pgid>` and `ps -o %cpu=,rss=` over the SDK service's whole
  process group (hypercorn's master and its worker, where the handlers run), summed; the group id is re-read after
  every restart.
- `MemorySampler` also records each container's CPU (`docker stats {{.CPUPerc}}`) and, for `restate`, the
  anonymous memory from the cgroup's `memory.stat` (`docker stats` usage includes page cache).
- A Kafka lag sampler: `group_progress` per ingress group every 2 s.
- The offset between the host and the Docker VM clocks: `time.time()` against `docker exec ems-restate-1 date
  +%s.%N`, the median of five, recorded in the manifest.

**Runner (`ems.experiments.run`), per run (sim):**
1. `reset_stack`.
2. `StackEnv.open()`; the SDK service gets `EMS_TIMING_LOG=<run dir>/timings.jsonl`.
3. `managed_params(rcem=…)` (proxy for synthetic data, PSE for history), then `configure`.
4. Changes that apply before publishing: configure the added objects, poll SQL (at most every 5 s) until each
   stored `params` equals the one sent, then send `set_strategies`. A send that fails with a transport error is
   retried with the same `Idempotency-Key` (R4); a config whose kill overlaps a change's sending is refused.
5. Start the `Publisher` and the samplers.
6. During the run:
   - kills at their data-time fraction of the schedule: `SdkService.kill9` then `start`, or `kill_restate` then
     `start_restate`, recording the kill, healthy and restart times;
   - later changes, each while the schedule is still at least 1 h of data before its `t`.
7. Wait for the publisher.
8. Read the records back and fold the **join only** (`offline_run` with no strategies); the `Meter` end does not
   depend on strategies.
9. End detection: tail `timings.jsonl` until every managed `Meter` has a line with `in_t` equal to its join end
   − 1. A stall (no `Meter` line advancing for `stall_s`) ends the run as `stalled`, with the diagnosis of
   AC-012-32 (invocations paused, backing off or with `retry_count > 0`; inboxes holding messages; last `t` per
   service).
10. Read the Household schedules from state, then `wait_run(ends=run_ends(params, join, schedules), poll=5)`, so
    every strategy has reached its own end before anything is compared (strategies get `Step` straight from
    `Household`, beside the Ems → Battery → Meter chain).
11. Fold the strategies with the schedules.
12. Exactly-once check, then the empty-inbox check once `sys_invocation` shows no pending invocation of the run.
13. Export.
14. Metrics.
15. `summary.json`, written with `allow_nan=False`.
16. `StackEnv.close()`.

**Live runs** differ:
- the topics use the `live` profile;
- the runner waits until `first_step` and starts `Publisher(mode="realtime")` with `--end` at `end_step`, and stops
  it before the checks;
- it ends at wall time `900·end_step + G + 1 s`, plus the end detection on the step before `end_step`;
  `stall_s` defaults to `1800 + G` s;
- it runs the invariant checks of AC-012-16 and -36 instead of the twin, and writes no `steps` table.

A run that fails at any point is written with `status: failed` and the reason, and the matrix goes on.

**Export (`ems.experiments.export`; CLI `--run-id`, `--out`).** It reads `state` through SQL as an Arrow
response, decodes with `serde_for`, and writes:

| Table | One row per | Columns |
|---|---|---|
| `households` | managed household | `run_id`, `household_key`, `location_id`, `has_pv`, `first_step`, `end_step`, `next_step`, `records`; the meter counters per side (`estimated`, `late`, `too_old`, `duplicate`, `conflict`); `after_end_late`, `price_replaced`, `clamped_price`, `clamped_forecast`, `changes`, `refused`; live only: `deadline_firings`, `stale_deadlines` |
| `strategies` | household × strategy window (and `real` for Battery+Meter) | `start_step`, `stop_step`, `compare_until_step`, `next_step`, `start_soc_kwh`, `end_soc_kwh`, `months_closed`, `bill_pln`, `deposit_closing_pln`, `ledger_net_cost_pln`, `partial`, `dropped_corrections`, `clamped` (real only) |
| `months` | household × strategy × closed month | the `MonthSettlement` scalars |
| `steps` (sim) | household × strategy × step, from `offline.trace` | `t`, `pv_kwh`, `load_kwh`, `rce_pln_per_kwh`, the flags, `forecast_issue_s`, `action`, `p_kw`, `soc_kwh`, `grid_kwh` |
| `timings` | the deduplicated timing lines | `ns`, `svc`, `h`, `key`, `inv`, `in_t`, `sent` (list of service and `t`), `lines` (lines seen for the invocation) |
| `lags` (sim) | released household-step | `household_key`, `t`, `stream` and `due_unix` of the releasing record, `released_unix`, `lag_s` (added during validation, so a slow tail can be traced to its input after the run's topics are gone) |

Every table carries `ems.schema_version = 1.0.0` in its Parquet metadata. `steps` is written only after the
exactly-once check passes.

**Metrics (`ems.experiments.metrics`, pure):**

| Metric | Definition |
|---|---|
| `records_per_s`, `invocations_per_s` | records handled (Σ `n`) and kept timing lines per wall second, from the first publish to the end |
| `decision_latency_ms` | per household-step: the `ns` of the first `Battery` line sending `["Ems", t]` minus that of the first `Household` line sending `["Ems", t]`. Reported as p50, p99, max, the count and the number missing; steps inside a recovery window are left out |
| `release_lag_s` | the `Household` release `ns` minus the `ingested_at` of the record that made the step releasable: the household's records and its market's prices, merged in publication order through the same join (`offline.released_by`). Amended 2026-11-25, after the first `sys-recovery` run: counting from the reading's due time put each household's first 7 steps, which wait for the first forecast run, at 13–70 s at speed 96 (p99 66 s against 9.4 s), so no speed was sustained |
| `publisher_lateness_s` | how far the publisher fell behind its own schedule (p99, max; `pace` records it in the generator's manifest), with the consumer-group lag and the container CPU beside it, so a speed that is not sustained shows whether the publisher, the emulated ingress, Restate or the SDK service set the limit |
| `sustained` | AC-012-31 |
| `recovery` | per kill: `healthy_s`, `first_commit_s` and `caught_up_s` (release lag back under the pre-kill p99) |
| `retries` | invocations with more than one line |
| `forecast_age_h` | `900·t − forecast_issue_s` over the released steps |
| memory, CPU, state | container peaks, CPU and OOM flags, the `restate` anonymous memory; SDK process-group CPU and RSS peaks; `SUM(value_length)` and `MAX(value_length)` per service |
| live only | firings, early and stale deadlines, estimates, `price_replaced`, release delay after the quarter end (`ns` − `900·(t+1)`) as a distribution, against the tolerance `G` + 5 s |

**Makefile:** enable `exp`:
1. `@test -n "$(NAME)"`;
2. the confirmation prompt, skipped with `CONFIRM=yes` as in `reset`;
3. `uv run python -m ems.experiments.run experiments/configs/$(NAME).yaml`.

### 8. Validation configs (AC-012-36)

| Config | Data | Run |
|---|---|---|
| `sys-scale` | synthetic, 2026-06-03, 1 day; `locations` 1400 | matrix households {100, 400} × strategies {[SelfConsumption], [NoBattery, SelfConsumption]}, 4 runs; speed 96 |
| `sys-speed` | synthetic, 2026-06-03, 1 day; 100 households | sweep speed 24 → 1536 (×2; it started at 96 until `sys-recovery` and `sys-strategy-change` showed 96 borderline at 100 households); stops after the first speed that is not sustained |
| `sys-recovery` | synthetic, 2026-06-03, 1 day; 100 households | 2 repetitions × kills `[sdk at 0.3, server at 0.7]` |
| `sys-strategy-change` | synthetic, from **2026-06-29T04:00Z**, 2 days; 100 households | starts with `[SelfConsumption]`; at hour 12 `[NoBattery, SelfConsumption]`; at hour 36 `[SelfConsumption]`. June closes on the actors (06-30T22:00Z + 2L) |
| `sys-live` | synthetic in real time, live profile, faults; 10 households (RCE-basis only) | `first_step` = the next full UTC hour, 2 h, `grace_s` 120 |

Every config uses the generator's seed from `config/generator.yaml`. All windows, lags and proxy months fall
inside 2026 (risk D9). T28 confirms that 1400 locations hold at least 400 managed households with a battery.

### 9. Fixes to existing code (Phase 0b; AC-012-38 … -42)

Code on `main` that the measurements rely on and that no task covered. Each lands before the tiers (Phase 5)
and the validation runs (Phase 6), with a commit `fix(<scope>): … (012/Tnn)` (user decision, 2026-11-23).

**F5, stored-state fixtures (first, before any type changes; AC-012-41).**
- A small generator writes every type of `STORED_AND_SENT` (state types, `RunParams` and the messages, because
  messages wait in Restate's queues too) as today's `main` encodes it, one value per line, to
  `tests/fixtures/state/1.0.0/<Type>.jsonl`. Its values:
  - the `values()` fixture (`tests/unit/actors/test_serde.py:67`: start states, a half run with held messages, a
    full run, every message);
  - the same with faults, at `L` = 8 and at `L` = 2 (`tests/network.py`, `FAULT_RUN_ID`), for `Late`,
    `Correction` and estimated `Closed` values;
  - hand-built settlement values: a month that uses an opening tranche, one where a tranche expires with a
    refund, one that creates a tranche, `SettleAcc` with an open hour and two months of rows; `Apply` with EXPORT
    (`level`) and SETPOINT (`p_kw`); a `Cohort` with `rce_from`. (The night window of `values()` has no export,
    so no `Tranche` and no offset.)
  The generator asserts that every dataclass reachable from `STORED_AND_SENT` (the `_set_types` walk of the serde
  tests) occurs in some line.
- `tests/unit/actors/test_state_compat.py`: every line decodes with the current serde, and the decoded value
  re-encoded keeps every key and value of the line, recursively. pydantic ignores unknown keys, so decoding alone
  would let a removed or renamed field pass; the subset check makes it fail, while an added field with a default
  passes.
- The 08 rule (Contract changes above) and row A14, by hand (T64).

**F1, harness polling (AC-012-38).**
- `Restate.invocations(run_id, *, suspect_only=False)`. With `suspect_only` the `WHERE` adds
  `status = 'paused' OR completion_result = 'failure' OR retry_count >= STRICT_ATTEMPTS`, a superset of
  `failed()`, which keeps filtering in Python. `failures()` uses it.
- `_transient(exc)`: `httpx.TransportError` or a 503, and any 5xx within 60 s after a kill the harness itself
  caused (`Restate.kill_window`). `wait_run` and `wait_handled` re-raise every other `httpx.HTTPError`. The fault
  tier (T43) records the statuses `/query` gives around `start_restate`, so the window rests on a measurement.
- The exact `WHERE`: `starts_with(target_service_key, '<run>:') AND (status = 'paused' OR completion_result =
  'failure' OR retry_count >= 4)`, with the `OR` terms in parentheses. The stall diagnosis uses
  `min_retries=1` and adds `status = 'backing-off'`.
- `make measure MODE=skew` is rerun after F1, F2 and F4 (T63) and compared with `ingress-skew-20261121T005630Z`;
  if the numbers move, 11 and `docs/thesis/kafka-ingress.md` get the new result directory.

**F2, Restate memory pools (AC-012-39).**
- Compose, service `restate`, beside `RESTATE_ROCKSDB_TOTAL_MEMORY_SIZE: "512 MiB"`:
  - `RESTATE_WORKER__INVOKER__MEMORY_LIMIT: "512 MiB"`: the largest state is about 160 kB (A9) and the
    invoker runs at most 1,000 invocations at once, so even 1,000 large states take about 160 MB;
  - `RESTATE_ADMIN__QUERY_ENGINE__MEMORY_SIZE: "384 MiB"`: after F1 the polling queries are small; the largest
    left is the export's `state` read (about 34 MB at 35 households, about 480 MB at 500; the export reads one
    service at a time);
  - `RESTATE_BIFROST__RECORD_CACHE_MEMORY_SIZE: "128 MiB"`.
- Sum 1.5 GiB plus the runtime, below `mem_limit: 2g`. T57 confirms with `restate-server --dump-config` that the
  `"512 MiB"` format is read for the new variables, as it was for RocksDB.
- Decision rule: if `restate` is OOM-killed in a validation run, or its peak anonymous memory (cgroup
  `memory.stat`, not `docker stats` usage, which includes page cache) exceeds about 1.8 GiB, `mem_limit` goes to
  `2560m` (the Docker Desktop VM has 7.75 GiB: Kafka 1 + Restate 2.5 + ingress 3 × 0.75 + `kafka-ui` 0.5 =
  6.25 GiB).
- The deploy checklist (`ems-deploy-checklist`) runs before the change.

**F3, `kafka-ui` memory (AC-012-39).** `mem_limit: 512m`. The compose test (T56) checks that every service
`make up` and `make register` start has a limit; `kafka-init` is one-shot, and `ems-gen` (profile `gen`, started by
hand) needs none (user decision, 2026-11-24).

**F4, the serde guard (AC-012-40).**
- `DataclassSerde.serialize`: after `dump_json`, `if self._adapter.validate_json(out) != obj: raise
  ValueError(f"{type(obj).__name__} does not survive JSON: a NaN or an infinity?")`.
- A `ValueError`, not a `TerminalError`: the invocation retries and pauses where the bad value was produced, and
  nothing is stored or sent (a `TerminalError` would lose the message and leave an inbox gap, A14). After a code
  fix: `restate invocations resume <id> --deployment latest` (F6).
- `==` skips `compare=False` fields (all of them integer counters), and NaN ≠ NaN, so both cases are caught. The
  same serde encodes sent messages and the new types of 012. `shell._commit` encodes every send before
  `ctx.set(STATE)`, so "nothing is stored" holds literally.
- The guard refuses no valid domain value (energy-domain review, round 2): negative prices, the floor's +0.0,
  zero export, `created=None`, remaining 0, -0.0 and an integer value in a float field all compare equal. It
  refuses only type drift (a list in a tuple field, a datetime in a date field), which is wanted; so
  `schedule.accept` returns tuples and `proxy_table` publishes `date` objects. Wire records cannot carry a NaN
  (`envelope.py:57`), so the guard never pauses a Kafka-fed key over a record's content. A NaN in a tariff or
  battery config is refused at `configure` in the harness, before anything is sent.

**F6, paused invocations (AC-012-42).** A section "Operations: paused invocations" in `02-subsystem-actors.md`,
and a pointer from risk A7:
- the SQL that lists them (`SELECT id, target_service_name, target_service_key, target_handler_name,
  last_failure FROM sys_invocation WHERE status = 'paused'`);
- the CLI in the server image: `docker exec -it ems-restate-1 restate invocations resume <id> [--deployment
  latest]`, and `restate invocations list --status paused`;
- HTTP: `PATCH /invocations/<id>/resume[?deployment=<id>]`;
- pipe JSON from `docker exec` without `-t`: with `-t`, the statistics line and `\r\n` land on stdout;
- resuming on a new deployment fails with RT0016 when the fix changes a journaled operation before the failing
  one: then the invocation has to be killed and its effect redone by a new run.

## Actor contract

### Actors

| Actor (Virtual Object) | Key | State (field: type, unit) | Owner of facts | New / changed |
|---|---|---|---|---|
| `Household` | `[run_id:]location_id` | as stage 3, plus: `schedule: Schedule` (accepted changes and refusals, ascending `t` [step]); `clock: Clock(expected: int \| None [step], stopped: bool, firings: int, early: int, stale: int)`; `join` gains `after_end_late`, `clamped_price`, `clamped_forecast` | the step sequence; its strategy schedule and refusals; its deadline chain | changed |
| `NoBattery`, `SelfConsumption` | `[run_id:]location_id` | `StrategyResult.dropped_corrections: int` | virtual SoC, shadow ledger | changed (one counter) |
| `Tariff`, `Weather`, `Ems`, `Battery`, `Meter` | unchanged | unchanged | unchanged | unchanged |

### Handlers

| Actor.handler | Kind | Input message (version) | Writes | Sends (one-way) | Calls (request-response, shared only) |
|---|---|---|---|---|---|
| `Household.configure` | exclusive | `RunParams` 1.1.0 | `params`, `state`, `n_at_configure` | `Tariff.subscribe` | none |
| `Household.on_prices` | exclusive | `PriceUpdate` 1.0.0 | `state` | steps; live, on the snapshot: `Household.on_deadline: Deadline(first_step)` | none |
| `Household.on_deadline` | exclusive | `Deadline` 1.0.0 | `state` | `<strategy>.on_step`, `Ems.on_step: Step`; `Household.on_deadline: Deadline(nxt)`, delayed | none |
| `Household.set_strategies` | exclusive | `StrategyChange` 1.0.0 (HTTP ingress, `Idempotency-Key`) | `state` | none | none |
| `Household.on_measurement`, `on_forecast` | exclusive | unchanged | `state` | steps routed by `strategies_at` | none |
| `Household.status` | shared | none | none | none | none |

### Messages

| Message | Version | Producer → consumer | Fields | Additive? | Golden examples updated? |
|---|---|---|---|---|---|
| `Deadline` | 1.0.0 | Household → itself | `t: int` [the step it is set for], `schema_version` | new | n/a (field-pin test) |
| `StrategyChange` | 1.0.0 | operator → Household | `t: int` [step, on a UTC hour], `strategies: tuple[str, …]`, `schema_version` | new | n/a (field-pin test) |
| `RunParams` | 1.1.0 | operator → every object | `mode: "sim" \| "live"`, `grace_s: int` [s] | yes (defaults) | field-pin test updated |

### Timers

| Timer | Owner | Sim mode | Live mode (`send_delay`) | Generation guard |
|---|---|---|---|---|
| The household deadline | `Household` | none (`clock.expected` stays None) | `900·nxt + G + 1 s − now`, from the journaled `now`; the chain starts on the price snapshot | yes: `Deadline.t` must equal `clock.expected`, otherwise counted stale |

Strategy changes are data in state (`schedule`), applied when their step is released, in both modes.

### Failure modes

| Failure | Expected behaviour | Test (tier) |
|---|---|---|
| SDK service `kill -9` mid-replay | Restate retries; results equal offline, also with strategy changes | fault (spec 011), `sys-recovery` |
| Restate server `kill -9` mid-replay | Healthy after restart (V10); results equal offline | fault (AC-012-33), `sys-recovery` |
| SDK `kill -9` with a deadline pending | The delayed send stays in Restate and fires after the restart; one deadline pending per household | integration (AC-012-10) |
| Deadline retried | The same journaled `now`, so the same writes and sends; the core's replay of the delay is VERIFY | unit (byte trace), integration |
| Deadline early (clock skew) | `k` has not advanced, so it reschedules the same step after at least 1 s and counts `early`; not stale | unit |
| First deadline before the price snapshot | Cannot happen: the chain starts on the snapshot | unit |
| Duplicate `set_strategies` | Deduplicated by the key; another key with the same `t` is refused `not_after_pending` | unit, integration |
| Change after its step was released | Refused `released`, counted, exported | e2e (AC-012-34) |
| Out-of-order or late message | As in stage 3 (hold-back by `t` and `seq`) | property (network, sim and live) |
| A record behind the household's watermark | Its visibility is clamped to the watermark and counted | property |

### Rule check

| Rule | Holds? | Evidence |
|---|---|---|
| R1/R2 | yes | New edges: Household → Household (`on_deadline`, one-way and delayed) and operator → Household (`set_strategies`, an ingress send). The scan test forbids calls |
| R3 | yes | `ctx.time()` appears only in `shell.handle_timed` and is journaled; the timing log is outside the journaled operations (AC-012-20) |
| R4 | yes | `set_strategies` carries an `Idempotency-Key` with `t` and the names |
| R5 | yes | Waiting is buffering; the deadline is a delayed message, not a sleep |
| R6 | yes | `Deadline.t` is checked against `clock.expected` |
| R7 | yes | `Deadline` and `StrategyChange` carry `schema_version` and `t` |
| Ordering (ADR 0017) | yes | The clock raises watermarks only to "now − G", only in live, and only after the snapshot; lateness is still judged in the record's own stream (horizon) |
| Single writer | yes | Schedule, refusals and the deadline chain are written by `Household` only; rows added to 01 |

## Risks (`09-risks-and-tech-debt.md`)

| Risk | In this change |
|---|---|
| A1 | History replay can be paced; experiments publish paced |
| A4 | Stays accepted: live runs are checked by invariants (AC-012-16, -36) |
| A6 | Fixed: journal retention 0 for experiments and live |
| A9 | Measured (state bytes per service); stays accepted |
| A12 | Widened: in live a curve delivered more than `G` after a quarter ends is replaced although it was published; `sys-live` reports `price_replaced` |
| A6 (journal × state) | Retention 0 also removes the cost of keeping a journal entry for every whole-state rewrite (about 11 MB per household-day at 1 d) |
| A7 | The procedure for paused invocations (F6) |
| H5 (new) | The harness's failure polling grew with the run and swallowed 500s: fixed (F1) |
| K15 (new) | Restate's memory pools above its container limit: fixed (F2); the validation runs report peaks and OOM kills |
| A14 (new) | A message lost to a terminal failure stalls its receiver: mitigated by the stored-state rule, the 1.0.0 fixtures (F5), stall detection (AC-012-32) and the empty-inbox check (AC-012-28) |
| A15 (new) | NaN and infinity in the serde: fixed (F4); latent today |
| A16 (new) | `Tariff` is one object per market; each curve fans out to every subscriber and a subscribe wave costs O(N²) bytes: accepted at hundreds of households; `sys-scale` reports the `Tariff` state size |
| K14 | Retries counted from the timing log |
| New | Observer cost of the timing log; queued lines lost at `kill -9` (counted missing); completed invocations cannot be inspected at retention 0; an added strategy starts at half capacity, so it is compared per hour with its ΔSoC; the core's replay check of a delayed send (VERIFY); where the thesis evaluation runs (OPEN) |

## Experiment impact

No reported thesis number changes. The stage writes validation results only, labelled as such in
`11-experiments.md` with their result directories.

## Test strategy

| Tier | Tests |
|---|---|
| Unit | `schedule.accept`: every reason, their order, normalisation. `strategies_at`, `windows`, `strategy_params`. The `on_deadline` and `on_prices` transitions: stale, early, catch-up, stop at the end, delay > 0, a window in the future, the chain starting only on the snapshot. `meter_close`. `costs` on hand-built months, held rows and opening tranches. Field pins of `RunParams`, `Deadline` and `StrategyChange`. The handler and edge tables and the forbidden-pattern scan. `timing.mark` (format; the file written on the listener thread). Metrics on synthetic logs. Export round-trip. `proxy_table` equal to `ref_window._daily` plus `proxy_rcem` (same seed, same months). `pace` with a fake clock. History `--speed` publishes the same events in the same order. Config validation. `run.py` against a fake stack: kill timing, change timing, configure → equal params → send, `status: failed`, manifest fields, mismatches → failed |
| Property | Clock-free runs equal stage 3. Records, updates and non-decreasing clock steps in any interleaving keep the accounting, the order, `end_step` and visibility. The clamp is a no-op without clock events. The sim network with changes, in any delivery order, equals the twin. The live network with a fake clock and delayed sends keeps the invariants of AC-012-16. Commits are identical with the timing log on and off. New state types are byte-deterministic under two hash seeds |
| Integration | The deadline chain (AC-012-35); a deadline across an SDK kill (AC-012-10); retention unset and set to 0, restored afterwards (AC-012-18); `set_strategies` idempotency |
| E2E | Add and remove through Kafka on 10 households, the accepted changes sent before publishing and one late change refused (AC-012-34); a paced run equals offline (AC-012-23) |
| Fault | `kill -9` of the server with households configured (AC-012-33) |
| Validation | The five configs (AC-012-36), recorded in 11 |
| Phase 0b | F5: every 1.0.0 fixture line decodes with the current serde (unit). F1: the fake `invocations` takes `*, suspect_only`; the SQL carries the filter; a 500 stops the wait; a `ConnectError` keeps polling (unit). F2, F3: every pool set and their sum below `mem_limit`; every service of `make up` and `make register` has a limit (unit); `--dump-config` shows the values (integration). F4: NaN and ±inf in a `float` and a `float \| None` field are refused, and the AC-011-33 tests stay unchanged (unit). Timing listener: N invocations, then SIGTERM, give N lines (integration, T20) |

## Docs and ADRs

| File | Change |
|---|---|
| `docs/context/00-overview.md` | Stage 4 status and the decisions of 2026-11-22; OPEN: where the evaluation runs (compose, per spec 006, or a cluster) |
| `docs/context/01-architecture.md` | Live mode (grace, catch-up, chain start, clamp, generation), the handlers table, ownership rows, the R3/R6 rows, stage 4 done, the stage 5 brief refreshed; the A8 sentence about "the month after" corrected (the proxy needs M−2 … the month of the last step) |
| `docs/context/02-subsystem-actors.md` | New handlers, the timing log, retention; Restate's memory pools; the serde's refusal of values that do not survive JSON; "Operations: paused invocations" (F6) |
| `docs/context/05-deploy-and-infra.md` | `EMS_RESTATE_JOURNAL_RETENTION`, `make exp`, `EMS_TIMING_LOG`; the memory pools and the `kafka-ui` limit |
| `docs/context/06-data-flows.md` | Live clock, pacing of history replays |
| `docs/context/08-contracts.md` | `RunParams` 1.1.0, `Deadline`, `StrategyChange`, the result schemas; the stored-state rule (F5) |
| `docs/context/09-risks-and-tech-debt.md` | A1, A4, A6, A7, A8, A9, A12 and K14 updated; new rows H5, K15, A14, A15, A16 with the statuses 012 gives them |
| `docs/thesis/kafka-ingress.md` | Only if T63's skew rerun moves the numbers: the new result directory |
| `docs/context/11-experiments.md` | Runner, configs, export, metric definitions, the comparison of an added strategy (per hour from `t`, against NoBattery, with ΔSoC; `partial` bills) and of a removed one (until `compare_until_step`), the deposit after the last closed month, `after_end_late` in the counts, validation results; stage 4 OPEN items closed |
| `docs/context/12-domain-model.md` | The cost helper; `after_end_late` (the correction is dropped and the estimate stays; it touches only held rows); RCEm publication: the 11th is PSE's practice, and the legal deadline is IRiESP's (art. 11y ust. 4, VERIFY) |
| `docs/adr/0020-live-deadline-clock.md` | Grace, catch-up, chain start on the snapshot, expected-step generation, clamp. It extends ADR 0018's closing rule, notes A12's delivery delay, and records that in live mode a step is released at least `G` + 1 s after it ends, so `Battery` is a real-time shadow of the inverter's local control (D6), not a command to a device |
| `docs/adr/0021-strategy-changes-at-run-time.md` | Hour boundary, exact or refused, the schedule as data, the offline twin, comparison per hour with ΔSoC, a removed strategy comparable until `4 · ⌊(stop − 2L)/4⌋` |
| `docs/adr/0022-experiment-measurement.md` | Timing log, fresh stack, journal retention per environment, export from state plus the offline series, live runs checked by invariants |
| `docs/adr/README.md`, `CLAUDE.md` (commands), `.claude/skills/ems-experiment/SKILL.md` (stale prerequisites) | Index, `make exp`, prerequisites |

## Review

2026-11-23: `architect-reviewer` (approve with changes) and `energy-domain-reviewer` (no blockers). Each finding,
and how it was resolved:

| Finding | Resolution |
|---|---|
| Blocker: the network with clock events cannot equal the offline twin, because `fold` has no clock (A4) | AC-012-16 split: in sim `==` the twin; in live, invariants |
| AC-012-01 claimed spec 011 tests pass unchanged, but `test_join.py:258-267` keeps an end-step `Late` | `after_end_late` is counted in `join._add` as a subset of `late`; that one test changes, and AC-012-01 names it |
| End detection needed the offline end before the fold ran | Runner order: fold the join only, detect the end, read the schedules, fold the strategies |
| `sys-live` cannot pass the twin check; the Publisher had no realtime mode; pacing was not tied to `first_step` | Live runner path; AC-012-27 and -28 reworded; `Publisher(mode="realtime")` started at `first_step` |
| No test task for `run.py` | A `[test]` task before the runner (tasks T36) |
| T01 leaked retention 0 into later modules | Restore the default in `finally` |
| Wall-clock read not pinned; the live edge never exercised; "configured object" vs Tariff; key without names; RCEm formula; `timings` columns and versions; `firings` on the last firing; task order; pyarrow move | Each fixed above (scan pins `time` to `timing.py`; a live household in the fixture; Tariff marked; names in the key; one formula; columns and versions listed; every non-stale firing counted; tasks reordered and split; pyarrow stays in `ingest`) |
| Open question: does the first deadline race the Tariff snapshot? | Yes. The chain now starts on the snapshot |
| Open questions: the `at` denominator, `run_ends` for an unreached `t`, T40 determinism, the 400-household catalog size | Data-time fraction; `max(t, next)`; changes sent before publishing; `locations` 1400, checked by T28 |
| Open question: the core's replay check of a delayed send | VERIFY in T40 under the stop rule |
| Energy S1, S2: the ledger must sum every held row (two months in the lag), the deposit every held tranche; no literal 1.23 | §5 |
| Energy S3: validation windows close no month | Null bills; `sys-strategy-change` from 2026-06-29T04:00Z so June closes |
| Energy S4: an added strategy's SoC differs from the incumbents' | `start_soc_kwh` and `end_soc_kwh` exported; compared per hour from `t` against NoBattery with ΔSoC; `partial` flag |
| Energy S5: `sys-live` RCEm source | Live refuses RCEm-basis sites; `sys-live` runs synthetic data in real time with RCE sites |
| Energy S6: a late-delivered curve is replaced in live, not in sim | ADR 0020 and A12; `sys-live` reports `price_replaced` |
| Energy N1–N5 | Upper bound `civil_month(end_step − 1)`; manifest lists the table built and the entries used; same seed in T26; ADR 0020 note on the release delay; 12 notes the IRiESP deadline |

Second round, 2026-11-23, after the independent review's gaps were folded in (Phase 0b, §9): `architect-reviewer`
(approve with changes, no blockers) and `energy-domain-reviewer` (no blockers).

| Finding | Resolution |
|---|---|
| Arch 1: the exactly-once check could run before the strategies finished (their ends need the schedules) | Runner steps 9–12: schedules read right after the timing-log end, then `wait_run(ends=run_ends(…, schedules))`, then the comparison; T36 pins the order |
| Arch 2: `HostSampler(pid)` sampled hypercorn's idle master | Samples the whole process group, re-read after restarts (T34) |
| Arch 3: a deadline release cannot meet "end + G + 1 s"; a VM clock lead loops early firings | Tolerance `G` + 5 s with the distribution (AC-012-36); host–VM offset in the manifest (AC-012-26); early firings counted and floored at 1 s (AC-012-07) |
| Arch 4 / Energy 1, 2: the 1.0.0 fixtures miss faults and tranches; decoding alone lets a removed field pass | Fault runs at `L` = 8 and 2, hand-built settlement and action values, every reachable dataclass asserted present, and the re-encoded subset check (AC-012-41, T53) |
| Arch 5: under coverage `atexit` never runs after SIGTERM | The SIGTERM flush is unconditional (T23); T20's case runs with and without coverage |
| Arch 6: the empty-inbox check races queued messages; the live publisher has no end; `stall_s` at `S` = 1 exceeds the run | Inbox check once no invocation is pending (AC-012-28); `--end` and a stopped publisher in live; live `stall_s` = `1800 + G` (AC-012-32) |
| Arch 7: `sys-speed` cannot tell where a speed stops being sustained | Publisher lateness, consumer-group lag and container CPU beside the release lag (AC-012-29, T25, T35) |
| Arch 8: only 503 counted as transient; the stall diagnosis missed short retries; the `WHERE` untested | Any 5xx within 60 s of a harness-caused kill, statuses recorded in T43 (AC-012-38); `retry_count > 0` and `backing-off` in the diagnosis; T54 pins the exact `WHERE` |
| Arch nits | `set_strategies` retried with the same key (T36); `reset_stack` starts no `kafka-ui`; the 1.8 GiB rule uses anonymous memory or OOM kills; T53's docs split into T64, and a fix's test task lands in the fix's commit; T11 named; VERIFY points at T40; T47 cites AC-012-39; F6 warns about RT0016 on a new deployment |
| Arch open question: does a restart at retention 0 change services registered before it? | T03 checks a new invocation of an already-registered service; the runner registers after every reset, so it is not affected |
| Arch open question: what does the late change in AC-012-34 contain? | A removal at a released hour: no configured-but-unfed objects (AC-012-34, T42) |
| Energy 3: a removed strategy misses corrections for `[stop − 2L, stop)` | `compare_until_step` = `4 · ⌊(stop − 2L)/4⌋` in the export (AC-012-27) and ADR 0021 |
| Energy 4: a zero-length window reads as a 0.0 cost | Ledger `None` with neither months nor rows; `summary.json` with `allow_nan=False` (AC-012-26, -27) |
| Energy 5: `after_end_late` undocumented | 12 and 11 (Docs table); it touches only held rows, never `bill_pln` |
| Energy nits | `_commit` encodes sends before storing; the deposit's scope stated in 11; tuples and dates noted for the guard. Not done: `allow_inf_nan=False` in the tariff config and `isfinite(p_max)`: the guard already refuses such a value at `configure`, in the harness, before anything is sent |
| Energy: VERIFY items relied on | D16 (hourly balancing; changes on whole hours stay exact either way) and the IRiESP RCEm deadline stay VERIFY in 09 and 12 |

User answers to the open questions, 2026-11-24: `ems-gen` needs no memory limit; the 60 s window after a kill stays,
checked by T43's recorded statuses; T63 (the skew rerun) stays; the new tasks are numbered T53–T64.

## Complexity Tracking

None. No principle is violated.
