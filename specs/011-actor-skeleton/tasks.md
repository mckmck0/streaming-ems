# Tasks: Actor skeleton

Spec: [spec.md](spec.md) · Plan: [plan.md](plan.md). Spec, plan and this list approved 2026-11-20.
- Tests come first (tasks marked [test], written by `test-engineer`); each implementation task turns its tests
  green. Every phase ends with `make format` and `make ci` green.
- Section numbers (§) refer to the plan's Design.
- Until phase 4 the new services are served beside `Probe` and the state-cost objects, so the spec 010 tiers keep
  passing.

## Phase 1: Domain

- [ ] T01 [test] (AC-011-01) `tests/unit/domain/test_quarter_split.py`: the spec 009 body of `quarter()` kept
  verbatim in the test, compared with `quarter()` for every action kind and both price bases; `prices`,
  `decide`, `apply` and `grid_kwh` composed by hand give the same result.
- [ ] T02 (AC-011-01, -34) `reference.py`: `Prices`, `prices`, `decide`, `apply`, with `quarter()` as their
  composition; `settlement.grid_kwh`; `actions.POLICIES` (§2).
- [ ] T03 [test] (AC-011-02 … -05) `tests/unit/domain/test_corrections.py`: the three cases of `correct` (open
  hour, closed hour, a flow not kept), the lagged close across the October close with opening tranches, lag 0
  equal to `settle_quarter` at every step. `tests/property/test_corrections.py`: rows equal `hour_rows` over the
  final flows, months equal `settle_month` over the final rows, a lag only delays, `correct` never misses a
  flow when corrections keep `t + L + 1 ≤ at_step ≤ t + 2L`, at most `2L + 3` flows kept.
- [ ] T04 (AC-011-02 … -05) `settlement.py`: `Correction`, `Flow`, `add_quarter`, `correct`, `due_month`,
  `close_month`, `settle_quarter` as their lag-0 composition; `stepper.py`: `settle_step`,
  `step(…, recent=(), corrections=(), lag=0)`, `StepOut.recent` (§2).
- [ ] T05 [test] (AC-011-33) `tests/unit/domain/test_calendar.py` compares the holidays with a sorted tuple; a
  test that `National` rejects unsorted or repeated dates.
- [ ] T06 (AC-011-33) `National.holidays` as a sorted `tuple[date, ...]` in `tariff.py`, `calendar.py` and
  `src/ems/tariffs/config.py`.
- [ ] T07 [test] (AC-011-14, -16, -20) `tests/unit/contracts/test_availability.py`: the generator's issue times
  are multiples of the cadence and its publications never precede the window; the window opens at 11:00Z on
  2026-03-29 and 12:00Z on 2026-10-25; unknown models and markets are refused.
- [ ] T08 (AC-011-14, -16) `ems.contracts.availability`: `FORECAST_CADENCE_MIN`, `forecast_cadence`,
  `price_window_opens` (§3).

## Phase 2: Assembler

- [ ] T09 [test] (AC-011-06) `tests/unit/assembler/test_purity.py` (import allow-list: standard library,
  `ems.contracts`, `ems.domain`; no wall clock); `tests/unit/test_post_edit_hook.py` (a Restate import under
  `src/ems/assembler/` is flagged, as under `src/ems/domain/`).
- [ ] T10 (AC-011-06) `src/ems/assembler/__init__.py`; the assembler pattern in `.claude/hooks/post-edit.sh`;
  `.claude/rules/assembler.md` (paths `src/ems/assembler/**`).
- [ ] T11 [test] (AC-011-37) `tests/unit/assembler/test_params.py`: every check of `RunParams.__post_init__`;
  a round trip of a real `RunParams`.
- [ ] T12 (AC-011-37) `assembler/params.py`: `RunParams` (§6).
- [ ] T13 [test] (AC-011-07, -26) `tests/unit/assembler/test_mapping.py`: PV and load sums with `math.fsum`;
  entries skipped by quantity, aggregation, unit and quality; PT1H and EUR curves; values beyond the bounds; an
  unknown model; DST curves. `tests/property/test_ingress_check.py` extended: `parse` and the mapping never
  raise on arbitrary bytes or mutated golden events.
- [ ] T14 (AC-011-07) `assembler/mapping.py`: `parse`, `reading`, `curve`, `run` (§4).
- [ ] T15 [test] (AC-011-08 … -13) `tests/unit/assembler/test_meter_stream.py`: the four late and missing cases
  of 01 at their boundaries, a gap at the start, a jump after a gap, duplicate and conflict, before start and
  after end. `tests/property/test_meter_stream.py`: slots closed once and in order, an on-time reading never
  estimated, `at_step` in `[t + L + 1, t + 2L]`, a duplicate changes nothing, at most `2L` slots, pruning
  changes no output.
- [ ] T16 (AC-011-08 … -13) `assembler/meter_stream.py`: `MeterStream`, `Closed`, `Late`, `meter_apply` (§4).
- [ ] T17 [test] (AC-011-14 … -16) `tests/unit/assembler/test_sparse_streams.py`: visibility and watermarks on
  schedule, a missing run, a late and an unscheduled run, a stale and a duplicate run, an older day, a
  republished day, `clamped` and `revised`.
- [ ] T18 (AC-011-14 … -16) `assembler/sparse_streams.py`: `ForecastStream`, `PriceStream`, `Run`, `Curve`,
  `forecast_apply`, `price_apply` (§4).
- [ ] T19 [test] (AC-011-17 … -22) `tests/unit/assembler/test_join.py`: release only on complete data; step
  content; a site without PV; corrections in order and removed; the price on 92- and 100-slot days; the
  replacement price across both DST days, up to two days back, then 0; pruning. `tests/property/
  test_interleaving.py`: any order-preserving merge of the four streams, any delivery order of updates and any
  subscription point give the steps and the final `Join` of a stream-by-stream fold; prefixes release
  prefixes; no step reads a curve or run not yet visible; nothing raises.
- [ ] T20 (AC-011-17 … -22) `assembler/join.py`: `Join`, `MeterSide`, `Step`, `start`, `on_reading`,
  `on_curves`, `on_run`, the price rule and pruning (§4).
- [ ] T21 [test] (AC-011-23) `tests/unit/assembler/test_offline.py`: a generated clean day inside one civil
  month folded offline releases every step and gives, per strategy, the `RefState` and quarter results of
  `reference.run` over inputs built by a plain per-slot lookup.
- [ ] T22 (AC-011-23) `assembler/offline.py`: `fold` (§4).

## Phase 3: Actors

- [ ] T23 [test] (AC-011-30) `tests/unit/actors/test_reorder.py` and `tests/property/test_reorder.py`: any
  permutation of numbered messages comes out in order, once; early ones held; stale ones counted; `begin`
  releases what is held; the counters are outside `==`.
- [ ] T24 (AC-011-30) `actors/reorder.py`: `Inbox`, `in_order`, `begin` (§5).
- [ ] T25 [test] (AC-011-32, -33) `tests/unit/actors/test_messages.py` (field names of every message and of
  `Curve`, `Run`, `Correction`, `Action`; `schema_version` and `t` on every message, `seq` on updates);
  `tests/unit/actors/test_serde.py` and `tests/property/test_serde.py` (every stored type round-trips with
  bit-identical floats; no set in any stored or sent type, found by walking the type hints);
  `tests/unit/actors/test_byte_determinism.py`, part 1 (values encoded in two subprocesses with different hash
  seeds give identical bytes).
- [ ] T26 (AC-011-32, -33) `actors/messages.py`, `actors/serde.py` (§5; Actor contract, Messages).
- [ ] T27 [test] (AC-011-24, -26 … -31, -33 … -36) `tests/unit/actors/test_transitions.py`: start states; the
  `configure` checks (equal, different, wrong key); `Tariff` without parameters; the snapshot and a known
  subscriber; unusable runs and curves; `Ems` decides `t` only after the acknowledgement of `t − 1`; `Battery`
  clamps and counts; `Meter` settles. `tests/unit/actors/test_byte_determinism.py`, part 2 (a fixed trace of
  transitions gives identical states and sends in both subprocesses). `tests/unit/actors/test_app.py`: the
  eight services and no other, handler kinds, every `Send` target exists, and no `sleep`, `send_delay`,
  `ctx.time`, `ctx.run`, `object_call`, `service_call` or `generic_call` in `src/ems/actors/`.
  `tests/property/test_ingress_check.py` extended: one transition per Kafka-fed actor never raises on mutated
  events. `tests/network.py` and `tests/property/test_network.py`: all transitions as an in-memory network with
  an arbitrary delivery order end with strategy states equal to the offline fold and `Battery` and `Meter`
  equal to SelfConsumption.
- [ ] T28 (AC-011-24, -26 … -31, -34 … -36) `actors/shell.py`; `household.py`, `tariff.py`, `weather.py`,
  `strategy.py`, `ems.py`, `battery.py`, `meter.py`; `app.py` with the eight services beside the stage 2
  objects; `ingress_check.py` gains public `parse`, `log_step` and `lead_step` (§5).

## Phase 4: Switch from the probe to the actors

- [ ] T29 [test] (AC-011-25, -37, -44 … -46) `tests/unit/experiments/test_runs.py` (managed sites of the
  10-household catalog, failures before sending); `tests/unit/test_stack_harness.py` (`wait_run` strict and
  lenient on canned `sys_invocation` rows; `ensure_registered` on canned service lists; grouping of ingress
  logs by service); `tests/unit/infra/test_compose_restate.py` (one target service per container);
  `tests/unit/infra/test_makefile_targets.py` (the handler checks in `register`, the `measure` modes); a test
  that `tests/fixtures/generator.faults.yaml` differs from `config/generator.yaml` only in its three rates;
  `tests/unit/test_ingress_measure_job.py`.
- [ ] T30 (AC-011-37) `src/ems/experiments/runs.py`: `managed_params` (§6).
- [ ] T31 (AC-011-44, -45, -47) `src/ems/experiments/stack.py` as in §7; `tests/stack_fixtures.py`: the helper
  that configures, publishes, waits and returns decoded states with the offline result, and writes the records
  and parameters under `logs/tests/` on a failed comparison.
- [ ] T32 (AC-011-25, -44) `infra/docker-compose.yml` (targets per container); `Makefile` (handler checks in
  `register`, `measure` modes, the stale planned lines removed); `tests/fixtures/generator.faults.yaml` (§8).
- [ ] T33 (AC-011-24, -46) Delete `actors/probe.py` and `actors/statecost.py` and their app entries;
  `jobs/ingress_measure.py`: `skew` and `backlog` on the actors, `state-cost` removed, the manifest records
  `late_slots`, the strategies and the live strategy (§9).
- [ ] T34 [test] (AC-011-47) Re-home the spec 010 stack tests on the actors, with no household configured:
  `test_ingress_delivery.py`, `test_ingress_bad_records.py`, `test_ingress_prepublish.py`,
  `test_stack_lifecycle.py` (the service listed and the object whose state must survive is a `Household`),
  `tests/e2e/test_ingress_backfill.py`, `tests/fault/test_ingress_kills.py`; AC-010 ids kept in the names.

## Phase 5: Stack tiers and runs

- [ ] T35 [test] (AC-011-25 … -29, -31) `tests/integration/test_actors.py`: `configure` twice and with other
  parameters; an internal message at an unconfigured object fails once and the key keeps working; one site ×
  6 h through every edge, compared with the offline fold; the shared handlers answer with the state.
- [ ] T36 [test] (AC-011-38 … -42) `tests/e2e/test_actor_backfill.py`, five runs: clean 2026-10-31 × 30 h
  (October closes); meters first and price first (2026-10-20 × 1 day, ingress containers started one after
  another); faults at `L` = 8 and at `L` = 2 (2026-10-20 × 1 day, the fault fixture). Each asserts the
  offline counters first, then household and strategy states, parity, ingress logs, unmanaged keys under 1 kB,
  `n_at_configure` 0 and no failed invocation.
- [ ] T37 [test] (AC-011-43) `tests/fault/test_actor_kills.py`: 30 households × 2 days, SDK service killed at
  30 % of the records handled, the proof of the window, `wait_run` lenient, then states equal and parity.
- [ ] T38 (AC-011-38 … -47, SC-002) `make up`; confirm on the pinned server the facts marked "task" in the
  plan (registration with `force: true` and whether `Probe` stays listed, the handler 404, the
  `sys_invocation` columns, the status of an interrupted invocation after a SIGKILL); `make test-integration`,
  `make test-e2e`, `make test-fault` green.
- [ ] T39 (AC-011-44, -46) By hand, output kept for the pull request: `make register` on a volume that still
  holds the spec 010 deployment refuses before ingestion; `make measure MODE=skew` (records V5) and
  `MODE=backlog`.

## Phase 6: Docs and pull request

- [ ] T40 [docs] (SC-001) `docs/adr/0018-late-and-missing-inputs.md`, `docs/adr/0019-run-parameters-as-control-messages.md`,
  the index in `docs/adr/README.md`.
- [ ] T41 [docs] (SC-001) `docs/context/01-architecture.md`, `02-subsystem-actors.md`, `08-contracts.md`,
  `12-domain-model.md` as listed in the plan's Docs and ADRs.
- [ ] T42 [docs] (SC-001, SC-003) `docs/context/00-overview.md` (stage 3 done), `05`, `06`, `07`, `09`, `11`,
  `13`; `docs/thesis/kafka-ingress.md`.
- [ ] T43 [docs] (SC-001) `README.md`, `CLAUDE.md`, `.claude/rules/actors.md`, the `ems-actor-contract`,
  `ems-plan-change` and `ems-context` skills.
- [ ] T44 (SC-002) `make format`, `make ci`; the `ems-pr` skill opens the pull request with the tier outputs.
