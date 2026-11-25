# 0021. Strategy changes at run time

- Status: accepted
- Date: 2026-11-25
- Spec: specs/012-clocks-systems-experiments

## Context
A household feeds every released step to the strategies of its run (ADR 0019 stores them in `RunParams`, which
cannot change: `configure` with other parameters fails). Stage 4 must add a strategy from a step `t0` without a
restart, and the user asked for removal as well. Households progress independently, so a change can arrive after
some household has already released its `t0`. Settlement nets per hour, and a strategy's first step must start a
UTC hour (`RunParams.first_step`).

## Decision
- One control call, `Household.set_strategies(t, strategies)` through HTTP ingress with an `Idempotency-Key`
  (`{key}:set_strategies:{t}:{sorted names}`): from step `t` the household feeds exactly that set.
- A change takes effect exactly at `t` or not at all. It is refused, recorded with its reason and counted when
  `t` is not on a UTC hour, already released, at or after `end_step`, not after the last accepted change, names an
  unknown policy, drops the live strategy (switching it is the Selector's, stage 6), or re-adds a strategy removed
  earlier in the run.
- The schedule is data in `Household` state (`schedule.changes`, `schedule.refused`); each released step is routed
  to the set in force at its own `t`, also inside one release batch that spans a change.
- An added strategy starts at `t` on a fresh virtual battery with the run's initial SoC
  (`strategy_params(params, change)`); it drops and counts a correction for a slot before its first step. A removed
  strategy receives nothing from `t` and keeps its state for export.
- The operator (the runner) configures every added object first, waits until each stores parameters equal to
  those sent, then sends the change: in sim while publication is at least one hour of data before `t`, in live with
  `t` the first full UTC hour after the request.
- The offline twin replays the schedule read from `Household` state (`offline.fold(..., schedule)`), so every
  strategy is compared with `==`.

## Consequences
- An added strategy starts at half capacity while the others carry their own SoC, and pays a month's fixed
  charges for a partial month: it is compared per hour of ledger cost from `t`, with its start and end SoC
  exported, and its bill flagged `partial`.
- A removed strategy no longer receives the corrections for slots in `[stop − 2L, stop)`, which ride on later
  steps: its hours are comparable only before `4 · ⌊(stop − 2L) / 4⌋` (`compare_until_step` in the export).
- A badly chosen `t` leaves some households without the change; the refusal shows in their state and in the
  export.
- Docs changed: `01-architecture.md` (handlers, ownership), `02-subsystem-actors.md`, `08-contracts.md`
  (`StrategyChange`), `11-experiments.md` (comparing an added or removed strategy).

## Alternatives considered
- **Defer to the next unreleased step and flag it.** The strategy always joins, but its start differs per
  household and depends on timing, and the strategy would have to learn its start from its first step (a new
  field and a hold-back rule).
- **Fail the call.** The fleet could end half-joined without all-or-nothing coordination.
- **The next quarter instead of the next hour.** Needs a partial first hour in the domain's hourly settlement,
  which the legal rules do not define.
- **Add only, removal with the Selector.** Fewer cases now; removal was wanted to drop a strategy that performs
  badly, and it costs one more refusal reason.
