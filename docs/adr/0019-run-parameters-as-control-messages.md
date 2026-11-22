# 0019. Run parameters arrive as control messages

- Status: accepted
- Date: 2026-11-20
- Spec: specs/011-actor-skeleton

## Context
Every object of an EMS-managed household needs the parameters of its run: the first and last step, the late
window `L`, the strategy list and the live strategy, the resolved site model (tariff, battery, cohort), the RCEm
table and the initial SoC. `01-architecture.md` requires them to be resolved once per object and kept in its
state, so a config edit or a later deployment cannot change a running household (P14), and left open how the
actor service obtains them. Handlers must stay deterministic and make no hidden external calls (R3). Sites the
EMS does not manage publish on the same topics and must cost O(1) state.

## Decision
- Before data flows, the operator (the test harness now, the experiment runner in stage 4) sends one
  `configure` call to each object of a managed household (`Household`, `Weather`, `Ems`, `Battery`, `Meter` and
  each strategy) through the HTTP ingress, with an `Idempotency-Key`. The body is one `RunParams` record, built
  by `ems.experiments.runs.managed_params`; the same records feed the offline side.
- An object stores the parameters once. An equal second call changes nothing; other parameters, or parameters
  for another key, fail the call without a retry.
- `managed_params` checks everything that could make a handler raise later before anything is sent: the site can
  be settled, the parameters are consistent, and every step of the window has a tariff version and RCEm entry.
- An object that was never configured only counts what it receives: that is how unmanaged sites stay O(1).
  `Tariff` is keyed by market and needs no parameters.

## Consequences
- Parameters are ordinary journaled messages: replay-safe, visible in Restate's introspection, and the same
  mechanism stage 4 uses to add a strategy at run time.
- A run must be configured before it is published; each object stores how many records it had received before
  `configure` (`n_at_configure`), and the harness checks that it is 0.
- Seven calls per managed household; negligible next to the records of a run.
- Docs changed: `01-architecture.md` (ownership row for run parameters), `02-subsystem-actors.md`,
  `08-contracts.md` (`RunParams`).

## Alternatives considered
- **A run file read by the service** (journaled with `ctx.run` on first use). No control calls, and the file
  doubles as a manifest. Not chosen: file I/O inside handlers is a hidden input, the harness and the service
  must share a filesystem, and an edited file would give objects of one run different parameters.
- **A `Run` configuration actor** read through a shared handler on first use. One write per run. Not chosen: a
  new actor, request-response on the step path and one hot key.
