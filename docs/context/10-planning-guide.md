# 10 · Planning guide

How a change gets from idea to merged code. The entry point is the `ems-plan-change` skill; it runs the
checklists and writes the spec files with a human approval gate after every phase.

## Current consensus

### The loop (per spec)

1. `ems-plan-change`: ground (`ems-context`), run the principle, contract and risk checklists.
2. `spec.md`: goal, scope, out-of-scope, EARS acceptance criteria with ids (`AC-001-03`). **Stop.**
3. Clarify with the user when the spec has open questions. **Stop.**
4. `plan.md`: Principles Check (P1–P16), Actor contract section when actors are touched, contract
   changes per `08-contracts.md`, test strategy per tier, docs and ADRs that change. Reviews by
   `architect-reviewer` (and `energy-domain-reviewer` for prices, settlement, battery, time/DST). **Stop.**
5. `tasks.md`: every task cites criterion ids; test tasks come before the tasks they test. **Stop.**
6. Implement after approval, then check the code against the spec, `make format`, `make ci`,
   docs and ADRs, PR description per `CLAUDE.md`.

### Specs are written as we go

Specs are created one piece at a time, when the work reaches that piece, rather than all up front; the backlog in
`00-overview.md` lists titles and order. A spec may be revised while it is in progress (edit the changed part and
record it).

## What to build

- Nothing further.

## Prerequisites

- `07-design-principles.md`.

## Open questions

- None.
