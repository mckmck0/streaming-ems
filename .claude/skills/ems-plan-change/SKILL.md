---
name: ems-plan-change
description: Plans a change to the EMS system the spec-driven way. Runs the design-principle, contract and risk checklists, then writes spec.md, plan.md and tasks.md in specs/NNN-feature/ with a human approval gate after every phase. Use before any new or changed actor, handler, message, contract, strategy, timer or schedule behaviour, or infrastructure change, and whenever the user asks to start, specify or plan a feature or a backlog spec such as 001.
argument-hint: "[feature description or backlog spec number]"
---

# ems-plan-change: plan a change

Work one phase at a time. After each phase, summarise what was produced, list open questions, and **stop
until the user approves**. Never start implementation before the task list is approved.

## 0. Precondition

Exempt? Typo, comment, formatting and behaviour-neutral doc fixes need no spec: do them directly.

## 1. Ground

- Invoke `ems-context` and read the docs it lists. For a backlog item, take the title, scope and order
  from the backlog in `docs/context/00-overview.md` (numbers are the next free three-digit one).
- Look for prior art: existing `specs/NNN-*`, ADRs in `docs/adr/`, current code.

## 2. Checklists

Run the principle, contract and risk checklists in [checklists.md](checklists.md) and keep the answers:
they feed the spec's constraints and the plan's Principles Check. If actors are touched, also load
`ems-actor-contract`.

## 3. Specify → stop

Create `specs/NNN-slug/spec.md` (next free three-digit number) with: goal, scope and out-of-scope, who or
what is affected, acceptance criteria in EARS form with ids (patterns in [checklists.md](checklists.md)),
constraints from the checklists, and open questions. Check that every criterion is testable. Stop for approval.

## 4. Clarify → stop (only if the spec has open questions)

Ask the user the open questions, fold the answers into the spec, stop.

## 5. Plan → review → stop

Write `plan.md`, using the stack and pinned versions from `docs/context/05-deploy-and-infra.md`.
The plan must contain:
- a Principles Check against all 16 principles;
- an **Actor contract** section when actors are touched (template in `ems-actor-contract`);
- contract changes per `docs/context/08-contracts.md` and their versioning;
- experiment impact (does a reported number change?) and the test strategy per tier;
- the `docs/context/` files and ADRs that change with it.

Then delegate a review to `architect-reviewer`, plus `energy-domain-reviewer` when settlement, prices,
net-billing, the battery model or time handling is involved. Fix each finding or record why it stays.
Stop for approval.

## 6. Tasks → stop

Write `tasks.md`. Each task cites the criterion ids it serves, and test tasks come before the
implementation they test (`test-engineer` writes them). Check that every criterion is covered by a task and
every task traces to a criterion. Stop for approval.

## 7. After implementation

Check the code against the spec, plan and tasks and list anything unbuilt, then `make format` and `make ci`
(or the tiers that exist so far). Update `docs/context/` and ADRs, and write the PR description using the
contract in `CLAUDE.md`.
