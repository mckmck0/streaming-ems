---
name: ems-plan-change
description: Plans a change to the EMS system the spec-driven way. Runs the design-principle, contract and risk checklists, then drives Spec Kit (/speckit-specify, /speckit-clarify, /speckit-plan, /speckit-tasks, /speckit-analyze) with a human approval gate after every phase. Use before any new or changed actor, handler, message, contract, strategy, timer or schedule behaviour, or infrastructure change, and whenever the user asks to start, specify or plan a feature or a backlog spec such as 001.
argument-hint: "[feature description or backlog spec number]"
---

# ems-plan-change: plan a change through Spec Kit

Work one phase at a time. After each phase, summarise what was produced, list open questions, and **stop
until the user approves**. Never start implementation: only the user runs `/speckit-implement`.

## 0. Preconditions

1. Exempt? Typo, comment, formatting and behaviour-neutral doc fixes need no spec: do them directly.
2. Spec Kit initialised? Both `.specify/` and `.claude/skills/speckit-specify/SKILL.md` must exist. If
   not, stop and point the user to "Spec Kit setup" in `docs/context/10-planning-guide.md` (pinned
   release, init command, `/reload-skills`). Do not install or initialise it yourself.
3. Constitution current? `.specify/memory/constitution.md` must carry the 16 principles of
   `docs/context/07-design-principles.md`. If it is the empty template or differs, propose
   `/speckit-constitution` with 07 as the input before anything else.

## 1. Ground

- Invoke `ems-context` and read the docs it lists. For a backlog item, take the title, scope and order
  from the backlog in `docs/context/00-overview.md` (numbers are assigned by Spec Kit at specify time).
- Look for prior art: existing `specs/NNN-*`, ADRs in `docs/adr/`, current code.

## 2. Checklists

Run the principle, contract and risk checklists in [checklists.md](checklists.md) and keep the answers:
they feed the spec's constraints and the plan's Constitution Check. If actors are touched, also load
`ems-actor-contract`.

## 3. Specify → stop

Run `/speckit-specify` with: goal, scope and out-of-scope, who or what is affected, acceptance criteria in
EARS form with ids (patterns in [checklists.md](checklists.md)), constraints from the checklists, and open
questions. Afterwards check that every criterion is in EARS form and testable; Spec Kit's template uses
Given/When/Then, so add the EARS form where it is missing. Stop for approval.

## 4. Clarify → stop (only if the spec has open questions)

Run `/speckit-clarify`, fold the answers into the spec, stop.

## 5. Plan → review → stop

Run `/speckit-plan`, passing the stack and pinned versions from `docs/context/05-deploy-and-infra.md`.
The plan must contain:
- a Constitution Check against all 16 principles;
- an **Actor contract** section when actors are touched (template in `ems-actor-contract`);
- contract changes per `docs/context/08-contracts.md` and their versioning;
- experiment impact (does a reported number change?) and the test strategy per tier;
- the `docs/context/` files and ADRs that change with it.

Then delegate a review to `architect-reviewer`, plus `energy-domain-reviewer` when settlement, prices,
net-billing, the battery model or time handling is involved. Fix each finding or record why it stays.
Stop for approval.

## 6. Tasks → analyze → stop

Run `/speckit-tasks`, then `/speckit-analyze`, and resolve every CRITICAL finding (constitution conflicts
are always critical). Each task cites the criterion ids it serves, and test tasks come before the
implementation they test (`test-engineer` writes them). Stop for approval.

## 7. After the user has run `/speckit-implement`

Run `/speckit-converge` until it reports converged, then `make format` and `make ci` (or the tiers that
exist so far). Update `docs/context/` and ADRs, and write the PR description using the contract in
`CLAUDE.md`.
