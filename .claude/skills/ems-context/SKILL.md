---
name: ems-context
description: Maps a task in the EMS thesis repo to the canonical docs in docs/context/ that must be read before planning or writing anything, and states which source wins when they disagree. Use first on every non-trivial task (code, specs, infra, experiments, docs), and whenever it is unclear which doc covers actors, strategies, forecasting, the domain model, contracts, deployment or the experiment protocol.
---

# ems-context: what to read before acting

Read only the docs the task needs, usually one to three, not the whole folder. Each doc separates
**Current consensus**, **What to build**, **Prerequisites** and **Open questions**. Treat anything marked
`OPEN` (undecided) or `VERIFY` (unchecked fact) as not decided, and say so when it matters for the task.

## Task → docs (`docs/context/`)

| The task touches | Read |
|---|---|
| Orientation, status of each piece, repo map, backlog, decisions so far | `00-overview.md` |
| Actors, keys, who owns which fact, the message diagram | `01-architecture.md` |
| Restate services and handlers, R1–R7, registration, Kafka ingress | `02-subsystem-actors.md` |
| Strategies, shadow vs live, Evaluator, Selector | `03-subsystem-strategies-and-evaluation.md` |
| Forecasters, MILP/MPC, the oracle | `04-subsystem-forecasting-and-optimization.md` |
| uv/pyproject, Makefile, pre-commit, Compose, kind, Helm, CI, pinned versions | `05-deploy-and-infra.md` |
| Tick, live loop, replay and sim clock, failure and recovery, timers | `06-data-flows.md` |
| Any plan, review or design decision | `07-design-principles.md` |
| Message schemas, Kafka topics, result schemas, versioning | `08-contracts.md` |
| Known issues, pitfalls, risks and their fix hints | `09-risks-and-tech-debt.md` |
| Planning a change, Spec Kit setup and loop, checklists | `10-planning-guide.md` |
| Experiments, metrics, statistics, spike results | `11-experiments.md` |
| Battery model, units, time base, settlement, net-billing, actions, glossary | `12-domain-model.md` |
| Data generators, site catalog, synthetic models, live sources, `ems-gen`, Kafka topics in use | `13-subsystem-ingest.md` |
| Why a decision was made | `docs/adr/README.md` (index of ADRs and candidates) |

## Which source wins

`CLAUDE.md` and `07-design-principles.md` → the other `docs/context/` files → skills and rules →
`docs/bootstrap/PROJECT_CONTEXT.md` (frozen seed brief, read only to trace where something came from) →
auto memory. The Spec Kit constitution is generated from 07 and must match it. Accepted ADRs explain
decisions; `docs/context/` must agree with them.

When docs disagree with each other or with the code, say so, name the files, and propose which one to fix.
Do not silently follow either.

## Then

Add the surface skill for the work: `ems-plan-change` (any behaviour or infra change),
`ems-actor-contract` (handlers, messages, timers, ownership), `ems-deploy-checklist` (infra and tooling),
or ask the user to run `/ems-experiment` (experiments).
