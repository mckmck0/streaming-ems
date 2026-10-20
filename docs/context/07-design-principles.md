# 07 · Design principles (the constitution source)

These 16 invariants govern every spec, plan, task and review. `CLAUDE.md` and this file sit together at the
top of the precedence order (see [README.md](README.md)). The Spec Kit constitution
(`.specify/memory/constitution.md`) is generated from this file with `/speckit-constitution` and must match it;
change a principle here first, then regenerate the constitution.

## Current consensus

Source: brief §10 (`docs/bootstrap/PROJECT_CONTEXT.md`), with the wording of `CLAUDE.md`. Each principle names how
it is checked; the per-plan questions are in `.claude/skills/ems-plan-change/checklists.md` (P1–P16).

| # | Principle | Rule | How it is checked |
|---|---|---|---|
| P1 | **Spec first** | No feature code without an approved spec, plan and task list in `specs/NNN-feature/`; every change traces to a task. Typo, comment, formatting and behaviour-neutral doc fixes are exempt | `/speckit-analyze`; review: each commit cites a task id |
| P2 | **Pure domain, thin actors** | `src/ems/domain/` is deterministic and has no I/O, clock, randomness source or runtime import. Actors parse, delegate to the domain, write only their own state, and send messages | Post-edit hook (Restate imports); unit and property tests; review |
| P3 | **Single writer per fact** | Every fact has exactly one writing actor (ownership table in `01-architecture.md`); others keep tagged views | Review against the ownership table |
| P4 | **No blocking cycles** | Exclusive handlers are reached only by one-way messages. Request-response targets shared (read-only) handlers only, and shared handlers call only shared handlers (R1, R2). This handler-level wording clarifies the brief's "every edge that can be part of a cycle" | Call graph in the plan's Actor contract section |
| P5 | **Async-first handlers** | No blocking or CPU-heavy work on the event loop. Non-deterministic or external work is journaled (`ctx.run`, `ctx.time`, `ctx.random`) and offloaded to a bounded executor (R3) | ruff `ASYNC` rules; review |
| P6 | **Exactly-once by construction** | Idempotent ingress, journaled side effects, and an automated bit-exact comparison against the offline reference for every strategy | e2e replay tests (`==` on stored state); fault tests |
| P7 | **Two clocks** | Experiments and replay run on simulated time carried by events; wall-clock durable timers only in live mode. No sleeping inside exclusive handlers (R5) | Review; sim-mode tests without wall clock |
| P8 | **Safety at the edge** | The Battery actor clamps or rejects infeasible commands whatever a strategy asks | Property tests on the battery model |
| P9 | **Versioned, additive contracts** | Every message and Kafka record carries `schema_version`; within a major version changes are additive only. A breaking change means a new version (and a new topic), an ADR, coordinated updates and refreshed golden examples (`08-contracts.md`) | Schema staleness and contract tests |
| P10 | **Batch jobs have no HTTP surface** | Training, backtests, the oracle and other batch work are CLIs (`jobs/`, `ems-gen` subcommands) over artefacts | Review |
| P11 | **No secrets in the repo** | Settings come from the environment through `src/ems/settings.py` (pydantic-settings); required ones fail fast; nothing defaults to a secret | pre-commit `detect-private-key`; review |
| P12 | **Fixed, pinned tooling** | Python ≥ 3.13, uv, ruff, ty, pytest, pre-commit. Every dependency comes from `uv.lock` under `[tool.uv] exclude-newer`; images and tools by exact tag plus digest, each at least two weeks old. `make format` after every change; `make ci` green before merge | `make ci`; `uv lock --check` |
| P13 | **Observability is all-or-nothing** | End-to-end tracing or none; no partial tracing | Review |
| P14 | **Reproducibility** | Every experiment result stores config, seed, data window, hardware, software versions and git commit | Experiment runner; review |
| P15 | **Docs and decisions land with the change** | Behaviour change → `docs/context/`; decision change → new ADR in `docs/adr/` | PR description contract (`CLAUDE.md`) |
| P16 | **Measure the system, not just the energy** | Systems metrics (events/s, decision latency p50/p99, recovery latency by phase, exactly-once check, view staleness) next to energy metrics in every experiment | Experiment report review |

The actor connection rules R1–R7 are summarised in the `ems-actor-contract` skill and will be specified in full
in `02-subsystem-actors.md`.

## What to build

- The constitution, generated from this table with `/speckit-constitution` (done once, then on every change here).

## Prerequisites

- Spec Kit initialised (v1.1.0, see `10-planning-guide.md`).

## Open questions

- None for the principles themselves. Their application to the data generator (P6 idempotency keys scoped by
  run, P7 clock modes) is decided in spec 001 and recorded in `08-contracts.md` and an ADR.
