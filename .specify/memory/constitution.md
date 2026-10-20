# EMS Constitution

Counterfactual multi-strategy energy management on durable stateful actors (MSc thesis).
Source of truth: `docs/context/07-design-principles.md`; this file is generated from it.

## Core Principles

### P1. Spec first
No feature code MUST be written without an approved spec, plan and task list in `specs/NNN-feature/`;
every change MUST trace to a task. Typo, comment, formatting and behaviour-neutral doc fixes are exempt.
Checked by `/speckit-analyze` and by review (each commit cites a task id).

### P2. Pure domain, thin actors
`src/ems/domain/` MUST be deterministic and MUST NOT contain I/O, a clock, a randomness source or a runtime
import. Actors parse, delegate to the domain, write only their own state and send messages.
Checked by the post-edit hook (Restate imports), unit and property tests, and review.

### P3. Single writer per fact
Every fact MUST have exactly one writing actor (ownership table in `docs/context/01-architecture.md`);
other actors keep tagged views. Checked by review against the ownership table.

### P4. No blocking cycles
Exclusive handlers MUST be reached only by one-way messages. Request-response MUST target shared
(read-only) handlers only, and shared handlers call only shared handlers (R1, R2).
Checked by the call graph in each plan's Actor contract section.

### P5. Async-first handlers
Handlers MUST NOT run blocking or CPU-heavy work on the event loop. Non-deterministic or external work MUST be
journaled (`ctx.run`, `ctx.time`, `ctx.random`) and offloaded to a bounded executor (R3).
Checked by ruff `ASYNC` rules and review.

### P6. Exactly-once by construction
Ingress MUST be idempotent, side effects MUST be journaled, and every strategy MUST have an automated
bit-exact comparison against the offline reference. Checked by e2e replay tests (`==` on stored state)
and fault tests.

### P7. Two clocks
Experiments and replay MUST run on simulated time carried by events; wall-clock durable timers are allowed
only in live mode. No sleeping inside exclusive handlers (R5). Checked by review and by sim-mode tests that
use no wall clock.

### P8. Safety at the edge
The Battery actor MUST clamp or reject infeasible commands whatever a strategy asks.
Checked by property tests on the battery model.

### P9. Versioned, additive contracts
Every message and Kafka record MUST carry `schema_version`; within a major version changes MUST be additive.
A breaking change requires a new version (and a new topic), an ADR, coordinated updates and refreshed golden
examples (`docs/context/08-contracts.md`). Checked by schema staleness and contract tests.

### P10. Batch jobs have no HTTP surface
Training, backtests, the oracle and other batch work MUST be CLIs (`jobs/`, `ems-gen` subcommands) over
artefacts. Checked by review.

### P11. No secrets in the repo
Settings MUST come from the environment through `src/ems/settings.py` (pydantic-settings); required settings
fail fast; nothing defaults to a secret. Checked by pre-commit `detect-private-key` and review.

### P12. Fixed, pinned tooling
Python ≥ 3.13, uv, ruff, ty, pytest, pre-commit. Every dependency MUST come from `uv.lock` under
`[tool.uv] exclude-newer`; images and tools by exact tag plus digest, each at least two weeks old.
`make format` after every change; `make ci` green before merge. Checked by `make ci` and `uv lock --check`.

### P13. Observability is all-or-nothing
Tracing MUST be end-to-end or absent; no partial tracing. Checked by review.

### P14. Reproducibility
Every experiment result MUST store config, seed, data window, hardware, software versions and git commit.
Checked by the experiment runner and review.

### P15. Docs and decisions land with the change
A behaviour change MUST update `docs/context/`; a decision change MUST add an ADR in `docs/adr/`, in the same
change. Checked by the PR description contract in `CLAUDE.md`.

### P16. Measure the system, not just the energy
Every experiment MUST report systems metrics (events/s, decision latency p50/p99, recovery latency by phase,
exactly-once check, view staleness) next to energy metrics. Checked by experiment report review.

## Development Constraints

- The actor connection rules R1–R7 (`ems-actor-contract` skill, `docs/context/02-subsystem-actors.md`) apply
  to every actor change.
- `make` is the only entry point for builds, checks and local infrastructure.

## Development Workflow

- `ems-plan-change` drives `/speckit-specify` → `/speckit-clarify` → `/speckit-plan` → `/speckit-tasks` →
  `/speckit-analyze`, with human approval after each phase. Only the user runs `/speckit-implement`;
  `/speckit-converge` follows until converged.
- Acceptance criteria use EARS form with ids (`AC-NNN-MM`); test tasks precede the tasks they test.
- Plans that touch actors contain an Actor contract section; plans are reviewed by `architect-reviewer`, and
  by `energy-domain-reviewer` when prices, settlement, the battery model or time/DST are involved.

## Governance

- `docs/context/07-design-principles.md` is the source of these principles. A principle changes there first;
  this constitution is then regenerated with `/speckit-constitution` and must match it.
- An amendment that changes a decision requires an ADR in `docs/adr/`.
- Precedence when sources disagree: `CLAUDE.md` and 07 → other `docs/context/` files → skills and rules →
  the frozen brief → auto memory. Contradictions are raised, not silently resolved.
- Versioning: MAJOR for removed or redefined principles, MINOR for added principles or materially expanded
  guidance, PATCH for wording fixes. Every plan's Constitution Check covers all 16 principles.

**Version**: 1.0.0 | **Ratified**: 2026-10-20 | **Last Amended**: 2026-10-20
