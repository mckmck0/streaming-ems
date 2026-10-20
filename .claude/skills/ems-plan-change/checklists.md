# Checklists for ems-plan-change

## Contents

1. Principles (P1–P16)
2. Contracts
3. Risks
4. EARS patterns

## 1. Principles

Source: `docs/context/07-design-principles.md`. For each principle answer: touched? how is it satisfied?
which test or review shows it?

| # | Question |
|---|---|
| P1 | Is there an approved spec, plan and task list, and does each change trace to a task? |
| P2 | Does any domain code gain I/O, a clock, randomness or a runtime import? Do handlers stay thin? |
| P3 | Does every new or changed fact have exactly one writer? Is the ownership table in 01 updated? |
| P4 | Draw the call graph. Is any exclusive handler reached by request-response? Does any wait-for cycle exist? |
| P5 | Is every blocking, CPU-heavy or non-deterministic call inside `ctx.run` and offloaded? |
| P6 | Idempotency keys at ingress? Side effects journaled? A bit-exact replay test against the offline reference? |
| P7 | Is sim-mode logic free of wall-clock timers? Do live-mode timers use delayed messages? |
| P8 | Does the Battery actor still clamp or reject infeasible commands? |
| P9 | Are contract changes versioned and additive? (section 2) |
| P10 | Do new batch jobs (training, backtests, oracle) stay CLIs without an HTTP surface? |
| P11 | Are new settings in `settings.py` (pydantic-settings), required ones failing fast, with no secret defaults? |
| P12 | Are new dependencies locked in `uv.lock` under the exclude-newer cutoff, and images or tools pinned by tag and digest, each at least two weeks old? |
| P13 | Is observability all-or-nothing (no partial tracing)? |
| P14 | Does every experiment result store config, seed, data window, hardware and git commit? |
| P15 | Do the `docs/context/` updates and ADRs land in the same change? |
| P16 | Are systems metrics reported next to energy metrics? |

## 2. Contracts

Source: `docs/context/08-contracts.md`.

- Which contracts change: measurement event, tick, step result, ranking, selection, command, SoC ack,
  energy flows, per-step result Parquet, run summary, strategy registry entry?
- Within a major version changes are additive only. Renaming or removing a field means a new version (and
  a new Kafka topic), an ADR, coordinated updates of all producers and consumers, and refreshed golden
  examples in the replay tests.
- Every message and Kafka record carries `schema_version`, `t` and a sim timestamp; the ingress
  idempotency key follows the documented format.
- Kafka record key = household id, so per-household order holds within a partition.

## 3. Risks

Source: `docs/context/09-risks-and-tech-debt.md`.

- List every known issue in scope. For each: fixed in this change, or "accepted for now" with the reason.
- Add newly discovered risks to 09 in the same change.

## 4. EARS patterns

Give every criterion an id such as `AC-001-03` (spec 001, criterion 3).

| Pattern | Template |
|---|---|
| Ubiquitous | THE SYSTEM SHALL `<response>`. |
| Event-driven | WHEN `<trigger>`, THE SYSTEM SHALL `<response>`. |
| State-driven | WHILE `<state>`, THE SYSTEM SHALL `<response>`. |
| Unwanted behaviour | IF `<condition>`, THEN THE SYSTEM SHALL `<response>`. |
| Optional feature | WHERE `<feature is included>`, THE SYSTEM SHALL `<response>`. |

Example: "WHEN experiment B kills the SDK service and the Restate server with `kill -9` mid-replay, THE
SYSTEM SHALL report 0 mismatches and 0 duplicates against the offline reference."
