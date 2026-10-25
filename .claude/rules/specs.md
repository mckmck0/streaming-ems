---
paths:
  - "specs/**"
---

# Feature specs (`specs/NNN-feature/`)

These files are the contract for a change; `ems-plan-change` drives the phases.

- `spec.md`: acceptance criteria in EARS form ("WHEN <trigger>, THE SYSTEM SHALL <response>"; also
  "WHILE <state>", "IF <condition> THEN"). Each criterion is testable and has an id the tasks can cite.
- `plan.md`: passes a Principles Check against `docs/context/07-design-principles.md`. If the change
  touches actors, it contains an **Actor contract** section (template in the `ems-actor-contract` skill)
  covering actors, keys, state schema, exclusive and shared handlers, messages, timers, failure modes, and
  a check against R1–R7 and the single-writer table.
- `tasks.md`: every task cites the criteria it serves; test tasks come before implementation tasks.
- Phase gates: specify → plan → tasks → implement each need the user's approval. Do not implement until
  the user has approved the task list.
- Behaviour or decision changes discovered while specifying go to `docs/context/` or a new ADR in the same
  change.
