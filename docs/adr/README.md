# Architecture decision records

One file per decision: `docs/adr/NNNN-short-title.md`. Accepted ADRs are never rewritten; a changed decision gets
a new ADR that supersedes the old one, and `docs/context/` is updated in the same change.

## Index

| ADR | Title | Status |
|---|---|---|
| 0001 | Restate (Python SDK) as the actor runtime | Candidate (brief §2), not yet written |
| 0002 | Fallbacks: Dapr Actors, Temporal; PyFlink only as a baseline | Candidate (brief §2) |
| 0003 | Python ≥ 3.13 and fixed tooling (uv, ruff, ty, pytest, pre-commit) | Candidate (brief §2) |
| 0004 | Pure, runtime-agnostic domain core | Candidate (brief §2) |
| 0005 | Cycles only over one-way messages | Candidate (brief §2) |
| 0006 | Two clocks | Candidate (brief §2) |
| 0007 | Licence boundary (Restate BSL) | Candidate (brief §2) |
| 0008 | One repository with subproject folders | Candidate (brief §2) |
| 0009 | Docker Compose inner loop, Helm on kind | Candidate (brief §2) |

New decisions start at 0010.

## Template

```markdown
# NNNN. Title

- Status: proposed | accepted | superseded by NNNN
- Date: YYYY-MM-DD
- Spec: specs/NNN-feature (if any)

## Context
What forces the decision; constraints; sources (URLs with the date checked).

## Decision
What we do.

## Consequences
What gets easier or harder; what it supersedes; follow-ups and docs changed.

## Alternatives considered
Each with the reason it was not chosen.
```
