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
| 0010 | [Apache Kafka 4.3 KRaft broker](0010-kafka-4-kraft-broker.md) | Accepted |
| 0011 | [External data enters through Kafka](0011-external-data-via-kafka.md) | Accepted |
| 0012 | [Price topic keyed by market](0012-market-keyed-price-topic.md) | Accepted |
| 0013 | [Two measurement topics](0013-two-measurement-topics.md) | Accepted |
| 0014 | [No simulation of EMS-managed batteries](0014-no-simulation-of-ems-managed-batteries.md) | Accepted |
| 0015 | [Wire contract v1 identity and run scope](0015-wire-contract-v1-identity.md) | Accepted |

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
