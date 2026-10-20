# 00 · Overview: purpose, status, repo map, backlog

## Current consensus

**Purpose.** An energy management system (EMS) for residential prosumers (PV, home battery, grid connection;
Polish market, 15-minute intervals) built on durable stateful actors (Restate Virtual Objects, Python SDK). The
same stream of measurements and prices feeds N battery strategies, each on its own virtual battery, so they can
be compared counterfactually; one live strategy drives the real battery and a Selector actor picks it.
Exactly-once settlement under failures; hundreds of households. Research framing and RQ1–RQ3: brief §3.

**Scope layers** (brief §3): the core (many strategies on one stream, closed feedback loop, entity decomposition
with durable timers, fleet scale), one research contribution (OPEN: which strategies), and zero or one extra.

### Status of every piece (2026-10-20)

| Piece | Status |
|---|---|
| Claude Code harness (`CLAUDE.md`, `.claude/`) | Built |
| Spec Kit v1.1.0 (`.specify/`, `.claude/skills/speckit-*`) | Built; constitution v1.0.0 generated from 07 |
| Canonical docs | 00, 07, 10 written; the others are written with the spec that needs them (see README index) |
| ADRs | Index and template in `docs/adr/README.md`; 0001–0009 are candidates from the brief, not yet written |
| Data pipeline (Kafka, contracts, generators) | Planned: specs 001–004 below |
| Dev environment beyond Kafka, spike reproduction, domain core, actors, sim clock, … | Documented in the brief only |
| `frontend/` | Reserved, out of scope |

### Decisions so far (not yet in an ADR)

- Helm 4 (4.2.4, released 2026-08-13) replaces the brief's Helm 3.
- The Python project is named `ems` (was `masters-thesis`).
- 2026-10-20: data comes first. The data-generator work ("Plan 1") is delivered as specs 001–004 and specs are
  written as we go, one piece at a time. Decisions taken for it (Kafka push for external data, market-keyed price
  topic, two measurement topics, the generator never simulating EMS-managed batteries, Apache Kafka 4.3 as the
  broker) become ADRs in spec 001.

### Repo map (current and planned)

```
CLAUDE.md, .claude/          harness (rules, skills, agents, hooks)
.specify/, specs/NNN-*/      Spec Kit workspace and feature specs
docs/context/                canonical docs (this folder); docs/adr/ decisions; docs/bootstrap/ frozen brief
src/ems/                     contracts/ (wire models, planned), ingest/ (generators, planned), domain/, actors/,
                             strategies/, forecasting/, optimization/, sim/, experiments/, settings.py
config/, mappings/, schemas/ generator config, live-source mappings, exported JSON Schemas (planned, spec 001/002)
jobs/                        batch CLIs (no HTTP surface)
infra/                       docker-compose.yml (Kafka now, Restate later), Dockerfiles
helm/ems/                    umbrella chart (later)
tests/                       unit/, property/, integration/, e2e/, fault/
templates/                   bootstrap templates; frontend/ reserved
```

## What to build: backlog (titles and order; numbers are assigned by Spec Kit)

| Order | Spec | Scope |
|---|---|---|
| 1 | **001 Data pipeline core** | Kafka 4.3 KRaft in `infra/docker-compose.yml`, idempotent topic ensure, wire contracts v1 (`src/ems/contracts`), producer, DLQ, tail/validate, synthetic weather/PV/base load/heat pump/prices for one EMS-managed household, backfill to Kafka |
| 2 | **002 Live data, record and replay** | Mapping engine; Open-Meteo, PSE RCE and day-ahead (csdac) adapters, ENTSO-E A44 behind a flag; record/replay; hybrid mode; calibration job |
| 3 | **003 Scale and faults** | Vectorised multi-location generation, unmanaged background sites with BESS, fault injection, stats, benchmark |
| 4 | **004 Other assets** | Wind, CHP, biogas, biomass, hydro, EV, farm and plant site types; 3-broker Kafka profile |
| next | Kafka ingestion into Restate | Subscriptions keyed by household, production/consumption join, dedup by event_id (the "Plan 2" input) |
| next | Dev environment and spike reproduction | Restate in compose, test tiers, spike experiments A–C (brief §11, former 001) |
| next | Domain core | Battery model, settlement with RCE/RCEm options, actions, `Policy`, offline reference |
| next | Actor skeleton; sim clock and replay driver | Brief §11 |
| later | Forecasting, MPC and oracle, adaptive Selector, experiment harness, Helm deployment, optional CLM-8B strategy | Brief §11 |

## Prerequisites

- None for spec 001.

## Open questions

- OPEN (brief §13): research-contribution strategies, GPU availability, thesis language, CI platform, battery wear
  model, Kafka on Kubernetes (Strimzi vs Redpanda), Restate Operator vs plain Deployments.
