# 00 · Overview: purpose, status, repo map, backlog

## Current consensus

**Purpose.** An energy management system (EMS) for residential prosumers (PV, home battery, grid connection;
Polish market, 15-minute intervals) built on durable stateful actors (Restate Virtual Objects, Python SDK). The
same stream of measurements and prices feeds N battery strategies, each on its own virtual battery, so they can
be compared counterfactually; one live strategy drives the real battery and a Selector actor picks it.
Exactly-once settlement under failures; hundreds of households. Research framing and RQ1–RQ3: brief §3.

**Scope layers** (brief §3): the core (many strategies on one stream, closed feedback loop, entity decomposition
with durable timers, fleet scale), one research contribution (OPEN: which strategies), and zero or one extra.

### Status of every piece (2026-11-08)

| Piece | Status |
|---|---|
| Claude Code harness (`CLAUDE.md`, `.claude/`) | Built |
| Canonical docs | 00, 05, 06, 07, 08, 09, 10, 11, 12, 13 written; 01–04 land with the actor and strategy specs |
| ADRs | 0010–0015 accepted (spec 001), 0016 (spec 005); 0001–0009 are candidates from the brief, not yet written |
| Data pipeline core (spec 001: Kafka stack, contracts v1, `ems-gen`, synthetic household) | Built |
| Live data, record and replay, hybrid mode (spec 002) | Built |
| Scale, background batteries, faults, stats, benchmark (spec 003) | Built |
| Other assets and site types, 3-broker profile (spec 004) | Built; plants removed by spec 005, 3-broker profile by spec 006 |
| Generator narrowed to PV prosumers, contract v1 frozen (spec 005) | Built |
| Simplified scope: one Kafka broker, optional Helm and LLM strategy, results from Restate state (spec 006) | Built |
| Dev environment beyond Kafka, spike reproduction, domain core, actors, sim clock, … | Documented in the brief only |
| `frontend/` | Reserved, out of scope |

### Decisions so far (not yet in an ADR)

- Helm 4 (4.2.4, released 2026-08-13) replaces the brief's Helm 3.
- The Python project is named `ems` (was `masters-thesis`).
- 2026-10-20: data comes first. The data-generator work ("Plan 1") is delivered as specs 001–004 and specs are
  written as we go, one piece at a time. Its decisions are ADRs 0010–0015.
- 2026-11-08: the generator models PV prosumers only (household, commercial, solar farm) and wire contract v1 is
  frozen after a one-time in-place amendment (spec 005, ADR 0016).
- 2026-11-08 (spec 006): keep the scope as small as the thesis allows.
  - One Kafka broker (RF 1) in every environment; broker replication and failover are out of scope (the broker
    itself is ADR 0010).
  - The Helm deployment and the LLM-based strategy (CLM-8B) are optional (gate below). Scale experiments run on
    Docker Compose.
  - The first experiment harness exports results from Restate Virtual Object state to Parquet; no Kafka result
    (egress) topic.

### Repo map (current and planned)

```
CLAUDE.md, .claude/          harness (rules, skills, agents, hooks)
specs/NNN-*/                 feature specs (spec.md, plan.md, tasks.md)
docs/context/                canonical docs (this folder); docs/adr/ decisions; docs/bootstrap/ frozen brief
src/ems/                     contracts/ (wire models), ingest/ (generators), domain/ (battery), actors/,
                             strategies/, forecasting/, optimization/, sim/, experiments/, settings.py
config/, mappings/, schemas/ generator config, live-source mappings, exported JSON Schemas (specs 001–005)
jobs/                        batch CLIs (no HTTP surface)
infra/                       docker-compose.yml (Kafka now, Restate later), Dockerfiles
helm/ems/                    umbrella chart (optional spec)
tests/                       unit/, property/, integration/, e2e/, fault/
templates/                   bootstrap templates; frontend/ reserved
```

## What to build: backlog (titles and order; numbers are assigned in order)

### Mandatory

| Order | Spec | Scope |
|---|---|---|
| 1 | **001 Data pipeline core** | Kafka 4.3 KRaft in `infra/docker-compose.yml`, idempotent topic ensure, wire contracts v1 (`src/ems/contracts`), producer, DLQ, tail/validate, synthetic weather/PV/base load/heat pump/prices for one EMS-managed household, backfill to Kafka |
| 2 | **002 Live data, record and replay** | Mapping engine; Open-Meteo, PSE RCE and day-ahead (csdac) adapters, ENTSO-E A44 behind a flag; record/replay; hybrid mode; calibration job |
| 3 | **003 Scale and faults** | Vectorised multi-location generation, unmanaged background sites with BESS, fault injection, stats, benchmark |
| 4 | **004 Other assets** | Wind, CHP, biogas, biomass, hydro, EV, farm and plant site types; 3-broker Kafka profile (removed by 006) |
| 5 | **005 Narrow generator assets** | Remove wind, CHP, biogas, biomass, hydro and the industrial and plant site types; trim and freeze contract v1 |
| 6 | **006 Simplify scope** | Remove the 3-broker profile (one broker everywhere); Helm and LLM strategy optional; results from Restate state |
| next | Kafka ingestion into Restate | Subscriptions keyed by household, production/consumption join, dedup by event_id (the "Plan 2" input) |
| next | Dev environment and spike reproduction | Restate in compose, test tiers, spike experiments A–C (brief §11, former 001) |
| next | Domain core | Battery model, settlement with RCE/RCEm options, actions, `Policy`, offline reference |
| next | Actor skeleton; sim clock and replay driver | Brief §11 |
| later | Forecasting, MPC and oracle, adaptive Selector, experiment harness (results exported from Restate state) | Brief §11 |

### Optional

Started only when every mandatory piece above is implemented, tested, validated and evaluated (spec 006).

| Spec | Scope |
|---|---|
| Helm deployment | Umbrella chart on kind (multi-node Restate); one Kafka broker |
| LLM-based strategy | CLM-8B (needs a GPU), compared against the mandatory strategies |

## Prerequisites

- None for spec 001.

## Open questions

- OPEN (brief §13): research-contribution strategies, thesis language, CI platform, battery wear model.
- OPEN only for the optional specs: GPU availability (LLM strategy); single-node Kafka on Kubernetes (Strimzi vs
  Redpanda) and Restate Operator vs plain Deployments (Helm).
