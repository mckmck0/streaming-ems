# 00 · Overview: purpose, status, repo map, backlog

## Current consensus

**Purpose.** An energy management system (EMS) for residential prosumers (PV, home battery, grid connection;
Polish market, 15-minute intervals) built on durable stateful actors (Restate Virtual Objects, Python SDK). The
same stream of measurements and prices feeds N battery strategies, each on its own virtual battery, so they can
be compared counterfactually; one live strategy drives the real battery and a Selector actor picks it.
Exactly-once settlement under failures; hundreds of households. Research framing and RQ1–RQ3: brief §3.

**Scope layers** (brief §3): the core (many strategies on one stream, closed feedback loop, entity decomposition
with durable timers, fleet scale), one research contribution (OPEN: which strategies), and zero or one extra.

### Status of every piece (2026-11-13)

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
| Realistic generator statistics: run-consistent forecast errors, persistent peaky base load, hybrid truth (spec 007) | Built |
| Historical backfill on real data: fetch once, build offline, replay; window 2026-04-02 … 06-30 fetched (spec 008) | Built |
| Domain core: 2026 tariffs of four DSOs, hourly net-billing settlement with the deposit, price signal, actions, `Policy`, offline reference and stepper (spec 009) | Built |
| Dev environment beyond Kafka, spike reproduction, actors, sim clock, … | Documented in the brief only |
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
- 2026-11-10 (spec 007): realistic generator statistics, no contract change.
  - Hybrid live-site truth is the latest live step minus the synthetic forecast error, behind a truth-source seam
    (`synthetic/truth.py`); satellite radiation comes with the historical-backfill spec.
  - Temperature and wind share the clear-sky index's error structure (shared by valid time plus per run), without
    the timing error; kt errors act in a bounded logit space.
  - Base-load noise has two time scales (fast plus slow AR(1), τ_slow fixed at 3 h), fitted jointly with the
    appliance impulses to the full-load statistics of WPuQ 2019 (German households, CC BY 4.0; Polish
    applicability VERIFY) through a neutral CSV layout. Impulse rates stay at literature values. pvmonitor.pl is
    not used without the operator's written permission.
  - The historical backfill on real data (finding 4) is its own spec.
- 2026-11-12/13 (spec 008): historical backfill on real data, pulled ahead of the Kafka-ingestion spec.
  - Fetch once, replay many: `ems-gen history fetch` stores raw responses with SHA-256 and a manifest;
    `ems-gen history build` builds events offline and byte-deterministically; meters are stored in the recording,
    not recomputed at replay. The `run_id` is chosen at build (default: the recording name).
  - Real: PSE RCE, archived forecast runs (Open-Meteo Single Runs; ICON-D2 inside its domain, DMI HARMONIE east of
    20.34° E, as live), observed truth (DWD MTG satellite radiation, ERA5 temperature and wind). Households stay
    synthetic. Real-data sites are `live.locations`; sites with a site-controlled battery are refused.
  - Forecast availability is one table per `forecast_model` (`ems.contracts.availability`): 120 min for ICON-D2
    and synthetic forecasts (was 90), 195 min for DMI; public availability plus Open-Meteo's 10-minute wait.
  - `source_system` values `open-meteo-single-runs` and `synthetic-history`; prices keep `pse-rce`.
  - `ems-gen replay` runs the topic preflight for every recording; `--background` generates the synthetic fleet
    around a history recording.
  - Not DMI everywhere: at Poznań ICON-D2's GHI error was 11–19 % lower and its 15-minute radiation native
    (spec 008 plan §3).
- 2026-11-13 (spec 009): the domain core, written in parallel with spec 008.
  - Settlement follows the 2026 bill: G11/G12/G12w of Enea, Energa, PGE and Tauron with their incumbent sellers,
    sourced config per version (`config/tariffs/`); hourly vector-balanced net-billing with RCE or RCEm by first
    feed-in and the deposit (1.23, m+1 assignment, energy-only offset, 12 months, refund cap before 1.23).
  - Brief §5.2 changes: `Tariff` stays keyed by market and serves price curves only; per-site tariff rules come
    from config through `ems.tariffs` and the domain functions. `Meter` calls `settle_quarter` only.
  - Cost metric: `bill_pln` primary, `deposit_closing_pln` beside it, `ledger_net_cost_pln` for additive statistics
    (11).
  - The action set gains `SELF_CONSUME` (the self-consumption baseline); the dynamic (day-ahead) tariff is dropped
    for now.

### Repo map (current and planned)

```
CLAUDE.md, .claude/          harness (rules, skills, agents, hooks)
specs/NNN-*/                 feature specs (spec.md, plan.md, tasks.md)
docs/context/                canonical docs (this folder); docs/adr/ decisions; docs/bootstrap/ frozen brief
src/ems/                     contracts/ (wire models), ingest/ (generators), domain/ (battery, tariffs,
                             settlement, actions, reference), tariffs/ (tariff config loader), actors/,
                             strategies/, forecasting/, optimization/, sim/, experiments/, settings.py
config/, mappings/, schemas/ generator and tariff config, live-source mappings, exported JSON Schemas
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
| 7 | **007 Realistic generator data** | Forecast errors shared by consecutive runs with timing errors; two-timescale log noise and appliance impulses in base load, daily factor on the local day, WPuQ fit; hybrid truth deviates from live forecasts (L5) |
| 8 | **008 Historical backfill on real data** | Fetch once, build offline, replay: PSE RCE, archived ICON-D2/DMI runs (Single Runs API), satellite and ERA5 truth behind the spec 007 seam; per-model forecast availability; replay preflight and `--background`; forecast-error candidate. Window 2026-04-02 … 06-30 (spring, no winter) |
| 9 | **009 Domain core** | 2026 purchase tariffs (four DSOs, G11/G12/G12w) and zone calendar, hourly net-billing ledger and monthly bill with the deposit, RCE/RCEm by cohort, price signal, actions and `Policy` with NoBattery and SelfConsumption, offline reference bit-identical to an incremental stepper |
| next | Kafka ingestion into Restate | Subscriptions keyed by household, production/consumption join, dedup by event_id (the "Plan 2" input) |
| next | Dev environment and spike reproduction | Restate in compose, test tiers, spike experiments A–C (brief §11, former 001) |
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
