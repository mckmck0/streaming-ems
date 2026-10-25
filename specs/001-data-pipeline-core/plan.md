# Implementation Plan: Data pipeline core

**Branch**: `001-data-pipeline-core` | **Date**: 2026-10-20 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/001-data-pipeline-core/spec.md`

## Summary

A local Kafka 4.3 (KRaft) stack in `infra/docker-compose.yml` with idempotent topic creation from
`config/topics.yaml`; a versioned wire contract (`src/ems/contracts`, pydantic, exported JSON Schemas); an
`ems-gen` CLI (`src/ems/ingest`) with a Java-compatible confluent-kafka producer, DLQ, tail and validate; and
vectorised, seed-deterministic synthetic data (weather truth and forecast, PV, BDEW base load, heat pump,
RCE-like prices) for the EMS-managed test household, streamed in backfill/accelerated/realtime modes. Facts and
pins: [research.md](research.md).

## Technical Context

**Language/Version**: Python 3.13 (uv project `ems`, `requires-python >=3.13`)

**Primary Dependencies**: core — pydantic 2.13.5, pydantic-settings, ruamel.yaml 0.19.1, tzdata, typer 0.27.2,
structlog 26.1.0 (so the `ems-gen` entry point works in every install); group `ingest` — confluent-kafka 2.15.1,
numpy, scipy, pandas 3, pyarrow, pvlib 0.16.1, demandlib 0.2.2, holidays 0.105, faker 40.40.0, jsonschema 4.26.0
(ingest commands print a clear error when the group is missing); dev — pytest, pytest-asyncio 1.4.0, hypothesis

**Storage**: Kafka topics (named volume); generated catalog as Parquet under `data/` (gitignored)

**Testing**: pytest tiers `unit`, `property` (no infrastructure), `integration` (real Kafka via `make up`);
hypothesis with a fixed CI profile

**Target Platform**: macOS arm64 host (Docker Desktop 29.7, Compose v5.4); generator image linux/arm64 and amd64

**Project Type**: library (contracts) + CLI (generator) + local infrastructure

**Performance Goals**: 2-day backfill for one site < 1 min including validation; one simulated year for one site
generated in < 10 s (keeps statistical tests in the `unit` tier)

**Constraints**: deterministic per (seed, location, time); no wall clock in generated values; records < 900,000
bytes; host ports on 127.0.0.1; broker heap 512 MB

**Scale/Scope**: one EMS-managed household plus small unmanaged fixtures for balance tests; design is
vectorised for later scale (spec 003)

## Principles Check

*GATE: Must pass before Phase 0 research. Re-checked after Phase 1 design: PASS.*

| # | Status | How satisfied in 001 |
|---|---|---|
| P1 | ✅ | spec approved; tasks written; commits cite task ids |
| P2 | ✅ | No change to `src/ems/domain/`. Generator models in `src/ems/ingest/synthetic` are pure functions of explicit seeds and time arrays (same discipline, not the EMS reference) |
| P3 | ✅ | No actor facts. New facts are external inputs (weather, measurements, prices) whose single writer is `ems-gen`; the generator never writes battery or grid facts of EMS-managed sites. 01 (ownership table) is not written in 001; the rows are recorded in 13 and carried into 01 when the actor spec writes it |
| P4, P5, P8 | n/a | No actors or handlers touched |
| P6 | ✅ | Deterministic `event_id` with run scope; idempotent producer; idempotency downstream is the ingestion spec's job (documented in 08) |
| P7 | ✅ | Backfill/accelerated use simulated time; wall clock only paces realtime and fills `ingested_at` |
| P9 | ✅ | Envelope v1.0.0, `.v1` topics; strict producers, tolerant consumers, open schemas checked FULL; golden examples in `schemas/examples/` |
| P10 | ✅ | `ems-gen` is a CLI with no HTTP surface |
| P11 | ✅ | `src/ems/settings.py` (pydantic-settings, `EMS_` prefix); no secrets needed in 001; `env.example` committed |
| P12 | ✅ | All deps under `exclude-newer 2026-10-02`; images tag+digest ≥ 2 weeks old (research R1, R4, R5); `make ci` |
| P13 | ✅ | No tracing added (structlog logs only) |
| P14 | ✅ | Run manifest (config, seed, window, run id, versions, commit) written by every run (AC-001-28) |
| P15 | ✅ | docs/context 05, 06, 08, 09, 12, 13 + ADRs land in this spec (see Docs) |
| P16 | ✅ | `ems-gen run` logs events/s and delivery latency (not an AC in 001; benchmarked in 003) |

No Actor contract section: no actors, handlers, timers or state ownership change.

## Contract changes (08-contracts.md)

New: wire envelope v1.0.0, measurement, weather-forecast and price payloads, topics/keys/headers/record time
([data-model.md](data-model.md), [contracts/topics.md](contracts/topics.md)). Versus Plan 1: added `t`,
`run_id`, origin-source event-id rule, `PL_SDAC` market; `algorithm_id` kept as always-null reserved field.

## Design

### Infrastructure (`infra/`)
- `docker-compose.yml` services: `kafka` (env block from research R1: listeners `PLAINTEXT://:29092`,
  `PLAINTEXT_HOST://:9092`, `CONTROLLER://:9093`; advertised `kafka:29092` / `localhost:9092`; quorum voters
  `1@kafka:9093`; all internal-topic RF/min-ISR 1 incl. share coordinator; auto-create off; `KAFKA_LOG_DIRS`
  `/var/lib/kafka/data`; heap 512 MB; fixed `CLUSTER_ID`; no `env_file`; healthcheck
  `/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka:29092` with `KAFKA_HEAP_OPTS=-Xmx128m`,
  start_period 30 s; mem limit 1 GB), `kafka-init` (ems-gen image, `topics ensure`, depends on kafka healthy),
  `kafka-ui` (read-only, 127.0.0.1:8088, depends on kafka-init completed, healthcheck), `ems-gen` (profile `gen`,
  `env_file: {path: ../.env, required: false}`, mounts `config/` and `data/`). Profile `registry` reserved
  (empty in 001).
- `ems-gen.Dockerfile`: multi-stage, uv 0.12.21 image → python 3.13.15-slim-trixie, `uv sync --frozen --no-dev
  --group ingest`, non-root, entrypoint `ems-gen`.

### Contracts (`src/ems/contracts/`)
`envelope.py` (models, discriminated payloads, `allow_inf_nan=False`, UTC type), `ids.py` (namespace, event_id,
GSRN check digit), `time.py` (canonical formatter, `t`, local-day slot sets via zoneinfo), `units.py`,
`topics.py` (load/validate topics.yaml with allow-listed keys, market → key table, topic prefix,
`route(envelope) -> (topic, key)`, `expected_partition` via pure-Python murmur2 — no Kafka client import),
`schema_export.py` (open schemas, FULL compatibility check, golden examples in `schemas/examples/`).

### Generator (`src/ems/ingest/`)
- `catalog/`: voivodeship table (16 rows: TERYT, bbox from GUGiK PRG, cities, postcode prefixes; provenance
  note), `EnergyProvider`, `sites.yaml` overrides, Parquet export.
- `rng.py`: splitmix64 counter RNG → uniforms/normals over uint64 arrays.
- `clock/`: modes; emission schedule (meters at interval end + delay; forecasts every 3 h + PT1H30M; prices D−1
  13:00–15:00 local, deterministic jitter; initial day D).
- `synthetic/`: `weather.py` (regional latent field + local AR(1); temperature harmonics; kt regime Markov chain
  over time; pvlib SPA numpy + Ineichen with Linke/altitude from the bundled h5; Erbs; cloud from kt; Weibull
  wind via Gaussian copula, shear to 100/120 m; forecast error σ(lead) correlated across steps, clipped),
  `pv.py`, `load.py` (H25/G25 renormalised, holidays, mean-one lognormal noise), `heat_pump.py`, `balance.py`
  (grid only for `control: site` fixtures), `prices.py` (calibration defaults in `config/calibration/prices.yaml`).
- `kafka/`: `sink.py` (Producer config from R3, callbacks, failure counters, flush/purge, exit codes),
  `admin.py` (ensure/describe/delete), `preflight.py`.
- `tools/`: `tail.py`, `validate.py`. `cli.py` (typer). `settings.py` at `src/ems/settings.py`.

### Config
`config/topics.yaml`, `config/generator.yaml` (clock, seed, streams, scale=1, faults off), `config/sites.yaml`
(test site), `config/calibration/{weather,prices}.yaml` (literature defaults).

### Tooling
- `pyproject.toml`: deps and groups above; `[project.scripts] ems-gen = "ems.ingest.cli:app"`; pytest config
  (`--strict-markers`, markers unit/property/integration/e2e/fault, `asyncio_mode=auto`).
- `Makefile`: enable `up`/`down`, `test-unit`, `test-integration` (without `--cov`: coverage belongs to the
  dev-environment spec); `ci: lint type-check test-unit`; remove the exit-5 workaround; `make test` = unit +
  integration. Add block "Spec 001: data pipeline" (targets in [contracts/cli.md](contracts/cli.md)); `make reset`
  asks unless `CONFIRM=yes`. Note for the user: `Bash(make *)` is allow-listed, so `make reset` bypasses the
  `ask` rule on `down -v`; adding `Bash(make reset*)` to `ask` in `.claude/settings.json` is the user's call.
- `.gitignore`: `data/`. `env.example` (no leading dot). `.claude/rules/tests.md`: integration = real services via
  `make up`. `.claude/rules/ingest.md` + CLAUDE.md row. `.claude/settings.json` allow list unchanged.

## Test strategy (by tier)

| Tier | Covers |
|---|---|
| unit | contracts (envelope rules, event_id incl. replay in-scope and ingested_at, `t`, GSRN, DST slot counts 92/96/100, canonical datetime, market→key table), topics.yaml parsing, allow-list and prefix, ensure decision logic with a fake admin, preflight, sink callback/exit logic and DLQ routing with a fake producer (AC-001-13), manifest (AC-001-28), schema staleness + FULL compatibility (v1.0 reader accepts v1.1 additive record) + golden examples, pure murmur2 equals `confluent_kafka.murmur2`, size < 900 kB, routing + murmur2 partition, catalog determinism, managed-site emits no bess/grid, one-year statistics (PV yield, base load ±5 %, negative-price share + seasonality) |
| property | balance closes (unmanaged fixtures), values ≥ 0, PV 0 at night, GHI ≤ 1.1 × clear-sky, forecast σ grows with lead, batch-size independence, DST days |
| integration | all tests use a per-run `EMS_TOPIC_PREFIX` on the shared broker; host ports bound to 127.0.0.1 (`docker compose port`, AC-001-01); ensure creates + second run no-op; profile switch applies time rules; missing-topic write fails (delivery report, `topic.metadata.propagation.max.ms=1000`) and topic stays absent; DLQ record lands with `error` header; data survives `restart kafka`; live rejects +2 h record, sim accepts; preflight refuses; 2-day backfill counts (fresh `run_id`, offsets taken at start, sim profile); kafka-ui healthy |

Tests are written first by `test-engineer`, named after AC ids.

## Docs and ADRs landing with 001

`docs/context/05-deploy-and-infra.md` (compose, pins with digests and dates), `06-data-flows.md` (clock and
emission schedule), `08-contracts.md` (wire contract v1, handoff notes), `09-risks-and-tech-debt.md`
(timestamp/retention, kafka-ui version display, Faker drift, windpowerlib maintenance for 004),
`12-domain-model.md` (time base, `t`, DST, units, RCE semantics incl. the corrected cohort rule: RCE default from
2024-07-01, quarter-hourly from 2025-10-01), new `13-subsystem-ingest.md` (+ README index and
ems-context table). ADRs 0010 Kafka 4.3 KRaft broker; 0011 external data via Kafka push; 0012 market-keyed price
topic; 0013 two measurement topics; 0014 no simulation of EMS-managed batteries; 0015 wire contract v1 identity
and run scope.

## Experiment impact

None: no reported numbers exist yet.

## Project Structure

### Documentation (this feature)

```text
specs/001-data-pipeline-core/
├── spec.md, plan.md, research.md, data-model.md, quickstart.md
├── contracts/ (topics.md, cli.md)
├── checklists/requirements.md
└── tasks.md
```

### Source Code (repository root)

```text
src/ems/
├── settings.py
├── contracts/   envelope.py ids.py time.py units.py topics.py schema_export.py
└── ingest/      cli.py rng.py catalog/ clock/ synthetic/ kafka/ tools/
config/          topics.yaml generator.yaml sites.yaml calibration/
schemas/         *.json (generated)
infra/           docker-compose.yml ems-gen.Dockerfile
tests/
├── unit/        contracts/ ingest/
├── property/
└── integration/
```

**Structure Decision**: single uv project per ADR candidate 0008 and the user's "repo conventions" decision;
contracts are a subpackage the actors will import directly.

## Reviews (2026-10-20)

- `architect-reviewer`: approve with changes. Fixed: tolerant readers + FULL compatibility (P9); replay keeps its id
  only within its run scope (AC-001-10); profile switching via `--apply-config` and per-run topic prefix for test
  isolation; Makefile `ci`/coverage/exit-5; market → key table and hashed form; DLQ, port-binding and manifest
  tests (AC-001-28); `gen-backfill START`; 01 ownership note; spec status; contracts free of Kafka-client imports;
  forecast `issued_at` and run counting defined; `typer` in core deps. Left to the user: `ask` rule for `make reset`.
- `energy-domain-reviewer`: claims confirmed. Fixed: C21 → G25; base load excludes the heat pump (+5–6 MWh/yr);
  routing note "unmanaged sites only"; market identifiers aligned; brief §4 net-billing cohort contradiction recorded
  for 12. Open VERIFY: GUS household consumption; RCE VAT note; net-billing deposit rules (later spec).

## Complexity Tracking

None.
