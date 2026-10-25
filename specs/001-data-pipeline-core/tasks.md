# Tasks: Data pipeline core

**Input**: `specs/001-data-pipeline-core/` (spec, plan, research, data-model, contracts, quickstart)

Tests are required (P1/P6; `.claude/rules/tests.md`): test tasks precede the tasks they test.
Format: `[ID] [P?] [Story] description (ACs)`.

## Phase 1: Setup

- [x] T001 Add dependencies to `pyproject.toml` (core: pydantic, pydantic-settings, ruamel.yaml, tzdata, typer, structlog; group `ingest`: confluent-kafka, numpy, scipy, pandas, pyarrow, pvlib, demandlib, holidays, faker==40.40.0, jsonschema; dev: pytest-asyncio, hypothesis), `[project.scripts] ems-gen`, pytest config (strict markers, tiers, `asyncio_mode=auto`); `uv lock` under the cutoff
- [x] T002 [P] Package skeleton `src/ems/{settings.py,contracts/,ingest/}`, `tests/{unit,property,integration}/conftest.py` with hypothesis CI profile; `.gitignore` adds `data/`; `env.example`
- [x] T003 [P] Makefile: enable `up`/`down`/`test-unit`/`test-integration` (no `--cov`), `ci: lint type-check test-unit`, remove exit-5 workaround, add block "Spec 001: data pipeline"

## Phase 2: Foundational (contracts)

- [x] T004 [P] Unit tests for time helpers: canonical formatter, `t`, local-day slot sets 92/96/100 (AC-001-08, -25) in `tests/unit/contracts/test_time.py`
- [x] T005 [P] Unit tests for ids: event_id determinism, ingested_at excluded, replay in scope keeps id, run_id changes id, GSRN check digit (AC-001-09, -10, -18) in `tests/unit/contracts/test_ids.py`
- [x] T006 [P] Unit tests for envelope validation rules, NaN rejection, payload discrimination, golden examples (AC-001-08) in `tests/unit/contracts/test_envelope.py`
- [x] T007 Implement `src/ems/contracts/time.py`, `ids.py`, `units.py`
- [x] T008 Implement `src/ems/contracts/envelope.py` (models, payloads, validators) and `schemas/examples/*.json`
- [x] T009 [P] Unit tests for topics.yaml parsing, allow-list, profiles, prefix, market→key table, routing, pure murmur2 vs `confluent_kafka.murmur2` (AC-001-06, -11) in `tests/unit/contracts/test_topics.py`
- [x] T010 Implement `config/topics.yaml` and `src/ems/contracts/topics.py`
- [x] T011 [P] Unit tests for schema export staleness and FULL compatibility, size limit (AC-001-15, -17) in `tests/unit/contracts/test_schemas.py`
- [x] T012 Implement `src/ems/contracts/schema_export.py`, generate `schemas/*.json`
- [x] T013 Implement `src/ems/settings.py` (pydantic-settings, `EMS_` prefix)

## Phase 3: US1 — local stack and topics (P1) 🎯

- [x] T014 [P] [US1] Unit tests for ensure decision logic with a fake admin (create, ok, fewer → fail, increase flag, more → warn, drift diff/apply, RF > brokers) (AC-001-05) in `tests/unit/ingest/test_admin.py`
- [x] T015 [US1] Implement `src/ems/ingest/kafka/admin.py` (ensure/describe/delete) and CLI `topics` commands in `src/ems/ingest/cli.py`
- [x] T016 [US1] `infra/ems-gen.Dockerfile` and `infra/docker-compose.yml` (kafka, kafka-init, kafka-ui, `gen` profile)
- [x] T017 [US1] Integration tests: stack healthy and ports on 127.0.0.1, ensure idempotent, missing-topic write fails, restart keeps data, live rejects +2 h / sim accepts (AC-001-01…04, -06) in `tests/integration/test_stack.py`

## Phase 4: US2 — contract-valid publishing (P1)

- [x] T018 [P] [US2] Unit tests for the sink with a fake producer: config, headers, timestamp, callbacks, failure counters, exit codes, DLQ routing (AC-001-12, -13, -14) in `tests/unit/ingest/test_sink.py`
- [x] T019 [P] [US2] Unit tests for preflight (AC-001-07) in `tests/unit/ingest/test_preflight.py`
- [x] T020 [US2] Implement `src/ems/ingest/kafka/sink.py` and `preflight.py`
- [x] T021 [US2] Implement `src/ems/ingest/tools/{tail,validate}.py` and CLI commands `tail`, `validate`, `schemas export`
- [x] T022 [US2] Integration test: DLQ record with `error` header, validate reports clean topics (AC-001-13, -16) in `tests/integration/test_publish.py`

## Phase 5: US3 — two realistic days for the test household (P1)

- [x] T023 [P] [US3] Unit tests for the counter RNG and batch-size independence (AC-001-26) in `tests/unit/ingest/test_rng.py`
- [x] T024 [P] [US3] Unit tests for the catalog: determinism, GSRN validity, tariff rule, sites.yaml override (AC-001-18) in `tests/unit/ingest/test_catalog.py`
- [x] T025 [US3] Implement `src/ems/ingest/rng.py`, `catalog/` (voivodeship table, provider, override, Parquet), `config/sites.yaml`
- [x] T026 [P] [US3] Property tests for weather and PV: GHI bounds, PV zero at night, forecast σ grows with lead (AC-001-20, -21) in `tests/property/test_weather_pv.py`
- [x] T027 [US3] Implement `src/ems/ingest/synthetic/{solar,weather,pv}.py` and `config/calibration/weather.yaml`
- [x] T028 [US3] Implement `src/ems/ingest/synthetic/{load,heat_pump,balance}.py`
- [x] T029 [US3] Implement `src/ems/ingest/synthetic/prices.py` and `config/calibration/prices.yaml`
- [x] T030 [P] [US3] Property and unit tests for balance (unmanaged fixtures), non-negative values, managed site emits no bess/grid (AC-001-19) in `tests/property/test_balance.py`
- [x] T031 [US3] Implement `src/ems/ingest/clock/` (modes, emission schedule) and `src/ems/ingest/generator.py` (envelopes per stream, manifest)
- [x] T032 [P] [US3] Unit tests for the schedule and backfill counts offline: 192/192/16/3 for 2026-10-20…22, manifest (AC-001-27, -28) in `tests/unit/ingest/test_generator.py`
- [x] T033 [US3] CLI `run` and `catalog`; `config/generator.yaml`
- [x] T034 [US3] Integration test: 2-day backfill to Kafka with a fresh run id and topic prefix; counts, one partition per key, monotonic, valid (AC-001-27) in `tests/integration/test_backfill.py`

## Phase 6: US4 — realism (P2)

- [x] T035 [US4] Unit tests over one simulated year: PV 850–1150 kWh/kWp, base load ±5 %, negative-price share 1–8 % with seasonality (AC-001-22…24) in `tests/unit/ingest/test_realism.py`; calibrate defaults until green

## Phase 7: Polish

- [x] T036 [P] Docs: `docs/context/05`, `06`, `08`, `09`, `12`, new `13-subsystem-ingest.md`, README index + ems-context table; ADRs 0010–0015; `.claude/rules/ingest.md` + CLAUDE.md row; `.claude/rules/tests.md` integration wording
- [x] T037 `make format`, `make ci`, `make up`, `make test-integration`, quickstart run; mark tasks done

## Phase 8: Convergence

- [x] T038 Make `make gen-fast` apply the sim profile before the accelerated run per contracts/cli.md gen-fast (contradicts)
- [x] T039 Smoke-test `make gen` (compose `ems-gen`, realtime) and make the run manifest writable from the container per plan: ems-gen compose service (partial)
- [x] T040 Add a follow mode to `ems-gen tail` so it shows new records per FR-012 (partial)
