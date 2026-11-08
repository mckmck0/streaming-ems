# Tasks: Simplify the scope

Spec: [spec.md](spec.md) · Plan: [plan.md](plan.md). Tests come first.

## Phase 1: Failing tests

- [x] T01 [test] [US1] `tests/unit/test_single_broker.py`: one `apache/kafka` service, `kafka`, combined
  broker+controller, no `cluster` profile (AC-006-01); every topic profile gives RF 1 and min ISR "1" (AC-006-02);
  `load_plan(config, "cluster")` → `TopicConfigError` listing the profiles, `EmsSettings(topics_profile="cluster")`
  → `ValidationError` (AC-006-03).
- [x] T02 [test] [US1] Delete `tests/integration/test_cluster.py`; drop the `cluster` RF 3 assertion in
  `tests/unit/contracts/test_topics_schemas.py` (AC-006-03).

## Phase 2: Implementation

- [x] T03 [US1] `infra/docker-compose.yml`: delete `kafka-ctl`, `kafka-1..3`; `Makefile`: delete `cluster-up`,
  `cluster-down` and their help, `cluster` in the `topics` help, relabel the commented Helm block optional;
  `is_local_bootstrap` in `ingest/kafka/admin.py` no longer accepts `kafka-*` hosts (test in `test_admin.py`)
  (AC-006-01).
- [x] T04 [US1] `config/topics.yaml`: delete profile `cluster`; `src/ems/settings.py`: drop `cluster` from
  `TopicsProfile` (AC-006-02, -03).

## Phase 3: Docs

- [x] T05 [US1] Single broker: README (ports, `EMS_TOPICS_PROFILE`, `make cluster-down`), `00` (status rows,
  backlog row 006), `05` (topic profiles, ports, one broker everywhere), `09` (K8 obsolete, K9), `13` (intro,
  remove the 3-broker section); history notes in specs 001 (`contracts/topics.md`, `data-model.md`) and 004
  (AC-006-01…03).
- [x] T06 [US3] Optional pieces and results: `00` (mandatory/optional backlog with the gate, decision log, open
  questions, repo map), `05` (Helm and Kafka on Kubernetes optional), `08` and `11` (results exported from Restate
  state, no result topic; OPEN export mechanism and state size), ADR index row 0009, deploy-checklist skill
  (SC-002).

## Phase 4: Verify

- [x] T07 `make format`, `make ci`; `docker compose -f infra/docker-compose.yml --profile '*' config --services`
  lists only `kafka`, `kafka-init`, `kafka-ui`, `ems-gen`; `make up`, `make test-integration`; SC-001 and SC-002
  greps (AC-006-01…04, SC-001, SC-002).

## Coverage

| Criterion | Tasks |
|---|---|
| AC-006-01 | T01, T03, T05, T07 |
| AC-006-02 | T01, T04, T07 |
| AC-006-03 | T01, T02, T04, T07 |
| AC-006-04 | T07 |
| SC-001 | T05, T07 |
| SC-002 | T06, T07 |
