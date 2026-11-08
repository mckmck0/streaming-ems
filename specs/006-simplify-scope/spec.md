# Feature Specification: Simplify the scope

**Feature Branch**: `006-simplify-scope` · **Created**: 2026-11-08 · **Status**: Approved 2026-11-08

**Input**: User decision (2026-11-08): the 3-broker Kafka profile adds complexity the thesis does not need, and
every decision has to be defended, so keep things simple. Replace it with the single broker everywhere; make the
Helm deployment and LLM-based strategies optional; export the first experiment results from Restate state instead
of building a Kafka egress topic.

## Goal

Keep only what the thesis needs. Three decisions, none of them a new ADR (ADR 0010 already decides the broker):

1. **One Kafka broker, RF 1, in every environment** (Compose now, any later Kubernetes deployment). Broker
   replication and failover are out of thesis scope: the fault experiments target Restate and the SDK service.
2. **Helm deployment and the LLM-based strategy (CLM-8B) are optional.** They start only when every mandatory piece
   is implemented, tested, validated and evaluated. Scale experiments run on Docker Compose.
3. **Results come from Restate state.** The first experiment harness reads Virtual Object state after a run and
   writes Parquet; no Kafka result (egress) topic is built.

## Scope

In scope:
- Remove the compose services `kafka-ctl`, `kafka-1`, `kafka-2`, `kafka-3` (profile `cluster`), the `cluster` topic
  profile, `make cluster-up` / `cluster-down`, the AC-004-09 integration test and the `cluster` value of
  `EMS_TOPICS_PROFILE`.
- Docs: README, `docs/context/` 00, 05, 08, 09, 11, 13, the ADR index (candidate 0009), the deploy-checklist
  skill, and history notes in specs 001 and 004.

Out of scope (stays):
- The `kafka` service and its configuration; the `live`, `sim` and `scale` topic profiles. (The `topics delete`
  guard stops treating any `kafka-*` host as local, since no such compose host remains: T03.)
- `replication_factor: 1` and `min.insync.replicas: "1"` defaults in `config/topics.yaml` (Kafka needs an RF at
  topic creation) and the generic guard in `src/ems/ingest/kafka/admin.py` that refuses an RF above the broker count.
- ADR 0010 (not rewritten); the commented Helm targets in the Makefile (relabelled optional, not removed);
  `docs/bootstrap/` (frozen).

## Affected

- Compose, Makefile, `config/topics.yaml`, `src/ems/settings.py`, tests. No wire contract change: topic names,
  keys and partition counts are unchanged.
- Anyone who set `EMS_TOPICS_PROFILE=cluster`: it now fails at settings load.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - One broker (Priority: P1)
The local stack has one Kafka broker and every topic profile fits it. **Acceptance**: AC-006-01, -02, -03.

### User Story 2 - Nothing else changes (Priority: P1)
`make up` and the existing integration tests behave as before. **Acceptance**: AC-006-04.

### User Story 3 - A smaller mandatory scope (Priority: P1)
The backlog shows which pieces are mandatory and which are optional, and how results leave Restate.
**Acceptance**: SC-002.

### Edge Cases
- A shell that still exports `EMS_TOPICS_PROFILE=cluster` fails fast with the allowed values.
- Containers left by an earlier `make cluster-up` become compose orphans; remove them once with
  `docker compose -f infra/docker-compose.yml up -d --remove-orphans` (they have no volumes).

## Requirements *(mandatory)*

- **FR-001**: The compose file MUST define one Kafka broker (combined broker+controller). → AC-006-01
- **FR-002**: Every topic profile MUST fit one broker (RF 1, min ISR 1). → AC-006-02
- **FR-003**: The `cluster` topic profile MUST be refused. → AC-006-03
- **FR-004**: The single-node stack MUST behave as before. → AC-006-04
- **FR-005**: The docs MUST mark Helm and the LLM strategy optional and state that results come from Restate
  state. → SC-002

### Acceptance criteria (EARS)
- **AC-006-01**: THE SYSTEM SHALL define exactly one Kafka broker service (`kafka`, combined broker+controller) in `infra/docker-compose.yml` and no service in a `cluster` profile.
- **AC-006-02**: WHEN a topic plan is built for any profile in `config/topics.yaml`, THE SYSTEM SHALL give every topic replication factor 1 and `min.insync.replicas` "1".
- **AC-006-03**: IF `cluster` is requested as the topic profile (`EMS_TOPICS_PROFILE` or `load_plan`), THEN THE SYSTEM SHALL refuse it with a settings `ValidationError` or a `TopicConfigError` that lists the allowed profiles.
- **AC-006-04**: WHEN `make up` starts the stack, THE SYSTEM SHALL pass the existing integration tests unchanged.

## Constraints (from the checklists)

- **P1**: this spec, its plan and tasks are approved before code (one gate, user decision 2026-11-08).
- **P2–P8, P13**: no domain, actor, handler, timer or tracing code is touched.
- **P9**: no wire contract change; only a settings value is removed.
- **P10**: the future result export is a batch CLI over Restate state, with no HTTP surface of its own.
- **P11**: an unknown topic profile fails fast (AC-006-03).
- **P12**: no new or bumped pins; the `apache/kafka` digest just loses three uses.
- **P14, P16**: no reported number changes (the benchmark ran on the single broker).
- **P15**: docs land with the change; no ADR changes (ADR 0010 stays; decisions go to 00 "Decisions so far").
- **Risks** (`09`): K8 becomes obsolete; new K9 (single broker, no failover) is accepted.

## Success Criteria
- **SC-001**: `make ci` is green; outside `specs/` and the AC-006-03 test, nothing mentions
  `cluster-up`, `cluster-down`, `kafka-ctl`, `kafka-1..3`, ports 19092–19094 or a `cluster` profile.
- **SC-002** (review): `docs/context/00-overview.md` splits the backlog into mandatory and optional with the gate
  stated; Helm and the LLM strategy appear only as optional; 05, 08 and 11 say results are exported from Restate
  state with no result topic; no doc in `docs/context/` or the skills treats Helm or CLM-8B as mandatory.

## Assumptions
- No experiment result depends on the `cluster` profile.

## Open questions
- None. How the export reads Restate state is decided in the experiment harness spec (recorded OPEN in 11).
