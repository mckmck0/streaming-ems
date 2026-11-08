# Implementation Plan: Simplify the scope

**Branch**: `006-simplify-scope` | **Date**: 2026-11-08 | **Spec**: [spec.md](spec.md)

## Summary

Delete the spec 004 `cluster` profile (compose services, topic profile, Make targets, integration test, settings
value) so one combined KRaft broker remains, as ADR 0010 decided. Record in the docs that this holds in every
environment, that Helm and the LLM strategy are optional behind a gate, and that results are exported from Restate
state. No new ADR.

## Principles Check (PASS)

| # | Touched? | How it is satisfied | Shown by |
|---|---|---|---|
| P1 | yes | spec, plan and tasks approved together on 2026-11-08 (user chose one gate); every change cites a task | review |
| P2–P8 | no | no domain, actor, handler, ingress, timer or battery code | — |
| P9 | no | topic names, keys, partitions and schemas unchanged; `TopicsProfile` is a setting, not wire | `make ci` |
| P10 | yes (docs) | the planned result export is a batch CLI over Restate state | 11, 08 |
| P11 | yes | `EMS_TOPICS_PROFILE=cluster` fails at settings load; `load_plan` lists the allowed profiles | AC-006-03 test |
| P12 | no | no dependency, image or tool change | `make lint` |
| P13 | no | no tracing | — |
| P14, P16 | no | no reported number changes | 11 |
| P15 | yes | README, `docs/context/` 00, 05, 08, 09, 11, 13, ADR index, deploy-checklist skill | review |

## Contract changes (`08-contracts.md`)

None on the wire. `08` "What to build" gains: result Parquet and the run summary are written by an export CLI
from Restate state; no result topic in v1.

## Design

- `infra/docker-compose.yml`: delete the `cluster` block (`kafka-ctl`, `kafka-1` with its YAML anchors,
  `kafka-2`, `kafka-3`). `kafka`, `kafka-init`, `kafka-ui`, `ems-gen` unchanged.
- `Makefile`: drop `cluster-up`, `cluster-down` (targets, `.PHONY`, help) and `cluster` from the `topics` help;
  relabel the commented Helm block as an optional spec.
- `config/topics.yaml`: delete profile `cluster`.
- `src/ems/settings.py`: `TopicsProfile = Literal["live", "sim", "scale"]`.
- `src/ems/ingest/kafka/admin.py`: unchanged; its RF > broker-count guard is generic and keeps its unit test.

## Risks (`09-risks-and-tech-debt.md`)

- K8 (combined KRaft nodes lose the quorum): obsolete with the profile.
- New K9: single broker, RF 1. A broker outage pauses producers and consumers until it restarts; data survives
  restarts on the named volume (restart test in `tests/integration/test_stack.py`). Accepted: broker failover is
  out of thesis scope.
- Results from Restate state: per-step history kept in object state grows with the run. How the export reads
  state and what stays in state is OPEN for the experiment harness spec (recorded in 11).

## Experiment impact

None. The recorded benchmarks ran on the single broker; no result used the profile.

## Test strategy

Unit tier, written first (`tests/unit/test_single_broker.py`):
- AC-006-01: parse `infra/docker-compose.yml` with `ruamel.yaml`; the services whose image is `apache/kafka` are
  exactly `{"kafka"}`, its `KAFKA_PROCESS_ROLES` is `broker,controller`, and no service lists profile `cluster`.
- AC-006-02: for every profile in `config/topics.yaml`, every topic of `load_plan` has RF 1 and
  `min.insync.replicas` "1".
- AC-006-03: `load_plan(config, "cluster")` raises `TopicConfigError` naming the allowed profiles;
  `EmsSettings(topics_profile="cluster")` raises `ValidationError`.
- Remove `tests/integration/test_cluster.py` and the RF 3 assertion in `test_topics_schemas.py`.
- AC-006-04: existing integration suite against `make up`.

## Docs and ADRs

- No new ADR. ADR index: candidate 0009 notes the Helm part is optional.
- `00-overview.md`: status rows (004, 006), backlog split into mandatory and optional with the gate, decision-log
  line, open questions scoped to the optional specs, repo map `helm/ems/` optional.
- `05`: topic profiles, ports, "one broker everywhere", Helm item and Kafka-on-Kubernetes question optional.
- `08`, `11`: results exported from Restate state; OPEN on the export mechanism and state size (VERIFY).
- `09`: K8 obsolete, K9 new. `13`: intro and the removed "3-broker profile" section.
- README: ports, `EMS_TOPICS_PROFILE`, `make cluster-down`.
- `.claude/skills/ems-deploy-checklist/SKILL.md`: Helm is an optional spec.
- History notes: `specs/004-assets-and-cluster/spec.md`, `specs/001-data-pipeline-core/contracts/topics.md`,
  `specs/001-data-pipeline-core/data-model.md`.

## Complexity Tracking

None. Deletion and docs.
