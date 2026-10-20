# 05 · Deploy and infrastructure

## Current consensus

### uv project and tooling

- One uv project `ems` at the repo root, `requires-python >= 3.13`, `[tool.uv] exclude-newer =
  2026-10-02T00:00:00Z` (every locked version at least two weeks old), `default-groups = ["dev", "ingest"]`.
- Dependency layout: `[project].dependencies` hold what contracts and settings need (pydantic, pydantic-settings,
  ruamel.yaml, structlog, typer, tzdata); group `ingest` holds the generator (confluent-kafka, numpy, scipy,
  pandas, pyarrow, pvlib, demandlib, holidays, faker, jsonschema); group `dev` the tooling.
- Exact pins only where behaviour depends on them: ruff 0.16.10 (= ruff-pre-commit rev), ty 0.0.84, faker
  40.40.0 (locale data feeds a golden catalog test), pvlib 0.16.1 and demandlib 0.2.2 (APIs used directly).
- Test tiers by directory and marker (`tests/conftest.py` adds the marker): `make test-unit` = unit + property,
  `make test-integration` = against the compose stack, `make ci` = lint + type-check + test-unit.

### Compose (`infra/docker-compose.yml`, spec 001)

| Service | Image (tag + digest, verified 2026-10-20) | Notes |
|---|---|---|
| `kafka` | `apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837` (released 2026-06-25) | JVM image; single KRaft broker+controller; host `127.0.0.1:9092` |
| `kafka-init` | `ems-gen:local` (built from `infra/ems-gen.Dockerfile`) | `ems-gen topics ensure`; one-shot |
| `kafka-ui` | `ghcr.io/kafbat/kafka-ui:v1.5.0@sha256:7cda86a33344160309fdb65146332e4da65db81a945614f2fe32e210803f6fd1` (2026-04-20) | read-only, host `127.0.0.1:8088` |
| `ems-gen` (profile `gen`) | `ems-gen:local` | realtime generator |

Generator image bases: `python:3.13.15-slim-trixie@sha256:7c61056e61ac89e852de05f3dc6fa51a6dd2181797bceed46aa725dd7cb2cd3b`
(pushed 2026-09-25) and `ghcr.io/astral-sh/uv:0.12.21@sha256:a7aed3216253ee804de3e2d8afa5073baa1a177335345d43845cd4165e43b711`
(2026-09-29). Debian/glibc is required: confluent-kafka 2.15.1 ships no musl wheels. The project is installed
non-editable so the runtime stage needs no source tree.

Kafka settings and why (sources: apache/kafka 4.3.1 `docker/` sources and examples, Kafka 4.3 docs; checked
2026-10-20):

- Every `KAFKA_*` variable becomes a broker property, so the kafka service has no `env_file` and generator
  settings use the `EMS_` prefix.
- Configuring from env drops the image's single-node defaults: offsets, transaction state **and**
  `share.coordinator.state.topic.*` need RF/min-ISR 1.
- `KAFKA_LOG_DIRS=/var/lib/kafka/data` with the named volume there; otherwise nothing persists.
- Static `KAFKA_CONTROLLER_QUORUM_VOTERS`; `controller.quorum.bootstrap.servers` breaks the image's auto-format.
- `CLUSTER_ID` (no prefix) is fixed in compose (not a secret); changing it needs `make reset`.
- Healthcheck uses `/opt/kafka/bin/kafka-broker-api-versions.sh` (not on PATH) with a 128 MB heap.
- Heap 512 MB, container memory limit 1 GB (Docker Desktop VM: 7.75 GiB shared with Restate and kind later).
- `auto.create.topics.enable=false`; clients also set `allow.auto.create.topics=false`.
- kafka-ui depends on kafka-init `service_completed_successfully`, which also makes `docker compose up --wait`
  wait for the one-shot job. kafka-ui v1.5.0 shows the 4.3 broker version as "Unknown" (cosmetic); fallback
  broker `apache/kafka:4.2.2@sha256:1213eb3943d551e5ed1fca7a4e109001cee35770b66a02a0c37a8964efe09b69`.
- Host clients set `broker.address.family=v4`: librdkafka on macOS otherwise tries `::1` first.

### Topic profiles (`config/topics.yaml`)

`live` (CreateTime, `message.timestamp.after.max.ms` 1 h, `before.max.ms` = retention), `sim` (retention −1,
any timestamp), `scale` (48/3 partitions, `retention.bytes` cap), `cluster` (RF 3, min ISR 2). Reason: Kafka 4.0
changed `after.max.ms` to 1 h (KIP-1030) and time-based retention deletes old-timestamped data within minutes;
`ems-gen run` preflight refuses incompatible combinations.

### Ports reserved on the host (all bound to 127.0.0.1)

9092 Kafka, 8088 Kafka UI; reserved for later: 8080 Restate ingress, 9070 admin, 5122 node, 9080/9081 SDK.

### Versions verified 2026-10-20

| Artefact | Version | Released | Source |
|---|---|---|---|
| confluent-kafka (bundles librdkafka 2.15.1, zstd) | 2.15.1 | 2026-09-10 | pypi.org/project/confluent-kafka |
| pvlib | 0.16.1 | 2026-09-26 | pypi.org/project/pvlib |
| demandlib | 0.2.2 | 2025-04-09 | pypi.org/project/demandlib |
| holidays | 0.105 | 2026-09-21 | pypi.org/project/holidays |
| Spec Kit | v1.1.0 | 2026-10-02 | github.com/github/spec-kit/releases |
| restate-server (later) | 1.7.13 (`restatedev/restate:1.7.13-20261005@sha256:65abc8016d40…`) | 2026-10-01 | github.com/restatedev/restate |

## What to build

- Dev-environment spec: Restate in the same compose project, `make register`, e2e/fault tiers and coverage.
- Spec 004: the 3-broker `cluster` compose profile.
- Helm deployment spec (later): umbrella chart on kind.

## Prerequisites

- Docker Desktop (Engine 29.7, Compose v5.4 on the author's Mac), uv 0.12.x.

## Open questions

- OPEN: Kafka on Kubernetes (Strimzi vs Redpanda) for the Helm spec.
- VERIFY at the next bump: Kafka 4.3.2/4.4.0 once 14 days old (4.4.0 also fixes zstd in `apache/kafka-native`).
