# 05 · Deploy and infrastructure

## Current consensus

### uv project and tooling

- One uv project `ems` at the repo root, `requires-python >= 3.13`, `default-groups = ["dev", "ingest",
  "actors"]`.
- **Version policy** (decided 2026-11-15, spec 010; P12): every version is pinned exactly (`uv.lock`, image tag
  plus digest, action commit SHA), and a new or bumped pin takes the newest release, with no minimum age,
  because security fixes arrive often. Read the release notes before a bump and run the tiers after it. Until
  2026-11-15 pins had to be at least two weeks old (`[tool.uv] exclude-newer`, now removed); the pins added
  before that date are listed under Open questions.
- Dependency layout: `[project].dependencies` hold what contracts and settings need (pydantic, pydantic-settings,
  ruamel.yaml, structlog, typer, tzdata); group `ingest` holds the generator (confluent-kafka, numpy, scipy,
  pandas, pyarrow, pvlib, demandlib, holidays, faker, jsonschema, httpx, tenacity, lxml); group `actors` the
  Restate SDK service (restate-sdk, hypercorn; spec 010); group `dev` the tooling (incl. respx, pytest-cov,
  coverage). The image also ships `mappings/` (`EMS_MAPPINGS_DIR`).
- Exact pins only where behaviour depends on them: ruff 0.16.10 (= ruff-pre-commit rev), ty 0.0.84, faker
  40.40.0 (locale data feeds a golden catalog test), pvlib 0.16.1 and demandlib 0.2.2 (APIs used directly).
- Test tiers by directory and marker (`tests/conftest.py` adds the marker): `make test-unit` = unit + property;
  `make test-integration`, `make test-e2e`, `make test-fault` = against the compose stack (each starts its own SDK
  service on 127.0.0.1:9080 and its own ingress containers on prefixed topics); `make ci` = lint + type-check +
  test-unit. Each tier writes coverage to `logs/.coverage.<tier>` (pytest-cov 7.1.0, coverage 7.17.3 with
  `patch = ["subprocess"]`, so the SDK service a tier starts is measured too); `make report-coverage` combines
  them into `logs/coverage.xml`.

### CI (spec 010)

`.github/workflows/ci.yml` runs `make ci` on every push to `main` and every pull request, on `macos-26` (arm64,
like the author's machine; three golden digests of the generator differ on Linux, risk G12) with
uv 0.13.8 (`astral-sh/setup-uv` v10.3.3, commit `68559e99cc807c1eb7e9238fd43a15bba460eec2`) after
`actions/checkout` v7.0.1 (commit `3d3c42e5aac5ba805825da76410c181273ba90b1`). Default permissions are read-only.
On `main` a second job, the only one with `contents: write`, force-pushes one orphan commit with `coverage.json`
(the unit and property tiers' total) to the `badges` branch, which the README's coverage badge reads through
shields.io. The integration, e2e and fault tiers need Docker, Kafka and Restate and run on the author's machine
(risk H2). The lockfile passes `uv lock --check` with both uv 0.12.5 (local) and 0.13.8 (CI).

### Compose (`infra/docker-compose.yml`, spec 001)

One Kafka broker, RF 1, in every environment (spec 006): broker replication and failover are out of thesis
scope (risk K9), so there is no multi-broker profile; the fault experiments target Restate and the SDK service.

| Service | Image (tag + digest, verified 2026-10-20) | Notes |
|---|---|---|
| `kafka` | `apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837` (released 2026-06-25) | JVM image; single KRaft broker+controller; host `127.0.0.1:9092` |
| `kafka-init` | `ems-gen:local` (built from `infra/ems-gen.Dockerfile`) | `ems-gen topics ensure`; one-shot |
| `kafka-ui` | `ghcr.io/kafbat/kafka-ui:v1.5.0@sha256:7cda86a33344160309fdb65146332e4da65db81a945614f2fe32e210803f6fd1` (2026-04-20) | read-only, host `127.0.0.1:8088` |
| `ems-gen` (profile `gen`) | `ems-gen:local` | realtime generator |
| `restate` (spec 010) | `restatedev/restate:1.8.1-hotfix.2-20261102@sha256:e3a11e7c694cfe0c134eca1652992c068bef0bcba3caeec9d2a3b3bc50daa62e` | one node `restate-1`, cluster `ems`, RocksDB 512 MiB, volume `restate-data`; ingress `127.0.0.1:8080`, admin `127.0.0.1:9070`; healthy when admin `/health` and ingress `/restate/health` answer; `mem_limit` 2 GB (peak 1,556 MiB) |
| `ingress-meters`, `ingress-forecasts`, `ingress-prices` (profile `ingress`, spec 010) | `ghcr.io/restatedev/ingress-integration-kafka:0.1.1-20261109@sha256:20704c886ee4dcc4046bab1743dc424bb08b2317ce1bc6da55ce738345ce01b1` | linux/amd64 only (emulated on Apple Silicon); started by `make register`, removed by `make down`; `restart: unless-stopped`; `-Xmx256m`, `mem_limit` 768 MB (peaks 475, 558 and 288 MiB); healthcheck bash `/dev/tcp` on the metrics port 9464; no host port |

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
- Heap 512 MB, container memory limit 1 GB (Docker Desktop VM: 7.75 GiB shared with Restate, and with kind only if the
  optional Helm spec runs).
- `auto.create.topics.enable=false`; clients also set `allow.auto.create.topics=false`.
- kafka-ui depends on kafka-init `service_completed_successfully`, which also makes `docker compose up --wait`
  wait for the one-shot job. kafka-ui v1.5.0 shows the 4.3 broker version as "Unknown" (cosmetic); fallback
  broker `apache/kafka:4.2.2@sha256:1213eb3943d551e5ed1fca7a4e109001cee35770b66a02a0c37a8964efe09b69`.
- Host clients set `broker.address.family=v4`: librdkafka on macOS otherwise tries `::1` first.

### Topic profiles (`config/topics.yaml`)

`live` (CreateTime, `message.timestamp.after.max.ms` 1 h, `before.max.ms` = retention), `sim` (retention −1,
any timestamp), `scale` (48/3 partitions, `retention.bytes` cap); all RF 1. Reason: Kafka 4.0
changed `after.max.ms` to 1 h (KIP-1030) and time-based retention deletes old-timestamped data within minutes;
`ems-gen run` preflight refuses incompatible combinations.

### Ports on the host (all bound to 127.0.0.1)

9092 Kafka, 8088 Kafka UI, 8080 Restate ingress, 9070 Restate admin and UI, 9080 the SDK service (`make dev`, the
test tiers), 9081 the spike's second deployment (`make spike`). The Restate node port 5122 stays inside compose.
Memory limits in the 7.75 GiB Docker Desktop VM: Kafka 1 GB, Restate 2 GB, 768 MB per ingress container (the
peaks are from `experiments/results/ingress-skew-20261116T081120Z`, an unpaced backfill of 34,627 records).

### Versions verified 2026-10-20

| Artefact | Version | Released | Source |
|---|---|---|---|
| confluent-kafka (bundles librdkafka 2.15.1, zstd) | 2.15.1 | 2026-09-10 | pypi.org/project/confluent-kafka |
| pvlib | 0.16.1 | 2026-09-26 | pypi.org/project/pvlib |
| demandlib | 0.2.2 | 2025-04-09 | pypi.org/project/demandlib |
| holidays | 0.105 | 2026-09-21 | pypi.org/project/holidays |
| httpx / tenacity / lxml / respx (spec 002) | 0.28.1 / 9.1.4 / 6.1.3 / 0.23.1 | 2024-12-06 / 2026-02-07 / 2026-09-02 / 2026-04-08 | pypi.org |
| restate-server (spec 010; verified 2026-11-15) | 1.8.1-hotfix.2, the newest release (`restatedev/restate:1.8.1-hotfix.2-20261102`, index digest `sha256:e3a11e7c694cfe0c134eca1652992c068bef0bcba3caeec9d2a3b3bc50daa62e`, arm64 and amd64) | 2026-11-02 12:58 UTC | github.com/restatedev/restate/releases, Docker Hub registry API |
| Kafka ingress integration (spec 010; verified 2026-11-15) | 0.1.1 (git tag `v0.1.1`, 2026-10-21), newest build `0.1.1-20261109`, digest `sha256:20704c886ee4dcc4046bab1743dc424bb08b2317ce1bc6da55ce738345ce01b1`, linux/amd64 only; bundles `kafka-clients` 4.2.0 | 2026-11-09 | github.com/restatedev/ingress-integration-kafka, ghcr.io registry API |
| restate-sdk (spec 010) | 1.5.2, exact; no base dependencies | 2026-11-15 | pypi.org/project/restate-sdk |
| hypercorn (spec 010) | 0.18.0, exact | 2025-11-08 | pypi.org/project/hypercorn |
| coverage / pytest-cov (spec 010) | 7.17.3 / 7.1.0 | 2026-11-12 / 2026-03-21 | pypi.org |
| uv in CI / setup-uv / checkout / runner (spec 010) | 0.13.8 / v10.3.3 / v7.0.1 / `macos-26` (arm64) | 2026-11-12 / 2026-10-28 / 2026-07-20 / GA | GitHub API, actions/runner-images |

### Restate (decided 2026-11-14, pinned in spec 010)

- Restate 1.8.1-hotfix.2 replaces the 1.7.13 named earlier: the Kafka ingress integration needs 1.8.1 or later,
  and Restate's in-server Kafka subscriptions are not used (`00-overview.md`, K6).
- The hotfix has no release notes and is not marked Latest. Its 26 commits over 1.8.1 (GitHub compare API,
  read 2026-11-14) are CI changes, dependency bumps for security advisories, a CLI change and one opt-in VQueues
  option that removes orphaned active markers (risk A5). The default configuration restarted cleanly after
  `kill -9` without that option (V10 in 01).
- Restate re-pushes its release lines weekly with OS updates (`1.8.1-20261109` is `latest`), but not the hotfix,
  so the hotfix's only image is `1.8.1-hotfix.2-20261102`; pins use a dated tag plus digest.
- Configuration in compose, all confirmed with `restate-server --dump-config`: `RESTATE_NODE_NAME=restate-1`,
  `RESTATE_CLUSTER_NAME=ems`, `RESTATE_ROCKSDB_TOTAL_MEMORY_SIZE="512 MiB"` (shown as `512.0 MiB`). The server's
  default retry policy (500 ms, ×2, at most 1 min, 70 attempts, then pause) is left as it is.

## What to build

- Refresh the pins that predate the version policy of 2026-11-15 to their newest releases (risk H4), in a
  change of their own.
- Optional, behind the gate in `00-overview.md` (spec 006): Helm deployment spec, umbrella chart on kind. Scale
  experiments run on Docker Compose.

## Prerequisites

- Docker Desktop (Engine 29.7, Compose v5.4 on the author's Mac) with Rosetta or QEMU emulation for the
  linux/amd64 ingress image; uv 0.12.5 or newer; `lsof` (the spike's `ctl.sh`).

## Open questions

- OPEN, only if the optional Helm spec is started: single-node Kafka on Kubernetes (Strimzi vs Redpanda).
- OPEN: the refresh of older pins (risk H4). Newest releases checked 2026-11-16: Kafka 4.4.0 (also fixes zstd in
  `apache/kafka-native`), Python image 3.13.18-slim-trixie, uv 0.13.8, ruff 0.17.5 (with the matching
  ruff-pre-commit rev), ty 0.0.104; kafka-ui v1.5.0 and pre-commit-hooks v6.0.0 are already the newest.
