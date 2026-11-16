# Implementation Plan: Restate environment and Kafka into a Virtual Object

**Branch**: `010-restate-kafka-ingress` | **Date**: 2026-11-15 | **Spec**: [spec.md](spec.md) | Plan approved
2026-11-15, revised the same day (version policy)

## Summary

The local stack gains Restate 1.8.1-hotfix.2 and three Kafka ingress containers. A `Probe` Virtual Object
receives the four topics and keeps, per key and topic, a log that shows order, loss and duplicates. The same pure
function computes the log offline from Kafka, so the e2e and fault tests compare with `==`. Two jobs record the
measurements and the spike reproduction under `experiments/results/`. The test tiers get coverage, and a GitHub
Actions workflow runs `make ci` and feeds the README badges.

No wire contract changes. The only actors are `Probe` and two measurement-only objects; stage 3 removes all three
and keeps the log function.

**Revised 2026-11-15 (user decision).** The two-week minimum age for pins is dropped: a new or bumped pin takes
the newest release. This plan therefore pins `restate-sdk` 1.5.2 (released that day), the integration's newest
build `0.1.1-20261109`, uv 0.13.8 and the `ubuntu-26.04` runner, removes `exclude-newer`, and writes the rule into
`CLAUDE.md`, P12, 05 and the harness. Pins that this spec does not add are refreshed in a separate change.

## Facts checked for this plan

Checked on 2026-11-14 unless a date is given. "Task" means the value is confirmed on the pinned version when its
task runs, and then written to the docs with source and date.

| Item | Value | Source | Confidence |
|---|---|---|---|
| Server image | `restatedev/restate:1.8.1-hotfix.2-20261102`, index digest `sha256:e3a11e7c694cfe0c134eca1652992c068bef0bcba3caeec9d2a3b3bc50daa62e`, arm64 and amd64, pushed 2026-11-02 12:58 UTC. It is the newest release; the weekly rebuilds (`1.8.1-20261109` = `latest`, `1.7.14-20261109`) are the 1.8.1 and 1.7.14 lines without the hotfix's dependency bumps | Docker Hub registry API, GitHub releases (2026-11-15) | confirmed |
| Server image contents | `debian:trixie-slim`; `curl` and `jq`; `restate-server`, `restate`, `restatectl`; entrypoint `restate-server`; runs as root; data in `restate-data` under `/` | `docker/Dockerfile` at tag `v1.8.1-hotfix.2` (2026-11-15) | confirmed |
| Server health | Admin `GET /health` answers 200 when the admin API is ready. The spike used `/restate/health` on the ingress | docs.restate.dev admin API; spike `experiments.py` | confirmed 2026-11-15 on the image: admin `/health` and ingress `/restate/health` answer 200 |
| Server options | `rocksdb-total-memory-size` 2.0 GiB (minimum 256 MiB). Default retry policy: 70 attempts, then `pause`, `max-interval` 1 min; `initial-interval` is 500 ms in the reference and 50 ms on the services page. `node-name` defaults to the hostname and must not change. `[ingress.ingestion-api]` (experimental gRPC) is on by default. Nested options map to `RESTATE_A__B` variables | docs.restate.dev/references/server-config (2026-11-15) | confirmed 2026-11-15: the registration response shows 500 ms, ×2, 1 min, 70 attempts, pause; `"512 MiB"` is accepted (`--dump-config` shows `512.0 MiB`) |
| Introspection | `sys_invocation` has `invoked_by` (`ingress_ingestion` for the integration), `status`, `created_at`, `running_at`, `completed_at`. `state` has `service_name`, `service_key`, `key`, `value_utf8` | docs.restate.dev/references/sql-introspection (2026-11-15) | confirmed 2026-11-15: `POST /query` with `{"query": …}` returns `{"rows": […]}`; `starts_with` works; the integration's invocations show `invoked_by = ingress`, not `ingress_ingestion` |
| Registration | `POST /deployments`; `force` defaults to **true**; 201 created; 200 "already exists, no change" with `force: false`. Adding handlers is allowed, removing or renaming needs `breaking`. What `force: false` returns at the same URI after handlers changed is not documented | docs.restate.dev admin API and services/versioning (2026-11-15) | confirmed 2026-11-15: with `force: false` a changed handler set still answers 200 and keeps the old handlers (02) |
| Integration image | `ghcr.io/restatedev/ingress-integration-kafka:0.1.1-20261109`, digest `sha256:20704c886ee4dcc4046bab1743dc424bb08b2317ce1bc6da55ce738345ce01b1` (also `0.1.1` and `latest`), one manifest, linux/amd64, Temurin JRE 25.0.5 (Ubuntu), port 9464. Newest build of the newest tag `v0.1.1` | ghcr.io registry API, GitHub tags (2026-11-15) | confirmed |
| Integration settings | Required: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_GROUP_ID`, `KAFKA_TOPICS`, `RESTATE_INGRESS_URL`, and for the static mapper `RESTATE_RECORD_MAPPER_SERVICE` and `_HANDLER`. `RESTATE_KAFKA_CONSUMER_INSTANCES` defaults to 2 × cores. Retries: 500 ms, × 2, up to 30 s, exit after `RESTATE_RETRY_MAX_ATTEMPTS` = 15. Metrics on 9464 at `/metrics`. JVM options through `JDK_JAVA_OPTIONS` | `CONFIGURATION.md` | confirmed |
| Integration consumer | `enable.auto.commit` is forced to false. `auto.offset.reset` is **not set**, so Kafka's default `latest` applies unless `KAFKA_AUTO_OFFSET_RESET` is given | `RestateKafkaIntegrationConfiguration.kt` at `v0.1.1` (2026-11-15) | confirmed |
| Integration mapping | Value as raw bytes, no content type; key becomes the object key and the `kafka.key` header; `kafka.*` metadata headers on by default; other record headers dropped except `traceparent` and `tracestate` | `StaticRecordMapper.kt` | confirmed |
| Integration delivery | Dedup id `group/topic/partition`, sequence = offset. Kafka offsets are committed after Restate's durable acknowledgement. A record Restate rejects is skipped. Back-pressure pauses the partition | `PartitionSession.kt`, `ConsumerVerticle.kt` | read from source; confirmed by the tests and `make measure` (2026-11-16) |
| Host reachability | From the compose network `ems_default`, a container reaches a host listener bound to `127.0.0.1:9080` through `host.docker.internal`, with and without the `host-gateway` entry | checked on the author's Mac, Docker Desktop 29.7.2 (2026-11-15) | confirmed |
| SDK | 1.5.2 released 2026-11-15, the newest; no base dependencies; cp313 wheels for macOS arm64 and manylinux. 1.3 to 1.5.2 change agents, sessions and the built-in server only. Read in 1.5.2: input that fails to deserialise raises `TerminalError` (`handler.py` 406); `BytesSerde` passes bytes; handler input is advertised with `required=False` and the `accept` content type (default `application/json`); lazy state is `state_preload=False`, and `enable_lazy_state` is marked deprecated; `ctx.time()` is a journaled `time.time()` in float seconds (`server_context.py` 889–891); `ctx.request().headers` carries the invocation headers | PyPI, GitHub releases, the installed package (2026-11-15) | confirmed |
| hypercorn, coverage, pytest-cov | 0.18.0 (2025-11-08), 7.17.3 (2026-11-12), 7.1.0 (2026-03-21), each the newest | PyPI (2026-11-15) | confirmed |
| Actions, uv, runner | `actions/checkout` v7.0.1 (2026-07-20), commit `3d3c42e5aac5ba805825da76410c181273ba90b1`. `astral-sh/setup-uv` v10.3.3 (2026-10-28), commit `68559e99cc807c1eb7e9238fd43a15bba460eec2`; input `version` pins uv. uv 0.13.8 (2026-11-12). Runner label `ubuntu-26.04` (no beta badge) | GitHub API, `actions/runner-images` README (2026-11-15) | confirmed |
| ty | `[tool.ty.src] exclude`; ignore files are respected by default, so the gitignored root copy of the spike is not checked today | docs.astral.sh/ty (2026-11-15) | confirmed |
| Coverage | The Makefile template uses `coverage xml --keep-combined`; `[tool.coverage.run] patch = ["subprocess"]` measures child processes | `templates/Makefile`; coverage docs | confirmed 2026-11-15 on coverage 7.17.3: `coverage combine --keep` plus `xml` and `report`; the SDK service a tier starts is measured (`probe.py` 96 % after the e2e tier) |
| Spike start | The spike needs `wipe`, `server`, `svc1` and `register` before `experiments.py`; its load generator retries without limit | spike `README.md`, `loadgen.py` | confirmed |

## Plan decisions (the spec's open questions)

| Question | Decision |
|---|---|
| Backfill sizes | e2e: 10 households × 1 day (spec). Fault tests: 120 households × 2 days (about 41,000 records), so an unkilled run lasts over a minute at the rate measured (about 400 records/s); the size is a constant in `tests/fault/test_probe_kills.py`. `make measure`: 100 households × 2 days by default (`LOCATIONS=`, `DAYS=`). State cost: 500 `touch` calls per size and variant. Backlog: 20 households × 1 day. Each result records the size it used |
| Kill window | Each kill test publishes while the module's ingress containers are stopped, so the run's size and end offsets are known before anything is consumed, then starts them. Trigger: for the SDK kill, `handled(run_id)` reaches 30 % of the published records; for the server and ingress kills, the meters group's committed offsets reach 30 % of the run's share of the end offsets. Proof from data that survives the kill: SDK kill, `handled()` right after the kill is below the published count; server and ingress kills, the committed offsets right after the kill are below the end offsets (ingestion was mid-run, hence processing too). A test without that proof fails |
| Memory | `restate`: `mem_limit: 2g`, RocksDB total 512 MiB. Each ingress container starts at `mem_limit: 768m` with `-Xmx256m` and no other JVM caps (emulation adds native memory outside the heap), consumers 3 (meters, forecasts) or 1 (prices), never more than the partitions they read. A tier or job stops the stack's own ingress containers when it starts and leaves ingestion closed; `make register` opens it again. The peaks of the first runs and of `make measure` set the final limits in 05 |
| Healthchecks | `restate`: `curl` on the admin `/health` and the ingress health path, confirmed on the image before it is wired. Ingress containers: `["CMD", "bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/9464"]` (the metrics port answers). It names bash explicitly because `CMD-SHELL` runs `/bin/sh`, which is dash on the image's Ubuntu base and has no `/dev/tcp` |
| Test-scoped ingress | A second compose project per test module or job run: the stack's compose file plus `tests/compose.it.yml`, which joins the stack's network and resets `depends_on` for the three services. The same service definitions give the same restart policy. Topic prefix, group prefix and retry attempts come from interpolation variables with the stack's values as defaults |
| Invocation time (AC-010-20) | From `sys_invocation`: `completed_at − running_at` per `touch` call, plus calls per second for the batch. The job sends each call with an `Idempotency-Key`, so completed calls stay queryable |
| Workflow pins | `actions/checkout@3d3c42e…` (v7.0.1), `astral-sh/setup-uv@68559e9…` (v10.3.3) with `version: "0.13.8"`, `runs-on: macos-26` (arm64). The lockfile is checked with uv 0.13.8 before the workflow lands. *Changed on the pull request: the first runs on `ubuntu-26.04` and `ubuntu-26.04-arm` failed three golden digests of the generator (specs 005, 008), which hold only on macOS arm64 (risk G12)* |
| Fault tier coverage | The fault tier writes `logs/.coverage.fault` like the other tiers (AC-010-27 names every tier). A process killed with SIGKILL writes no data; that is accepted |
| Dedup hazard | No test. 09 cites the docs and `PartitionSession.kt`; `make reset` wipes both volumes |
| `force: false` with changed handlers | One check on the pinned stack in the registration task, for an added and for a removed handler; commands and output go into the pull request, the result into 02 and the stage 3 brief |
| Shared harness | Tests and jobs use one module, `src/ems/experiments/stack.py`, to start the SDK service, register, run a test-scoped ingress set, publish, read back and query. `make measure` therefore also runs on prefixed topics and never writes to the stack's. Every test and every job run uses a fresh `run_id` |

## Principles Check (PASS)

| # | Touched | How it holds | Shown by |
|---|---|---|---|
| P1 | yes | This spec, plan and tasks; commits cite task ids | review |
| P2 | yes | `src/ems/domain/` untouched. Handlers read the headers, call the pure functions of `ingress_check`, write state | purity test (AC-010-12); post-edit hook |
| P3 | yes | `Probe` alone writes the ingress log and the leads of its key; row added to 01 | Actor contract |
| P4 | yes | No edge between actors. Exclusive handlers: Kafka or one-way HTTP sends. Shared: `status`, `count`, from outside | Actor contract |
| P5 | yes | Handlers parse JSON (up to about 60 kB) and hash a few bytes; no external call, no clock | review; ruff `ASYNC` |
| P6 | yes | Restate's offset dedup at the ingress; `==` against the offline fold, with and without kills | e2e and fault tiers |
| P7 | yes | No timer; leads in data time. Wall clock only in `jobs/` and the harness | review |
| P8 | no | | |
| P9 | no change | No wire, topic or message change | contract tests stay green |
| P10 | yes | `jobs/ingress_measure.py`, `jobs/spike_repro.py`: CLIs. They start the app as a child process and serve nothing themselves | review |
| P11 | yes | `EMS_RESTATE_INGRESS_URL`, `EMS_RESTATE_ADMIN_URL`, `EMS_SERVICE_URL` in `ems.settings`; the Makefile takes its defaults from the same variables; the workflow has no secret | unit test of the defaults |
| P12 | yes, reworded | Images by tag and digest, actions by SHA, `restate-sdk` and `hypercorn` exact, each the newest release; `exclude-newer` removed; the spike's unpinned packages are frozen into its manifest. P12 itself loses the two-week clause (user decision 2026-11-15) | AC-010-02; `uv lock --check` |
| P13 | no change | Tracing stays off; the integration forwards `traceparent` only if a record carries one | review |
| P14 | yes | Each result directory has a manifest; `/logs/` is git-ignored so a tier run does not mark the commit dirty | AC-010-18, -24 |
| P15 | yes | Docs listed below land in the same change | SC-002 |
| P16 | yes | Records per second, memory, state cost and backlog beside the skew | AC-010-18 … -21 |

## Contract changes (`08-contracts.md`)

None on the wire and none between actors. The handoff section gains facts: Kafka-fed handlers take raw bytes;
the handler also sees `kafka.key`; record headers are not forwarded (only `traceparent` and `tracestate`); the
consumer groups are `ems-ingress-meters`, `ems-ingress-forecasts` and `ems-ingress-prices` and start at the
earliest offset.

## Design

### 1. Layout

```
infra/docker-compose.yml          + restate, ingress-meters, ingress-forecasts, ingress-prices (profile ingress)
infra/spike/ctl.sh                the spike's verbs for a containerised server
tests/compose.it.yml              joins a test project to the stack's network, resets depends_on
Makefile                          register, dev, test-e2e, test-fault, report-coverage, spike, measure
pyproject.toml, uv.lock           group actors; pytest-cov, coverage; no exclude-newer; coverage and ty config
.gitignore                        /logs/
src/ems/settings.py               Restate URLs
src/ems/actors/app.py             restate.app(services=[probe, state_cost_eager, state_cost_lazy])
src/ems/actors/ingress_check.py   pure: log, leads, parsing of the Kafka headers
src/ems/actors/probe.py           Probe
src/ems/actors/statecost.py       StateCostEager, StateCostLazy
src/ems/experiments/stack.py      harness shared by tests and jobs
jobs/ingress_measure.py           modes skew (default), state-cost, backlog
jobs/spike_repro.py               runs A, B, C and judges the result
reference/restate-ems-spike/      the 16 spike files, unedited
tests/fixtures/spike-files.sha256
tests/unit/actors/, tests/property/, tests/integration/, tests/e2e/, tests/fault/
.github/workflows/ci.yml
```

### 2. Compose (AC-010-01, -02, -06, -08)

- `restate`: the pinned image; `RESTATE_NODE_NAME=restate-1`, `RESTATE_CLUSTER_NAME=ems`, the RocksDB limit;
  ports `127.0.0.1:8080:8080` and `127.0.0.1:9070:9070` (5122 stays inside); `extra_hosts:
  ["host.docker.internal:host-gateway"]`; volume `restate-data:/restate-data`; `mem_limit: 2g`; the healthcheck
  above. No restart policy: the fault tests start it themselves.
- One YAML anchor holds what the three ingress services share: the pinned image, `platform: linux/amd64`,
  `profiles: ["ingress"]`, the label `ems.role=ingress`, `restart: unless-stopped`, `mem_limit: 512m`, the
  healthcheck, `depends_on` (`kafka-init` completed, `restate` healthy), and the environment
  `KAFKA_BOOTSTRAP_SERVERS=kafka:29092`, `KAFKA_AUTO_OFFSET_RESET=earliest`,
  `RESTATE_INGRESS_URL=http://restate:8080`, `RESTATE_RECORD_MAPPER_SERVICE=Probe`,
  `JDK_JAVA_OPTIONS=-Xmx256m`, `RESTATE_RETRY_MAX_ATTEMPTS=${EMS_INGRESS_RETRY_MAX_ATTEMPTS:-15}`.
- Per service:

  | Service | `KAFKA_TOPICS` | `KAFKA_GROUP_ID` | Handler | Consumers |
  |---|---|---|---|---|
  | `ingress-meters` | `ems.production.v1,ems.consumption.v1` | `ems-ingress-meters` | `on_measurement` | 3 |
  | `ingress-forecasts` | `ems.weather.forecast.v1` | `ems-ingress-forecasts` | `on_forecast` | 3 |
  | `ingress-prices` | `ems.price.v1` | `ems-ingress-prices` | `on_price` | 1 |

  Every topic name and group id is written as `${EMS_INGRESS_PREFIX:-}name`, so the stack's values are the
  defaults and a test project passes its prefix.
- The ingress containers publish no host port.

### 3. Makefile (AC-010-03 … -07, -26, -27)

- `down` and `reset` run compose with `--profile ingress` and then remove every container labelled
  `ems.role=ingress`, so ingress containers of a crashed test run go too. The `reset` prompt names both volumes.
- `RESTATE_ADMIN` and `SERVICE_URL` default to `EMS_RESTATE_ADMIN_URL` and `EMS_SERVICE_URL` when those are set,
  otherwise to the same literals as `ems.settings` (`http://127.0.0.1:9070`, `http://host.docker.internal:9080`).
- `register`: `curl -fsS -X POST $(RESTATE_ADMIN)/deployments` with `{"uri": "$(SERVICE_URL)", "force":
  $(FORCE)}` (`FORCE ?= false`), then `compose --profile ingress up -d --wait` for the three services. `curl -f`
  fails on any HTTP error, so the second line never runs then.
- `dev`: `uv run hypercorn ems.actors.app:app --bind 127.0.0.1:9080 --reload`.
- Test tiers: the commented block at the bottom of the Makefile is enabled, with a coverage file for the fault
  tier as well (`logs/.coverage.<tier>`); `report-coverage`; `test` becomes unit, integration, e2e, then the
  report.
- `spike`: `uv run python jobs/spike_repro.py A B C`. `measure`: `uv run python jobs/ingress_measure.py
  $(MODE)`.

### 4. The pure ingress check (`ingress_check.py`; AC-010-09, -10, -12, -17)

Imports: the standard library and `ems.contracts` only. Two functions carry the two parts of the probe's state.

- `kafka_meta(headers) -> (topic, partition, offset) | None` reads `kafka.topic`, `kafka.partition` and
  `kafka.offset`.
- `log_apply(log, meta, raw) -> log` is the part compared with `==`. `log` holds one entry per topic plus the
  key's `rejected` count.
  - An entry holds `partition`, `n`, `last_offset`, `digest`, `regressions` and `partition_changes`.
  - `digest` is a SHA-256 chain: `sha256(previous digest + ":" + partition + ":" + offset)` in hex.
  - An offset that is not above `last_offset` in the same partition is a regression.
  - A new partition is counted as a partition change and starts a new offset run.
  - `rejected` rises when the metadata is missing or `raw` does not parse with
    `ems.contracts.envelope.parse_event`.
  - The result for one key does not depend on how its topics interleave. The offline side folds this same
    function over the records read back from Kafka (topic, partition, offset, value).
- `lead_apply(leads, raw) -> leads` is the measured part. It keeps `newest` per stream and the largest lead
  per ordered pair: when a record of stream a arrives, `newest[a]` rises to its slot, and for every other
  stream b already seen the lead `newest[a] − newest[b]` replaces the stored maximum if it is larger. The slot
  is `available_at` (from `ems.contracts.availability.available_at`) in whole quarter-hours since the epoch. A
  record that does not parse, or a forecast whose model has no availability delay, changes nothing but a
  `skipped` count.
- Both functions are total: any exception from parsing or from the availability rule is caught and counted
  (a `delivery_day` at the end of the calendar raises `OverflowError` in `ems.contracts.time`, which pydantic
  does not wrap). A handler therefore never raises on the content of a record.

State is plain JSON types, so `==` on the dictionaries is the comparison.

### 5. Objects (`probe.py`, `statecost.py`, `app.py`)

Described in the Actor contract below. `Probe` keeps three state keys: `log`, `leads` and `n` (records
handled). A handler writes `log`, then `leads`, then `n`; the three writes belong to one invocation and are
journaled together. `n` is its own key so that one SQL query over `state` gives the progress of a run without
calling every object.

### 6. Harness (`src/ems/experiments/stack.py`)

Used by the test fixtures and by the jobs. All settings come from `ems.settings`. It imports `ems.ingest`
(Kafka client, reader), so it needs the `ingest` group, like the jobs do.

- `SdkService`: starts `python -m hypercorn ems.actors.app:app --bind 127.0.0.1:9080` (never `--reload`, which
  only `make dev` uses) in a new session, so hypercorn and its worker share one process group; refuses to start
  when port 9080 is in use; `kill9()` sends SIGKILL to that process group (`os.killpg`) and then checks by a
  bind that nothing listens on 9080; `stop()` sends SIGTERM to the group and waits for the exit, so the
  children's coverage data is written.
- `register(force=False)`: the admin API call of `make register`; returns the deployment id and the protocol
  versions.
- `close_stack_ingestion()`: stops the stack's own ingress containers.
- `IngressSet(prefix, retry_max_attempts=None)`: `docker compose -p ems-it-<prefix> -f infra/docker-compose.yml
  -f tests/compose.it.yml --profile ingress up -d --no-deps` for the three services, with
  `EMS_INGRESS_PREFIX`; `kill9(service)`, `start(service)`, `restart_count(service)`, `committed(service)`
  (the group's committed offsets against the end offsets, from the Kafka admin API), `down()`. Creating a set
  first removes projects left by earlier runs.
- `publish_backfill(prefix, run_id, locations, days, start)`: runs `ems-gen run --mode backfill --run-id` with
  `EMS_TOPIC_PREFIX` and the `sim` profile, after `ensure` created the prefixed topics.
- `offline_logs(prefix, run_id)`: reads the four prefixed topics back with
  `ems.ingest.tools.reader.read_topic`, keeps the keys of `run_id`, and folds `ingress_check.log_apply` per key.
- `probe_status(key)`; `handled(run_id)`: selects `value_utf8` of the state key `n` where `service_name =
  'Probe'` and `starts_with(service_key, '<run_id>:')`, and sums in Python (the column is Utf8, and `LIKE` would
  treat `_` as a wildcard); `wait_handled(run_id, expected, timeout)`.
- `manifest()`: config, seed, window, hardware, image references (parsed from `infra/docker-compose.yml`, no
  Docker call), package versions (`_versions` of `ems.ingest.runner` plus `restate-sdk` and `hypercorn`) and
  the git commit (`_git_commit`), as `jobs/benchmark.py` does.

### 7. Jobs

`jobs/ingress_measure.py` writes one directory `experiments/results/ingress-<UTC>/`. Each run takes a fresh
`run_id` and prefix, closes the stack's ingestion, and starts its own SDK service and ingress set.
- `skew` (default): unpaced backfill, wait until handled. Writes the leads per key and pair, their median, 95th
  percentile and maximum, records per second, and the peak memory of every container of both compose projects
  (sampled with `docker stats` every 2 s) with their `OOMKilled` flags.
- `state-cost`: for `StateCostEager` and `StateCostLazy` and each of 1 kB, 100 kB and 1 MB: one `pad` send, 500
  `touch` sends, wait for `count`, then the times from `sys_invocation`.
- `backlog`: stops the SDK service, publishes, samples every second the lag of each consumer group (Kafka admin
  API) and the invocations of the run by status (`sys_invocation`), starts the service after the lag and the
  pending count have been stable for 30 s, and samples until everything is handled.

`jobs/spike_repro.py` (AC-010-23 … -25):
1. Fails if port 8080, 9070, 9080 or 9081 is in use.
2. Creates `experiments/results/spike-<UTC>/` with a copy of `infra/spike/ctl.sh` and a throwaway virtualenv:
   `uv venv --python 3.13`, then `uv pip install -r reference/restate-ems-spike/requirements.txt`. Saves `uv pip
   freeze` of that virtualenv.
3. Runs every later step with the run directory as the working directory, the virtualenv's `bin` first on
   `PATH`, `PYTHONDONTWRITEBYTECODE=1` (nothing is written into `reference/`), the server image reference
   (parsed from the compose file) and the path of `reference/restate-ems-spike/` in the environment.
4. Brings the spike up as its README does: `./ctl.sh wipe`, `server`, wait for health, `svc1`, wait for the
   port, `register http://127.0.0.1:9080`.
5. Runs `python <repo>/reference/restate-ems-spike/experiments.py A B C` with a time limit. The spike finds
   `./ctl.sh` in the working directory and writes `results.json` there.
6. `./ctl.sh wipe`, removes the virtualenv, writes the manifest (hardware, server image digest, the frozen
   packages, git commit).
7. Reads `results.json` and `ctl.log` and exits 0 only on the verdict of AC-010-24.

`infra/spike/ctl.sh` keeps the spike's verbs. `server` runs the pinned server image as the container
`ems-spike-restate` with its own volume and ports 8080 and 9070 on 127.0.0.1; `kill-server` is `docker kill -s
KILL`; `svc1` and `svc2` start the spike apps with the virtualenv's hypercorn on 127.0.0.1; `kill-svc1` and
`kill-svc2` kill every listener on the port; `register URL` posts to the admin API with `127.0.0.1` replaced by
`host.docker.internal`; `wipe` removes the container and its volume. Every verb appends what it did (container
exit code, killed pids) to `ctl.log`.

### 8. Coverage, CI and badges (AC-010-27 … -31)

- `pyproject.toml`: `pytest-cov` and `coverage` in `dev`; `[tool.coverage.run]` with `source = ["ems"]`,
  `parallel = true` and `patch = ["subprocess"]`, so handler code running in the SDK service counts in the
  integration and e2e tiers. Each tier's files share the prefix its `COVERAGE_FILE` gives them.
- `.github/workflows/ci.yml`:
  - triggers: `push` to `main`, `pull_request`; top-level `permissions: contents: read`; one concurrency group
    per ref, cancelling runs in progress for pull requests only;
  - job `ci` on `macos-26` (arm64; see Workflow pins): checkout (no persisted credentials), setup-uv with uv 0.13.8, `uv sync
    --locked --all-groups`, `make ci`, `make report-coverage`; the total goes to the job summary and a job
    output;
  - job `badge`, only for a push to `main`, with `permissions: contents: write`: plain `git` builds a fresh
    orphan commit (`git checkout --orphan badges`) holding only `coverage.json` (`{"schemaVersion": 1, "label":
    "coverage", "message": "<total>%", "color": …}`) and runs `git push --force origin HEAD:badges`, so the
    branch keeps one commit and never meets a non-fast-forward rejection. No third-party action or service.
- README badge block, first lines of the file: CI status (GitHub's workflow badge), coverage (shields.io
  `endpoint` on `coverage.json`), Python (shields.io `python/required-version-toml` on `pyproject.toml`), uv,
  Ruff and ty (their published endpoint badges), Restate and Kafka (static shields.io badges).
- `tests/unit/test_readme_badges.py` compares the Restate and Kafka badge texts with the image tags in the
  compose file.
- The coverage badge has a value only after the first workflow run on `main`. AC-010-29 is therefore checked
  right after the merge; a failure is fixed forward under this spec.

### 9. Spike move (AC-010-22)

1. Copy the 16 files to `reference/restate-ems-spike/` and compare SHA-256 with the root copy.
2. Write `tests/fixtures/spike-files.sha256` and the unit test that checks the list and the hashes.
3. `[tool.ty.src] exclude = ["reference"]`; ruff and pre-commit already exclude it.
4. Ask the user to confirm, then delete the root `restate-ems-spike/` and its `.gitignore` line.

## Actor contract

### Actors

| Actor (Virtual Object) | Key | State (field: type, unit) | Owner of facts | New / changed |
|---|---|---|---|---|
| `Probe` | the record key: `[run_id:]location_id` or `[run_id:]market_key` | `log: {topics: {topic: {partition: int, n: int, last_offset: int, digest: str, regressions: int, partition_changes: int}}, rejected: int}`; `leads: {newest: {stream: int [quarter-hours since the epoch]}, max: {"a>b": int [quarter-hours]}, skipped: int}`; `n: int` | The ingress log and the stream leads of its key | new (removed in stage 3) |
| `StateCostEager`, `StateCostLazy` | a key chosen by the job | `blob: str` (padding), `c: int`. Eager: the default `state_preload`; lazy: `state_preload=False` | nothing (measurement only) | new (removed in stage 3) |

### Handlers

| Actor.handler | Kind | Input message (version) | Writes | Sends (one-way) | Calls (request-response, shared only) |
|---|---|---|---|---|---|
| `Probe.on_measurement` | exclusive | raw bytes of a measurement event (wire v1), from `ingress-meters` | `log`, `leads`, `n` | none | none |
| `Probe.on_forecast` | exclusive | raw bytes of a weather forecast event (wire v1), from `ingress-forecasts` | `log`, `leads`, `n` | none | none |
| `Probe.on_price` | exclusive | raw bytes of a price event (wire v1), from `ingress-prices` | `log`, `leads`, `n` | none | none |
| `Probe.status` | shared | none | none | none | none |
| `StateCost*.pad` | exclusive | `int` (bytes), one-way from the job through the HTTP ingress | `blob` | none | none |
| `StateCost*.touch` | exclusive | none, one-way from the job | `c` | none | none |
| `StateCost*.count` | shared | none | none | none | none |

### Messages

| Message | Version | Producer → consumer | Fields added or changed | Additive? | Golden examples updated? |
|---|---|---|---|---|---|
| none | | | | | n/a |

The Kafka records are wire contract v1, unchanged. `pad` and `touch` are measurement calls, not contract.

### Timers

| Timer | Owner | Sim mode | Live mode | Generation guard |
|---|---|---|---|---|
| none | | none | none | n/a |

### Failure modes

| Failure | Expected behaviour | Test (tier) |
|---|---|---|
| SDK service `kill -9` mid-replay | Restate retries the invocations when the service is back; every log equals the offline log | AC-010-14 (fault) |
| Restate server `kill -9` mid-replay | The server comes back healthy unaided; the integration reconnects and resends from its last acknowledged offset; Restate drops what it already has | AC-010-13 (fault) |
| Ingress container `kill -9` mid-replay | The new process resumes from the committed offset; redelivered records are dropped by offset | AC-010-15 (fault) |
| Restate unreachable longer than the reconnect budget | The integration keeps reconnecting every 500 ms and does not exit (observed; the docs say it exits); once Restate is back no record is lost or doubled. A container that exits on its own is restarted by its policy | AC-010-16, revised (fault) |
| Duplicate delivery | A redelivered record (same group, topic, partition, offset) never reaches the handler; if it did, `regressions` would count it | AC-010-09 (e2e), -13 … -15 |
| Out-of-order or late message | Not expected inside one partition; counted as a regression | AC-010-09 (e2e) |
| Bad record | Counted in `rejected`; the handler returns; later records of the key are handled | AC-010-10 (integration); totality by property test |
| Registration of a new deployment while traffic flows | Not exercised on `Probe`. The spike's C registers a second deployment mid-stream on the pinned server | AC-010-24 (job) |
| Stale view | none: no object reads another | n/a |

### Rule check

| Rule | Holds? | Evidence |
|---|---|---|
| R1/R2 | yes | No call between objects. Exclusive handlers are reached by Kafka or by one-way HTTP sends; only `status` and `count` are called request-response, from tests and jobs |
| R3 | yes | Handlers make no external call and read no clock or random source |
| R4 | yes | Restate deduplicates by consumer group, topic, partition and offset; the job's sends carry an `Idempotency-Key` |
| R5 | yes | No sleep, no await other than state access |
| R6 | n/a | No self-rescheduling job |
| R7 | n/a | No message between actors. The wire events carry `schema_version` and `t` already |
| Ordering (ADR 0017) | n/a | No step is released in this stage. The log is judged per topic of a key; the leads are a measurement |
| Single writer per fact | yes | One new row in 01's ownership table: the ingress log and stream leads of a key, written by `Probe` until stage 3 |

## Risks (`09-risks-and-tech-debt.md`)

| Id | Change |
|---|---|
| K6 | Narrowed: order, restarts, bad records and the give-up path are tested; header forwarding is known from the source. Still a first release |
| A1 | First numbers: leads from `make measure`, state cost from V7 |
| A5 | Resolved by AC-010-13 and the spike's B, or the stop rule fires |
| H1 | `make reset` now deletes the Restate volume as well; the prompt says so |
| K10 (new) | The integration image is linux/amd64 only and runs emulated on Apple Silicon. Accepted: its cost is in the measurements; an arm64 build needs a decision |
| K11 (new) | Restate deduplicates by consumer group, topic, partition and offset. Wiping Kafka alone or recreating a consumed topic makes new records look like duplicates. Mitigated: `make reset` wipes both volumes. Read from the docs and the integration source, not tested |
| K12 (new) | A record whose target service is not registered is undefined in the docs, and the integration skips records Restate rejects. Mitigated: ingress containers start only from `make register` and are removed by `make down` |
| H3 (new) | New and bumped pins take the newest release with no waiting period (user decision 2026-11-15), so a regression or a compromised release reaches the stack as soon as it is pinned. Accepted: security fixes arrive sooner; exact pins and digests keep every run reproducible, release notes are read before a bump, and the tiers run after it |
| H4 (new) | Pins added before 2026-11-15 (Kafka 4.3.1, kafka-ui, the generator's base and uv images, the locked Python packages, pre-commit revs) predate the policy and are not the newest | Open: refreshed in a separate change |
| A6 (new) | Every Kafka record becomes an invocation that Restate keeps after completion for its retention time. Accepted for this stage; stage 4 sets the retention for scale runs |
| A7 (new) | An SDK service that stays down longer than the retry budget (70 attempts) leaves its invocations paused; they need a manual resume. Accepted, documented in 02 |
| H2 (new) | CI runs `make ci` only; the integration, e2e and fault tiers run on the author's machine. One job holds `contents: write` to update the `badges` branch |

## Experiment impact

No reported number changes. New result directories: `experiments/results/ingress-<UTC>/` and
`experiments/results/spike-<UTC>/`. 11 gets the measured values with their result paths, and the two thesis
notes quote them.

## Test strategy

Tests are written first by `test-engineer`. Fixtures that touch Restate are module-scoped and wrap the harness
of §6: each module closes the stack's ingestion, starts its own SDK service, registers, and creates its own
prefix and ingress set; each test takes a fresh `run_id`. A test that needs a different order or different
settings (AC-010-11, -16) creates its own ingress set. One session-scoped check fails the run before the first
test when port 9080 is already in use (AC-010-26).

| Tier | Tests | Criteria |
|---|---|---|
| unit | Compose: pins by digest, ports on 127.0.0.1, the three ingress services (topics, groups, handlers, `earliest`, profile, label, restart policy). `uv.lock`: exact SDK and hypercorn. Settings defaults equal the Makefile defaults. Makefile: `down` and `reset` carry the profile and the label sweep; every tier target sets its coverage file under `logs/`. `ingress_check`: log, regression, partition change, rejected, leads, purity (import allow-list). `SdkService` refuses a busy port. Spike job: the port check against a bound socket, the manifest keys, the verdict on canned `results.json` and `ctl.log`. Spike file list and hashes. README badges against the compose tags. The workflow file: actions pinned by 40-character SHA, read-only default permissions, write permission on the badge job only, runner label | AC-010-02, -06, -08 (definition), -12, -17, -22, -24, -25, -26, -27, -28, -30, -31 |
| property | For any interleaving of a key's topics, `log_apply` gives the log of folding each topic alone. `regressions` equals the number of non-increasing steps. Two different offset sequences give different digests. `log_apply` and `lead_apply` never raise, for arbitrary bytes and for valid events with mutated fields | AC-010-09, -10, -12 |
| integration | `test_stack_lifecycle.py`, the one module that owns port 9080 and the stack's own ingress containers, in order: after `make up` the services are healthy, bound to 127.0.0.1, no ingress container; `make dev` listens on 127.0.0.1 only; `make register` registers and the three containers run; again: same id; against a dead SDK port and against an unreachable admin URL: non-zero and no container started; `make down` and `make up`: deployment and state kept, no ingress container. Other modules: a record on each topic reaches its handler under its key; a non-JSON and a contract-breaking record are counted and later records follow; records published before the ingress set starts are handled once; the end offsets of the unprefixed topics are unchanged by the module | AC-010-01, -03 … -08, -10, -11, -26 |
| e2e | 10 households × 1 day: every log `==` the offline log, 0 regressions, 0 rejected | AC-010-09 |
| fault | Kill of the server, the SDK service and `ingress-meters`, each with the trigger and the proof of the kill window; with its own ingress set (two reconnect attempts), a 30 s Restate outage that the containers survive without a restart, then a SIGTERM to one container's JVM from inside and its restart by the policy | AC-010-13 … -16 |
| by hand, output in the pull request | `make measure` in its three modes; `make spike`; `make reset` leaves no ingress container and no volume; after `make test-e2e`, `make report-coverage` lists `src/ems/actors/probe.py` with covered lines; the `force: false` check for an added and a removed handler | AC-010-06, -18 … -21, -23, -27, SC-004 |
| after the merge | The first workflow run on `main` writes `coverage.json`; the coverage badge shows the number | AC-010-29 |

## Docs and ADRs

No ADR (the stop rule would add one).

| File | Change |
|---|---|
| `docs/context/02-subsystem-actors.md` (new) | Registration (`force`, idempotence, the changed-handler case), the SDK service, the Kafka ingress (containers, groups, `earliest`, start order, restart policy, retry exhaustion), the probe and its log |
| `docs/context/README.md`, `.claude/skills/ems-context/SKILL.md` | 02 is no longer "planned" |
| `00-overview.md` | Status row for stage 2; decisions of 2026-11-14 and -15 (SDK pin, probe, skew, CI platform closed); repo map |
| `01-architecture.md` | Restate facts confirmed on the pinned versions; V1–V4 and V7–V11 resolved; ownership row; stage 3 brief refreshed |
| `05-deploy-and-infra.md` | Compose services and pins, memory limits, ports in use, the `actors` group, the version policy (no `exclude-newer`), coverage, the workflow and its pins |
| `07-design-principles.md`, `CLAUDE.md` | P12 and the "Do not" line: exact pins, newest release when pinned, no minimum age |
| `docs/context/README.md` | The note on when versions were checked |
| `.claude/skills/ems-plan-change/checklists.md` | The P12 question |
| `.pre-commit-config.yaml`, `infra/ems-gen.Dockerfile` | Comments that cite the two-week rule |
| `08-contracts.md` | The handoff facts under "Contract changes" |
| `09-risks-and-tech-debt.md` | The rows above |
| `11-experiments.md` | The measurements and the spike reproduction with result paths |
| `13-subsystem-ingest.md` | "What to build": stage 2 is built |
| `docs/thesis/spike-reproduction.md`, `docs/thesis/kafka-ingress.md` (new) | Numbers from the named result files; the provenance of the tracked `results.json` |
| `README.md` | Badge block; "Start the engine" with the commands; `make reset` wording; ingestion is closed after a tier run |
| `CLAUDE.md` | Commands table and test tiers |
| `.claude/rules/tests.md`, `.claude/rules/actors.md` | Restate is part of the stack; raw bytes are decided |
| `.claude/skills/ems-actor-contract/SKILL.md` | Handler checklist: Kafka-fed handlers take raw bytes and parse inside |
| `.claude/skills/ems-deploy-checklist/SKILL.md` | The image example, `force` defaults to true, both healthchecks |
| `.gitignore` | `/logs/`; drop the root spike line once the root copy is gone |

## Review

`architect-reviewer`, 2026-11-15: approve with changes; two blockers and ten smaller findings, all folded in.

| Finding | Change |
|---|---|
| The kill window could not be judged after a server kill, and a backfill of 4,000 records lasts seconds | Trigger and proof per kill type on data that survives the kill; the fault backfill is sized from the measured rate |
| The spike job started nothing before `experiments.py`, and its environment did not outlive one command | A virtualenv in the run directory, the README's bring-up before, `wipe` after, no bytecode written into `reference/` |
| Session-scoped fixtures could not serve the `make dev`, pre-publish and give-up tests | Module-scoped fixtures; one lifecycle module; own ingress sets for AC-010-11 and -16 |
| `make down` left test projects behind; `make reset` had no check | A label on the ingress services, removed by `down` and `reset`; unit test on the recipes; a check by hand |
| Progress and offline logs were filtered by topic prefix only | Filter by `run_id`; a fresh `run_id` per test and job run; write order stated |
| The pure check could raise on a valid-looking record | Both functions are total; property test |
| The offline fold saw offsets only, so `rejected` was not comparable | The offline side folds `log_apply` over the raw values |
| No test for coverage; the fault tier had no coverage file | Coverage file for every tier; unit test on the targets; a check by hand; `stop()` waits |
| The manifest called Docker and missed two packages; `logs/` made the commit look dirty | Image references parsed from the YAML; both packages added; `/logs/` ignored |
| Makefile variables duplicated the settings | Defaults from the `EMS_` variables; unit test |
| Tests missing for AC-010-05 (admin down), -25 and -26 | Added |
| Docs missing | `ems-actor-contract` skill, 13 and risk H1 added |
| Open: `depends_on` in the test project, six JVM containers, healthcheck without an HTTP client | `tests/compose.it.yml` resets `depends_on`; tiers and jobs close the stack's ingestion; the healthcheck uses bash's `/dev/tcp` |

A second review (2026-11-15) was folded in the same way:

| Finding | Change |
|---|---|
| `SUM` over the Utf8 column `value_utf8` may fail; `LIKE` treats `_` as a wildcard | `handled()` selects the values and sums in Python, filtered by service, key and `starts_with` |
| The SDK service should not run with `--reload` under coverage; killing by port scan is slower and flakier than by process group | The harness never uses `--reload`; `kill9()` kills the process group and checks the port afterwards |
| `CMD-SHELL` runs dash, which has no `/dev/tcp` | The ingress healthcheck calls bash explicitly |
| Emulated JVMs use more native memory than the heap suggests | Ingress containers start at 768 MiB; the measured peaks set the final limits |
| The badge job must not append commits | One fresh orphan commit, force-pushed |
| The ingress health path and consumer counts against partitions | The path is confirmed before the healthcheck is wired; consumers never exceed partitions (already planned) |

## Complexity Tracking

| Addition | Why it is needed | Simpler option not taken |
|---|---|---|
| Compose profile for the ingress containers | Ingestion must not start before a service is registered (K12) | Starting them with `make up`: a record could be skipped silently |
| A second compose project for tests | Tests must not write to the stack's topics, and need the real restart policy | Publishing to the stack's topics under a fresh `run_id`: invalid test records would stay where `make validate` reads, and the topics would be switched to the `sim` profile |
| Two measurement-only objects | V7 needs state of a chosen size with and without lazy state, reached by one-way calls | Serving them from the job: a job would get an HTTP surface (P10) |
| A `badges` branch written by CI | The coverage badge needs a place the README can read | A coverage service: a third party and a token |
