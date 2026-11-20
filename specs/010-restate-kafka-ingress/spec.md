# Feature Specification: Restate environment and Kafka into a Virtual Object

**Feature Branch**: `010-restate-kafka-ingress` · **Created**: 2026-11-14 · **Status**: Approved 2026-11-15,
revised 2026-11-15 (version policy, below; AC-010-16 after the first fault run)

**Input**: The stage 2 brief in `docs/context/01-architecture.md`. User decisions of 2026-11-14: the probe lives
in `src/` and its check stays in the real handlers from stage 3; the skew is measured per household in data time;
test coverage, a GitHub Actions workflow and README badges are part of this spec. User decision of 2026-11-15:
the two-week minimum age for pinned versions is dropped, because security fixes come out often; a new pin takes
the newest release. This replaces the earlier choice of `restate-sdk` 1.2.0, which only that rule required.

## Goal

Run Restate next to Kafka on pinned versions, deliver the four topics into a Virtual Object through the Kafka
ingress integration, and produce the evidence the actor stages rest on:

1. each key's records arrive in partition order, none lost or doubled, also after `kill -9` of the server, the
   SDK service or an ingress container;
2. the skew between a household's streams during a backfill, measured and recorded for the thesis;
3. the reference spike's experiments A, B and C reproduced on the pinned server;
4. V1–V4 and V7–V11 of `01-architecture.md` resolved with value, source and date.

It also adds the e2e and fault test tiers, coverage, a CI workflow and README badges.

## Decisions

| Item | Decision |
|---|---|
| Version policy | Images and tools by exact tag plus digest, actions by commit SHA, Python packages through `uv.lock`, as before; a new or bumped pin takes the newest release, with no minimum age. `[tool.uv] exclude-newer` is removed. The rule is written into `CLAUDE.md`, P12 in `07-design-principles.md`, 05 and the harness. Pins this spec does not add are refreshed in a separate change |
| Server | `restatedev/restate:1.8.1-hotfix.2-20261102` by digest, the newest release (2026-11-02). Restate's weekly rebuilds cover the 1.8.1 and 1.7.14 lines, not the hotfix |
| Kafka ingress integration | `ghcr.io/restatedev/ingress-integration-kafka:0.1.1-20261109` by digest, the newest build of the newest release. The image is linux/amd64 only and runs emulated on Apple Silicon |
| Python | `restate-sdk` 1.5.2 and `hypercorn` 0.18.0, exact, in a new `actors` dependency group; `pytest-cov` and `coverage` in the dev group at their newest releases. The packages already locked are not upgraded |
| Kafka-fed handlers | Take raw bytes, parse inside, count a bad record in state and return (V3) |
| Ingress containers | One per target handler (meters, forecasts, prices): a fixed consumer group, `auto.offset.reset=earliest`, `restart: unless-stopped`, a memory limit and a bounded heap. They sit in a compose profile: only `make register` starts them, and `make down` and `make reset` remove them |
| SDK service | A host process on `127.0.0.1:9080` (`make dev`), registered as `host.docker.internal:9080` with `force: false` |
| Probe | `Probe` in `src/ems/actors/probe.py`, part of `ems.actors.app`. Its state has two parts. The **log** (per topic: record count, last offset, digest of the offset sequence, regressions; per key: rejected records) does not depend on how topics interleave; one pure function computes it and it is compared with `==`. The **leads** depend on arrival order across topics; they are measured and never compared. Stage 3 points the ingress containers at `Household`, `Weather` and `Tariff`, deletes `Probe` and keeps the log function in those handlers |
| Skew | For one key, the lead of stream a over stream b is the newest `available_at` handled from a minus that from b, in quarter-hours. The probe keeps the largest lead per pair. Prices arrive under the market key, so their skew is first measured in stage 3 at `Household` |
| State cost | `StateCostEager` and `StateCostLazy`, two measurement-only objects in the app for this stage; stage 3 removes them with `Probe` |
| Test isolation | Tests create topics under a fresh prefix and feed them through their own ingress containers (the stack's service definitions with the topic list and the group id overridden). They never write to the stack's topics |
| Spike | Tracked byte-identical under `reference/restate-ems-spike/`: `README.md`, `ctl.sh`, `experiments.py`, `loadgen.py`, `rerun.py`, `requirements.txt`, `package.json`, `package-lock.json`, `ems_spike/__init__.py`, `ems_spike/model.py`, `ems_spike/restate_app.py`, `ems_spike/extra_app.py`, `tools/killport.py`, `results.json`, `results_run1.json`, `guard_test.txt`. No virtualenv, `node_modules`, server data or logs |
| Reproducing A–C | Unedited spike files; the packages of the spike's `requirements.txt` in an isolated environment, frozen into the run's manifest; a fresh Restate instance of the pinned image, separate from the stack's; only the `ctl.sh` verbs supplied from outside `reference/`, with the `register` verb replacing `127.0.0.1` by `host.docker.internal`. If a spike file would have to change, the experiment counts as not reproduced and the stage stops for a decision |
| CI and badges | GitHub Actions runs `make ci`. On `main` it writes the coverage number to a `badges` branch, which the README badge reads. This closes the OPEN "CI platform" |

## How each VERIFY item is resolved

| Item | Resolved by |
|---|---|
| V1 order per key, also across restarts | AC-010-09, -13, -14, -15 |
| V2 failing handler, invalid payload, Restate down | Invalid payload: AC-010-10. Handler unavailable: AC-010-14 and -21 (the integration sees only Restate's acknowledgement); a handler that keeps raising holds its own key, cited from Restate's retry-policy docs. Restate down: AC-010-13, -16 |
| V3 undeserialisable input | The source line of the pinned SDK, and AC-010-10 |
| V4 image pin | AC-010-02 |
| V7 state cost, lazy state | AC-010-20; the option from the SDK source and docs |
| V8 backlog | AC-010-21 |
| V9 `ctx.time()`, compatibility, memory | The SDK source line; the protocol versions in the registration response (AC-010-03); AC-010-19 |
| V10 restart after `kill -9` | AC-010-13, -24 |
| V11 Kafka client against broker 4.3.1 | AC-010-09; the client version from the image's NOTICE file |

## Scope

In scope:
- Compose: `restate` and three ingress containers; `make register`, `make dev`, `make test-e2e`,
  `make test-fault`, `make report-coverage`, `make spike`, `make measure`; `make down` and `make reset`.
- `src/ems/actors/`: the app, `Probe`, the pure ingress check, the two state-cost objects; Restate URLs in
  `ems.settings`.
- Tests in the unit, property, integration, e2e and fault tiers; the measurement and spike jobs in `jobs/`.
- The spike moved to `reference/`, excluded from `ty` as it is from ruff; `.github/workflows/ci.yml`; the
  README badge block.
- Docs: a new `02-subsystem-actors.md` (registration and Kafka ingress) with its rows in
  `docs/context/README.md` and the `ems-context` skill; 00 (status, decisions); 01 (Restate facts, VERIFY list,
  ownership row, stage 3 brief); 05 (pins, ports, CI, version policy); 07 (P12); 08 (handoff: raw bytes,
  headers); 09; 11; `docs/thesis/spike-reproduction.md` and `docs/thesis/kafka-ingress.md`; the README;
  `CLAUDE.md`; `.claude/rules/tests.md` and `actors.md`; the `ems-deploy-checklist`, `ems-actor-contract` and
  `ems-plan-change` skills; the comments on pins in `.pre-commit-config.yaml` and `infra/ems-gen.Dockerfile`.

Out of scope:
- The actors of `01-architecture.md`, the assembler and watermarks, timers, strategies (stages 3 to 6).
- The spike's experiments D, D2 and E; recovery latency by phase and the export CLI (stage 4).
- Running the integration, e2e and fault tiers in CI; tracing; Helm.
- Refreshing the pins this spec does not add (Kafka, kafka-ui, the generator's base images, the locked Python
  packages) to their newest releases: a separate change.
- Any change to wire contract v1, the topics or the generator.

## Affected

- Anyone running the local stack: `make up` now also starts Restate, and ingestion opens with `make register`,
  again after every `make down`.
- `make reset` deletes the Restate volume together with the Kafka volume.
- The first `make register` on a stack with data ingests everything already in the four topics.
- No wire contract, topic, key or generator change. No reported number changes.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - A pinned Restate environment (Priority: P1)
One command starts Kafka and Restate; a second registers the SDK service and opens ingestion.
**Acceptance**: AC-010-01 … -07.

### User Story 2 - Four topics into a Virtual Object (Priority: P1)
Every record of the four topics reaches the probe under its key, in partition order.
**Acceptance**: AC-010-08 … -12.

### User Story 3 - Exactly once under failures (Priority: P1)
Killing the server, the SDK service or an ingress container loses nothing and doubles nothing.
**Acceptance**: AC-010-13 … -16.

### User Story 4 - Measurements for the thesis (Priority: P2)
The skew, the memory of the stack, the cost of object state and the place of a backlog are measured and stored.
**Acceptance**: AC-010-17 … -21.

### User Story 5 - The reference spike on the pinned server (Priority: P1)
The spike is tracked unedited and its A, B and C pass on Restate 1.8.1-hotfix.2.
**Acceptance**: AC-010-22 … -25.

### User Story 6 - Test tiers, coverage, CI and badges (Priority: P2)
The e2e and fault tiers exist, coverage is reported, every push runs `make ci`, and the README shows the state.
**Acceptance**: AC-010-26 … -31.

### Edge Cases
- Records published while no ingress container runs wait in Kafka (AC-010-11).
- A duplicate published by `--faults` is two Kafka records; both are delivered. Dropping it is stage 3.
- Records of unmanaged sites reach the probe like any other record.
- Wiping Kafka or recreating a consumed topic without resetting Restate would make new offsets look like
  duplicates, because Restate deduplicates by consumer group, topic, partition and offset. `make reset` wipes
  both volumes; the hazard is documented.
- An SDK service that stays down keeps its invocations queued in Restate; after the retry policy is exhausted
  they pause and need a manual resume. Documented, not tested here.
- After a restart of Docker the ingress containers come back by their restart policy. That is safe: the
  registration lives in Restate's volume.

## Requirements *(mandatory)*

- **FR-001**: The stack MUST run Restate on pinned images with idempotent registration. → AC-010-01 … -07
- **FR-002**: Kafka MUST reach a Virtual Object only through the Kafka ingress integration, one container per
  target handler, and only after a registration. → AC-010-08, -11
- **FR-003**: Per-key partition order and exactly-once delivery MUST hold, with and without failures.
  → AC-010-09, -13 … -16
- **FR-004**: A bad record MUST be counted and MUST NOT block its key. → AC-010-10
- **FR-005**: The ingress log MUST be a pure function shared with the offline side. → AC-010-12
- **FR-006**: The skew, memory, state cost and backlog MUST be measured into result files. → AC-010-17 … -21
- **FR-007**: The spike MUST be tracked unedited and reproduced on the pinned server. → AC-010-22 … -25
- **FR-008**: The e2e and fault tiers, coverage, CI and badges MUST exist. → AC-010-26 … -31
- **FR-009**: The VERIFY items and docs MUST be resolved and written. → SC-001, SC-002

### Acceptance criteria (EARS)

Environment
- **AC-010-01**: WHEN `make up` returns, THE SYSTEM SHALL have the services `kafka` and `restate` healthy, every
  published Restate port (ingress 8080, admin 9070) bound to 127.0.0.1, and no ingress container running.
- **AC-010-02**: THE SYSTEM SHALL reference the Restate server and the Kafka ingress integration by exact tag
  plus `@sha256:` digest, lock `restate-sdk` 1.5.2 and `hypercorn` 0.18.0 exactly in `uv.lock`, and set no
  `[tool.uv] exclude-newer`.
- **AC-010-03**: WHEN `make dev` serves `ems.actors.app` and `make register` runs, THE SYSTEM SHALL register that
  deployment with `force: false`, list the service `Probe` in Restate's admin API, and then have the three
  ingress containers in state `running`.
- **AC-010-04**: WHEN `make register` runs again with nothing changed, THE SYSTEM SHALL keep the same deployment
  id and exit 0.
- **AC-010-05**: IF `make register` cannot reach the SDK service or the Restate admin API, THEN THE SYSTEM SHALL
  exit non-zero without starting an ingress container.
- **AC-010-06**: WHEN the stack is stopped with `make down` and started again with `make up`, THE SYSTEM SHALL
  still hold the registered deployment and the state of every object; after `make down` or `make reset` no
  ingress container SHALL exist.
- **AC-010-07**: WHEN `make dev` runs with its defaults, THE SYSTEM SHALL listen on 127.0.0.1:9080 and on no
  other address.

Kafka into a Virtual Object
- **AC-010-08**: THE SYSTEM SHALL define one ingress container per target handler: `ems.production.v1` and
  `ems.consumption.v1` to `Probe.on_measurement`, `ems.weather.forecast.v1` to `Probe.on_forecast` and
  `ems.price.v1` to `Probe.on_price`, each with a fixed consumer group id and starting at the earliest offset of
  a topic its group has not read; WHEN a record is published to one of these topics, THE SYSTEM SHALL invoke
  that exclusive handler on the object whose key equals the record key.
- **AC-010-09**: WHEN a backfill of 10 households × 1 day has been published and the probe has counted every
  published record, THE SYSTEM SHALL hold for every record key and topic a log that equals with `==` the log
  the same pure function gives offline for the records read back from Kafka in partition order, with 0
  regressions; the test fails if the count is not reached within its timeout.
- **AC-010-10**: IF a record's value is not valid JSON or fails the v1 contract, THEN THE SYSTEM SHALL count it
  as rejected in that key's state, complete the invocation without an error, and handle the later records of
  the same key and partition in order.
- **AC-010-11**: WHILE no ingress container runs, THE SYSTEM SHALL consume no record from the topics; WHEN the
  ingress containers then start, THE SYSTEM SHALL handle each record published in the meantime exactly once.
- **AC-010-12**: THE SYSTEM SHALL compute the log with a pure function (no Restate import, no clock, no I/O)
  that the `Probe` handlers and the offline comparison share.

Failures (each kill lands after the first and before the last record of the backfill has been handled; a test
whose kill lands outside that window fails)
- **AC-010-13**: WHEN the Restate server is killed with SIGKILL during a backfill and started again with its
  configuration unchanged, THE SYSTEM SHALL become healthy without a manual step and end with every log equal
  (`==`) to the offline log and 0 regressions.
- **AC-010-14**: WHEN every process of the SDK service (each listener on port 9080) is killed with SIGKILL
  during a backfill and the service is started again, THE SYSTEM SHALL end with every log equal (`==`) to the
  offline log and 0 regressions.
- **AC-010-15**: WHEN an ingress container is killed with SIGKILL during a backfill and started again, THE
  SYSTEM SHALL end with every log equal (`==`) to the offline log and 0 regressions.
- **AC-010-16** (revised 2026-11-15, see below): WHEN Restate stays unreachable for longer than an ingress
  container's reconnect budget (`RESTATE_RETRY_MAX_ATTEMPTS`), THE SYSTEM SHALL keep the container running and
  reconnecting and, once Restate is back, end with every log equal (`==`) to the offline log; IF an ingress
  container exits on its own, THEN THE SYSTEM SHALL start it again through its restart policy, and records
  published afterwards SHALL be handled exactly once (fault tier).

  *Revision.* The criterion first read "IF Restate stays unreachable until an ingress container … gives up and
  exits, THEN …". On the pinned integration (0.1.1) that never happened: with Restate stopped, the containers
  reconnected every 500 ms for over ten minutes with a budget of two attempts and did not exit, because each
  gRPC stream opens before it fails and a successful open resets the back-off (`PartitionSession.kt`). The
  documented exit after the budget is therefore not what protects the ingress during a Restate outage; the
  restart policy still covers a process that exits (start-up failures, crashes), and the revised criterion
  tests both. Exactly-once delivery, the stop rule's concern, is unaffected.

Measurements
- **AC-010-17**: WHEN a record is handled, THE SYSTEM SHALL update for its key the newest `available_at` handled
  per stream and the largest lead of each stream over each other stream of that key, in quarter-hours.
- **AC-010-18**: WHEN `make measure` runs an unpaced backfill and every published record has been handled, THE
  SYSTEM SHALL write to `experiments/results/ingress-<UTC>/` the largest lead per key and pair, their median,
  95th percentile and maximum across keys, the records handled per second between the first publish and the
  last handled record, and the run's config, seed, data window, hardware, image digests, package versions and
  git commit.
- **AC-010-19**: WHEN that run ends, THE SYSTEM SHALL record in the same directory the peak memory of every
  container of the stack and whether any was OOM-killed.
- **AC-010-20**: WHEN the state-cost mode runs, THE SYSTEM SHALL fill one key of `StateCostEager` and of
  `StateCostLazy` (lazy state, `state_preload=False`; the SDK marks `enable_lazy_state` deprecated) to 1 kB,
  100 kB and 1 MB through a one-way `pad` call, send one-way
  `touch` calls that read and write one small key, and record in the result directory the mean, median and
  99th percentile of the invocation time for each size and variant.
- **AC-010-21**: WHEN the backlog mode publishes a backfill while the SDK service is stopped, THE SYSTEM SHALL
  record in the result directory, over time, the consumer lag of each ingress group and the invocations pending
  in Restate, until the service is back and every record has been handled.

Spike
- **AC-010-22**: THE SYSTEM SHALL keep under `reference/restate-ems-spike/` exactly the files listed under
  Decisions; IF the SHA-256 of one differs from the hash recorded in `tests/fixtures/spike-files.sha256` when
  the files were moved, or a file is missing or added, THEN a unit test SHALL fail.
- **AC-010-23**: WHEN `make spike` runs, THE SYSTEM SHALL run the spike's experiments A, B and C from those
  files against a fresh Restate instance of the pinned server image, with the packages of the spike's
  `requirements.txt` in an isolated environment, and with
  these verbs supplied from outside `reference/`: `kill-server` sends SIGKILL to the server, `kill-svc1` and
  `kill-svc2` send SIGKILL to every process listening on 9080 and 9081, and each verb logs what it killed.
- **AC-010-24**: WHEN that run ends, THE SYSTEM SHALL write the spike's `results.json`, the verb log and a
  manifest (hardware, image digest, resolved packages, git commit) to `experiments/results/spike-<UTC>/`, and
  SHALL exit 0 only if A and C report 0 mismatches and 0 meter-against-SelfConsumption mismatches, B reports 0
  mismatches with 96 events in every strategy state, and the verb log confirms that B killed the SDK service
  and the server.
- **AC-010-25**: IF port 8080, 9070, 9080 or 9081 is in use when `make spike` starts, THEN THE SYSTEM SHALL exit
  non-zero before starting anything.

Tiers, coverage, CI and badges
- **AC-010-26**: WHEN `make test-integration`, `make test-e2e` or `make test-fault` runs, THE SYSTEM SHALL start
  its own SDK service and register it, create its topics under a fresh prefix, feed them through its own
  ingress containers and publish nothing to the unprefixed topics; IF port 9080 is already in use, THEN it
  SHALL fail before the first test with a message that says so.
- **AC-010-27**: WHEN a tier runs through its `make` target, THE SYSTEM SHALL write that tier's coverage data
  under `logs/`, and `make report-coverage` SHALL combine the files present into `logs/coverage.xml` and print
  the total.
- **AC-010-28**: WHEN a commit is pushed to `main` or a pull request is opened or updated, THE SYSTEM SHALL run
  `make ci` in GitHub Actions on a fixed runner image, with every action pinned by commit SHA, read-only
  default permissions and no secret besides the built-in token.
- **AC-010-29**: WHEN that workflow succeeds on `main`, THE SYSTEM SHALL write the total line coverage of the
  unit and property tiers to `coverage.json` on the `badges` branch, from a job that alone holds
  `contents: write` and uses no third-party service.
- **AC-010-30**: THE SYSTEM SHALL show at the top of `README.md` badges for the CI status, the coverage, the
  required Python version, uv, Ruff, ty, the Restate server version and the Kafka version.
- **AC-010-31**: IF the Restate or Kafka version on a badge differs from the image tag in
  `infra/docker-compose.yml`, THEN a unit test SHALL fail.

## Constraints (from the checklists)

- **P1**: spec, plan and tasks are approved before code; each commit cites a task.
- **P2**: `src/ems/domain/` is untouched. `Probe` handlers parse, call the pure check, write their own state
  and send nothing.
- **P3**: `Probe` is the only writer of the ingress log and the stream leads; the ownership table in 01 gets
  the row, and stage 3 moves it to the Kafka-fed actors.
- **P4, R1, R2**: no object of this stage has an edge to another actor. Exclusive handlers are reached by Kafka
  or by one-way calls; `status` and `count` are shared and called only from outside.
- **P5, R3, R5**: handlers make no external call, read no clock and never wait.
- **P6, R4**: Restate deduplicates redelivered records by consumer group, topic, partition and offset; no
  idempotency key is taken from a record. The e2e and fault tests compare the log with `==` against the offline
  fold; the leads are never part of a comparison.
- **P7**: no timer. The skew is in data time; wall-clock numbers exist only in the jobs under `jobs/`.
- **P8**: not touched.
- **P9**: no wire contract or topic change and no message between actors. The probe's state is not a contract.
- **P10**: the measurement and spike jobs are CLIs in `jobs/` with no HTTP surface of their own.
- **P11**: Python code reads the Restate URLs through `ems.settings` (`EMS_` prefix). The workflow uses no
  secret.
- **P12**: every image by tag and digest, every action by commit SHA, each the newest release when pinned
  (user decision 2026-11-15); `uv lock --check` stays green.
- **P13**: no tracing is switched on.
- **P14, P16**: each result directory stores config, seed, window, hardware, versions and commit, and reports
  records per second and memory beside the skew.
- **P15**: docs land with the change. No ADR is needed unless the stop rule fires.
- **Stop rule**: IF AC-010-09, -10 or -13 … -16 cannot be met on the pinned versions, the stage stops and the
  way forward is re-planned through an ADR. Nothing is built on Restate's in-server Kafka subscriptions.
- **Risks** (`09`): K6 is narrowed by the tests of this spec; A1 gets its first numbers (skew, V7); A5 is
  resolved by AC-010-13 and the spike's B (V10). New rows: the integration image is amd64 only and runs
  emulated on Apple Silicon; Restate's offset dedup makes a Kafka-only reset unsafe; records for an unregistered
  service are skipped, hence the start order; new pins take the newest release with no waiting period.

## Success Criteria

- **SC-001** (review): V1–V4 and V7–V11 in `01-architecture.md` each carry a value, a source and the date. A
  value measured in this stage names its test or result file; a value read from source code or docs cites the
  file and line or the URL.
- **SC-002** (review): every doc listed under Scope matches the built state; `02-subsystem-actors.md` is
  indexed; the stage 3 brief is refreshed and carries the re-pointing of the ingress containers, the kept log
  function, what `force: false` returns for a changed handler set, and the price skew; the two files in
  `docs/thesis/` quote only numbers from named result files; the README's "Start the engine" section holds the
  commands.
- **SC-003**: `make ci` is green locally and in GitHub Actions; `make test-integration`, `make test-e2e` and
  `make test-fault` are green on the author's machine, with their output in the pull request.
- **SC-004**: `make spike` exits 0 and its result directory is named in `docs/thesis/spike-reproduction.md`.

## Assumptions

- The emulated integration image is fast and stable enough for the tests on the author's Mac. If it is not, the
  stage stops and re-plans; no arm64 image is built locally without a decision.
- A container reaches a host service bound to 127.0.0.1 through `host.docker.internal` on Docker Desktop.
  `plan.md` confirms it on the author's machine; if it does not hold, AC-010-07 is amended before the tasks.
- The spike's SDK 1.0.5 runs against server 1.8 (the SDK's compatibility table says so).
- The unit and property tiers pass on the Linux runner as they do on macOS. *Did not hold (2026-11-16): three
  golden digests of the generator differ on Linux x86_64 and arm64, so CI runs on macOS arm64 (risk G12).*

## Open questions

Decided in `plan.md`:
- the size of the backfills for the fault tests and for `make measure`, and the number of `touch` calls;
- the final memory limits, after the first measurement, and whether the test tiers pause the stack's own
  ingress containers while they run;
- the healthcheck of each new container;
- how the tests create their ingress containers so that they keep the restart policy;
- where the invocation times of AC-010-20 come from;
- the exact pins of the workflow's actions and runner image;
- whether one test recreates a consumed topic to confirm the dedup hazard, or the docs cite the source only.
