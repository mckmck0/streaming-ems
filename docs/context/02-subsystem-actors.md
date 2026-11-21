# 02 · Actors subsystem: Restate services, registration, Kafka ingress

## Current consensus

Built in stage 2 (spec 010): the Restate environment, registration and the Kafka ingress. Built in stage 3
(spec 011): the actors of `01-architecture.md` (Household, Tariff, Weather, NoBattery, SelfConsumption, Ems,
Battery, Meter) and their handlers, below. The ordering model is ADR 0017; late and missing inputs ADR 0018; run
parameters ADR 0019.

### Runtime and SDK service

- Restate server `1.8.1-hotfix.2` in compose (service `restate`, pins in `05-deploy-and-infra.md`): one node,
  node name `restate-1`, cluster `ems`, RocksDB memory 512 MiB (`RESTATE_ROCKSDB_TOTAL_MEMORY_SIZE`, confirmed
  with `restate-server --dump-config`), data on the named volume `restate-data`. Ingress `127.0.0.1:8080`,
  admin API and UI `127.0.0.1:9070`; the node port 5122 is not published. Healthy when the admin `/health` and
  the ingress `/restate/health` both answer 200.
- The SDK service is `ems.actors.app`, served by hypercorn on `127.0.0.1:9080` (`make dev`, with `--reload`).
  The Restate container reaches it as `host.docker.internal:9080` (checked on Docker Desktop 29.7.2 with the
  service bound to 127.0.0.1 only; `extra_hosts: host-gateway` covers Linux engines). Restate talks HTTP/2
  bidirectional streaming to it and negotiates service protocol 5–7 with `restate-sdk` 1.5.2.
- Default invocation retry policy of the server, as Restate reports it for every service: 500 ms initial
  interval, factor 2, at most 1 min between attempts, 70 attempts, then **pause** (a paused invocation waits for
  a manual resume, risk A7).

### Registration

- `make register` posts `{"uri": "http://host.docker.internal:9080", "force": false}` to `POST /deployments`
  (`curl -f`), then starts the ingress containers. The admin API's default for `force` is **true**
  (overwrite), so the Makefile always sends false.
- An unchanged deployment answers 201 the first time and 200 after, with the same deployment id; Restate stores
  the URI with a trailing slash. A failed discovery (no service at the URI) or an unreachable admin API makes
  `make register` exit non-zero before any ingress container starts.
- **Stale registrations (spec 011).** Removing a service means removing its deployment, and a deployment at the
  same URI keeps its old handlers under `force: false` (below), so a re-pointed ingress container could find no
  target handler and the integration would skip every record (risk K12). `make register` therefore checks, after
  the POST, that Restate lists `Household/on_measurement`, `Weather/on_forecast` and `Tariff/on_price`
  (`GET /services/<service>/handlers/<handler>`, 404 when missing) and stops before any ingress container starts;
  `make register FORCE=true` replaces the deployment in development. The test harness
  (`Restate.ensure_registered`) compares the handlers Restate lists for the eight services with those of
  `ems.actors.app` and, on an idle stack, registers again with `force: true`. Checked on 2026-11-21 on the
  pinned server: on a volume holding the spec 010 deployment, `make register` exited 2 before any ingress
  container started; the harness's `force: true` registration kept the deployment id and replaced its whole
  service set, so `Probe` and the state-cost objects were no longer listed.
- **Changed handlers at the same URI.** With `force: false`, Restate answers 200 and keeps the deployment's old
  handlers, without discovering again: an added handler is not registered and a removed one stays advertised
  (checked 2026-11-15 on 1.8.1-hotfix.2 with a throwaway `ForceCheck` object at `host.docker.internal:9091`:
  handlers a, b registered; adding c and then dropping b both answered 200 with a, b at revision 1; only
  `force: true` re-discovered, revision 2 with a). In development `make register FORCE=true` overwrites the
  deployment; Restate warns that this can leave in-flight invocations in an error state, so it is never used
  in an experiment. Stage 3, which adds handlers, re-registers with `FORCE=true` on an idle stack or registers a
  new URI.

### Kafka ingress integration

- Kafka reaches Restate only through the standalone Kafka ingress integration (image
  `ingress-integration-kafka` 0.1.1; Restate's in-server subscriptions are documented as old and are not used).
  One container per target handler, in the compose profile `ingress`:

  | Container | Topics | Consumer group | Handler | Consumers |
  |---|---|---|---|---|
  | `ingress-meters` | `ems.production.v1`, `ems.consumption.v1` | `ems-ingress-meters` | `Household.on_measurement` | 3 |
  | `ingress-forecasts` | `ems.weather.forecast.v1` | `ems-ingress-forecasts` | `Weather.on_forecast` | 3 |
  | `ingress-prices` | `ems.price.v1` | `ems-ingress-prices` | `Tariff.on_price` | 1 |

  Each container sets its own `RESTATE_RECORD_MAPPER_SERVICE` and `_HANDLER` (spec 011; until then all three
  targeted `Probe`).

- Settings that matter: `KAFKA_AUTO_OFFSET_RESET=earliest` (the integration sets none, and Kafka's default
  `latest` would skip what a new group finds in a topic); consumer instances never more than the partitions
  read; `restart: unless-stopped`; heap 256 MB in a 768 MiB container; a healthcheck on the metrics port 9464
  with bash's `/dev/tcp` (the image's `/bin/sh` is dash). The image is linux/amd64 only and runs emulated on
  Apple Silicon (risk K10); it bundles `kafka-clients` 4.2.0 (`/NOTICE`), which works against broker 4.3.1.
- **Start order.** `make up` never starts an ingress container; `make register` starts them after the
  registration succeeded; `make down` and `make reset` remove them, together with any container labelled
  `ems.role=ingress` (test projects included). A record whose target Restate rejects is skipped by the
  integration (`PartitionSession.kt`), so ingestion must not open before the service is registered (risk K12).
- **Delivery.** The integration sends each record as an invocation of the exclusive handler on the object whose
  key is the record key; `sys_invocation` shows them as `invoked_by = ingress`. The value is passed as raw
  bytes without a content type. The handler sees the headers `kafka.topic`, `kafka.partition`, `kafka.offset`,
  `kafka.timestamp` and `kafka.key`; other record headers are not forwarded (only `traceparent` and
  `tracestate`, as trace context). Restate deduplicates by consumer group, topic, partition and offset, and the
  integration commits a Kafka offset only after Restate's durable acknowledgement.
- **Outages.** While Restate is down the integration reconnects every 500 ms and never exits, whatever
  `RESTATE_RETRY_MAX_ATTEMPTS` says: each gRPC stream opens before it fails, and a successful open resets the
  back-off (tested in spec 010, AC-010-16); the records wait in Kafka. A process that exits for another reason
  is restarted by `restart: unless-stopped`. While the SDK service is down, records still flow into Restate and
  queue there as invocations per key, so Kafka's consumer lag says nothing about how far the handlers are behind
  (`docs/thesis/kafka-ingress.md`).
- **Dedup hazard (risk K11).** Because of that offset dedup, wiping Kafka alone or recreating a consumed topic
  would make new records look like ones Restate already has. `make reset` wipes both volumes together.
- **Bad records.** Kafka-fed handlers take raw bytes (`BytesSerde`, `accept="*/*"`), parse inside, count a record
  that is not valid JSON or breaks the v1 contract, and return. A handler that raised would be retried by Restate
  and hold its key (the SDK turns an input it cannot deserialise into a `TerminalError`, `handler.py`).

### The actors (stage 3, spec 011)

**One shape for every handler.** An actor file holds its state dataclass and its pure transitions
`(params, state, message) -> (state, sends)`, which import no Restate, and short handlers that call
`ems.actors.shell`: read own state, call the transition, write own state, send. Sends are `Send(service,
handler, key, message)` values delivered with `ctx.generic_send`, addressed by service name, so a household routes
steps to the strategies named in its parameters and no actor file imports another. Why: every transition can be
tested without a server (and as an in-memory network with any delivery order, `tests/network.py`), and the
handlers stay a few lines each.

| Actor | Key | Exclusive handlers | Shared handler (returns `state`) |
|---|---|---|---|
| `Household` | `[run_id:]location_id` | `configure`, `on_measurement` (Kafka), `on_prices`, `on_forecast` | `status` |
| `Tariff` | `[run_id:]market_key` | `on_price` (Kafka), `subscribe` | `status` |
| `Weather` | `[run_id:]location_id` | `configure`, `on_forecast` (Kafka) | `status` |
| `NoBattery`, `SelfConsumption` | `[run_id:]location_id` | `configure`, `on_step` | `summary` |
| `Ems` | `[run_id:]location_id` | `configure`, `on_step`, `on_ack` | `status` |
| `Battery` | `[run_id:]location_id` | `configure`, `apply` | `state` |
| `Meter` | `[run_id:]location_id` | `configure`, `on_flows` | `bill` |

- **State keys.** `params` (the `RunParams` of `configure`), `state` (one frozen dataclass), and on Kafka-fed
  objects `log` (the ingress log of `ingress_check.log_step`, compared with the offline fold), `n` (records
  handled, written last) and `n_at_configure` (records that arrived before `configure`; the harness checks 0).
- **Serialisation.** State, parameters and messages are frozen dataclasses encoded by one serde over
  `pydantic.TypeAdapter` (`ems.actors.serde`): floats round-trip bit for bit. No stored or sent type contains a
  set: Restate compares a retried handler's writes with its journal (RT0016), and a set's JSON order depends on
  the hash seed (`National.holidays` became a sorted tuple for this). Tests check both.
- **Configuration** (ADR 0019). `configure` stores the parameters once; an equal second call changes nothing,
  other parameters or another key fail the call without a retry (`TerminalError`). An internal message at an
  unconfigured object fails the same way, and the key goes on with its next invocation.
- **Kafka-fed handlers** take raw bytes (`BytesSerde`, `accept="*/*"`), parse once, update `log`, run the
  transition only on a configured object (`Tariff`, keyed by market, starts from a default state), and never
  raise on the content of a record: the mapping (`ems.assembler.mapping`) turns what it cannot use into a
  counter.
- **Ordering between actors.** Every receiver applies messages through `ems.actors.reorder`: steps, commands,
  acknowledgements and flows by `t`, price and forecast updates by `seq`; early ones are held, stale ones
  dropped, both counted outside the compared state.
- **Unmanaged sites** are never configured: their `Household` and `Weather` keep only `log` and `n`.

The stage 2 `Probe` and the measurement-only `StateCostEager` and `StateCostLazy` were deleted in stage 3; their
results stay in `docs/thesis/kafka-ingress.md` (reproducing the state-cost numbers needs the spec 010 commit).

### Test projects and jobs

`src/ems/experiments/stack.py` runs a test-scoped set of the same three ingress services as a compose project
`ems-it-<prefix>` (infra/docker-compose.yml plus tests/compose.it.yml) on the stack's network, with topic names
and groups prefixed through `EMS_INGRESS_PREFIX`. Tests, `make measure` and `make spike` never write to the
stack's topics. A Restate tier or job stops the stack's own ingress containers and leaves ingestion closed;
`make register` opens it again.

Since stage 3 a run is configured before it is published (`Restate.configure` with the records of
`ems.experiments.runs.managed_params`), and `Restate.wait_run` returns once every record is handled and every
strategy, `Ems`, `Battery` and `Meter` expects the step after the last one the offline side released
(`offline_run`, `run_ends`). It fails at once on a failed or paused invocation and, unless a fault test asks
otherwise, on an invocation still failing after four attempts: the first e2e runs on the pinned stack showed
HTTP/2 transport errors between Restate and the SDK service (`h2 pool connection error: stream closed because of
a broken pipe`) on single attempts, which Restate retried with success (risk K14). `make measure` has two modes: `skew` (configured households: leads per stream
pair, prices included, and the re-order counters of every receiving service) and `backlog`.

### How R1–R7 hold (stage 3)

- R1, R2: every edge between actors is a one-way `generic_send`, including the two cycles (Household and Tariff;
  Ems and Battery). Request-response is used only from outside (tests, jobs) to the shared handlers. A test
  scans `src/ems/actors/` for `object_call`, `service_call` and `generic_call`.
- R3, R5: handlers make no external call, read no clock and never wait: waiting is buffering in state until the
  watermarks pass. Stored bytes are deterministic (no sets), so a retry matches its journal.
- R4: Restate's offset dedup at the ingress; the slot table and the sparse streams drop a record published twice;
  `configure` carries an `Idempotency-Key`; a handler's own sends are deduplicated by Restate.
- R6: no timer and no self-rescheduling chain yet (the live deadline is stage 4).
- R7: every message carries `schema_version` and `t` (the simulated time), updates also `seq`; receivers apply
  them in order through `ems.actors.reorder`.

## What to build

- Built: stage 3 (spec 011). Stage 4 adds the live deadline (`Household.on_deadline`) and adding a strategy at
  run time (`Household.set_strategies`).

## Prerequisites

- Spec 010 (this environment); spec 009 (the domain core the handlers call); spec 011 (the actors).

## Open questions

- None owned by this doc. The VERIFY items of stage 2 are resolved in `01-architecture.md`.
