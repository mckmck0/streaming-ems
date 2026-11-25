# 02 · Actors subsystem: Restate services, registration, Kafka ingress

## Current consensus

Built in stage 2 (spec 010): the Restate environment, registration and the Kafka ingress. Built in stage 3
(spec 011): the actors of `01-architecture.md` (Household, Tariff, Weather, NoBattery, SelfConsumption, Ems,
Battery, Meter) and their handlers, below. The ordering model is ADR 0017; late and missing inputs ADR 0018; run
parameters ADR 0019.

### Runtime and SDK service

- Restate server `1.8.1-hotfix.2` in compose (service `restate`, pins in `05-deploy-and-infra.md`): one node,
  node name `restate-1`, cluster `ems`, memory pools sized to fit its 2 GiB container (RocksDB 512 MiB, invoker
  512 MiB, query engine 384 MiB, Bifrost record cache 128 MiB; spec 012, confirmed with
  `restate-server --dump-config`), data on the named volume `restate-data`. Ingress `127.0.0.1:8080`,
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
handler, key, message, delay_s=None)` values delivered with `ctx.generic_send` (with `send_delay` when
`delay_s` is set; only the live deadline uses it), addressed by service name, so a household routes
steps to the strategies named in its parameters and no actor file imports another. Why: every transition can be
tested without a server (and as an in-memory network with any delivery order, `tests/network.py`), and the
handlers stay a few lines each.

| Actor | Key | Exclusive handlers | Shared handler (returns `state`) |
|---|---|---|---|
| `Household` | `[run_id:]location_id` | `configure`, `on_measurement` (Kafka), `on_prices`, `on_forecast`, `on_deadline` (live), `set_strategies` | `status` |
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
  the hash seed (`National.holidays` became a sorted tuple for this). Tests check both. JSON has no NaN or
  infinity, so the serde refuses a value that does not decode back equal to itself, before it is stored or sent
  (spec 012): the invocation fails with a retryable error and pauses where the value was produced, and
  `shell._commit` encodes every message before it writes the state.
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
- **Live deadline** (spec 012, ADR 0020). `Household.on_deadline(Deadline(t))` is the only handler that reads the
  clock: `shell.handle_timed` takes `await ctx.time()` (journaled, so a retry sees the same value) and passes it to
  the pure transition, which releases the due steps and returns the next `Deadline` as a delayed self-send. In
  sim no `Deadline` is ever sent. A unit test allows `ctx.time` and `send_delay` only in this path.
- **Strategy changes** (spec 012, ADR 0021). `Household.set_strategies(StrategyChange(t, strategies))`, sent by the
  operator through HTTP ingress with an `Idempotency-Key`, accepts or refuses the change (`ems.assembler.schedule`)
  and records it in `state.schedule`. The runner configures an added strategy object with
  `strategy_params(params, change)` before it sends the change, and checks the stored parameters.
- **Timing log** (spec 012, ADR 0022). With `EMS_TIMING_LOG=<path>` set, `ems.actors.timing` writes one JSON line
  per handler commit at a configured object: host time in ns, service, handler, key, invocation id, the `t` of the
  message handled and of each message sent. It makes no `ctx` call and writes nothing to state, messages or the
  journal; a queue thread writes the file, and atexit and a chained SIGTERM handler flush it. A retried
  invocation writes its line again: the analysis keeps the first line per invocation id and counts the rest as
  retries. A `kill -9` loses the lines still queued.

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

The experiment runner (`make exp`, spec 012) uses the same harness on a fresh stack per run (`reset_stack`
removes the Kafka and Restate volumes and starts them with journal retention `0s`), with ingress projects named
`ems-exp-<prefix>` so the test cleanup, which removes `ems-it-*`, never touches them. `Restate.wait_run` and
`wait_handled` treat connection errors and 5xx answers as transient for 60 s after a server kill
(`Restate.note_kill`) and raise any other error at once.

### Operations: paused invocations (spec 012)

An invocation that keeps failing pauses after the retry policy's 70 attempts (risk A7), for example when the SDK
service stays down for about an hour or a handler keeps raising. It waits for a manual resume.

- List them: `curl -s 127.0.0.1:9070/query --json "{\"query\": \"SELECT id, target_service_name,
  target_service_key, target_handler_name, last_failure FROM sys_invocation WHERE status = 'paused'\"}"`.
- Resume one through the admin API: `curl -X PATCH 127.0.0.1:9070/invocations/<id>/resume`. To resume it on a
  newer deployment after a code fix, add `?deployment=latest` or `?deployment=<deployment id>`
  (<https://docs.restate.dev/services/invocation/managing-invocations> and the admin API reference "Resume an
  invocation", read 2026-11-25).
- The CLI ships in the server image (`restate-cli` 1.8.1-hotfix.2, checked 2026-11-22):
  `docker exec -it ems-restate-1 restate invocations list --status paused`,
  `docker exec -it ems-restate-1 restate invocations resume <id> [--deployment latest]`. Pipe JSON from
  `docker exec` without `-t` (for example `docker exec ems-restate-1 restate sql --json "…"`): with `-t` the
  statistics line and `\r\n` land on stdout.
- A resume on a new deployment replays the journal against the new code. If the fix changes a journaled
  operation before the one that failed, the replay fails with RT0016 ("journal mismatch"). Then kill the
  invocation and redo its effect in a new run (a new `run_id`).

### How R1–R7 hold (stages 3 and 4)

- R1, R2: every edge between actors is a one-way `generic_send`, including the two cycles (Household and Tariff;
  Ems and Battery). Request-response is used only from outside (tests, jobs) to the shared handlers. A test
  scans `src/ems/actors/` for `object_call`, `service_call` and `generic_call`.
- R3, R5: handlers make no external call and never wait: waiting is buffering in state until the watermarks
  pass. Only `Household.on_deadline` reads the clock, through the journaled `ctx.time()`. Stored bytes are deterministic (no sets), so a retry matches its journal.
- R4: Restate's offset dedup at the ingress; the slot table and the sparse streams drop a record published twice;
  `configure` carries an `Idempotency-Key`; a handler's own sends are deduplicated by Restate.
- R6: the live deadline is the only self-rescheduling chain. Household stores the step it expects; a
  `Deadline` for another step is stale, counted and dropped; an early one is set again for the same step.
- R7: every message carries `schema_version` and `t` (the simulated time), updates also `seq`; receivers apply
  them in order through `ems.actors.reorder`.

## What to build

- Built: stage 3 (spec 011); the live deadline, strategy changes and the timing log in stage 4 (spec 012).

## Prerequisites

- Spec 010 (this environment); spec 009 (the domain core the handlers call); spec 011 (the actors); spec 012
  (the clock, strategy changes, the timing log).

## Open questions

- None owned by this doc. The VERIFY items of stage 2 are resolved in `01-architecture.md`.
