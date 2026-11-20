# 02 · Actors subsystem: Restate services, registration, Kafka ingress

## Current consensus

Built in stage 2 (spec 010): the Restate environment, registration and the Kafka ingress, feeding one probe
object. The actors of `01-architecture.md` (Household, Tariff, Weather, Ems, Battery, Meter, the strategies) and
their handlers come with stage 3, which adds them to this doc. The ordering model is ADR 0017.

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
  | `ingress-meters` | `ems.production.v1`, `ems.consumption.v1` | `ems-ingress-meters` | `Probe.on_measurement` | 3 |
  | `ingress-forecasts` | `ems.weather.forecast.v1` | `ems-ingress-forecasts` | `Probe.on_forecast` | 3 |
  | `ingress-prices` | `ems.price.v1` | `ems-ingress-prices` | `Probe.on_price` | 1 |

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

### The probe (stage 2 only)

`Probe` (key: the record key) has one exclusive handler per ingress container and a shared `status`. Each record
updates, through the pure functions of `ems.actors.ingress_check`:
- `log`: per topic the record count, last offset, a SHA-256 chain over (partition, offset), regressions and
  partition changes, plus the key's rejected count. It does not depend on how the key's topics interleave, so the
  tests compare it with `==` against the same fold computed offline from the records read back from Kafka;
- `leads`: the newest `available_at` handled per stream and the largest lead of each stream over each other, in
  quarter-hours (the skew measurement; it depends on the interleaving and is never compared);
- `n`: records handled, written last, so a progress query never counts a record whose log is not written.

Two measurement-only objects, `StateCostEager` and `StateCostLazy` (`state_preload=False`; the SDK marks
`enable_lazy_state` deprecated), measure the cost of shipping object state (V7). Stage 3 removes all three and
keeps `ingress_check.log_apply` in the real Kafka-fed handlers.

### Test projects and jobs

`src/ems/experiments/stack.py` runs a test-scoped set of the same three ingress services as a compose project
`ems-it-<prefix>` (infra/docker-compose.yml plus tests/compose.it.yml) on the stack's network, with topic names
and groups prefixed through `EMS_INGRESS_PREFIX`. Tests, `make measure` and `make spike` never write to the
stack's topics. A Restate tier or job stops the stack's own ingress containers and leaves ingestion closed;
`make register` opens it again.

### How R1–R7 hold in stage 2

- R1, R2: no object calls another. Exclusive handlers are reached by Kafka or by one-way HTTP sends; only the
  shared `status` and `count` are called request-response, from tests and jobs.
- R3, R5: handlers make no external call, read no clock and never wait.
- R4: Restate's offset dedup at the ingress; job calls through the HTTP ingress carry an `Idempotency-Key`.
- R6, R7: no timer and no message between actors yet.

## What to build

- Stage 3: the handlers of the actors in `01-architecture.md`; the ingress containers point at `Household`,
  `Weather` and `Tariff` instead of `Probe`.

## Prerequisites

- Spec 010 (this environment); spec 009 (the domain core the stage 3 handlers call).

## Open questions

- None owned by this doc. The VERIFY items of stage 2 are resolved in `01-architecture.md`.
