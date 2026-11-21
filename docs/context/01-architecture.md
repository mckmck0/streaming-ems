# 01 · Architecture: actors, ownership, time and ordering

## Current consensus

Designed on 2026-11-14. Stage 2 (spec 010, 2026-11-15) built the runtime under it: Restate, the SDK service,
registration and the Kafka ingress (`02-subsystem-actors.md`). Stage 3 (spec 011, 2026-11-20) built the actors
below on it, with the assembler that orders a household's inputs (`src/ems/assembler/`). The time and ordering
model is ADR 0017; its rules for late and missing inputs are ADR 0018; run parameters arrive as control messages
(ADR 0019). The domain functions the actors call exist since spec 009 (`12-domain-model.md`). Changes to the
brief §5:
- `Tariff` is keyed by market and holds price curves only (ADR 0012, spec 009); `Weather` is keyed by location.
  Both are fed from Kafka and call no API (ADR 0011). `Tariff` pushes curves to households as one-way messages;
  no step reads another actor through a shared handler.
- Scheduled commands are data in `Ems` state, fired at their step in both modes. Durable timers remain for the
  live deadline and, later, forecaster refreshes (this narrows a consequence of ADR 0011).
- There is no `Registry`, `Evaluator`, `Selector`, forecaster or inverter actor yet. One `Battery` actor is the
  plant: mode actions model the inverter's local control (risk D6).
- `Ems` evaluates the live policy itself. `Strategy.decide` (brief §5.4) is not used: policies are pure functions
  of battery state and observation, and a call into a strategy object could read a state ahead of or behind the
  step.
- The sim clock is not a component: it is the watermark rule below.

### Actors and keys

```mermaid
flowchart LR
  subgraph Kafka
    P[ems.production.v1]; C[ems.consumption.v1]; F[ems.weather.forecast.v1]; PR[ems.price.v1]
  end
  P & C --> HH[Household]
  F --> W[Weather] -->|run id, visibility, watermark| HH
  PR --> T[Tariff] -->|curves, visibility, watermark| HH
  HH -->|subscribe| T
  HH -->|Step q| S[NoBattery, SelfConsumption]
  HH -->|Step q| E[Ems]
  E -->|Apply q: action| B[Battery]
  B -->|Ack q: SoC| E
  B -->|Flows q| M[Meter]
```

- Every edge between actors is a one-way message. Kafka reaches the actors through the Kafka ingress
  integration (`08-contracts.md`).
- Keys are `[run_id:]location_id`; `Tariff` is `[run_id:]market_key`. `run_id` is read from the event, never
  parsed from the key (market keys contain `:`).
- Each strategy is its own Virtual Object service, keyed by household, as in the reference spike. The household
  routes by service name, so a strategy can arrive in a later deployment while the system runs.

### Single writer per fact

| Fact | Only writer |
|---|---|
| Measurements, forecasts, prices on Kafka | `ems-gen` (`13-subsystem-ingest.md`) |
| A household's step sequence: joined flows, `estimated` flags, what was visible at each step; its effective watermark per stream; its strategy list | `Household` |
| Price curves and their versions; the price stream's watermark and the visibility stamped from it | `Tariff` |
| Forecast runs; the forecast stream's watermark and the visibility stamped from it | `Weather` |
| The action commanded for the real battery at each step; the live strategy in force | `Ems` (the choice moves to the Selector, stage 6) |
| Real SoC; the battery power executed at each step | `Battery` |
| Grid energy per step, real ledger, bills and deposit | `Meter` |
| Virtual SoC, shadow ledger and bills | each strategy, for its own key |
| Per-site tariff rules, battery parameters, the RCEm table | config, through `ems.tariffs` and the site catalog (not an actor) |
| A run's parameters (`RunParams`: window, `L`, strategies, live strategy, site model, RCEm table, initial SoC) | the run's operator, through `configure` (ADR 0019); each object of a managed household stores its own copy once |
| The ingress log of a record key (count, last offset, offset digest, regressions, rejected records) | the Kafka-fed object of that key: `Household` (meters), `Weather` (forecasts), `Tariff` (prices); `Probe` in stage 2 |
| A managed household's stream leads (measured, never compared) | `Household` |

### Time and ordering model (ADR 0017)

**Terms**
- `t` is the UTC quarter-hour index and `s_t` its start. `available_at` comes from `ems.contracts.availability`.
- A household has four input **streams**: production, consumption, the prices of its market (through `Tariff`)
  and the forecasts of its location (through `Weather`). A site without PV has no production stream.
- **Step `q`** covers the quarter-hour `[s_q, s_{q+1})`. From the meter streams it reads slot `q`; from prices
  and forecasts it reads the records with `available_at ≤ s_q`.
- A stream's **watermark** `W` is a promise: everything a step ending at or before `W` may read from that
  stream has arrived or has been declared missing. It only rises, and it starts with nothing covered.
- A household's **safe time** is the minimum of the watermarks of the streams it needs. The set of needed
  streams is fixed per run.
- Step `q` is released when the safe time is at or past its end (`W ≥ s_{q+1}` for every needed stream). Steps
  are released strictly in order, from the run's first step. That step is a run parameter on a UTC hour (the
  domain requires it), never the first record seen; earlier records are ignored and counted.

**How each watermark advances:** from the stream's content and schedule, never from the wall clock in sim. In
code every time is an integer step (stage 3): a record may be read by the steps from `ceil(available_at / 900 s)`
on, and a watermark covers the steps below `floor(watermark / 900 s)`.

| Stream | Advances when | To |
|---|---|---|
| production, consumption | a record arrives | the end of the longest unbroken run of closed slots. A slot is closed when its record is present, or when the same stream has delivered a record `L` or more slots newer |
| forecasts | a run arrives at `Weather` | that run's `available_at` plus the model's run cadence: the next run cannot be usable earlier |
| prices | the curve of a new delivery day arrives at `Tariff` | 13:00 local on that delivery day, when the next publication window opens |

- A sparse stream's watermark is the earliest moment its next record can become usable. A step that starts at
  that moment is therefore not covered until the next record arrives: after the 00:00 run (usable 02:00,
  watermark 05:00) the step starting at 05:00 waits for the 03:00 run.
- The cadence and the window opening are schedule constants, per forecast model and per market, in
  `ems.contracts.availability` (code, not wire contract) with a test of the generator against them: every model
  runs every 180 min, and the window opens at 13:00 Europe/Warsaw. Only `PL:RCE` is defined, the market the
  skeleton consumes.
- A missing forecast run makes the household wait for the next one; a late run keeps its place in its key's
  order (`06-data-flows.md`, faults). A run of a model without a delay or cadence is counted and dropped, like a
  missing run (ADR 0018).
- A forecast run not newer than the newest one accepted, and a price curve of a day older than the newest one, are
  counted and dropped where they arrive: they could only become visible after a newer record, so no step would
  read them.

**Visibility.** A record becomes visible from `max(available_at, the stream's watermark when it arrived)`. On
schedule that is `available_at`. A record that arrives behind the watermark (a republished price day, an
unscheduled forecast run) becomes visible at the watermark, so no step that may already have run depends on it.
A step uses the newest version visible at its start, for the decision and for settlement alike.

**Late and missing measurements.** `L` (default 8 quarter-hours, the generator's `late_max`) is counted in
positions of the record's own stream, so the outcome does not depend on the other streams.

| Case | What happens |
|---|---|
| The record for slot `q` arrives before its stream is `L` slots past `q` | Used normally; the step waited for it |
| The stream reaches `q + L` without it | Step `q` runs on an estimate, flagged `estimated`: the value the stream used for the slot before (0 before the first reading; ADR 0018) |
| It arrives after that, while the stream is less than `2L` past `q` | It never changes a decision. It corrects the settlement of its hour, carried by the first step after the stream's newest slot at that moment (at most `2L` steps after `q`) |
| It arrives later still | Dropped and counted (`too_old`); the slot is already pruned |

A correction recomputes the slot's grid energy from the corrected PV or load and the battery power that was
already executed, then the hour's row (allowed lateness, ADR 0018). The command, the executed power, the SoC and
every later decision stay as they were on the estimate. So whoever settles keeps each recent quarter's PV, load
and executed power, back to the start of the hour that holds the oldest slot still open to correction, and a
civil month is settled `2L` quarter-hours after it ends, with its own fixed charges and price basis.

**A quarter-hour with no visible price** uses the statutory replacement (RES act art. 4b ust. 10): the price of
the same local quarter-hour on the nearest earlier delivery day with a visible curve, at most two days back, or 0
if there is none; the step is flagged `price_replaced` (ADR 0018).

**Dedup and revisions.** State keeps one entry per slot (stream, `t`) from `2L` below the stream's newest slot.
The first reading of a slot wins; a second one is counted (`duplicate` for the same `event_id`, `conflict` for
another). A measurement's `event_id` does not depend on its content, so no source can send a revision with a new
id (ADR 0018). A price revision follows the visibility rule; a forecast run with an issue time already seen is
stale.

**Between actors.** Order between different actors is not assumed. `Tariff` numbers its curves once per market
and sends a household a snapshot of every accepted curve on subscribe, then one update per curve; `Weather`
numbers its runs. The household applies them in sequence, holding back any that arrive early. Steps, commands,
acknowledgements and flows carry `t` and are applied the same way, through one hold-back function
(`ems.actors.reorder`) that counts early and stale messages outside the compared state.

**Live mode.** The same rules run, with one more source: a durable wall-clock timer in each household. The
household keeps its own effective watermark per stream, the higher of the one it received and "now minus a
grace period", so a missing reading or a late publication cannot stall a live decision. A record that reaches
the household behind its effective watermark becomes visible from that watermark. In sim the timer does not
exist (P7), and the effective watermark equals the received one.

**End of a run.** No marker and no flush. Steps the rules cannot release stay unreleased, for example after a
gap in the last `L` slots of a stream. The offline side runs the same assembler over the recorded streams and
feeds the released steps to `stepper.step`, which equals `reference.run` bit for bit on whole hours (spec 009).
A run that stops mid-hour is therefore compared with an open hour in its state; `reference.run` alone would
reject it.

**Why it is deterministic.**
1. Each stream's state (dedup, closed slots, visibility, watermark) depends only on that stream's own order,
   and Restate keeps the order of one topic partition per key.
2. A step is released only when every needed watermark covers it, and it reads only records visible at its own
   start. How the streams interleave cannot change its content.
3. Everything after the household applies steps in `t` order.

The offline side applies the same rules through the same pure functions, reading each stream separately.

**Limits** (risks A1–A4 in `09-risks-and-tech-debt.md`): buffers grow with the skew between a household's
inputs, so large backfills are paced; the schedule constants are assumptions about the sources; a republished
price day is visible only at the next publication window; a live run and its replay can differ where a deadline
fired.

### State, handlers and messages

E = exclusive, S = shared (read-only; each returns the object's `state`). Every handler goes through
`ems.actors.shell`: read own state, call a pure transition, write own state, send (`02-subsystem-actors.md`).
Every configured object also stores `params` (its `RunParams`); Kafka-fed objects also `log`, `n` and
`n_at_configure`.

| Actor | State | Handlers | Sends |
|---|---|---|---|
| `Household` | The join: next step, per meter stream the slots from `2L` below its newest, the closed values not yet released and the late readings waiting for their step; curves and runs with their visibility and the watermarks; counters. The price and forecast inboxes; the stream leads (measured) | `configure` (E) · `on_measurement` (E, Kafka: production and consumption) · `on_prices` (E, from Tariff) · `on_forecast` (E, from Weather) · `status` (S) · stage 4: `on_deadline`, `set_strategies` | `Tariff.subscribe` on `configure` · `on_step` of each strategy · `Ems.on_step` |
| `Tariff` | The price stream (watermark, newest day and its ids, sequence number, counters), every accepted curve of the run with its visibility, the subscribers. No parameters | `on_price` (E, Kafka) · `subscribe` (E, from Household) · `status` (S) | `Household.on_prices`: a snapshot on subscribe, then one update per accepted curve |
| `Weather` | The forecast stream: watermark, newest issue time, sequence number, counters. No forecast payload in stage 3 (stage 5 decides storage and retention) | `configure` (E) · `on_forecast` (E, Kafka) · `status` (S) | `Household.on_forecast`: issue time, visibility and watermark, no payload |
| `NoBattery`, `SelfConsumption` | The strategy result (virtual SoC, last flows, ledger accumulator, flows open to correction, closed months) and the step inbox | `configure` (E) · `on_step` (E, from Household) · `summary` (S) | nothing in stage 3 |
| `Ems` | The real SoC from the latest acknowledgement, the last decided step's flows, the last step commanded, the step and acknowledgement inboxes, released steps not yet decided | `configure` (E) · `on_step` (E, from Household) · `on_ack` (E, from Battery) · `status` (S) | `Battery.apply` |
| `Battery` | Real SoC, the command inbox, clamp counter | `configure` (E) · `apply` (E, from Ems) · `state` (S) | `Ems.on_ack` · `Meter.on_flows` |
| `Meter` | The ledger accumulator, the flows open to correction, closed months, the flow inbox | `configure` (E) · `on_flows` (E, from Battery) · `bill` (S) | nothing |

Internal Restate messages (not wire contract; field lists in `08-contracts.md`). Each carries `schema_version`
and `t`, the step it belongs to or, for a stream update, the stream's watermark; `t` is the simulated time (R7).

| Message | From, to | Content |
|---|---|---|
| `Step` | Household to each strategy and to Ems | `QuarterIn` of spec 009 (`t`, PV and load energy, the RCE of `t` or its replacement), the `estimated` and `price_replaced` flags, the issue time of the newest forecast run visible at `s_t`, and any corrections due |
| `PriceUpdate` | Tariff to Household | Sequence number, a snapshot flag, curves with their visibility, the watermark |
| `ForecastUpdate` | Weather to Household | Sequence number, the run's issue time and visibility, the watermark |
| `Subscribe` | Household to Tariff | The household key and its first step |
| `Apply` | Ems to Battery | The `Action`, the step's flows and price, corrections |
| `Ack` | Battery to Ems | `t` and the SoC after it |
| `Flows` | Battery to Meter | PV, load and the executed battery power of `t`, its price, corrections. `Meter` derives the grid energy with the domain function the reference uses |
| `RunParams` (control) | Operator to every object of a managed household | The run's parameters (ADR 0019); its time is `first_step` |

- **The real loop.** `Ems` holds the real SoC from the last acknowledgement and decides `t` only once `t − 1`
  is acknowledged, with `reference.prices` and `reference.decide`. `Battery` resolves the action against the
  step's flows with `reference.apply`, which clamps it (P8), and reports the new SoC. With SelfConsumption live,
  the real SoC and ledger equal that strategy's virtual ones (parity check).
- **Corrections travel inside the next `Step`** and on through `Apply` and `Flows`, so they never overtake the
  step they correct.
- **A site the EMS does not manage** is never configured: its `Household` and `Weather` keep the ingress log and
  the record counter, nothing else, although its records still reach them, because the topics carry every site.
- **Run parameters** are resolved once by `ems.experiments.runs.managed_params` and sent by `configure` before
  data flows; each object keeps its copy, so a config edit or a later deployment cannot change a running
  household (P14, ADR 0019).
- **The `estimated` flag** marks the household's own estimate for a missing reading. It is not the wire
  `quality` of a reading.
- **The RCEm table is a run parameter** read as config, as `reference.run` and `stepper.step` take it: PSE values
  for real-data runs, a proxy table computed before the run for synthetic runs (it needs whole months). A site
  whose months are missing from it fails before anything is sent.
- **Cost:** 6 invocations per household-step plus one per strategy; 8 with two strategies.

### How R1–R7 hold

| Rule | How it holds |
|---|---|
| R1, R2 no blocking cycles | Every edge between actors is one-way, including both cycles (Household to Tariff and back; Ems to Battery and back). Request-response is used only from outside (tests, the export CLI) to shared handlers |
| R3 journal non-determinism | Handlers make no external calls. Run parameters arrive by `configure` and are stored in state. The wall clock appears only in the live deadline handler (stage 4), through `ctx.time()`. Stored state and messages encode to the same bytes in every process (no sets), because Restate compares a retried handler's writes with its journal (RT0016) |
| R4 idempotent ingress | Three kinds of duplicate, each caught where it arises. A Kafka record redelivered after a restart or rebalance: Restate deduplicates by consumer group, topic, partition and offset. Messages a handler sends: Restate deduplicates them across retries of that execution, so they need no idempotency key. The same event published twice: the slot table keeps the first reading of a meter slot, the forecast stream drops a run not newer than its newest, the price stream a curve id it holds. No idempotency key is taken from records: the slot table is needed anyway for the join and late records. Control calls through HTTP ingress (`configure`) carry an `Idempotency-Key` |
| R5 never wait in an exclusive handler | Waiting means buffering in state until a watermark passes. Live deadlines are delayed self-messages |
| R6 generations | The live deadline timer is the only self-rescheduling chain; it carries a generation number. Sim has none |
| R7 envelope | Every message carries `schema_version` and `t`, the simulated time (stage 3: no separate timestamp); stream updates also carry `seq`. Receivers apply messages in `t` or sequence order through `ems.actors.reorder`, hold back early ones, and count every reordering |

### Restate facts

docs.restate.dev is not versioned: each statement was read in the current docs on the date given, and stage 2
confirms the behaviour on the pinned versions (`05-deploy-and-infra.md`).

| Fact | Checked |
|---|---|
| At most one handler with write access runs at a time per object key; shared handlers run concurrently | 2026-11-14, <https://docs.restate.dev/concepts/services> |
| Kafka ingress integration: "requires Restate v1.8.1 or later"; records reach a Virtual Object's exclusive handler "in the order in which they arrived on the topic partition"; the record key becomes the object key and the value the JSON payload; one container can consume several topics into one handler; the handler sees `kafka.topic`, `kafka.partition`, `kafka.offset`, `kafka.timestamp` | 2026-11-14, <https://docs.restate.dev/services/integrations/kafka> |
| Exactly-once from Kafka: "Restate deduplicates based on consumer group id, topic, partition and offset" | 2026-11-14, same page |
| Calls and sends made during one handler execution are deduplicated automatically; independent callers need an idempotency key. "Calls to a Virtual Object execute in order of arrival, serially"; other invocations may interleave between two sends | 2026-11-14, <https://docs.restate.dev/develop/go/service-communication> (the Python page says the same) |
| The in-server Kafka subscriptions are "the old Kafka support which we plan to remove in future releases"; not used | 2026-11-14, <https://docs.restate.dev/services/invocation/kafka> |
| Nothing is documented about order or relative speed across topics or partitions, or about the order of messages between two objects across separate invocations; the model assumes none | 2026-11-14 |
| HTTP ingress: `Idempotency-Key` header, result kept 24 h; one-way sends and delays | 2026-11-14, <https://docs.restate.dev/services/invocation/http> |
| A retried handler whose commands differ from its journal (operation order, state keys, call payloads) fails with RT0016, "journal mismatch" | 2026-11-20, <https://docs.restate.dev/references/errors>, <https://docs.restate.dev/services/versioning> |
| Removing a service means removing its deployment; adding handlers is allowed, removing or renaming them needs `breaking`. `POST /deployments` with `force: true` overrides the deployment at the same URI and implies `breaking`; `DELETE /deployments/{id}` exists only as a forced, asynchronous delete; `GET /services/{service}/handlers/{handler}` answers 404 for a missing handler | 2026-11-20, <https://docs.restate.dev/services/versioning>, <https://docs.restate.dev/admin-api/deployment/register-deployment> and the sibling admin-API pages (the reference shows API version 1.7.14). Confirmed on 1.8.1-hotfix.2 on 2026-11-21: the handler lookup answers 404 and 200; a `force: true` registration at the same URI keeps the deployment id and replaces its whole service set (`Probe` and the state-cost objects were no longer listed) |
| Delayed sends are durable; a sleep in an exclusive handler blocks its key; state is sent with every invocation unless lazy state is enabled | 2026-10-30, <https://docs.restate.dev/develop/python/durable-timers>, <https://docs.restate.dev/develop/python/state> |

Confirmed on the pinned versions in stage 2 (spec 010; Restate 1.8.1-hotfix.2, integration 0.1.1, `restate-sdk`
1.5.2), with the test or result that shows it:
- Per-key partition order and exactly-once delivery from Kafka hold, also across `kill -9` of the server, the SDK
  service and an ingress container (spec 010's `test_probe_backfill.py` and `test_probe_kills.py`, re-homed on
  the actors in spec 011 as `tests/e2e/test_ingress_backfill.py` and `tests/fault/test_ingress_kills.py`).
- The value reaches the handler as raw bytes without a content type; besides `kafka.topic`, `kafka.partition`,
  `kafka.offset` and `kafka.timestamp` the handler sees `kafka.key`; other record headers are not forwarded
  (only `traceparent`, `tracestate`) (`StaticRecordMapper.kt` at v0.1.1; `tests/integration/test_ingress_*.py`, formerly `test_probe_*.py`).
  `sys_invocation` shows these invocations as `invoked_by = ingress`.
- The integration commits a Kafka offset after Restate's durable acknowledgement, not after the handler ran:
  with the SDK service down the consumer lag still fell to 0 (`ingress-backlog-20261116T081316Z`).
- Registration with `force: false` at a known URI answers 200 and keeps the old handlers, even when they changed
  (checked by hand 2026-11-15, `02-subsystem-actors.md`). The admin API's own default for `force` is true.
- Lazy state is `state_preload=False` in `restate-sdk` 1.5.2; the SDK marks `enable_lazy_state` deprecated
  (`object.py`).

## What to build

One spec per stage, each planned in its own conversation with `ems-plan-change`. The stage table (scope and
"done when") is in `00-overview.md`; the briefs below are the handoff. A stage that finishes refreshes the next
brief.

### Stage 2 · Restate environment and Kafka into a Virtual Object

**Done 2026-11-15 (spec 010).** What it decided is in `02-subsystem-actors.md`, `05-deploy-and-infra.md` and the
resolved VERIFY items below; its measurements are in `docs/thesis/kafka-ingress.md` and
`docs/thesis/spike-reproduction.md`. The brief as it was handed over:

- **Read first:** this doc (model, Restate facts, VERIFY list), `05-deploy-and-infra.md`, `08-contracts.md`
  (topics, keys, handoff), `09-risks-and-tech-debt.md` (K6, A5), `11-experiments.md`, brief §6 (the spike and
  experiments A–C).
- **Already decided:** Restate 1.8.1-hotfix.2. Kafka reaches Restate only through the Kafka ingress integration,
  one container per target handler (meters, forecasts, prices); nothing is built on the in-server
  subscriptions and no fallback is kept. One Kafka broker. Pins by exact tag and digest (P12). If V1 or V2
  fails, the stage stops and the way forward (a newer release, an upstream report) is re-planned through an
  ADR.
- **This stage decides:**
  - the `restate-sdk` and `hypercorn` pins;
  - whether Kafka-fed handlers take raw bytes (V3);
  - the ingress containers' restart policy and memory limits;
  - the spike: it sits untracked at `restate-ems-spike/` (with a virtualenv and logs). The stage moves it to
    `reference/restate-ems-spike/`, chooses the files to track, and says what counts as reproducing A–C if the
    spike's own pins (server 1.7.12, SDK 1.0.5) cannot run unedited on the stage's server;
  - where the probe object lives and whether stage 3 removes it;
  - the definition of the skew between topics and where it is recorded (`docs/thesis/` does not exist yet).
- **VERIFY items it owns:** V1–V4, V7–V11.
- **Docs it writes:** the registration and Kafka ingress part of `02-subsystem-actors.md`; the pins in
  `05-deploy-and-infra.md`; the resolved VERIFY items here; the stage 3 brief.
- **Done when:** the stage table's criteria hold: a healthy stack, idempotent registration, a probe object that
  sees each key's records in partition order with none lost or doubled after `kill -9` of the server, the SDK
  service or an ingress container, the measured skew between topics recorded for the thesis, spike A and C with
  0 mismatches and B with 0 duplicates.
- **Start with:** "Plan stage 2 from its brief in `docs/context/01-architecture.md`."

### Stage 3 · Actor skeleton

**Done 2026-11-20 (spec 011).** The rules it decided are in this doc and ADRs 0018 and 0019; the handlers in
`02-subsystem-actors.md`; the messages in `08-contracts.md`; corrections, the month lag and the replacement price
in `12-domain-model.md`. The brief as it was handed over:

- **Read first:** this doc, ADR 0017, `02-subsystem-actors.md` (runtime, registration, Kafka ingress, the probe),
  `12-domain-model.md` (actions, `Policy`, stepper, reference, the notes for the actor stages), `08-contracts.md`
  (handoff, availability), `06-data-flows.md` (emission schedule, faults), `specs/009-domain-core/`,
  `specs/010-restate-kafka-ingress/` and `docs/thesis/kafka-ingress.md` (the stage 2 results). Where the actor
  notes in `specs/009-domain-core/plan.md` differ (shared price reads, live RCEm on `Tariff`), this doc and
  `12-domain-model.md` win.
- **Already decided:** the ordering model; the actors, edges and messages above; `L` = 8 by default; the
  household gates on forecasts and records the visible run per step; `Ems` evaluates the live policy; the first
  step and the RCEm table are run parameters and the tests use RCE sites; results are read from state. From
  stage 2:
  - the three ingress containers are re-pointed from `Probe` to `Household.on_measurement`,
    `Weather.on_forecast` and `Tariff.on_price` (`RESTATE_RECORD_MAPPER_SERVICE` and `_HANDLER` in
    `infra/docker-compose.yml`); `Probe`, `StateCostEager` and `StateCostLazy` are deleted;
  - Kafka-fed handlers take raw bytes (`BytesSerde`, `accept="*/*"`), parse with `parse_event`, count a bad
    record and return; they keep `ingress_check.log_apply`'s log in their own state, so the e2e and fault tests
    keep comparing it with `==` against the offline fold;
  - the harness `ems.experiments.stack`, the fixtures in `tests/stack_fixtures.py` and the e2e and fault tiers
    exist; extend them rather than adding new ones;
  - registration with `force: false` keeps a deployment's old handlers: adding handlers needs
    `make register FORCE=true` on an idle stack, or a new URI;
  - an unpaced backfill lets one of a household's streams run up to 167 quarter-hours ahead of another
    (`docs/thesis/kafka-ingress.md`); buffers are sized for that, or the tests publish paced;
  - the skew of prices against a household's other streams is measured at `Household` (stage 2 could not: prices
    arrive under the market key).
- **This stage decides** (each is OPEN until then):
  - how a correction reaches the ledger. Either the hour row is recomputed, and a correction that lands after
    its month closed is dropped and counted; or each quarter is settled only once it can no longer change, so
    the month closes `2L` later and nothing is dropped. They differ only for readings late across a month
    boundary. Both need a domain addition, because `settle_quarter` and `stepper.step` close an hour on its
    fourth quarter and reject an out-of-order `t`;
  - the estimate for a missing reading (from earlier slots of its own stream only);
  - a split of `reference.quarter` into its decide and apply halves, so `Ems` and `Battery` run the same code
    as the reference;
  - how the actor service obtains a run's parameters (first step, site parameters, strategy list, RCEm table);
  - the rule for a quarter-hour with no visible price, which also covers a republished price day (risk A3);
  - the delay used for a `forecast_model` missing from the availability table (open in `08-contracts.md`);
  - the schemas of the internal messages (`08-contracts.md`).
- **VERIFY item it owns:** V5 (the reorder counter measures it).
- **Docs it writes:** the handlers part of `02-subsystem-actors.md`; the internal messages in `08-contracts.md`;
  the correction and the estimate in `12-domain-model.md`; `06-data-flows.md`; the stage 4 brief.
- **Done when:** 10 households × 1 day replayed through Kafka give strategy and `Meter` state equal to the
  offline side with `==`. The result is the same when the topics arrive in different orders (each ingress
  container started after the other finished), with
  `--faults` at the default `L`, and with `--faults` at an `L` below the generator's `late_max` (for example
  2), which is the run that produces corrections and drops. The parity check holds in each run. Unmanaged
  sites keep O(1) state. Property tests show that any interleaving of the per-stream orders gives identical
  steps.
- **Start with:** "Plan stage 3 from its brief in `docs/context/01-architecture.md`."

### Stage 4 · Clocks and systems experiments

- **Read first:** this doc, `11-experiments.md`, `06-data-flows.md`, risks A1–A12, ADRs 0018 and 0019,
  `specs/011-actor-skeleton/` and `02-subsystem-actors.md` (handlers, state keys, the harness).
- **Already decided:** live mode adds one durable wall-clock deadline timer per household, with a generation
  number (R6); a strategy is added by a control message through HTTP ingress with an `Idempotency-Key` and an
  effective step `t0`, next to `configure` (ADR 0019); results are exported from state by a batch CLI (spec 006).
  From stage 3:
  - a run is configured before it is published (`ems.experiments.runs.managed_params`, `Restate.configure`), and
    `Restate.wait_run` waits for the end the offline side computes (`stack.offline_run`, `run_ends`); its lenient
    mode tolerates the retries a kill causes;
  - every object stores one `state` value, a frozen dataclass encoded by `ems.actors.serde`; the export decodes
    it with the same serde, and the shared handlers return it;
  - a month is settled `2L` quarter-hours after it ends (ADR 0018): bill windows need that many more steps, and a
    synthetic run with an RCEm site needs the RCEm entry of the month after the window;
  - stage 3 killed only the SDK service with households configured; the server and ingress kills ran on
    unconfigured households (re-homed spec 010 tests).
- **This stage decides:** the grace period and catch-up of the live timer; what happens to a strategy change
  that arrives after `t0` (flagged, never silent); the experiment runner (`make exp`), how the export reads
  state and where systems metrics come from (open in `11-experiments.md`); pacing a history replay; the
  builder of the proxy RCEm table; whether to split an object's `state` value (it is rewritten every step, risk
  A9); the invocation retention for scale runs (A6).
- **VERIFY items it owns:** none new; it re-runs V10 at scale.
- **Docs it writes:** `11-experiments.md` (runner, export, metrics), `06-data-flows.md` (live clock), the
  stage 5 brief.
- **Done when:** results stay identical under `kill -9` of the SDK service and the server; recovery latency by
  phase, sustained speed, events per second and decision latency are reported (P16); a strategy added from `t0`
  joins every household without a restart.
- **Start with:** "Plan stage 4 from its brief in `docs/context/01-architecture.md`."

### Stage 5 · Strategies and forecasts

- **Read first:** this doc, `12-domain-model.md` (price signal and its gaps), `11-experiments.md`, the brief
  §7 (forecasting and optimisation stack).
- **Already decided:** a policy sees only what was visible at the start of its step; a forecast run is
  immutable and identified by its issue time; each `Step` already names the run visible at its start. A new
  strategy is a policy in `ems.domain.actions.POLICIES` plus its name: `ems.actors.strategy.strategy_object`
  builds its service.
- **Open from stage 3:** a site without PV (PL-LOC-000002 in the 10-household catalog) is not a prosumer under
  the RES act (art. 2 pkt 27a); with NoBattery and SelfConsumption it never exports. Decide how its export is
  valued before a strategy can export from its battery.
- **This stage decides:** how a run's payload reaches a policy, and how long it is kept. Retention must follow
  the reader's progress, not the forecast stream's, or a household that lags would find its run gone (the
  payload travels with the update, or the household reports its released step). Also: whether `Ems` can still
  evaluate a policy with plans in process; the price rules, the MPC and its oracle, the forecasters and their
  persistence baseline.
- **VERIFY item it owns:** V6, if runs are read through a shared handler.
- **Docs it writes:** `03-subsystem-strategies-and-evaluation.md`, `04-subsystem-forecasting-and-optimization.md`,
  the stage 6 brief.
- **Done when:** per-strategy results against the offline reference, with the cost metric of
  `11-experiments.md`.
- **Start with:** "Plan stage 5 from its brief in `docs/context/01-architecture.md`."

### Stage 6 · Adaptive Selector

- **Read first:** this doc, the brief §3 (RQ3) and §5 (Evaluator, Selector), the stage 5 results.
- **Already decided:** `Ems` owns the live strategy in force; the Selector will own the choice.
- **This stage decides:** the Evaluator and Selector actors, the switching rule and its hysteresis, the step
  from which a selection takes effect, and how `Ems` reaches a strategy added at run time (a redeploy of `Ems`,
  or a stateless service call that reads no state).
- **Docs it writes:** `03-subsystem-strategies-and-evaluation.md` (Evaluator, Selector), the ownership table
  here.
- **Done when:** the Selector is compared with the best fixed strategy (RQ3).
- **Start with:** "Plan stage 6 from its brief in `docs/context/01-architecture.md`."

## Prerequisites

- The domain core (spec 009) and the data pipeline (specs 001–008).

## Open questions

VERIFY on the pinned versions (stage in brackets):

| # | Item | Why it matters |
|---|---|---|
| V1 (2) | Per-key order from one partition holds through the integration, also across a restart of the server, the SDK service and the ingress container | **Resolved 2026-11-16:** holds. Every key's log equals the offline fold with 0 regressions, also after `kill -9` of each component mid-run (`tests/e2e/test_ingress_backfill.py`, `tests/fault/test_ingress_kills.py`, formerly `test_probe_*`) |
| V2 (2) | What the integration does when the handler fails, the payload is invalid, or Restate is down; the docs do not say, and it exits after repeated failed reconnects | **Resolved 2026-11-16.** Invalid payload: the integration validates nothing and passes raw bytes; the handler counts it (`test_ingress_bad_records.py`, formerly `test_probe_bad_records.py`). Handler unavailable (SDK service down): records are still ingested and queue as invocations in Restate (`ingress-backlog-20261116T081316Z`); a handler that keeps raising is retried by Restate's policy (70 attempts, then pause) and holds only its own key. Restate down: the integration reconnects every 500 ms and does **not** exit, whatever `RESTATE_RETRY_MAX_ATTEMPTS` says, because each stream opens before it fails (AC-010-16, `PartitionSession.kt`); a process that exits is restarted by `restart: unless-stopped`. A record Restate rejects is skipped (`PartitionSession.kt`), hence the start order in 02 |
| V3 (2) | What the SDK does when a handler's input cannot be deserialised | **Resolved 2026-11-15:** `restate-sdk` 1.5.2 raises `TerminalError("Unable to parse an input argument")` (`handler.py` 406), which would fail the invocation. Kafka-fed handlers therefore take raw bytes and parse themselves |
| V4 (2) | Pinning the integration image: the docs show only the `latest` tag | **Resolved 2026-11-15:** GHCR carries dated tags; pinned `ghcr.io/restatedev/ingress-integration-kafka:0.1.1-20261109@sha256:20704c886ee4dcc4046bab1743dc424bb08b2317ce1bc6da55ce738345ce01b1` (linux/amd64 only; registry API) |
| V5 (3) | Order of one-way messages between the same two objects across invocations | Undocumented; not relied on; the reorder counter measures it |
| V6 (5) | State written before a send is visible to a shared read triggered by that message | Undocumented; matters for forecast reads |
| V7 (2) | Cost of state shipped with every invocation; the lazy-state option of the pinned SDK | **Resolved 2026-11-16:** with eager state a call that touches one small key takes a median of 2, 3 and 9 ms for 1 kB, 100 kB and 1 MB of object state (299, 235 and 84 calls/s); lazy state (`state_preload=False`) keeps the median at 3–4 ms but showed one 39 s call in 500 at 1 MB, cause unknown (`ingress-state-cost-20261116T081221Z`) |
| V8 (2) | Whether Kafka ingestion slows down when handlers fall behind | **Resolved 2026-11-16:** it does not. With the SDK service stopped the consumer lag of all groups fell to 0 within 7.4 s and 3,426 records waited in Restate as invocations; the backlog drained in 8.8 s after the service came back (`ingress-backlog-20261116T081316Z`) |
| V9 (2) | The unit of `ctx.time()`; SDK and server compatibility for the chosen pins; memory of three JVM ingress containers beside Kafka and Restate | **Resolved:** `ctx.time()` is a journaled `time.time()`, float seconds since the epoch (`server_context.py` 889–891, 2026-11-15). Server 1.8.1-hotfix.2 and `restate-sdk` 1.5.2 negotiate service protocol 5–7 over HTTP/2 (registration response). Peak memory in an unpaced backfill: ingress containers 558, 475 and 288 MiB of 768; Kafka 797 MiB of 1 GiB; Restate 1,556 MiB of 2 GiB; no OOM kill (`ingress-skew-20261116T081120Z`) |
| V10 (2) | The server restarts cleanly after `kill -9` with VQueues on (risk A5) | **Resolved 2026-11-16:** yes, healthy without a manual step, in the fault tier (AC-010-13) and in the spike's experiment B (healthy 0.5 s after restart, `spike-20261116T080544Z`) |
| V11 (2) | The integration's Kafka client works against broker 4.3.1 (ADR 0010 checked Restate's bundled client, which is no longer used) | **Resolved 2026-11-15:** the image bundles `kafka-clients` 4.2.0 (`/NOTICE`, `/app/libs`), which consumed every test and measurement run against broker 4.3.1 |

- Closed in stage 3 (2026-11-20): how a correction reaches the ledger, the estimate, the price of a quarter-hour
  without a visible price, an unknown forecast model (ADR 0018); how the actors obtain a run's parameters
  (ADR 0019); the split of `reference.quarter` (`12-domain-model.md`); the internal messages (`08-contracts.md`).
- OPEN (stage 5): how forecast payloads reach a policy and how long they are kept.
- OPEN: delivering a newly published RCEm to running actors in live mode. It needs a new event, which is wire
  contract (ADR 0016); no stage owns it yet.
- OPEN: exact replay of a live run needs a per-step record of what was visible (risk A4).
- `03`–`04` (strategies and evaluation, forecasting and optimisation) are written with the stages that build
  them; `02` has the actor handlers since stage 3.
