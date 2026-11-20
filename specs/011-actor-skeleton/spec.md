# Feature Specification: Actor skeleton

**Feature Branch**: `011-actor-skeleton` · **Created**: 2026-11-20 · **Status**: Approved 2026-11-20; revised
the same day after the plan reviews (the missing-price rule and the clean run's window by user decision; criteria
AC-011-02, -04, -07, -14, -16, -18 … -20, -23, -26 … -28, -32, -35, -37 … -39, -44, -45, -47 reworded; see
`plan.md`, Review)

**Input**: The stage 3 brief in `docs/context/01-architecture.md`. User decisions of 2026-11-20, each taken
after comparing alternatives (table below; the alternatives are recorded in ADRs 0018 and 0019).

## Goal

Build the actors of `01-architecture.md` on the runtime of spec 010, so that data replayed through Kafka gives
the same results as the offline side, bit for bit:

1. a pure assembler that turns a household's four streams into ordered steps, whatever order the records
   arrive in;
2. late and missing readings handled by an allowed-lateness model that corrects the ledger without changing a
   decision;
3. the Virtual Objects `Household`, `Tariff`, `Weather`, `Ems`, `Battery`, `Meter`, `NoBattery` and
   `SelfConsumption`, connected by one-way messages only;
4. a run configured by control messages, with unmanaged sites costing O(1) state;
5. the first comparison of actor state with the offline side through Kafka: 10 households, in five runs (one
   that crosses the end of October, two arrival orders, two fault runs).

## Decisions

| Item | Decision |
|---|---|
| Late reading and the ledger | Allowed-lateness model. An hour's row is written when the hour ends. A reading that arrives up to `2L` slots late re-fires its hour: the row is recomputed from the corrected PV or load and the battery power already executed. A civil month closes `2L` quarter-hours after it ends, so no correction inside the allowed lateness is lost. Decisions, executed power and SoC never change |
| Estimate for a missing reading | The value its stream used one slot earlier (a reading or an estimate; 0 before the first reading) |
| No visible price | The statutory replacement (RES act art. 4b ust. 10): the price of the same local quarter-hour on the nearest earlier delivery day whose curve is visible, at most two days back; 0 if there is none. The step is flagged `price_replaced` and counted. Decision and settlement use the same price (ADR 0017). *First decided as "price 0, flagged"; changed 2026-11-20 after the energy-domain review found the statutory rule and that 0 biases the comparison against strategies that export more* |
| Unknown `forecast_model` | The run is ignored and counted; it behaves like a missing run |
| Second record for a meter slot | First reading wins; a later record is counted `duplicate` (same `event_id`) or `conflict` (another id). A measurement's `event_id` does not depend on content, so no source can send a revision with a new id. This narrows the revision sentence of 01 and ADR 0017 to prices (a forecast run with an issue time already seen is ignored as stale) |
| Run parameters | One idempotent `configure` control message per object, through the HTTP ingress, before data flows. An object that was never configured only counts its records |
| Code structure | A new pure package `src/ems/assembler/` holds what the offline side also runs. `src/ems/actors/` holds one file per actor: its state and pure transition, then its handlers. Dependencies: `contracts` ← `domain` ← `assembler` ← `actors`, `experiments` |
| Serialisation | State, parameters and messages are frozen dataclasses stored through one serde over `pydantic.TypeAdapter`. No stored type contains a set (`National.holidays` becomes a sorted tuple) |
| Envelope | Every internal message carries `schema_version` and `t`; `t` is the sim timestamp. Price and forecast updates also carry `seq`. This rewords R7 |
| E2E runs | The clean run starts 2026-10-31T00:00Z and lasts 30 h, so October closes inside it (user decision 2026-11-20); the other runs cover 2026-10-20 × 1 day. Arrival order is fixed by starting the three ingress containers one after another. The fault runs use a test-only generator config with raised rates, and assert the expected counters on the offline side before comparing |
| `quarter()` | Split into `prices`, `decide`, `apply` and `grid_kwh`; `quarter()` is their composition and returns the same bits |
| `Tariff` numbering | One sequence number per `Tariff` object; the snapshot carries the number it is valid for. 01 says "per receiver"; the guarantee is the same |
| Older sparse records | A forecast run or a price day older than the newest one accepted is counted and ignored where it arrives. It could only become visible after a newer one, so no step would read it |
| Hold-back | Every receiver uses the same hold-back function, also where causality already keeps the order (`Battery`, `Ems.on_ack`) |
| Run window | `first_step` and `end_step` are run parameters. Readings outside them are counted and ignored |
| Initial SoC | Half the battery capacity |
| State layout | One `state` value per object, plus `params`, and `log` and `n` on Kafka-fed objects. A managed household's stream leads live in its `state`, outside every comparison |
| Stage 2 objects | `Probe`, `StateCostEager` and `StateCostLazy` are deleted. `make measure` keeps `skew` and `backlog` on the real actors and loses `state-cost` |
| Strategies in this stage | `NoBattery` and `SelfConsumption`; SelfConsumption is live |

## How each OPEN and VERIFY item is resolved

| Item (01, 08) | Resolved by |
|---|---|
| How a correction reaches the ledger | AC-011-03, -04; ADR 0018 |
| The estimate for a missing reading | AC-011-10 |
| The split of `reference.quarter` | AC-011-01 |
| How the actor service obtains a run's parameters | AC-011-27, -28, -37; ADR 0019 |
| A quarter-hour with no visible price | AC-011-19 |
| A `forecast_model` missing from the availability table | AC-011-15 |
| The schemas of the internal messages | AC-011-32; field lists in `plan.md` and `08-contracts.md` |
| V5: order of one-way messages between two objects | The re-order counters of AC-011-30, reported by AC-011-46 and recorded in 01 |

## Scope

In scope:
- `src/ems/domain/`: the `quarter()` split, corrections and the month close with a lag, `POLICIES`, holidays as
  a tuple. `src/ems/contracts/availability.py`: run cadence per model and the price window opening.
- `src/ems/assembler/` (new): parameters, event mapping, meter stream, price and forecast streams, join, offline
  fold.
- `src/ems/actors/`: messages, hold-back, serde, shell, the seven actor files and the app; `probe.py` and
  `statecost.py` removed.
- `src/ems/experiments/`: `runs.py` (run parameters) and the harness `stack.py`; `tests/stack_fixtures.py`.
- `infra/docker-compose.yml` (ingress targets), `Makefile` (registration guard, `measure` modes),
  `jobs/ingress_measure.py`.
- Tests in all five tiers; `tests/fixtures/generator.faults.yaml`.
- Docs: 00, 01, 02, 05, 06, 08, 09, 11, 12, 13; ADRs 0018 and 0019; `README.md`; `CLAUDE.md`;
  `.claude/rules/actors.md`, a new `.claude/rules/assembler.md`, the post-edit hook; the `ems-actor-contract` and
  `ems-plan-change` skills (R7 wording).

Out of scope:
- The live deadline timer, adding a strategy at run time, the export CLI, the experiment runner, pacing, and
  the full `kill -9` matrix (stage 4).
- Forecast payloads, price rules, MPC (stage 5); the Selector (stage 6).
- Opening tranches and the proxy RCEm builder: the tests use RCE sites.
- Any change to wire contract v1, the topics or the generator.

## Affected

- Anyone running the local stack: the ingress containers now target `Household`, `Weather` and `Tariff`. A
  Restate volume that still holds the stage 2 deployment must have it replaced before ingestion opens;
  `make register` refuses otherwise.
- `make measure MODE=state-cost` no longer exists.
- The spec 009 functions keep their signatures and results. `National.holidays` changes type (tuple).
- No wire contract, topic, key or generator change. No reported number changes: no experiment result exists
  yet.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Late and missing readings follow one model (Priority: P1)
A reading can be on time, late or missing. The step waits `L` slots, runs on an estimate if it must, and a late
reading then corrects the ledger and nothing else.
**Acceptance**: AC-011-01 … -05, -08 … -12.

### User Story 2 - Any arrival order gives the same steps (Priority: P1)
A household's steps are a function of its streams' contents, not of how Kafka or Restate interleave them.
**Acceptance**: AC-011-06, -07, -13 … -23.

### User Story 3 - The actors run on Restate, fed from Kafka (Priority: P1)
Each record reaches its actor; steps flow to the strategies and through the real loop; nothing blocks or waits.
**Acceptance**: AC-011-24 … -26, -29 … -36.

### User Story 4 - A run is configured by messages (Priority: P1)
The operator configures the managed households once; everything else on the topics costs a counter.
**Acceptance**: AC-011-27, -28, -37.

### User Story 5 - Through Kafka equals offline (Priority: P1)
Ten households give household and strategy state equal to the offline side, and a real battery and meter equal
to the SelfConsumption strategy, in five runs and after a kill.
**Acceptance**: AC-011-38 … -43.

### User Story 6 - The environment moves from the probe to the actors (Priority: P2)
The harness, the compose file, registration and the measurement job work against the real services.
**Acceptance**: AC-011-44 … -47.

### Edge Cases
- A site without PV has no production stream: it is not waited for, its PV is 0 and never estimated.
- The first steps of a run have no visible forecast: the first run is usable at its issue time plus the
  model's delay (2 h for synthetic and ICON-D2 runs started on an issue time, longer for DMI or a start between
  issue times). Those steps carry no forecast issue time.
- A gap at the very start of a stream closes on the estimate 0.
- A stream that jumps ahead after a gap must not lose the on-time reading behind it (close before pruning).
- Production and consumption can both be late for one slot: the step carries both corrections in a fixed
  order.
- A correction attached to a step that is never released (the run ends) is never applied, on both sides.
- A delivery day published only on the day itself (PSE did this for 2025-10-23 at 12:34 and 2026-03-04 at
  08:05 local) has no visible price until 13:00 local: those steps use the statutory replacement, flagged
  `price_replaced`. A day whose only curve arrives after the next day's never becomes visible: every step of it
  uses the replacement.
- A duplicate published by `--faults` is two Kafka records; both are delivered and the second is dropped by
  the slot table (meters), by issue time (forecasts) or by `event_id` (prices).
- A missing forecast run makes the household wait for the next one; steps in between see the older run.
- The clean e2e run crosses the end of October, so a month close with its lag is compared through Kafka once;
  the October close with corrections and opening tranches is shown by pure tests.

## Requirements *(mandatory)*

- **FR-001**: The domain MUST expose the pieces of `quarter()` to the actors without changing its result, and
  MUST settle late readings by the allowed-lateness model. → AC-011-01 … -05
- **FR-002**: The assembler MUST be pure and MUST share one event mapping with the offline side.
  → AC-011-06, -07
- **FR-003**: Meter streams MUST apply the late, missing and duplicate rules inside the record's own stream.
  → AC-011-08 … -13
- **FR-004**: Price and forecast streams MUST set visibility and watermarks from content and schedule.
  → AC-011-14 … -16
- **FR-005**: Steps MUST be released in order, only on complete data, with no lookahead, and identically under
  any interleaving. → AC-011-17 … -23
- **FR-006**: The eight services MUST exist with one-way edges, thin handlers and Kafka-fed handlers that never
  raise. → AC-011-24 … -26
- **FR-007**: A run MUST be configured by idempotent control messages; unmanaged sites MUST keep O(1) state.
  → AC-011-27 … -29, -37
- **FR-008**: Receivers MUST order messages themselves, and stored bytes MUST be deterministic.
  → AC-011-30 … -33
- **FR-009**: Strategies and the real loop MUST run the reference's code, and the real loop MUST equal the
  SelfConsumption strategy. → AC-011-34 … -36
- **FR-010**: Actor state MUST equal the offline side through Kafka, in every run of the done-when and after a
  kill. → AC-011-38 … -43
- **FR-011**: The harness and environment MUST work against the real services and MUST refuse a stale
  registration. → AC-011-44 … -47

### Acceptance criteria (EARS)

`L` is the run parameter `late_slots` (default 8). "Equal" means `==` on the decoded dataclasses.

Domain
- **AC-011-01**: WHEN `reference.quarter` is called, THE SYSTEM SHALL return a result equal to that of the
  spec 009 body kept verbatim in a test, for every action kind and both price bases, while being the
  composition of `prices`, `decide`, `apply` and `grid_kwh`.
- **AC-011-02**: WHEN quarter-hours are settled with a lag of 0 and no corrections, THE SYSTEM SHALL give
  accumulators, hour rows and month settlements equal to those of spec 009; `settle_quarter` SHALL keep its
  signature and the spec 009 settlement, stepper and reference tests SHALL pass unchanged.
- **AC-011-03**: WHEN a correction for a quarter-hour is applied, THE SYSTEM SHALL recompute that quarter's grid
  energy from the corrected PV or load and the executed battery power and replace its hour's row (or the open
  hour's entry), so that every row of a month not yet closed equals `settle_hour` over that hour's final flows;
  the action, the executed power, the SoC and the flows the next decision observes SHALL stay unchanged.
- **AC-011-04**: WHILE the lag is above 0, THE SYSTEM SHALL close a civil month when the step that lies the lag
  after the month's last quarter-hour has been processed, after that step's corrections, with that month's own
  fixed charges and price basis; the settlement SHALL equal `settle_month` over the month's final rows, and
  with no corrections the settlement accumulator SHALL equal the lag-0 accumulator at every step outside those
  lag steps.
- **AC-011-05**: WHILE stepping with lag `2L`, THE SYSTEM SHALL keep at most `2L + 3` quarter-hours of flows
  for corrections and the rows of at most two months.

Assembler
- **AC-011-06**: THE SYSTEM SHALL keep every file of `src/ems/assembler/` free of Restate, I/O, clock and
  randomness; an import allow-list test (standard library, `ems.contracts`, `ems.domain`) SHALL fail otherwise,
  and the post-edit hook SHALL flag a Restate import there.
- **AC-011-07**: WHEN a measurement event is mapped, THE SYSTEM SHALL give PV energy as the sum of its `pv`
  generation entries and load as the `math.fsum` of its `load`, `heat_pump` and `ev_charger` consumption
  entries, ignoring battery, grid and SoC entries, and counting and ignoring an entry of those asset types that
  is not `active_energy`, `interval_sum`, in `kWh`, or that is marked `missing`; WHEN a price event is mapped,
  THE SYSTEM SHALL convert each slot through `pln_per_mwh_to_pln_per_kwh`. IF an event's resolution is not
  PT15M, a `PL:RCE` curve is not in `PLN/MWh`, a value lies beyond the mapping's bound (10⁴ kWh per entry,
  10⁵ PLN/MWh per price), or mapping it raises, THEN THE SYSTEM SHALL count the event and use none of it. The
  actors and the offline side SHALL use this one mapping.
- **AC-011-08**: WHEN the reading of slot `q` arrives before its stream has delivered slot `q + L` or a newer
  one, THE SYSTEM SHALL close slot `q` with that reading, not flagged.
- **AC-011-09**: THE SYSTEM SHALL close the slots of a stream once each, in ascending order, and SHALL keep at
  most `2L` slots per stream; pruning SHALL change no released step.
- **AC-011-10**: WHEN a stream delivers slot `q + L` or a newer one while slot `q` has no reading, THE SYSTEM
  SHALL close slot `q` with the value the stream used for slot `q − 1` (0 before the first reading), flagged
  `estimated`.
- **AC-011-11**: WHEN the reading of slot `q` arrives after `q` was closed on an estimate and while the stream's
  newest slot `n` is below `q + 2L`, THE SYSTEM SHALL emit one correction for `q` attached to step `n + 1` and
  SHALL change no step already released.
- **AC-011-12**: WHEN a reading arrives for a slot at or below the stream's newest slot minus `2L`, before
  `first_step`, or at or after `end_step`, THE SYSTEM SHALL count it (`too_old`, `before_start`, `after_end`)
  and ignore it.
- **AC-011-13**: WHEN a second record arrives for a slot that holds a reading, THE SYSTEM SHALL ignore it and
  count it `duplicate` (same `event_id`) or `conflict` (another id).
- **AC-011-14**: WHEN a forecast run of a known model arrives, THE SYSTEM SHALL make it visible from the later
  of its availability (issue time plus the model's delay) and the stream's watermark at arrival, and SHALL move
  the watermark to its availability plus the model's cadence; a run not newer by issue time than the newest
  accepted one SHALL be counted and ignored, and a run whose visibility the watermark moved SHALL be counted.
- **AC-011-15**: IF a forecast run names a `forecast_model` that is missing from the availability table, THEN
  THE SYSTEM SHALL count it, forward nothing and leave the watermark unchanged.
- **AC-011-16**: WHEN a price curve arrives, THE SYSTEM SHALL make it visible from the later of its publication
  time and the watermark at arrival; WHEN it belongs to a delivery day newer than every day seen, THE SYSTEM
  SHALL move the watermark to 13:00 Europe/Warsaw on that day; a curve of an older day SHALL be counted and
  ignored, an identical `event_id` SHALL be counted as a duplicate, and a curve whose visibility the watermark
  moved, or a second curve of the newest day, SHALL be counted.
- **AC-011-17**: THE SYSTEM SHALL release step `q` only when every needed meter stream (consumption, and
  production if the site has PV) has closed slot `q` and the price and forecast watermarks are at or past the
  end of `q`, strictly in order from `first_step`.
- **AC-011-18**: WHEN step `q` is released, THE SYSTEM SHALL give it: the closed PV and load values with their
  `estimated` flags (PV 0, not flagged, for a site without PV); the price of slot `q` from the curve of its
  delivery day that is visible at the start of `q` and has the latest publication time (ties: the first
  accepted), or the replacement of AC-011-19; the issue time of the newest forecast run visible at the start of
  `q`, or none; and the corrections attached to `q`, production's before consumption's.
- **AC-011-19**: IF no curve of the delivery day of `q` is visible at the start of `q`, THEN THE SYSTEM SHALL use
  the price of the same local quarter-hour (by Europe/Warsaw clock time; the first of the two copies on a
  100-slot day) from the nearest earlier delivery day, at most two days back, whose curve is visible at the
  start of `q` and has that quarter-hour, or 0 if there is none, and SHALL flag the step `price_replaced` and
  count it.
- **AC-011-20**: WHEN a step lies on a 92- or 100-slot delivery day (2026-03-29, 2026-10-25), THE SYSTEM SHALL
  read slot `q − t0` of that day's curve, each slot of the day exactly once; WHEN the replacement of AC-011-19
  crosses one of these days, THE SYSTEM SHALL match quarter-hours by local clock time.
- **AC-011-21**: WHEN the records of a household's streams are applied in any interleaving that keeps each
  stream's own order, and updates are delivered in any order and for any subscription point, THE SYSTEM SHALL
  release the same sequence of steps, every prefix of an interleaving SHALL release a prefix of it, and no
  transition SHALL raise.
- **AC-011-22**: THE SYSTEM SHALL give no step a curve or a forecast run whose availability lies after the
  step's start.
- **AC-011-23**: WHEN the offline fold runs over the records of a generated day without faults inside one civil
  month, THE SYSTEM SHALL release every step of the day and give, per strategy, a final `RefState` and quarter
  results equal to those of `reference.run` over inputs that a test builds by a plain per-slot lookup of the
  mapped readings and prices, without the assembler.

Actors
- **AC-011-24**: THE SYSTEM SHALL serve the Virtual Objects `Household`, `Tariff`, `Weather`, `Ems`, `Battery`,
  `Meter`, `NoBattery` and `SelfConsumption` from `ems.actors.app`, and no `Probe`, `StateCostEager` or
  `StateCostLazy`. Every edge between them SHALL be a one-way message addressed by service name, and the read
  handlers (`status`, `summary`, `state`, `bill`) SHALL be shared and write nothing.
- **AC-011-25**: THE SYSTEM SHALL deliver `ems.production.v1` and `ems.consumption.v1` to
  `Household.on_measurement`, `ems.weather.forecast.v1` to `Weather.on_forecast` and `ems.price.v1` to
  `Tariff.on_price`, one ingress container each.
- **AC-011-26**: WHEN a Kafka-fed handler receives a record, THE SYSTEM SHALL update that object's ingress log
  with `ingress_check.log_apply` and its record counter; IF the record is not valid JSON, breaks the v1
  contract or cannot be mapped, THEN THE SYSTEM SHALL count it and complete the invocation without an error.
- **AC-011-27**: WHEN `configure` reaches an object that has no parameters, THE SYSTEM SHALL store them, the
  number of records the object had already received, and the initial state, and `Household` SHALL send
  `Tariff.subscribe` for its market; WHEN `configure` reaches an object again with equal parameters, THE SYSTEM
  SHALL change nothing; IF the parameters differ from the stored ones or name another key than the object's,
  THEN THE SYSTEM SHALL fail that invocation without a retry and change nothing.
- **AC-011-28**: WHILE a `Household` or `Weather` object has no parameters, THE SYSTEM SHALL keep for it only
  the ingress log and the record counter, under 1 kB in total whatever the number of records, and SHALL send
  no message.
- **AC-011-29**: IF an internal message reaches an object that has no parameters, THEN THE SYSTEM SHALL fail
  that invocation without a retry and SHALL keep handling later invocations of the key.
- **AC-011-30**: WHEN messages reach a receiver out of order, THE SYSTEM SHALL apply them in `t` order (steps,
  commands, acknowledgements, flows) or `seq` order (price and forecast updates), hold back early ones, drop
  and count stale ones and count every re-ordering; the receiver's compared state SHALL not depend on the
  delivery order.
- **AC-011-31**: WHEN a household subscribes, THE SYSTEM SHALL send it a snapshot of every accepted curve with
  the watermark and the sequence number it is valid for, then one update per curve accepted afterwards.
- **AC-011-32**: THE SYSTEM SHALL give every internal message a `schema_version` and a `t`, every price and
  forecast update a `seq`, and the control record `RunParams` a `schema_version`; IF the field names of a
  message type or of a type nested in it change, THEN a unit test that pins them SHALL fail.
- **AC-011-33**: WHEN a state, parameter or message value is encoded and decoded, THE SYSTEM SHALL return a
  value equal to the original with every float bit-identical; no such type SHALL contain a set; and two
  processes with different hash seeds SHALL produce identical bytes for the same value.
- **AC-011-34**: WHEN a strategy object applies step `q`, THE SYSTEM SHALL run `stepper.step` with that
  strategy's policy, the step's corrections and lag `2L` on its own virtual battery and ledger.
- **AC-011-35**: WHEN `Ems` holds step `t` and the acknowledgement of `t − 1` (or `t` is the first step), THE
  SYSTEM SHALL decide `t` with the live policy from the SoC of that acknowledgement and send `Apply`; `Battery`
  SHALL resolve the action against the step's flows, clamp it to its physical limits, count a clamp, store the
  new SoC and send `Ack` and `Flows`; `Meter` SHALL derive the grid energy with `settlement.grid_kwh` (the
  function `quarter()` uses) and settle with `stepper.settle_step`. `Ems` SHALL never decide `t` before the
  acknowledgement of `t − 1`.
- **AC-011-36**: WHILE SelfConsumption is the live strategy, THE SYSTEM SHALL hold, once a step has passed
  through both paths, a `Battery` SoC equal to the SelfConsumption object's virtual SoC and a `Meter` ledger
  (accumulator, kept flows, closed months) equal to that object's; WHEN all transitions run as an in-memory
  network over generated streams with faults and messages are delivered in an arbitrary order, THE SYSTEM
  SHALL end with these equalities and with strategy states equal to the offline side's.
- **AC-011-37**: WHEN the parameters of a run are built, THE SYSTEM SHALL return one record per EMS-managed site
  of the generator's catalog (`first_step` on a UTC hour, `end_step`, `late_slots`, strategies, live strategy,
  `has_pv`, initial SoC, the `SiteModel` of `ems.tariffs.site_model`, the RCEm table); the same records SHALL
  go to the actors, each `configure` call carrying an `Idempotency-Key`, and to the offline fold; IF a managed
  site cannot be settled, `first_step` is not on a UTC hour, the initial SoC lies outside the battery's bounds,
  or a step of the window has no tariff version or RCEm entry, THEN THE SYSTEM SHALL fail before anything is
  sent.

End to end (10 households through Kafka; three managed sites, one of them without PV)
- **AC-011-38**: WHEN the managed households are configured and a backfill without faults from
  2026-10-31T00:00Z over 30 h has been handled, THE SYSTEM SHALL hold for every managed household (its join)
  and strategy a state equal to the offline side's, every step of the window released, October's settlement
  closed in every strategy and in `Meter`, AC-011-36 true for `Battery` and `Meter`, and no failed or paused
  invocation.
- **AC-011-39**: WHEN a backfill without faults of 2026-10-20 × 1 day is ingested with the meter topics first
  and the price topic last, and again with the price topic first and the meter topics last (one ingress
  container started after the other has finished), THE SYSTEM SHALL end in both runs with states equal to the
  offline side's as in AC-011-38, every step of the day released and AC-011-36 true.
- **AC-011-40**: WHEN a backfill of 2026-10-20 × 1 day with the test fault configuration is replayed at
  `L` = 8, THE SYSTEM SHALL end
  with states equal to the offline side's and AC-011-36 true; IF the offline side shows no estimated slot, no
  duplicate, or any correction, THEN the test SHALL fail.
- **AC-011-41**: WHEN a backfill of 2026-10-20 × 1 day with the test fault configuration is replayed at
  `L` = 2, THE SYSTEM SHALL end
  with states equal to the offline side's and AC-011-36 true; IF the offline side shows no correction or no
  `too_old` reading, THEN the test SHALL fail.
- **AC-011-42**: WHEN any of these runs has been handled, THE SYSTEM SHALL hold for every Kafka-fed object and
  topic an ingress log equal to the offline fold of the records read back from Kafka, for every unmanaged key
  only what AC-011-28 allows, and for every managed object 0 records received before its `configure`.
- **AC-011-43**: WHEN every process of the SDK service is killed with SIGKILL during a configured backfill,
  after the first and before the last step, and the service is started again, THE SYSTEM SHALL end with states
  equal to the offline side's and AC-011-36 true (fault tier).

Harness and environment
- **AC-011-44** (reworded with the plan, 2026-11-20): IF a target handler of an ingress container
  (`Household.on_measurement`, `Weather.on_forecast`, `Tariff.on_price`) is not registered in Restate, THEN
  `make register` SHALL exit non-zero before starting an ingress container; IF the handlers Restate lists for
  the eight services differ from those of `ems.actors.app`, THEN the test harness SHALL replace the stale
  deployment before it opens ingestion and SHALL fail if they still differ. *(First form: `make register`
  compared the whole service set. The three target handlers are what protects ingestion, and they can be
  checked with `curl` alone.)*
- **AC-011-45**: WHEN a test waits for a configured run, THE SYSTEM SHALL return only when every published record
  has been handled and every managed strategy, `Ems`, `Battery` and `Meter` expects the step after the last one
  the offline side released; it SHALL fail at once when an invocation of the run has failed or is paused, and,
  outside the fault tier, when one has recorded a failed attempt.
- **AC-011-46**: WHEN `make measure MODE=skew` runs, THE SYSTEM SHALL report per managed household the largest
  lead between its streams, prices included, and the re-order counters of every receiving actor;
  `MODE=backlog` SHALL work against the real services, and no `state-cost` mode SHALL exist.
- **AC-011-47**: THE SYSTEM SHALL keep AC-010-01, AC-010-03 … -16 and AC-010-26 true with `Household`,
  `Weather` and `Tariff` as the targets (in AC-010-03 the service listed is `Household`; in AC-010-06 the
  object whose state must survive is a `Household`), tested with no household configured.

## Constraints (from the checklists)

- **P1**: spec, plan and tasks are approved before code; each commit cites a task.
- **P2**: the new domain functions and `src/ems/assembler/` are pure (allow-list tests, post-edit hook). Every
  exclusive handler has one shape: read own state, call a pure transition, write own state, send.
- **P3**: owners as in 01. The ingress log row moves from `Probe` to the Kafka-fed actors. New row: a run's
  parameters are written once per object by the run's operator (`configure`).
- **P4, R1, R2**: every edge between actors is a one-way message, including the two cycles (Household and
  Tariff; Ems and Battery). Request-response is used only from outside, to shared handlers.
- **P5, R3, R5**: handlers make no external call, read no clock and never wait; waiting is buffering in state
  until the watermarks pass.
- **P6, R4**: Restate deduplicates redelivered records and a handler's own sends; the slot table drops an event
  published twice; `configure` carries an `Idempotency-Key`. No result depends on arrival order across topics
  or actors (AC-011-21, -30, -36, -39). The e2e and fault tests compare with `==`.
- **P7, R6**: no timer and no self-rescheduling chain in this stage; the live deadline is stage 4.
- **P8**: `Battery` clamps every command through the battery model, whatever the policy asked (AC-011-35).
- **P9, R7**: the internal messages are new at version 1.0.0; no wire contract or topic changes. R7 is
  reworded: `t` is the sim timestamp.
- **P10**: `jobs/ingress_measure.py` stays a CLI; the offline fold is a library function without HTTP.
- **P11**: no new setting and no secret.
- **P12**: no new dependency and no version bump (pydantic is already locked).
- **P13**: no tracing.
- **P14, P16**: `make measure` results keep their manifest and report the skew and re-order counters; the e2e
  runs are tests, not experiments.
- **P15**: docs and ADRs 0018 and 0019 land with the change.
- **Stop rule**: IF AC-011-38 … -43 cannot be met on the pinned versions, the stage stops and the cause is
  recorded before any rule is changed.
- **Risks** (`09`): K7 is built (the join). A1 is exercised by the arrival-order runs and stays accepted. A2
  gets its constants and a generator test. A3 gets its rule (AC-011-19). K12 is tightened by AC-011-44. New
  rows: month bills trail the month's end by `2L`; an object's whole state is rewritten every step; `Tariff`
  keeps every curve of a run; unstable bytes would cause a journal mismatch on a retry (guarded by
  AC-011-33); removing a service means removing its deployment; a month's bill appears `2L` quarter-hours after
  the month ends.

## Success Criteria

- **SC-001** (review): every doc listed under Scope matches the built state; ADRs 0018 and 0019 are accepted
  and indexed; the OPEN items of the table above are closed in 01 and 08; V5 carries a value, the result file
  and the date; the stage 4 brief is refreshed.
- **SC-002**: `make ci` is green locally and in GitHub Actions; `make test-integration`, `make test-e2e` and
  `make test-fault` are green on the author's machine, with their output in the pull request.
- **SC-003**: the stage 3 row of the stage table in `00-overview.md` is marked done, with "arriving in
  different orders" as its wording.

## Assumptions

- In a clean backfill the price and forecast watermarks pass the end of the window, so every step is released.
  *Confirmed in `plan.md` (Facts) by a prototype over the generator: 96 steps for 2026-10-20 × 1 day, 120 for
  the 30 h from 2026-10-31.*
- Replacing a stale deployment with `force: true` works on the pinned server as the Restate admin API
  describes. A task confirms it; if it does not hold, AC-011-44 is amended before the switch.
- With the raised fault rates a 10 × 1 day run contains at least one estimate, duplicate, correction and
  `too_old` reading with a probability that makes a vacuous run negligible; the tests assert it anyway.
- The emulated ingress image stays fast and stable enough for the tiers on the author's machine, as in
  spec 010.

## Open questions

Decided in `plan.md`:
- the field list of every message and state type (Actor contract);
- how `ensure_registered` replaces a stale deployment, and the exact check in `make register`;
- the exact fault rates of the test fixture and the size and kill point of the fault-tier run;
- where the arrival-order-dependent counters are kept so that they stay out of every comparison;
- the hypothesis sizes of the interleaving and network properties;
- the file names of ADRs 0018 and 0019.
