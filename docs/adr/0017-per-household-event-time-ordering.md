# 0017. Per-household event-time watermarks order the actors' inputs

- Status: accepted
- Date: 2026-11-14
- Spec: none (docs-only decision; the model is in `docs/context/01-architecture.md`)

## Context
A household's inputs arrive on four Kafka topics (production, consumption, forecasts, prices; ADRs 0011–0013).
Restate keeps the order of one topic partition per Virtual Object key, and documents nothing about order or
relative speed across topics or partitions, or about the order of messages between two objects across separate
invocations (<https://docs.restate.dev/services/integrations/kafka>,
<https://docs.restate.dev/develop/go/service-communication>, checked 2026-11-14). In a backfill each topic is
consumed at its own speed: a household can be on day 10 while its forecasts are on day 2. Results would then
depend on consumption speed, not on data. That breaks deterministic replay, the bit-exact check against the
offline reference (P6) and the fairness of the strategy comparison (every strategy must see identical inputs).
The same design has to work live, where time advances on the wall clock (P7), through the one ingestion path
of ADR 0011.

## Decision
Each household orders its own inputs by event time (option B2 below); there is no global clock or barrier.
- Step `q` covers one quarter-hour. It reads its own meter slot, and the prices and forecasts with
  `available_at` at or before its start.
- Every input stream of a household has a **watermark**: everything a step ending at or before it may read
  from that stream has arrived or is declared missing. The household's safe time is the minimum of the
  watermarks of the streams it needs. It releases steps strictly in order from a first step given as a run
  parameter, each only when the safe time is at or past the step's end.
- Watermarks come from the stream's content and schedule, never from the wall clock in sim: a meter stream
  closes a slot when its record is there or the same stream is `L` slots past it; a forecast run moves the
  watermark to its availability plus the run cadence; the curve of a new delivery day moves it to the opening
  of the next publication window. A sparse stream's watermark is thus the earliest moment its next record can
  become usable, and a step starting at that moment waits for that record.
- A record becomes visible from the later of its `available_at` and the stream's watermark when it arrived.
- Lateness is judged inside the record's own stream: wait `L` slots, then decide on an estimate flagged
  `estimated`; a record up to `2L` late corrects the settlement of its hour and never a decision, the executed
  battery power or the SoC; later ones are dropped and counted. `L` defaults to 8 quarter-hours.
- Duplicates are dropped by slot (stream, `t`) and `event_id` in actor state; different content for a slot is a
  revision.
- Order between actors is not assumed: updates carry a sequence number or `t` and are applied in order.
- Live mode runs the same rules and adds one durable wall-clock timer per household. The household's effective
  watermark is the higher of the received one and "now minus a grace period".
- The offline side applies the same rules through the same pure functions and feeds the released steps to the
  stepper of spec 009.

## Consequences
- A household's steps are a function of its streams' contents, so replays are deterministic and the reference
  can read each stream separately. Property tests check that any interleaving gives identical steps (stage 3).
- Sim and live share the handlers and the Kafka path; households stay independent, so the design scales with
  households and strategies.
- Buffers grow with the skew between a household's inputs. An unpaced backfill stays correct but gets slow;
  paced publication bounds the skew (risk A1).
- The run cadence and the publication window are assumptions about the sources (risk A2). A republished price
  day becomes visible only at the next window, for decisions and settlement alike (risk A3). A live run and its
  replay can differ where a deadline fired (risk A4).
- A run needs no end marker: steps the rules cannot release stay unreleased, in the actors and on the offline
  side alike. A run can therefore stop mid-hour, so the comparison uses the stepper, not the batch reference.
- The domain needs an addition for corrections: `settle_quarter` and `stepper.step` close an hour on its fourth
  quarter and reject an out-of-order quarter (stage 3).
- Scheduled commands become data in actor state, fired at their step in both modes. This narrows a consequence
  of ADR 0011, which kept durable timers for them; durable timers remain for the live deadline and forecaster
  refreshes.
- It replaces the sim-clock sketch of the brief §5.5 and of `06-data-flows.md` (virtual timers fired by event
  timestamps). Changed docs: `docs/context/` 00, 01 (new), 05, 06, 08, 09, 11, 12, 13, the `ems-actor-contract`
  and `ems-plan-change` skills and the actor rule.

## Alternatives considered

| Criterion | A: replay driver feeding HTTP ingress | B1: global Clock actor | B2: per-household gating (chosen) |
|---|---|---|---|
| Determinism | Only if the driver waits for the actors after every step; messages between actors still race | Yes, by one barrier | Yes, by rules that look at one stream at a time |
| Same code path live and sim | No: sim bypasses the Kafka path, against ADR 0011 | Partly: the Clock is a sim-only piece | Yes |
| Scaling with households × strategies | One sequencer | Poor: one hot key, and the slowest household holds the fleet | Good: households are independent |
| Simplicity | A second ingestion path and a driver | One more actor and a barrier, and it still needs every stream's watermark | One pure assembler per household |
| Defensibility | The measured system is not the live system | The barrier contradicts the fleet-scale claim | The standard event-time model, applied per key |

- A driver that publishes one simulated day at a time and advances watermarks through ingress: a global
  barrier and a sim-only path.
- Watermarks from record times alone, without the schedule: households would trail the price stream by up to a
  day.
- Heartbeat records on Kafka: a wire-contract change (ADR 0016) that the schedule makes unnecessary.
