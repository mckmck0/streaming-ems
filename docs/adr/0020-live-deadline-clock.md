# 0020. The live deadline clock

- Status: accepted
- Date: 2026-11-25
- Spec: specs/012-clocks-systems-experiments

## Context
ADR 0017 orders a household's inputs by event-time watermarks that come from each stream's content and
schedule. Live, that alone can stall: a meter slot closes only on its reading or `L` newer readings (up to two
hours of wall time), and a late price or forecast publication holds every step that needs it. ADR 0017 named
the remedy, a durable wall-clock timer per household that raises the effective watermarks to "now minus a
grace period", and left its grace, catch-up and generation open for stage 4. Experiments stay on simulated time
(P7), so the timer must change nothing in sim.

## Decision
- One grace `G` per run (`RunParams.grace_s`, default 120 s) for all four streams. The clock step is
  `⌊(now − G) / 900 s⌋`, capped at `end_step`: every step below it ended at least `G` ago.
- `join.on_clock` (pure) closes each needed meter stream below the clock step, on its reading or on the
  ADR 0018 estimate, raises the price and forecast watermarks to it, and releases what the rule allows. The
  stream's horizon (the higher of its newest reading and its last closed slot) stands in for the newest reading
  wherever lateness is judged, so a reading for a slot the clock closed becomes a correction or `too_old`
  exactly as in sim. A correction that could only attach at or after `end_step` is counted (`after_end_late`).
- A curve or run that reaches the household behind its watermark becomes visible from that watermark (counted).
  In sim the sources already stamp visibility at or above their watermark, so nothing moves.
- `Household.on_deadline` is a delayed self-send (`send_delay`), the only self-rescheduling chain. It reads the
  time once through the journaled `ctx.time()`, so a retried firing writes the same bytes. It starts when the
  household has applied its price snapshot (not at `configure`, so a first firing cannot raise the price
  watermark past curves the household has not seen) and stops at `end_step`.
- Catch-up: a firing releases every step that is due at once, then sets the next deadline for when the clock step
  next increases, plus a 1 s margin. A firing that comes early (the Docker VM clock leads the host) reschedules
  the same step after at least 1 s and is counted as early.
- Generation (R6): `Deadline.t` is the step the deadline was set for; the household stores the one it expects
  and counts any other as stale.

## Consequences
- Live decisions never wait more than `G` (plus Restate's latency) after a quarter ends; a missing reading costs
  an estimate and later a correction (ADR 0018), never a stall.
- In live mode a step is released at least `G` + 1 s after it ends. `Battery` resolves mode actions against the
  realised flows (risk D6), so it is a real-time shadow of the inverter's local control, not a command sent to a
  device ahead of the quarter.
- A curve published before a quarter starts but delivered more than `G` after it ends (Kafka lag, a kill) is
  priced by the statutory replacement in live, where sim would use it: risk A12 widens to delivery delay, and
  results report `price_replaced`.
- A live run has no offline twin (where the clock closes a slot depends on when it fires, risk A4): live runs are
  checked by invariants, not `==`.
- Docs changed: `01-architecture.md` (live mode, handlers, R3/R6), `02-subsystem-actors.md`, `06-data-flows.md`,
  `08-contracts.md` (`RunParams` 1.1.0, `Deadline`), `09-risks-and-tech-debt.md` (A4, A12),
  `12-domain-model.md` (`after_end_late`).

## Alternatives considered
- **A grace per stream.** Three parameters with little gain: the price and forecast watermarks already have
  slack (the 13:00 window opening, the run cadence), so only meters are sensitive to `G`.
- **A grace of one quarter-hour.** Fewer estimates for readings minutes late, but a missing reading stalls its
  household for 15 min.
- **Starting the chain at `configure`.** Simpler, but a first firing could raise the price watermark before the
  snapshot arrives and clamp every curve to it.
- **A generation counter bumped on restart.** Nothing restarts a chain (`configure` is idempotent); the expected
  step detects a duplicate chain as well and needs no extra state.
