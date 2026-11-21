# 0018. Late and missing inputs: allowed lateness, estimates and the replacement price

- Status: accepted
- Date: 2026-11-20
- Spec: specs/011-actor-skeleton

## Context
ADR 0017 orders a household's inputs by event-time watermarks and left five rules to the actor stage
(`01-architecture.md`, stage 3 brief): how a late reading reaches the ledger, the estimate for a missing
reading, the price of a quarter-hour with no visible price, what to do with a forecast run of an unknown model,
and how a second record for a meter slot is treated. Each rule must be deterministic, so the actors and the
offline side give the same results bit for bit (P6), and identical for every strategy, so the comparison stays
fair.

Facts the decisions rest on (read 2026-11-20):
- `settle_quarter` closes an hour on its fourth quarter and a month on its last hour, and refuses a quarter out
  of order (spec 009). Hourly balancing: RES act art. 4 ust. 2b.
- Stream processors keep a window open for an allowed lateness: a late element is added to its window and fires
  it again, and the new result replaces the earlier one
  (<https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/windows/>).
- RES act art. 4b ust. 10: when RCE cannot be determined for a period, the value of the same period on the
  nearest preceding day for which it was determined applies (Dz.U. 2021 poz. 2376, unchanged by Dz.U. 2024
  poz. 1847).
- PSE published the RCE of 2025-10-23 and 2026-03-04 only on the day itself (12:34 and 08:05 local); every other
  day of 2025-10-01 … 2026-11-21 was published on D−1 from 13:17 local on (PSE API `rce-pln`).
- A measurement's `event_id` is built from stream, site, interval, source and run, not from content
  (`ems.contracts.ids`), so a re-sent reading with new values keeps its id.

## Decision
- **Allowed lateness.** A step waits `L` slots of its stream for a missing reading. A reading that arrives up to
  `2L` slots late re-fires its hour: the quarter's grid energy is recomputed from the corrected PV or load and
  the battery power that was executed, and the hour's row is replaced. The decision, the executed power and the
  SoC never change. A civil month is settled `2L` quarter-hours after it ends, with its own fixed charges and
  price basis, so no correction inside the lateness window is lost. A reading older than `2L` is counted and
  dropped. The settlement code is `add_quarter`, `correct`, `due_month` and `close_month`; with a lag of 0 they
  equal spec 009's `settle_quarter`.
- **Estimate.** A slot closed without its reading takes the value its stream used for the slot before (0 before
  the first reading), flagged `estimated`.
- **Replacement price.** A quarter-hour without a visible curve of its delivery day uses the price of the same
  local quarter-hour on the nearest earlier delivery day with a visible curve, at most two days back (by clock
  time; the first copy of the repeated hour on a 100-slot day). With none, the price is 0. The step is flagged
  `price_replaced` and counted. Decision and settlement use the same price (ADR 0017).
- **Unknown forecast model.** A run whose model has no delay or cadence in `ems.contracts.availability` is
  counted and dropped; the household waits for the next known run, as for a missing run.
- **First reading wins.** A second record for a meter slot is counted (`duplicate` for the same `event_id`,
  `conflict` for another) and ignored. This narrows ADR 0017's "different content for the same slot is a
  revision" to prices; a forecast run whose issue time was seen is ignored as stale.

## Consequences
- The ledger is current: an hour's row exists as soon as the hour ends and is updated by late readings. Month
  bills and tranches appear `2L` quarter-hours after the month ends (16 quarter-hours, 4 h, at the default
  `L` = 8), so a run must extend that far past a month it wants closed (risk A8): June of the
  spring-2026 recording closes only at `L` ≤ 4, December 2026 cannot close while the tariffs end with 2026, and an
  RCEm site needs the RCEm entry of the following month.
- State: each strategy and `Meter` keep the flows of the hours still open to correction (at most `2L + 3`
  quarter-hours) and, during a lag, the rows of two months.
- A corrected quarter keeps the battery power resolved on the estimate, so the mode-action invariants of
  `12-domain-model.md` hold for the step's inputs, not for the corrected flows (D6). NoBattery is corrected
  exactly; SelfConsumption keeps the estimate's error as grid exchange.
- The replacement follows the statute where no price was ever determined. Where PSE published late, the legal
  bill would use the published price; the model uses the replacement for both, because a step may read only what
  was visible at its start (risk A12). Results report `price_replaced`, and estimate and correction counts.
- Docs changed: `01-architecture.md` (rules as built), `06-data-flows.md`, `12-domain-model.md`,
  `11-experiments.md` (bill windows), `09-risks-and-tech-debt.md` (A3, A8, A12).

## Alternatives considered
- **Settle only what is final.** Each quarter waits `2L` and is settled once; the ledger is append-only and
  nothing is dropped. Not chosen: rows and bills lag the steps by `2L` everywhere, while allowed lateness keeps
  rows current and needs the lag only at month ends.
- **Close the month on time and drop late corrections.** Simplest settlement. Not chosen: a reading late across a
  month end would be lost from that month's bill.
- **Estimate by the same quarter yesterday.** Follows the daily shape, but needs 96 slots of history per stream
  and a rule for the first day. **Zero.** No state, but always underestimates load and biases an uncorrected
  quarter towards export.
- **Price 0, flagged** (the first decision). No state and never stalls, but has no legal source and is not neutral
  between strategies: it removes export revenue in proportion to export, so NoBattery loses most. **The previous
  step's price** carries an evening price into midday. **Stopping the household** blocks a long run on one bad
  day.
- **The most conservative delay and cadence for an unknown model.** Keeps stepping, but rests on two guesses; a
  wrong one means lookahead or a stale forecast.
- **Revisions by payload version.** Implements ADR 0017 as written, but no source emits such records and it needs
  a rule against an old duplicate flipping the value back.
