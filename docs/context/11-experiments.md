# 11 · Experiments

## Current consensus

- Protocol (brief §12): each experiment has an id, hypothesis, YAML config, dataset and period, seeds, metrics,
  acceptance criteria, output location (Parquet) and records git commit and hardware (principle 14).
- Energy metrics: cost and savings vs NoBattery and SelfConsumption, self-consumption ratio, self-sufficiency,
  equivalent full cycles, constraint violations, regret vs the oracle. Systems metrics (principle 16): events/s,
  invocations/s, decision latency p50/p99, recovery latency by phase, exactly-once check, view staleness.
- Statistics: per-household paired comparisons; report distributions, not only means.
- Raw results live under `experiments/results/` (gitignored, never edited by hand); numbers quoted in docs name
  the result file they come from.
- Energy and per-step results are read from Restate Virtual Object state after a run and written to Parquet by a
  batch CLI (P10). No Kafka egress topic is built for results (spec 006).

### Cost metric (spec 009; definitions in `12-domain-model.md`)

Every energy result reports three cost numbers per household and strategy, from the offline reference or actor
state:
- `bill_pln`: Σ monthly `payable_pln` − Σ refunds, the primary cost. A refund is booked in its tranche's expiry
  month (paid in the next). Bill metrics use windows of whole civil months plus `2L` quarter-hours: the actors
  settle a month `2L` quarter-hours after it ends, so late readings can still correct it (ADR 0018, risk A8).
- Every energy result reports, per household, its estimated slots, corrections, readings dropped as too old and
  quarter-hours priced by the statutory replacement (`price_replaced`); they are identical for every strategy.
- `deposit_closing_pln`: the deposit left at the window's end, shown beside the bill and never netted (valuing it
  needs an assumption about the future).
- `ledger_net_cost_pln`: Σ hourly (energy + distribution cost − 1.23 × export value). Additive, so it suits
  per-interval paired statistics; it values export as if the deposit is always used, like the price signal.

Savings are reported against NoBattery and SelfConsumption on both `bill_pln` and `ledger_net_cost_pln`. Results
record the tariff version ids, the RCEm entries used (source `pse` or `proxy`) and the cohort (P14).

### Data-generator benchmark (spec 003)

`make bench` → `experiments/results/benchmark-<UTC>/results.json` (window, config, versions, hardware, commit).
First run 2026-10-20 (`benchmark-20261020T202144Z`): one simulated day, 1,000 sites 14 s for 174,562 events,
10,000 sites 199 s for 1.74 M events; Kafka producer 4.9k events/s for 1,000 sites. Rerun 2026-11-11 after spec 007
(`benchmark-20261111T005219Z`): the same event counts, array generation 4.4 s for 10,000 sites (was 3.0 s), event
building 8.9k/s, producer 5.1k events/s. Details in `13-subsystem-ingest.md`.

### Real-data window (spec 008)

`data/recordings/spring-2026` (not committed; rebuild with `make history NAME=spring-2026 START=2026-04-02
END=2026-07-01`): 2026-04-02 … 2026-06-30 at the Poznań test site (ICON-D2 forecasts) and a Radom household
(DMI HARMONIE). Real: PSE RCE prices with their publication times, archived forecast runs of record, and the
weather the meters respond to (satellite and ERA5). Synthetic: household behaviour (base load, appliances, EV) and
the PV and heat-pump models that turn the observed weather into energy. Numbers (36,079 events, gap shares,
missing runs, hash `75fc631fc194…`) are in `13-subsystem-ingest.md`.

- Experiments replay it (`make replay NAME=spring-2026`, with `BACKGROUND=1` for a synthetic fleet around the two
  sites) into the sim profile and never refetch. A result names the recording, its `events.jsonl` SHA-256 and
  `run_id` (from `build.json`) beside config, seed, hardware and commit (P14).
- Per-day scoring uses only the complete local delivery days in `build.json` (2026-04-03 … 06-30); the window
  starts and ends at 02:00 local.
- Forecasts become usable 120 min (ICON-D2) or 195 min (DMI) after issue; the first ones arrive after `start`, so
  the first hours are a warm-up.
- Limits: spring only (13.5 % negative RCE quarter-hours in April, 8.3 % in May, 4.4 % in June); households stay
  modelled; background sites use synthetic weather and forecasts uncorrelated with the window's real prices, so they
  serve scale and systems metrics, not energy results.

### Kafka ingress and the reference spike (spec 010)

Run with `make measure [MODE=skew|state-cost|backlog]` and `make spike` on commit `78e6ecd` (Apple M4, Docker
Desktop 29.7.2, 7.75 GiB VM); each result directory stores its manifest (P14). Details and reading in
`docs/thesis/kafka-ingress.md` and `docs/thesis/spike-reproduction.md`.

- `ingress-skew-20261116T081120Z`: 100 households × 2 days (34,627 records) unpaced through the ingress into a
  probe object; every key's log equal to the offline fold. 404 records/s server-side (85.6 s). Largest lead of one
  of a household's streams over another, in quarter-hours: weather forecast over production median 160, max 167;
  consumption over production median 127, max 130; production over consumption median 6, max 15. Peak memory:
  Restate 1,556 MiB, Kafka 797 MiB, ingress containers 288–558 MiB; no OOM kill.
- `ingress-state-cost-20261116T081221Z`: a one-key call costs a median of 2 / 3 / 9 ms with 1 kB / 100 kB / 1 MB
  of eager object state (299 / 235 / 84 calls/s); lazy state 3 / 4 / 3 ms, with one 39 s outlier at 1 MB.
- `ingress-backlog-20261116T081316Z`: with the SDK service stopped, the Kafka lag still fell to 0 within 7.4 s and
  3,426 records waited in Restate as invocations; 8.8 s to drain after the service came back.
- `spike-20261116T080544Z`: the spike's A, B and C, unedited, on Restate 1.8.1-hotfix.2 with its own
  `restate-sdk` 1.0.5: 0 mismatches in A (150 states) and C (200), and in B after confirmed `kill -9` of the SDK
  service and the server (14,400 events, 96 per strategy state).

### Actors through Kafka (spec 011)

The first comparison of actor state with the offline side through Kafka is a test tier, not an experiment
(`tests/e2e/test_actor_backfill.py`): 10 households (three EMS-managed), five runs (clean across the end of
October, two arrival orders, faults at `L` = 8 and `L` = 2). In each, the household join and every strategy equal
the offline fold with `==`, and the real `Battery` and `Meter` equal the SelfConsumption strategy. `make measure
MODE=skew` now runs on configured households and reports the price leads and the re-order counters (V5 in 01).

- `ingress-skew-20261121T005630Z` (commit `92d0f0c`, clean tree): 100 households × 2 days unpaced, 35 of them
  managed and configured, 34,627 records; every ingress log equal to the offline fold, 0 rejected, 0
  regressions. 257.8 records/s server-side (134.3 s), against 404 records/s with the stage 2 probe: each step now
  also runs two strategies, `Ems`, `Battery` and `Meter`. Largest lead of prices over a household's other
  streams: median 122 quarter-hours over consumption and production, 97 over forecasts; of forecasts over
  production 169. Re-order counters 0 for every receiving service (V5). Peak memory: Restate 1,833 MiB of
  2 GiB, Kafka 838 MiB, ingress containers 284–565 MiB; no OOM kill.
- `ingress-backlog-20261121T005744Z` (code of commit `92d0f0c`; the tree had uncommitted doc edits only): 20
  households × 1 day published with the SDK service stopped and no household configured. The Kafka lag fell to
  0 within 8.6 s; 3,385 invocations waited `pending` and 41 `ready` (one at the head of each key's queue: 20
  `Household`, 20 `Weather`, 1 `Tariff`); the backlog drained 10.4 s after the service came back.

### After the harness, memory and serde fixes (spec 012)

- `ingress-skew-20261125T101839Z` (commit `90bb678`; the tree carried uncommitted spec 012 tests and runner stubs,
  not on the measured path): the same 100 × 2-day unpaced run with 35 managed households. Every ingress log equal
  to the offline fold, 0 rejected, 0 regressions, re-order counters 0. 285.6 records/s server-side (121.2 s),
  against 257.8 before the fixes (H5 polling in SQL, K15 memory pools, A15 serde check). Peak memory (`docker
  stats`, page cache included): Restate 1,897 MiB of 2 GiB, Kafka 990 MiB; no OOM kill; Restate's anonymous memory
  after the run 1,506 MiB. Details in `docs/thesis/kafka-ingress.md`.

### Experiment runner (spec 012, ADR 0022)

`make exp NAME=<id>` reads `experiments/configs/<id>.yaml` (`ems.experiments.config`): id, hypothesis, data
(synthetic: start, days or hours, locations, managed households, faults, seed), run (strategies, live strategy,
`L`, `mode`, `grace_s`, speed), kills (`sdk` or `server` at a fraction of the run), strategy changes (at an hour
of the run), and a `matrix` (households × strategy sets), a speed `sweep` (doubling until a speed is not
sustained) or `repetitions`. Each run, on a fresh stack (`ems.experiments.run.run_one`):
1. wipes and starts Kafka and Restate with journal retention `0s`, the SDK service with the timing log, the
   ingress containers (`ems-exp-*`);
2. builds the parameters and the proxy RCEm table, and configures every managed object;
3. publishes paced (`ems-gen run --mode accelerated --speed S`, or `realtime` in live mode), sends the strategy
   changes before their hour and kills on schedule;
4. detects the end from the timing log (each household's `Meter` committed its last step), or a stall (no
   `Meter` advanced for a set time, reported with the stuck invocations and the last `t` per service);
5. checks exactly-once: in sim every object's state equals the offline twin (with the recorded schedules) with
   `==`; in live, invariants (every step released, `Battery` and `Meter` equal the live strategy's shadow); then
   that no object still holds messages;
6. exports the Parquet tables of `08-contracts.md` and writes `summary.json` and `manifest.json`.

A run that fails is recorded with `status: failed` and its reason; the next run still starts. A publisher that
exits with an error fails the run at once, with the end of its log in the reason (live runs check it every 5 s). A run during which
the host slept (the wall clock ran more than 1 s ahead of the monotonic clock, which stops in sleep) fails as
`the host slept N s`, since a sleep stalls the stack and the publisher and inflates every lag; `host_slept_s` is in
every summary. On macOS `make exp` runs under `caffeinate -i`, which prevents idle sleep but not a closed lid.

**Systems metrics** (`ems.experiments.metrics`; from the timing log, first line per invocation):
- `decision_latency_ms`: per household-step, `Battery`'s commit that sends the `Ack` of `t` minus `Household`'s
  commit that releases `t`; p50, p99 (nearest rank), max; steps inside a recovery window are left out, steps
  with one side only count as missing.
- `release_lag_s`: a step's release time minus the publication time (`ingested_at`) of the record that made it
  releasable, found by merging the household's records and its market's prices in publication order through the
  same join (`ems.assembler.offline.released_by`). So a step that waited for a forecast run or a price curve
  counts from that record: the first steps of a window wait for its first forecast run (about 2 h of data time),
  and counting from their readings would charge that wait to the system. Sim runs keep each step's lag, with
  the releasing record's stream, in `lags.parquet`, so a slow tail can be traced to its input after the run.
- `sustained`: speed `S` is sustained when the p99 release lag is at most `900/S` s and the last step is released
  within `2·900/S` s after the last record that releases a step is published.
- `recovery` per kill: kill → healthy, healthy → first commit, first commit → the first release whose lag is back
  under the pre-kill p99.
- `records_per_s` and `invocations_per_s` over the wall span; `retries` (invocations with more than one line,
  K14); `forecast_age_h` at decision; `publisher_lateness_s` from the generator manifest; live:
  `live_release_delay_s` (release time minus the end of the quarter).
- `samples`: memory peaks and limits per container (page cache included) and anonymous memory, CPU peaks, the SDK
  process's CPU and RSS, Kafka lag peaks, OOM kills, the state bytes per service, the host and VM clock offset.

**Comparing a strategy that joins or leaves a run** (ADR 0021). An added strategy starts at its hour `t` on a fresh
battery at the run's initial SoC and pays the whole month's fixed charges, so it is compared per step from `t`
(the `steps` table) against NoBattery over the same steps, with its ΔSoC; its month bills are flagged `partial`. A
removed one is comparable only until `compare_until_step` (`4·⌊(stop − 2L)/4⌋`), because it no longer receives
the corrections for its last `2L` slots. The deposit reported is the one after the last closed month. Corrections
dropped as `after_end_late` touch only held rows, never `bill_pln`.

Configs ready (`experiments/configs/`): `sys-scale` (100 and 400 households × one and two strategies, speed 96,
1 day), `sys-speed` (100 households, speed doubling from 24 to 1536; it started at 96 until the first validation runs showed 96 borderline), `sys-recovery` (100 households, `kill -9` of
the SDK service at 30 % and of the server at 70 %, two repetitions), `sys-strategy-change` (SelfConsumption, add
NoBattery at hour 12, remove it at hour 36, 2 days across the end of June), `sys-live` (10 households, 2 h real
time, faults). Scaling up is a config change.

### Validation runs (spec 012)

Laptop validation of the runner and the measurement, not thesis evidence (the evaluation runs later at growing
scale; OPEN in `00-overview.md`: where). One host (Apple silicon laptop, Docker Desktop VM), Restate limited to
2 GiB, one hypercorn worker. Every run below ended `ok` with 0 exactly-once mismatches and no OOM kill. Latency
is p50 / p99 and release lag is p99, from `summary.json`.

- `sys-recovery-20261125T122428Z` (commit `ee4e40d`): 100 households, 1 day, speed 96, `kill -9` of the SDK
  service at 30 % and of the server at 70 %, two repetitions. The state equals the offline twin after both kills.

  | Run | SDK kill: healthy / first commit / caught up | Server kill: healthy / first commit / caught up | Decision latency | Release lag | Sustained |
  |---|---|---|---|---|---|
  | r0 | 0.22 / 3.75 / 0.08 s | 1.15 / 0.54 / 0.06 s | 252 ms / 2.76 s | 10.6 s | no |
  | r1 | 0.23 / 3.99 / 0.09 s | 1.53 / 0.64 / 0.00 s | 292 ms / 2.05 s | 3.8 s | yes |

  About 66 records/s and 85 invocations/s. Restate's anonymous memory peaked at 1.5 GB.
- `sys-strategy-change-20261125T125749Z` (commit `ee4e40d`): NoBattery joined all 100 households at step 48 and
  left at step 144. Each household has 2 changes and 0 refusals, June closed for SelfConsumption and the real
  battery, and the state equals the twin. Decision latency 229 ms / 3.05 s; release lag 10.66 s (max 21.8 s),
  not sustained at 96.
- `sys-scale-20261125T134051Z` (commit `cef8c21`), speed 96, 1 day:

  | Households × strategies | Sustained | Release lag | Decision latency | Invocations/s | Retries |
  |---|---|---|---|---|---|
  | 100 × 1 | yes | 5.88 s | 388 ms / 2.62 s | 74 | 0 |
  | 100 × 2 | yes | 3.83 s | 548 ms / 2.17 s | 85 | 0 |
  | 400 × 1 | no | 19.45 s | 963 ms / 18.9 s | 296 | 48 |
  | 400 × 2 | no | 11.97 s | 1.57 s / 16.3 s | 338 | 28 |

  At 400 households Restate's total memory peaked at 2.13 GB (page cache included) against its 2 GiB limit;
  anonymous memory stayed at 1.66–1.68 GB. State at 400 × 2 totals 2.1 MB for Household, 3.7 MB for Meter,
  4.0 MB for NoBattery and 3.7 MB for SelfConsumption. Tariff state is 13.8 kB. So 400 households is beyond what
  one laptop sustains at speed 96.
- `sys-speed-20261125T191457Z` (commit `3015765`), 100 households × 2 strategies, `host_slept_s` ≤ 0.07 s in
  every run:

  | Speed | Sustained | Release lag (p99 / max) | Drain | Decision latency | Invocations/s | Retries |
  |---|---|---|---|---|---|---|
  | 24 | yes | 2.81 / 10.71 s | 0.70 s | 256 ms / 2.07 s | 21 | 0 |
  | 48 | yes | 3.48 / 10.72 s | 0.63 s | 297 ms / 2.78 s | 43 | 0 |
  | 96 | yes | 3.27 / 11.13 s | 0.81 s | 314 ms / 2.45 s | 85 | 0 |
  | 192 | no | 7.48 / 42.77 s | 31.57 s | 361 ms / 4.66 s | 160 | 530 |

  The highest sustained speed is 96. At 192, the slow steps are early steps (27–41) released by consumption
  readings (`lags.parquet`). An earlier sweep (`sys-speed-20261125T144855Z`) is invalid from speed 96 on: the
  laptop's lid was closed and the stack stalled for 123 s. That run is the reason for the host-sleep check
  (T46).
- `sys-live-20261126T001003Z` (commit `b429bf0`): 10 households, 2 h in real time from 01:00 UTC, faults on,
  grace 120 s. The run published 689 records (1 price curve, 289 production and 399 consumption readings) with a
  p99 publisher lateness of 0.05 s.
  - All 80 steps were released by their end + `G` + 5 s: the delay after the quarter's end was 122.03 s at p50
    and 122.06 s at max.
  - Each household had 9 deadline firings, 0 early and 0 stale.
  - `Battery` and `Meter` equal the SelfConsumption shadow. Decision latency was 35 ms / 117 ms.
  - Restate peaked at 1.20 GB, 0 retries, `host_slept_s` 0.01.
  - In this window the faults left no reading missing or late (0 estimates, 0 late, one duplicate production
    reading), `price_replaced` was 0, and no forecast run fell inside it (`forecast_age_h` count 0).
  - Every step was therefore released by the deadline. The estimate path of a missing reading (ADR 0018) is
    covered by the integration tier, not by this run.

Observation for the evaluation: the maximum release lag is about 10.7 s at every speed from 24 to 96, so it does
not scale with the load. It points to a periodic stall somewhere in the stack, not to queueing. Find its source
(Restate, Kafka or the Docker Desktop VM) when the runs move to the cluster.

## What to build

- Built: the experiment runner, export, manifest and metrics (spec 012). Next: the energy experiments of stage 5
  and the evaluation at scale.
- Stage 3 delivered the first bit-exact comparison of actor state through Kafka (above); stage 2 compared the
  ingress log only.

## Prerequisites

- Actors (stage 3, built). The domain core and the cost metric exist (spec 009).

## Open questions

- OPEN: which strategies form the research contribution (brief §13). The LLM-based strategy (CLM-8B) is optional
  (gate in `00-overview.md`).
- Closed (spec 012, ADR 0022): the export reads state through SQL on the admin API's `state` table; no per-step
  data stay in object state, per-step series come from the offline fold once it equals the actors' state; the
  systems metrics come from the timing log and the runner's samples.
