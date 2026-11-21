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

## What to build

- Stage 4 (brief in `01-architecture.md`): the experiment runner (`make exp`), the export of Restate state to
  Parquet, the run manifest and reports. Scale runs are published paced (`accelerated` mode), and the highest
  speed the system sustains is a result (risk A1).
- Stage 3 delivered the first bit-exact comparison of actor state through Kafka (above); stage 2 compared the
  ingress log only.

## Prerequisites

- Actors (stage 3, built). The domain core and the cost metric exist (spec 009).

## Open questions

- OPEN: which strategies form the research contribution (brief §13). The LLM-based strategy (CLM-8B) is optional
  (gate in `00-overview.md`).
- OPEN (stage 4): how the export reads state (Restate SQL introspection on the admin API vs a shared read
  handler; VERIFY against the docs of the pinned Restate, 1.8.1-hotfix.2) and which per-step data stay in object
  state for export (state size per object: VERIFY, V7 in 01).
- OPEN (stage 4): where the systems metrics come from (Restate metrics or traces, runner timings); object
  state cannot supply latencies or throughput.
