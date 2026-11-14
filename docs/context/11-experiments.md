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
  month (paid in the next). Bill metrics use windows of whole civil months.
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

## What to build

- Stage 4 (brief in `01-architecture.md`): the experiment runner (`make exp`), the export of Restate state to
  Parquet, the run manifest and reports. Scale runs are published paced (`accelerated` mode), and the highest
  speed the system sustains is a result (risk A1).
- Stage 3 delivers the first bit-exact comparison through Kafka (10 households × 1 day).

## Prerequisites

- Actors (stage 3). The domain core and the cost metric exist (spec 009).

## Open questions

- OPEN: which strategies form the research contribution (brief §13). The LLM-based strategy (CLM-8B) is optional
  (gate in `00-overview.md`).
- OPEN (stage 4): how the export reads state (Restate SQL introspection on the admin API vs a shared read
  handler; VERIFY against the docs of the pinned Restate, 1.8.1-hotfix.2) and which per-step data stay in object
  state for export (state size per object: VERIFY, V7 in 01).
- OPEN (stage 4): where the systems metrics come from (Restate metrics or traces, runner timings); object
  state cannot supply latencies or throughput.
