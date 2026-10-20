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

### Data-generator benchmark (spec 003)

`make bench` → `experiments/results/benchmark-<UTC>/results.json` (window, config, versions, hardware, commit).
First run 2026-10-20 (`benchmark-20261020T202144Z`): one simulated day, 1,000 sites 14 s for 174,562 events,
10,000 sites 199 s for 1.74 M events; Kafka producer 4.9k events/s for 1,000 sites. Details in
`13-subsystem-ingest.md`.

## What to build

- Experiment harness spec (`make exp`, runner, Parquet results, reports).

## Prerequisites

- Actors and the domain core (later specs).

## Open questions

- OPEN: which strategies form the research contribution (brief §13).
