# 06 · Data flows

## Current consensus

### External data → Kafka (spec 001; ADR 0011)

All external data (weather, meters, prices) enters through Kafka topics written by `ems-gen`; actors do not
poll external APIs. One path serves live, synthetic and replay data, so experiments replay exactly what a live
run saw.

### Emission schedule (simulated time)

| Stream | Emitted at | Record timestamp |
|---|---|---|
| production, consumption | interval end + `meters.availability_delay_s` (default 0) | interval end |
| weather forecast | issue time + 90 min; runs at 00, 03, … UTC; 192 quarter-hour steps | issue time |
| price | publication D−1 (13:00–16:15 local); days already published at the start are emitted at the start | publication time |

Within a run, events leave in emission-time order; per key, `interval_start` is monotonic. A two-day backfill
from 2026-10-20T00:00Z gives 192 production, 192 consumption, 16 forecast and 3 price events (D, D+1, D+2).

### Clock modes of the generator

- `backfill`: as fast as possible, simulated time only (experiments, tests). Topics must use the `sim` profile.
- `accelerated`: simulated seconds per wall second = `--speed` (default 96: one day per 15 min). `sim` profile.
- `realtime`: starts at the current quarter-hour, wall-clock pacing; `live` profile.
- Preflight refuses a run whose record timestamps the target topics would reject or expire.

### Live, hybrid, record and replay (spec 002)

- Live sources poll on the wall clock (allowed outside experiments): PSE every 15 min in the D−1 publication
  window, then hourly; Open-Meteo model metadata every 5 min, forecasts only for new runs.
- Hybrid: live locations get live forecasts and synthetic meters driven by them; other locations stay fully
  synthetic; one price source (live PSE when prices are live).
- Experiments use recordings: `ems-gen replay` re-emits recorded events with their ids (`replay:<source>`), so a
  live session can be replayed into the sim profile any number of times.

### EMS (later specs)

Sim mode advances on event timestamps with virtual timers in entity state; live mode uses durable delayed
messages (brief §5.5, principle 7). The ingestion spec joins the two measurement topics per `(location, t)` and
applies the availability rule in `08-contracts.md`.

## What to build

- Sim clock and replay driver for the actors (later spec).

## Prerequisites

- None.

## Open questions

- None for spec 001.
