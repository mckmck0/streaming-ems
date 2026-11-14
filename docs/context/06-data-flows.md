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
| weather forecast | issue time + the model's delay (`ems.contracts.availability`: 120 min synthetic and ICON-D2, 195 min DMI); runs at 00, 03, … UTC; 192 quarter-hour steps | issue time |
| price | publication D−1 (13:00–16:15 local); days already published at the start are emitted at the start | publication time |

Within a run, events leave in emission-time order; per key, `interval_start` is monotonic. One exception: a PSE
day republished after the next day's publication (live revisions, flagged history days) arrives after it. A two-day backfill
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
  live session can be replayed into the sim profile any number of times. Replay runs the topic preflight first and
  refuses topics that would reject or expire the recording's record times (spec 008).
- Historical backfill (spec 008): `ems-gen history fetch` stores a real-data window once (archived forecasts, PSE,
  satellite and ERA5), `ems-gen history build` turns it offline into events in availability order, and replay
  publishes it, optionally with a synthetic background fleet (`--background`). Experiments replay it into the sim
  profile; they never refetch.

### Faults (spec 003)

With faults on, a duplicate is the identical record published twice, a late record keeps its event time and id
but arrives up to `late_max` after newer intervals of its key, and a missing record never arrives. Downstream
consumers must deduplicate by `event_id` and tolerate out-of-order intervals per key.

### EMS: time and ordering (ADR 0017; designed, built from stage 3)

Kafka gives an order inside one topic partition and none across topics, and in a backfill each topic is consumed
at its own speed. So each household orders its own inputs by event time; the full model is in
`01-architecture.md`.

- **Watermarks.** Every input stream of a household (production, consumption, prices through `Tariff`, forecasts
  through `Weather`) has a watermark: everything a step ending at or before it may read from that stream has
  arrived or is declared missing. A quarter-hour step is released, in order from the run's first step (a run
  parameter on a UTC hour), only when the watermarks of all needed streams are at or past its end. There is no
  global clock, barrier or replay driver.
- **Where watermarks come from in sim:** the stream's content and the emission schedule above, never the wall
  clock. A meter slot closes when its record is there or its stream is `L` slots past it; a forecast run moves
  the watermark to its availability plus the run cadence; the curve of a new delivery day moves it to 13:00
  local on that day, when the next publication window opens. A step that starts at a sparse stream's watermark
  waits for that stream's next record; a missing forecast run makes the household wait for the run after it.
- **Join.** Step `q` needs the production and consumption records of `q` (consumption only for a site without
  PV), the price of `q`, and the forecast run visible at its start (`08-contracts.md`, availability rule).
- **Late and missing readings.** A step waits `L` slots (default 8) of the reading's own stream, then runs on
  an estimate flagged `estimated`. A reading up to `2L` late corrects the settlement of its hour and never a
  decision, the executed battery power or the SoC; later ones are dropped and counted. With the default `L`
  equal to the generator's `late_max`, every late reading of a `--faults` run is waited for; corrections and
  drops appear only at a smaller `L`.
- **Duplicates** are dropped by slot (stream, `t`) and `event_id` in actor state.
- **The real loop.** `Ems` decides step `t` only after `Battery` acknowledged `t−1`, so the decision uses the
  real SoC and never a stale view.
- **Live mode** runs the same rules; one durable wall-clock timer per household raises the watermarks to "now
  minus a grace period" (P7). Scheduled actions are data in state, fired at their step in both modes.
- **Pacing.** Results never depend on publication speed, but buffers grow with the skew between a household's
  inputs. Large backfills are published in `accelerated` mode (risk A1).
- **End of a run.** No marker: steps the rules cannot release stay unreleased, in the actors and on the offline
  side alike, so a run can stop mid-hour.

## What to build

- The assembler and the actors (stage 3), the live timer and pacing for history replays (stage 4); briefs in
  `01-architecture.md`.

## Prerequisites

- None.

## Open questions

- None for spec 001.
