# 0011. External data enters through Kafka, in every mode

- Status: accepted (supersedes the brief §5.1 Tariff/Weather "own periodic timer" fetching)
- Date: 2026-10-20
- Spec: specs/001-data-pipeline-core

## Context
The brief had the Tariff and Weather actors refresh themselves on durable timers (calling APIs inside `ctx.run`).
Plan 1 pushes weather, meters and prices to Kafka from `ems-gen`. The user chose Kafka push in all modes.

## Decision
`ems-gen` polls external APIs (spec 002) and publishes canonical events; actors consume topics and never call
external APIs for these inputs. Synthetic, live and replay data share one path.

## Consequences
Experiments replay exactly what a live run saw, on simulated time (principle 7). Durable timers remain for
scheduled commands ("charge at 02:00") and forecaster refreshes. The ingestion spec owns subscriptions.

## Alternatives considered
Actor timers in live mode with Kafka only for sim/replay: two code paths to test and keep equivalent.
