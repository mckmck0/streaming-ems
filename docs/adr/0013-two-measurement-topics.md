# 0013. Production and consumption on separate topics

- Status: accepted
- Date: 2026-10-20
- Spec: specs/001-data-pipeline-core

## Context
Plan 1 splits metered data into `ems.production.v1` and `ems.consumption.v1`. One combined topic would give one
record (one invocation) per household and interval, as in the spike. The user kept the split.

## Decision
Two topics, each with one event per location and interval; generation (and battery discharge) on production;
loads, battery charge and grid flows on consumption.

## Consequences
The ingestion spec must join the two records per `(location, t)` (no cross-topic ordering) before ticking
strategies, with a sim-time rule for a missing side.

## Alternatives considered
One `ems.measurement.v1` topic carrying all measurements of an interval.
