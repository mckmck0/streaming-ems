# Implementation Plan: Scale and faults

**Branch**: `003-scale-and-faults` | **Date**: 2026-10-20 | **Spec**: [spec.md](spec.md)

## Summary
Chunk sites in `SyntheticModel` (configurable `scale.site_chunk`), add the pure battery model in
`src/ems/domain/battery.py` with a vectorised rule policy for site-controlled batteries
(`ingest/synthetic/bess.py`), fault injection as a deterministic transform of the emission stream
(`ingest/faults.py`), a `stats` tool over topics, optional cumulative registers, and `jobs/benchmark.py`.

## Technical Context
Python 3.13; no new dependencies. Performance: one day for 1k sites < 2 min. Memory: chunks of ≤ 2,000 sites.

## Principles Check (PASS)
| # | Notes |
|---|---|
| P2 | battery model in `src/ems/domain/` is pure (numpy only, no I/O, deterministic); unit + property tests |
| P3 | site-controlled batteries are not EMS facts; EMS-managed sites still emit no battery or grid |
| P6/P9 | faults reuse identical events (duplicates keep ids); no contract change |
| P8 | battery clamps power and SoC (domain rule) |
| P14/P16 | benchmark writes results with metadata; events/s reported |
| others | unchanged from 001/002 |

## Design
- `domain/battery.py`: `BatteryParams` (capacity, p_max, eta, soc_min, soc_max), `step(soc, p_cmd, dt_h)`
  vectorised: charge `soc += p·η·Δt`, discharge `soc += p/η·Δt`, clamped by p_max and SoC bounds; returns new
  SoC and actual power (AC side). Scalar and array use the same function.
- `synthetic/bess.py`: policy per interval (vectorised over sites, loop over time): surplus charge, cheap-night
  grid charge, evening/expensive discharge to cover net load; returns charge/discharge kWh and SoC.
- Generator: site arrays for site-controlled batteries; meter builders add `bess` measurements and grid flows
  including the battery; optional `meters.emit_registers` adds cumulative import registers.
- `faults.py`: `FaultConfig` (rates, late_max) and `apply_faults(emissions, seed)`: per event a keyed uniform
  decides missing/duplicate/late/stuck/quality; register reset per site-day; stuck replaces values by the previous
  interval's values for that site. Late emissions are re-ordered through a heap.
- `tools/stats.py`: per topic/key: count, partitions, first/last event time, gaps (expected 15-min sequence),
  duplicate ids, late (ingested_at − interval_end > threshold or interval_start going back).
- `jobs/benchmark.py`: generation-only (no Kafka) for 1k/10k sites and a Kafka produce run for 1k sites; JSON to
  `experiments/results/benchmark-<timestamp>/`.

## Test strategy
unit/property: battery invariants (hypothesis), BESS balance, chunk equality, catalog share, fault determinism,
duplicate/late semantics, stats on synthetic records; integration: test 4 (faults on → stats rates).

## Docs
13 (scale, BESS, faults, stats, benchmark), 12 (battery model implemented), 06 (late/duplicate semantics),
09, 11 (benchmark result location), 00.

## Complexity Tracking
None.
