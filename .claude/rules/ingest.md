---
paths:
  - "src/ems/ingest/**"
  - "src/ems/contracts/**"
  - "config/**"
  - "mappings/**"
  - "schemas/**"
---

# Data pipeline (`src/ems/contracts/`, `src/ems/ingest/`, `config/`)

Contract and design: `docs/context/08-contracts.md`, `13-subsystem-ingest.md`, ADRs 0010–0015.

- Storage is UTC; Europe/Warsaw only for calendar logic. Delivery days have 92/96/100 quarter-hours
  (2026-03-29, 2026-10-25). Intervals are half-open; adapters convert the source's labels.
- Canonical units: kWh, kW, W/m², °C, m/s; prices in native currency per MWh (the domain divides by 1000).
- `event_id` follows the identity rule in 08 (origin source, canonical `Z` datetimes, `run_id`); `ingested_at`
  never enters identity; `issued_at` comes from the source, never the poll time.
- Keys: location topics `[run_id:]location_id`, prices market keys (`PL:RCE`); never null keys. Partition = Java
  murmur2 (`ems.contracts.topics.expected_partition`). Record timestamp = event time.
- Topics are created only by `ems-gen topics ensure` from `config/topics.yaml`; backfill and accelerated runs
  use the `sim` profile (preflight refuses otherwise). Never change partition counts of existing topics casually.
- `src/ems/contracts/` imports no Kafka client, numpy or I/O beyond reading `topics.yaml`: actors import it.
- Synthetic models are pure functions of (seed, location, variable, `t`): use `ems.ingest.rng`, never a shared
  numpy Generator over a batch; loop over time, never over sites. EMS-managed sites emit no battery or grid
  measurements.
- After changing a contract model run `uv run ems-gen schemas export` and keep the change additive.
