---
paths:
  - "src/ems/assembler/**"
---

# Assembler (`src/ems/assembler/`)

The assembler turns a household's four input streams into ordered steps (ADR 0017, spec 011). The actors and
the offline side run the same functions, so purity and determinism are correctness requirements here, as in the
domain core.

- Imports: the standard library, `ems.contracts` and `ems.domain` only; no Restate, no I/O, no clock, no
  randomness (`tests/unit/assembler/test_purity.py`; the post-edit hook flags Restate imports).
- Every function that sees a wire event is total: what it cannot use becomes None and is counted. A Kafka-fed
  handler that raised would be retried and block its key.
- Judge lateness and duplicates inside one stream, in that stream's slots; never by arrival time, another
  stream or the Kafka record timestamp.
- Time is an integer step past the mapping: a record may be read from `ceil(available_at / 900 s)` on; a
  watermark covers the steps below `floor(watermark / 900 s)`.
- State is frozen dataclasses of tuples, never sets: actor state must encode to the same bytes in every process,
  or a retried handler fails its journal check.
- Rules and their reasons: `docs/context/01-architecture.md` (time and ordering), ADR 0018 (late and missing
  inputs).
