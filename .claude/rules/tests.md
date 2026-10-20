---
paths:
  - "tests/**"
---

# Tests (`tests/`)

- Tiers and markers: `unit` and `property` need no infrastructure; `integration` runs against real services
  from `make up` (Kafka now, Restate later), isolated per run by a topic prefix; `e2e` replays data and compares strategy state bit-exactly with the offline reference;
  `fault` kills the SDK service or the server with `kill -9` during replay.
- Directory = tier: `tests/unit/`, `tests/property/`, `tests/integration/`, `tests/e2e/`, `tests/fault/`.
  Every test carries exactly one tier marker, added from its directory by `tests/conftest.py` (strict markers:
  an unknown marker is an error).
- Write the failing test first (the `test-engineer` subagent does this for approved tasks). Every change adds
  at least one new test; invariants get `hypothesis` property tests with a fixed seed profile in CI.
- Integration and e2e tests use real services, never mocks of Restate. Pin any container image by digest.
- Tests are deterministic: no wall clock, no unseeded randomness, no dependence on test order or on data
  left behind by earlier runs (use a fresh household-id prefix per run).
- E2E comparisons are exact (`==` on the stored state), not approximate, unless the spec of an ML strategy
  defines a tolerance.
