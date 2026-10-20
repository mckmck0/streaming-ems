# Contract: CLI additions (002)

| Command | Behaviour |
|---|---|
| `ems-gen run --mode realtime` with `streams.<s>.mode: live` | live sources for live locations (`live.locations`), synthetic for the rest (hybrid) |
| `ems-gen record --name N [--duration PT2H] [--once]` | run live sources, store raw + events + manifest under `data/recordings/N/` (no Kafka needed) |
| `ems-gen replay --name N [--speed S]` | publish a recording (events keep ids; `source_system = replay:<orig>`) |
| `ems-gen record --name N --out tests/fixtures/<dir>` | one-off live fetch of test fixtures (redacted); the committed fixtures were recorded on 2026-10-20 |
| `jobs/calibrate.py prices|weather --out config/calibration/candidates/` | fit candidates from history |

Make: `make gen-live` (hybrid realtime), `make record NAME=…`, `make replay NAME=…`.
