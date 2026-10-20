# Quickstart: validate 001 end to end

Prerequisites: macOS with Docker Desktop (≥ 2 GB free in the VM), uv 0.12.x, ports 9092 and 8088 free
(`lsof -nP -iTCP -sTCP:LISTEN | grep -E ':(8088|9092)\b'`).

1. `make install` then `make ci` → lint, `ty`, unit and property tests green (AC-001-08…26 offline parts).
2. `make up` → kafka healthy, kafka-init exited 0, kafka-ui healthy at http://127.0.0.1:8088 (AC-001-01, -02).
3. `make topics` again → "no change", exit 0 (AC-001-02). `make topics-describe` shows 3/1 partitions.
4. `make test-integration` → topic ensure idempotent, missing-topic write fails, data survives restart,
   live-vs-sim timestamp rules, backfill counts (AC-001-02…07, -27).
5. Manual end-to-end: `make gen-backfill DAYS=2` (applies the sim profile, starts 2026-10-20T00:00Z), then `make validate`
   → 192 production, 192 consumption, 16 forecast, 3 price events, 0 errors; `make tail TOPIC=ems.price.v1`
   shows slots in PLN/MWh.
6. Negative check: `EMS_TOPICS_PROFILE=live` with a backfill window older than 7 days → `ems-gen run` refuses
   at preflight naming `retention.ms` (AC-001-07).
7. `make down` (data kept) or `make reset` (wipes the volume, asks first).

Contracts: [topics](contracts/topics.md), [CLI](contracts/cli.md); data model: [data-model.md](data-model.md).
