# Contract: `ems-gen` CLI and Make targets (001)

Settings come from the environment (`src/ems/settings.py`, prefix `EMS_`): `EMS_KAFKA_BOOTSTRAP`
(default `localhost:9092`; compose sets `kafka:29092`), `EMS_TOPICS_PROFILE` (`live`), `EMS_TOPIC_PREFIX` (empty), `EMS_CONFIG_DIR` (`config`).
Behaviour config is YAML without interpolation.

| Command | Behaviour | Exit |
|---|---|---|
| `ems-gen topics ensure [--profile P] [--allow-partition-increase] [--apply-config]` | Create missing topics; report ok / created / increased / drift; fail on fewer partitions or RF > brokers | 0 ok, 1 refused, 2 broker error |
| `ems-gen topics describe` | Desired vs actual per topic (partitions, RF, configs) | 0 |
| `ems-gen topics delete --yes` | Delete defined topics; refuses unless bootstrap host is localhost/127.0.0.1/`kafka` | 0 / 1 |
| `ems-gen schemas export [--check]` | Write `schemas/*.json`; `--check` fails if stale | 0 / 1 |
| `ems-gen catalog --n N --out PATH` | Write the site catalog (Parquet) | 0 |
| `ems-gen run [--mode backfill\|accelerated\|realtime] [--start T] [--end T] [--speed S] [--run-id ID]` | Preflight, generate, publish; DLQ invalid events | 0 all delivered; 1 preflight refused; 3 delivery failures/fatal |
| `ems-gen tail --topic T [--key K] [--from-beginning] [--max N]` | Pretty-print key, partition, offset, headers, value | 0 |
| `ems-gen validate --topic T\|--all --from-beginning` | Report counts, schema/key/partition errors | 0 clean, 1 errors found |

| Make target | Runs |
|---|---|
| `make up` / `make down` | `docker compose -f infra/docker-compose.yml up -d --wait` / `down` |
| `make reset` | asks for confirmation (`CONFIRM=yes` skips it for scripts), then `down -v` |
| `make topics`, `make topics-describe` | ensure / describe |
| `make topics PROFILE=p` | ensure with `--profile p --apply-config` |
| `make gen`, `make gen-fast`, `make gen-backfill DAYS=n START=…` | compose profile `gen` realtime; accelerated ×96; backfill n days from START (default 2026-10-20T00:00Z, sim profile applied first) |
| `make tail TOPIC=…`, `make validate` | tail / validate all |
| `make test-unit`, `make test-integration` | `-m "unit or property"` / `-m integration` (needs `make up`); no coverage flags in 001 |
| `make ci` | lint + type-check + `test-unit` (no infrastructure); the exit-5 "no tests" workaround is removed |
