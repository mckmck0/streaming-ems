# Quickstart: validate 002

1. `make ci` → mapping, adapter (respx-mocked), replay and router tests pass offline.
2. `make up && make test-integration` → replay of a fixture recording into Kafka validates with original ids.
3. Live smoke (network): `uv run ems-gen record --name smoke --once` → `data/recordings/smoke/` holds PSE D/D+1
   prices and one Open-Meteo run; `grep -r "$EMS_OPEN_METEO_API_KEY" data/recordings/smoke` finds nothing.
4. `make topics PROFILE=sim && make replay NAME=smoke && make validate`.
5. `make gen-live` → live prices and forecasts for PL-LOC-000001 next to synthetic sites (`make tail TOPIC=ems.price.v1`).
