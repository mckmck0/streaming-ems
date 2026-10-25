# Implementation Plan: Live data, record and replay

**Branch**: `002-live-data-replay` | **Date**: 2026-10-20 | **Spec**: [spec.md](spec.md)

## Summary

A small YAML mapping engine (`src/ems/ingest/mapping/`) turns raw payloads into canonical rows; async live
adapters (`src/ems/ingest/live/`: PSE, Open-Meteo, ENTSO-E) poll with httpx + tenacity and assemble contract
events; a source router (`src/ems/ingest/sources/`) mixes live, synthetic and replay per stream and location;
`ems-gen record` / `replay` persist and re-emit sessions; `jobs/calibrate.py` fits candidate calibration files.
Facts and decisions: [research.md](research.md).

## Technical Context

**Language/Version**: Python 3.13 · **Primary Dependencies**: spec 001 stack + httpx 0.28.1, tenacity 9.1.4,
lxml 6.1.3 (group `ingest`), respx 0.23.1 (dev) · **Storage**: Kafka; recordings under `data/recordings/` ·
**Testing**: unit (respx mocks, network blocked by a fixture), integration (replay into compose Kafka) ·
**Target Platform**: macOS host + compose image · **Project Type**: CLI/library · **Constraints**: no secrets in
URLs, logs, recordings; free Open-Meteo API ≤ 10 calls per run per site · **Scale**: one live site, any number
of synthetic sites

## Principles Check (PASS, re-checked after design)

| # | Status | Notes |
|---|---|---|
| P1 | ✅ | spec approved; tasks below |
| P2 | ✅ | no domain change; adapters are I/O at the edge, mapping engine pure |
| P3 | ✅ | `ems-gen` remains the single writer of external inputs |
| P4, P5, P8 | n/a | no actors |
| P6 | ✅ | deterministic ids from source times; replay keeps ids |
| P7 | ✅ | live polling only in realtime/record; replay uses recorded times |
| P9 | ✅ | no breaking change; only existing enum values used |
| P10 | ✅ | `jobs/calibrate.py` CLI over artefacts |
| P11 | ✅ | keys/tokens from `EMS_*` SecretStr settings; header-only; redaction tested |
| P12 | ✅ | new deps under the cutoff |
| P13 | ✅ | logs only |
| P14 | ✅ | recordings carry manifests; calibration candidates carry data windows |
| P15 | ✅ | 13, 06, 09, 05 updated; no new decision needing an ADR (ADR 0011 covers the flow) |
| P16 | ✅ | poll latency and counts logged |

## Design

- `mapping/engine.py`: `load_mapping(path)` (ruamel + jsonschema), `apply(mapping, payload) -> list[Row]`;
  path resolver (dotted / JSON pointer, `*` iteration), column zip, time parsing, unit conversion, transforms,
  null policy, quality map. `mappings/*.yaml`: `pse.rce.v2`, `pse.csdac.v2`, `openmeteo.forecast.v1`,
  `entsoe.a44.v1`; `mappings/_schema/mapping.schema.json`.
- `live/http.py`: shared `httpx.AsyncClient` factory, `fetch_json`/`fetch_text` with tenacity (retry 429/5xx/
  timeouts; `NonRetryable` for 4xx), redaction helper, raw capture hook for recording.
- `live/pse.py`: `PseSource(report)` → `price_event_for_day(day)`; poll schedule; completeness; revision tracking.
- `live/openmeteo.py`: site → model rule, meta.json poll, run-anchored forecast fetch, mapping → steps → event.
- `live/entsoe.py`: XML normalisation (local names), A01 selection, A03 forward-fill, dedup, slots → event.
- `sources/router.py`: per stream/location mode from `generator.yaml` (`streams.*.mode`, `live.locations`);
  `sources/live_runner.py`: asyncio tasks per live source feeding the sink; synthetic emissions for the
  remaining sites; hybrid weather override for live sites (latest live run → truth arrays for PV/HP).
- `sources/recording.py`: recorder (raw + events + manifest) and `replay_events(name)`.
- CLI: `record`, `replay`, `fixtures record`; Makefile `gen-live`, `record`, `replay`.
- `jobs/calibrate.py`: prices and weather fits → candidates.

## Test strategy

| Tier | Covers |
|---|---|
| unit | engine ops and schema (AC-002-01/-02); PSE mapping on DST fixtures, empty/partial, revision (-03/-04/-05); Open-Meteo mapping + unchanged-run suppression (-06/-07); A44 fixtures (-08); retry policy with respx (-09); recording redaction (-10); router/hybrid (-12); calibration on fixtures (-13); network blocked (-14); NOTICE files (-15) |
| integration | replay a fixture recording into Kafka with original ids and clean validation (-11) |

## Docs

13 (live sources, router, record/replay), 06 (hybrid, replay), 05 (deps), 09 (new risks), README/CLAUDE
commands.

## Project Structure

```text
src/ems/ingest/{mapping,live,sources}/  mappings/  jobs/calibrate.py  tests/fixtures/{pse,openmeteo,entsoe}/
```

## Complexity Tracking

None.
