# Research: 001 data pipeline core

All facts were checked 2026-10-20 against primary sources (read-only research run; full claim tables with URLs
are summarised here and move into `docs/context/05`, `08`, `12`, `13` during implementation). Pins follow the
two-week rule; PyPI also the uv cutoff `exclude-newer = 2026-10-02`.

## R1 Broker: Apache Kafka 4.3.1 (KRaft, JVM image)

- **Decision**: `apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`
  (released 2026-06-25; tag not re-pushed since 2026-06-23). Single combined broker+controller.
- **Rationale**: newest announced stable; 4.3.2 (2026-10-20) and 4.4.0 (in release) too new. librdkafka CI tests
  against 4.3; KIP-896 removed only protocol versions older than librdkafka 1.8.2.
- **Alternatives**: `apache/kafka-native` (experimental, no CLI tools for healthchecks, zstd broken until 4.4,
  KAFKA-19839); 4.2.2 (fully understood by kafka-ui; kept as fallback); Redpanda (brief OPEN; no reason to prefer).
- **Image facts that shape the compose file**: every `KAFKA_*` env var becomes a broker property (so no
  `env_file` on the kafka service); with env config the image's single-node defaults are dropped (all internal
  topic RFs incl. `share.coordinator.state.topic.*` must be 1); `log.dirs` must be set to `/var/lib/kafka/data`
  or nothing persists; storage is auto-formatted from `CLUSTER_ID` (no prefix); static
  `KAFKA_CONTROLLER_QUORUM_VOTERS` only; CLI tools live in `/opt/kafka/bin` (not on PATH); runs as uid 1000;
  default heap 1 GB.

## R2 Record timestamps and retention

- **Decision**: record timestamp = event time (CreateTime); topic **profiles** carry the time rules:
  `live` (after.max 1 h, before.max = retention, retention 7/30/14 d) and `sim` (retention −1, after.max
  Long.MAX). Generator preflight refuses incompatible combinations. DLQ uses LogAppendTime.
- **Rationale**: Kafka 4.0 changed `message.timestamp.after.max.ms` default to 1 h (KIP-1030): accelerated
  runs fail with `INVALID_TIMESTAMP` (whole batch, non-retriable). Retention deletes segments whose largest
  timestamp is older than `retention.ms`, checked every 5 min: backfills older than 7 days vanish silently.
  `message.timestamp.difference.max.ms` was removed in 4.0. Restate passes the record timestamp to handlers as
  `kafka.timestamp`, so event time is useful downstream.
- **Alternatives**: LogAppendTime everywhere (loses event time in the record, accelerated mode still fine);
  clamping timestamps to now+1h (hides the bug).

## R3 Kafka client: confluent-kafka 2.15.1

- **Decision**: synchronous `Producer`, `produce()` then `poll(0)` on the event-loop thread; config
  `acks=all, enable.idempotence=true, compression.type=zstd, linger.ms=20, partitioner=murmur2_random,
  allow.auto.create.topics=false, delivery.timeout.ms=120000`. AdminClient for ensure.
- **Rationale**: cp313 wheels for macOS arm64 and manylinux aarch64/x86_64 (glibc ≥ 2.28; no musl), bundled
  librdkafka 2.15.1 with zstd; ships `py.typed`. `murmur2_random` equals Java's partitioner (default
  `consistent_random` does not). `confluent_kafka.murmur2(key, n)` (≥ 2.13) lets tests assert partitions.
  `AIOProducer` (2.15.1) rejects headers and per-message timestamps.
- **Gotchas used in the design**: delivery errors only arrive in callbacks (missing topic after
  `topic.metadata.propagation.max.ms`, default 30 s); `list_topics(topic=x)` can auto-create, so ensure lists
  all topics; `incremental_alter_configs` with SET only; no RF change API; header values must be str/bytes;
  `timestamp` is int ms; `flush()==0` is not success; on `fatal()` exit non-zero.
- **Alternatives**: aiokafka (no Java-compatible partitioner config, slower); AIOProducer (above).

## R4 Kafka UI: kafbat/kafka-ui v1.5.0

- **Decision**: `ghcr.io/kafbat/kafka-ui:v1.5.0@sha256:7cda86a33344160309fdb65146332e4da65db81a945614f2fe32e210803f6fd1`,
  read-only, `DYNAMIC_CONFIG_ENABLED=false`, `GITHUB_RELEASE_INFO_ENABLED=false`, healthcheck
  `/actuator/health`, host port 127.0.0.1:8088 (Restate ingress takes 8080 later).
- **Known issue**: shows broker version "Unknown" on Kafka 4.3 (reads a removed config); smoke test in M0;
  fallback broker 4.2.2.

## R5 Generator image

- **Decision**: `python:3.13.15-slim-trixie@sha256:7c61056e61ac89e852de05f3dc6fa51a6dd2181797bceed46aa725dd7cb2cd3b`
  (pushed 2026-09-25) plus `ghcr.io/astral-sh/uv:0.12.21@sha256:a7aed3216253ee804de3e2d8afa5073baa1a177335345d43845cd4165e43b711`
  (2026-09-29); `uv sync --frozen --no-dev --group ingest`; non-root user.
- **Rationale**: glibc base required by confluent-kafka wheels; uv within `uv_build<0.13`.

## R6 Python libraries (resolved under the cutoff, cp313 wheels on all three platforms)

| Library | Version | Use / note |
|---|---|---|
| pydantic, pydantic-settings | 2.13.5 / lock | Models; JSON Schema 2020-12; writes `Z` for UTC; set `allow_inf_nan=False` |
| ruamel.yaml | 0.19.1 | topics.yaml, generator.yaml, sites.yaml (no extras) |
| numpy, scipy, pandas (3.x), pyarrow | lock | arrays; AR(1) via `scipy.signal.lfilter`; catalog Parquet |
| pvlib | 0.16.1 | `spa.solar_position_numpy` broadcasts over sites; `clearsky.ineichen`, `irradiance.erbs`, `get_total_irradiance`, `temperature.faiman`, `pvsystem.pvwatts_dc(effective_irradiance=…)` (keyword changed in 0.16), `inverter.pvwatts`; Linke turbidity/altitude from bundled h5 (h5py dependency) |
| demandlib | 0.2.2 | BDEW 2025 `H25`/`G25` on a tz-aware Europe/Warsaw 15-min index; never `ElecSlp` (alters warning filters) or P25/S25 (net grid draw) |
| holidays | 0.105 | `holidays.Poland(years=…)`; 24 Dec is a holiday from 2025 |
| faker | 40.40.0 (exact pin) | street/building only; voivodeship table is ours |
| typer | 0.27.2 | CLI; vendors Click (never `import click`) |
| structlog, tenacity, jsonschema | 26.1.0 / 9.1.4 / 4.26.0 | logging, retries (later), schema validation |
| tzdata | lock | direct dependency (pandas 3 no longer pulls it) |
| hypothesis, pytest-asyncio | lock / 1.4.0 | dev group |

## R7 Determinism and vectorisation

- **Decision**: counter-based hash RNG (splitmix64 over uint64 arrays) keyed by (global seed, location hash,
  variable id, step); a regional latent weather field from the global seed plus local noise; recursions loop over
  time only.
- **Rationale**: numpy `Generator` draws over an `[n_loc, n_steps]` block depend on batch size and order, which
  breaks AC-001-26; per-location Generators require a Python loop over sites. Hashing is order-free and
  vectorised.
- **Alternatives**: one `SeedSequence` per location (simple, but loops over sites; acceptable at 1 site, not at
  10k).

## R8 Domain facts used by the synthetic models

- PV yield Poland: measured 869–1076 kWh/kWp (mean ~990), PVGIS central PL ~930 → test bound 850–1150.
- RCE (PSE): PLN/MWh excl. VAT, quarter-hourly distinct values since 2025-10-01, negative share 3.8–3.9 % over the
  last 12 months (0 % Nov–Dec, 13.5 % April), tail to ~−1,500, spikes ~3,000; published D−1 mostly 13:00–15:00
  local (77 % 13–14 h). RCE is the RES-act net-billing reference price, not the SDAC price.
- DST 2026: 29 Mar (92 quarter-hours) and 25 Oct (100) per Dz.U. 2022 poz. 539.
- PPE: 18-digit GS1 GSRN = 590 + 4-digit operator (Tauron 3224 confirmed) + 10 digits + GS1 mod-10 check digit.
- Tariffs: C11 ≤ 40 kW **and** fuse ≤ 63 A, else C21; G12 zones differ per DSO (Energa: night 13–15 and 22–6).
- Household consumption: GUS (secondary) mean 2,375, median ~2,000 kWh/yr; heat pump adds 5–6 MWh. **VERIFY**
  against GUS BDL before the realism test is tightened; until then the test site's base load is configured
  explicitly (2,500 kWh/yr) and AC-001-23 checks ±5 % of the configured value, independent of the default.
- Weather: Open-Meteo (spec 002) labels radiation as the preceding-interval mean at the interval end; temperature,
  wind and cloud are instants; ICON-D2 runs every 3 h with a 48 h horizon and ~1.5 h latency → synthetic forecast
  cadence 3 h, `availability_delay` PT1H30M, 192 steps.

## R9 Contract decisions

- `t` = `floor(unix_seconds(interval_start) / 900)` (R7 of the actor rules).
- `run_id` optional; when set it prefixes the key (`run:location`) and enters the event-id hash (R4 run scope).
- event_id name: `stream|location_or_market|interval_start|issued_at|origin_source|run_id|major`, datetimes in
  RFC 3339 UTC with milliseconds and `Z`; origin source = `source_system` without a `replay:` prefix.
- Restate 1.7.13 does not forward record headers and cannot set an idempotency key from a record → everything is
  in the JSON; `event_id` stays top-level for a later JSON-pointer mapper.
- Producers validate strictly; consumers ignore unknown fields; exported schemas open; compatibility FULL. (A closed
  schema checked only BACKWARD would let a v1.1 additive field break v1.0 readers: architect review 2026-10-20.)
- Golden example events per stream ship in `schemas/examples/` and are validated in unit tests (P9).
- `expected_partition` uses a pure-Python murmur2 in `ems.contracts` (no Kafka client import), tested against
  `confluent_kafka.murmur2`.
- Brief §4 is outdated on net-billing: RCE is the default for installations connected from 2024-07-01 (RCEm for
  earlier cohorts unless they opt in) and is quarter-hourly since 2025-10-01 (energy-domain review). Record in 12;
  settlement is a later spec.
