# Streaming EMS

MSc thesis project: an energy management system for residential prosumers (PV + home battery + grid,
Polish/EU market, 15-minute intervals) built on durable stateful actors (Restate Virtual Objects, Python
SDK). The same live stream feeds many battery strategies, each on its own virtual battery, so they can be
compared counterfactually while one of them drives the real battery.

## Contents

- [About](#about)
- [Setup](#setup)
  - [Prerequisites](#prerequisites)
  - [Install](#install)
  - [Environment variables](#environment-variables)
  - [Start Kafka](#start-kafka)
  - [Generate data](#generate-data)
  - [Check the setup](#check-the-setup)
  - [Start the engine (TBD)](#start-the-engine-tbd)
  - [Stop and reset](#stop-and-reset)
  - [Troubleshooting](#troubleshooting)
- [Data](#data)
  - [What the data is](#what-the-data-is)
  - [Topics at a glance](#topics-at-a-glance)
  - [Record format](#record-format)
  - [Envelope](#envelope)
    - [Fields of every event](#fields-of-every-event)
    - [Identity](#identity)
  - [Time: event time, availability time and processing time](#time-event-time-availability-time-and-processing-time)
    - [The timestamps](#the-timestamps)
    - [Why event time alone is not enough](#why-event-time-alone-is-not-enough)
    - [What ingested_at is, and is not](#what-ingested_at-is-and-is-not)
    - [Using the times](#using-the-times)
  - [Streams](#streams)
    - [Measurements (production and consumption)](#measurements-production-and-consumption)
    - [Weather forecasts](#weather-forecasts)
    - [Prices](#prices)
    - [Dead-letter topic](#dead-letter-topic)
  - [Live sources: raw API responses](#live-sources-raw-api-responses)
    - [PSE RCE and SDAC prices](#pse-rce-and-sdac-prices)
    - [Open-Meteo forecast](#open-meteo-forecast)
    - [ENTSO-E A44 (day-ahead prices)](#entso-e-a44-day-ahead-prices)
  - [When events are emitted](#when-events-are-emitted)
  - [Rules for consumers](#rules-for-consumers)

## About

- **Status:** the data pipeline is built (specs 001 to 004): the generators, the Kafka stack and the wire
  contract. The EMS engine itself (Restate actors, strategies, settlement) is **not built yet**; see
  [Start the engine (TBD)](#start-the-engine-tbd) and the backlog in
  [docs/context/00-overview.md](docs/context/00-overview.md).
- **Canonical docs:** [docs/context/README.md](docs/context/README.md) (index and status of every piece).
  Prerequisites and pinned versions: [docs/context/05-deploy-and-infra.md](docs/context/05-deploy-and-infra.md).
- **Working with Claude Code:** [CLAUDE.md](CLAUDE.md) and the harness in `.claude/` (skills, rules,
  subagents, hooks). Changes go spec-first through the `ems-plan-change` skill.

## Setup

### Prerequisites

| Tool | Why | Notes |
|---|---|---|
| [uv](https://docs.astral.sh/uv/) | Python environment and dependencies | uv provides Python 3.13 or newer itself (`requires-python >= 3.13`). The generator image pins uv 0.12.21. |
| Docker with Compose v2 | Kafka, Kafka UI and the containerised generator | Docker Desktop or Docker Engine. The Kafka container is limited to 1 GB of memory. |
| GNU Make | The only entry point for commands | The macOS default (3.81) works. |
| Network access | Only for live sources | Synthetic data needs none. Live runs call PSE and Open-Meteo (and ENTSO-E with a token). |

Host ports used, all bound to `127.0.0.1`: 9092 (Kafka), 8088 (Kafka UI), and 19092 to 19094 for the optional
3-broker profile.

### Install

```bash
make install        # uv sync for all dependency groups, plus the pre-commit hooks
```

### Environment variables

Settings are read from `EMS_*` environment variables ([src/ems/settings.py](src/ems/settings.py)); behaviour lives
in YAML under [config/](config/). Nothing is required for a local synthetic run, since every default points at
the local stack. A template with all variables is in [env.example](env.example).

| Variable | Default | Needed for |
|---|---|---|
| `EMS_KAFKA_BOOTSTRAP` | `localhost:9092` | A different broker (the compose container sets `kafka:29092` itself). |
| `EMS_TOPICS_PROFILE` | `live` | Topic profile: `live`, `sim`, `scale` or `cluster`. The `make gen-*` targets choose `sim` themselves. |
| `EMS_TOPIC_PREFIX` | empty | Prefix for every topic name, to isolate test runs. |
| `EMS_OPEN_METEO_API_KEY` | unset | Paid Open-Meteo API instead of the free non-commercial tier. |
| `EMS_ENTSOE_TOKEN` | unset | ENTSO-E prices (also set `live.entsoe.enabled: true` in the config). |

**Where they are read.** `cp env.example .env` creates a gitignored file that only the `ems-gen` **container**
reads (`make gen`). Commands run on your machine through `uv run` and `make` do not load `.env`, so export the
variables in your shell instead, for example `export EMS_OPEN_METEO_API_KEY=...`. Never commit secrets.

**Using ENTSO-E.** It needs both the token and the flag: export `EMS_ENTSOE_TOKEN` and set
`live.entsoe.enabled: true` in the config you run (`config/generator.live.yaml` for `make gen-live`). If the flag
is on but the token is not exported, ENTSO-E is skipped without any message. Check that
`data/recordings/<name>/raw/entsoe/` exists after `make record`, or that records with key `ENTSOE:PL` appear on
`ems.price.v1`. The adapter's status and what the live API has to confirm are in
[ENTSO-E A44](#entso-e-a44-day-ahead-prices).

### Start Kafka

```bash
make up             # Kafka (KRaft), one-shot topic creation, Kafka UI; waits until healthy
```

- Kafka listens on `localhost:9092`. The read-only Kafka UI is at <http://127.0.0.1:8088>.
- The one-shot `kafka-init` job creates the topics from [config/topics.yaml](config/topics.yaml) with the profile
  in `EMS_TOPICS_PROFILE` (default `live`). `make topics PROFILE=sim` applies another profile later.

### Generate data

The generator is the `ems-gen` command ([src/ems/ingest/](src/ems/ingest/)). With the default config it produces
one site, the Poznań test household `PL-LOC-000001`.

```bash
make gen-backfill DAYS=2     # 2 simulated days from START=2026-10-20T00:00:00Z, written as fast as possible
make validate                # check every record against the contract
make stats                   # counts per topic and key, gaps, duplicates, late events
make tail TOPIC=ems.price.v1 # look at the first records of a topic
```

Other ways to run it:

| Command | What it does |
|---|---|
| `make gen` | Synthetic data in real time, in the `ems-gen` container. It keeps running; stop it with `docker compose -f infra/docker-compose.yml stop ems-gen`. |
| `make gen-fast` | Synthetic data at 96 times real time, starting at `START`; stop with Ctrl+C. |
| `make gen-live` | Hybrid real time: live PSE prices and Open-Meteo weather for the configured live site, synthetic for the rest. Needs network. |
| `make record NAME=x` / `make replay NAME=x` | Record the live sources once, then publish that recording (event ids are kept). |
| `make gen-backfill START=2025-10-25T00:00:00Z DAYS=365` | A year of history for one site takes seconds to generate (about 10 s measured without Kafka). It is synthetic climate, not the real weather of that year. |

More sites, fault injection and run isolation are not exposed through `make`. Use the CLI directly:

```bash
EMS_TOPICS_PROFILE=sim uv run ems-gen run --mode backfill --start 2026-10-20T00:00:00Z --days 2 \
    --locations 50 --faults --run-id test1
```

- `--locations N` generates `PL-LOC-000001` to `N`; the others are generated households (or other site types via
  `scale.site_mix`) with random assets.
- `--faults` injects missing, duplicate, late and stuck readings (rates in `faults:` of the config).
- `--run-id X` prefixes the Kafka keys and enters the event ids. Without it, a second run of the same window
  republishes identical event ids and the topic holds duplicates.

Every generator parameter, the config files and what they connect to are described in
[docs/context/13-subsystem-ingest.md](docs/context/13-subsystem-ingest.md); `uv run ems-gen --help` lists all
commands. What the generated events contain is described in [Data](#data) below.

### Check the setup

```bash
make ci                # lint, type-check and the unit and property tests; needs no infrastructure
make test-integration  # tests against the real stack; run `make up` first
```

### Start the engine (TBD)

**Not available yet.** The EMS engine (Restate server, the Python service with the household, battery and
strategy actors, the Kafka subscriptions that feed it from the topics above) has not been built. It is next in the
backlog in [docs/context/00-overview.md](docs/context/00-overview.md): first the Kafka ingestion into Restate,
then the dev environment, the domain core and the actors. This section will hold the start commands (the
planned `make` targets for it are listed, commented out, at the bottom of the [Makefile](Makefile)).

Until then, the generated topics are the engine's input. Everything an engine needs to consume them is in
[Data](#data): the topics, keys, event format and the rules for consumers.

### Stop and reset

```bash
make down              # stop the Kafka stack, keep the data volume (stop a `make gen` container separately, see above)
make reset             # also delete the volume and all topic data (asks for confirmation)
make cluster-down      # stop the optional 3-broker profile
```

### Troubleshooting

- **"preflight refused" (exit code 1) when running `ems-gen run`.** The topics would reject or expire the
  record timestamps, for example when backfilling old data on the `live` profile. Use `make gen-backfill` or set
  `EMS_TOPICS_PROFILE=sim` and run `make topics PROFILE=sim`.
- **`make test-integration` says Kafka is not reachable.** Run `make up` first.
- **`make validate` or `make stats` shows duplicates after a re-run.** The same window was published twice without
  a `--run-id`; see above.
- **The generator exits with code 3.** Some records were not delivered; the run manifest in
  `data/runs/<run>/manifest.json` has the counts per error.

## Data

### What the data is

The data generator (`ems-gen`, [src/ems/ingest/](src/ems/ingest/)) publishes JSON events for Polish sites to
Kafka: weather forecasts, production and consumption meter readings, and day-ahead prices. The data is either
synthetic or live (PSE prices, Open-Meteo weather, ENTSO-E behind a flag), and the format is identical for both.
Everything the EMS knows about the outside world arrives through these topics.

- Format: wire contract **v1.0.0**. Source of truth: the pydantic models in
  [src/ems/contracts/envelope.py](src/ems/contracts/envelope.py); exported JSON Schemas in
  [schemas/](schemas/) and golden examples in [schemas/examples/](schemas/examples/).
- Design and rules: [docs/context/08-contracts.md](docs/context/08-contracts.md); generator internals and
  every config parameter: [docs/context/13-subsystem-ingest.md](docs/context/13-subsystem-ingest.md).
- Topic definitions: [config/topics.yaml](config/topics.yaml). Create them with `make topics`.

### Topics at a glance

| Topic | Stream | Kafka key | One event is | Partitions (live, sim · scale) | Retention (live profile) |
|---|---|---|---|---|---|
| `ems.production.v1` | `production` | `[run_id:]location_id` | what one site generated in one 15-minute interval | 3 · 48 | 7 days |
| `ems.consumption.v1` | `consumption` | `[run_id:]location_id` | what one site consumed in one 15-minute interval | 3 · 48 | 7 days |
| `ems.weather.forecast.v1` | `weather_forecast` | `[run_id:]location_id` | one weather model run for one site: 192 steps of 15 minutes (48 h) | 3 · 48 | 7 days |
| `ems.price.v1` | `price` | `[run_id:]market key` (`PL:RCE`, `PL:SDAC`, `ENTSOE:PL`) | all prices of one market for one local delivery day | 1 · 3 | 30 days |
| `ems.gen.dlq.v1` | none | the original key | a record that could not be published to its own topic | 1 · 3 | 14 days |
| `ems.weather.actual.v1` | `weather_actual` | `[run_id:]location_id` | reserved for observed weather; **disabled**, nothing produces it | 3 · 48 | 7 days |

Production and consumption are two separate topics, so a consumer that wants a site's balance for one interval
has to join them on `(location_id, t)`. Kafka gives no ordering across topics.

### Record format

| Part | Content |
|---|---|
| **Value** | One UTF-8 JSON object (the event, described below). No schema-registry prefix. Always below 900 kB including key and headers. |
| **Key** | The location id (`PL-LOC-000001`) or the market key (`PL:RCE`), never empty. When the event has a `run_id`, the key is `run_id:subject`, so every run gets fresh per-key state downstream. |
| **Partition** | Java-compatible murmur2 hash of the key, so all events of one site (and run) stay in one partition, in the order they were produced. |
| **Timestamp** | The event time, not the produce time: `interval_end` for measurements, `issued_at` for forecasts and prices. |
| **Headers** | For tooling only (the JSON value is self-contained): `schema`, `schema_version`, `source_system`, `content-type` (`application/json`) and `synthetic` (`true` or `false`). The dead-letter topic adds `error`. |
| **Producer** | `acks=all`, idempotent, zstd compression. |

### Envelope

#### Fields of every event

These top-level fields are identical on all three event types. The type-specific content is in `payload`.

| Field | Type | Meaning |
|---|---|---|
| `schema` | `ems.measurement` \| `ems.weather_forecast` \| `ems.price` | Which payload this event carries and which parser to use. |
| `schema_version` | string, `1.x.y` | Contract version. Changes within major version 1 only add fields. |
| `event_id` | UUID | Deterministic identity (UUIDv5) of the event, see [Identity](#identity). The same event always has the same id, so consumers can deduplicate on it. |
| `stream` | `production` \| `consumption` \| `weather_forecast` \| `weather_actual` \| `price` | Which topic the event belongs to. |
| `source_system` | string | Where the data came from: `synthetic`, `open-meteo`, `pse-rce`, `pse-csdac` or `entsoe`. A replayed recording is prefixed `replay:` (for example `replay:open-meteo`). |
| `source_record_ref` | string or null | Pointer to the source record, never a secret. For example `open-meteo:icon_d2:<issued_at>`, `rce-pln:<day>:<publication time>`, `entsoe:A44:<day>`. Null for synthetic data. |
| `location_id` | string or null | Site id `PL-LOC-` plus six digits. Required on all location streams, **null on prices**. |
| `metering_point_id` | string or null | The site's metering point (PPE) as an 18-digit GS1 GSRN with a valid check digit. Set on measurements, null on forecasts and prices. |
| `algorithm_id` | null | Reserved, always null in v1. |
| `run_id` | string or null | Scope of one generator run (`A-Z a-z 0-9 . _ -`, up to 64 characters). Null for live data. When set, it prefixes the Kafka key and is part of the `event_id`. |
| `interval_start`, `interval_end` | UTC datetime | The half-open interval `[start, end)` the event describes. Measurements: the 15-minute interval. Forecasts: from the first step's start to the last step's end. Prices: the delivery day, from local midnight to local midnight, expressed in UTC. |
| `resolution` | `PT1M` \| `PT15M` \| `PT1H` \| `P1D` | Length of one interval, step or slot. `PT15M` for measurements and forecasts; prices are `PT15M` (`PT1H` is allowed for history before 2025-10-01). |
| `t` | integer | UTC quarter-hour index of `interval_start`: `floor(unix_seconds(interval_start) / 900)`. A convenient join key across streams. |
| `event_time_kind` | `interval` \| `forecast` \| `instant` | `interval` for measurements and prices, `forecast` for forecasts. `instant` is reserved. |
| `issued_at` | UTC datetime or null | When the source issued the data. Null on measurements. On forecasts, the model run's initialisation time; on prices, the publication time. |
| `ingested_at` | UTC datetime | Processing time: the wall-clock time the generator handed the record to Kafka. Not part of the identity, and never used for decisions. See [Time](#time-event-time-availability-time-and-processing-time). |
| `synthetic` | boolean | True for generated data, false for live data. |
| `seed` | integer or null | The seed the synthetic values were derived from (a per-site seed on location streams, the global seed on prices). Null for live data. |
| `payload` | object | The stream-specific content. |

All datetimes are UTC, written as RFC 3339 with a `Z`. Numbers are always finite. Storage is UTC throughout; Polish
local time only matters for delivery days, which have 92 quarter-hours on 2026-03-29, 100 on 2026-10-25 and 96
otherwise.

#### Identity

`event_id = uuid5(namespace, "stream|subject|interval_start|issued_at|origin source|run_id|major version")`,
where the subject is the location id or the market key and the origin source is `source_system` without the
`replay:` prefix. A replay therefore keeps its event ids, a new `run_id` produces new ones, and a re-run of the same
window without a `run_id` republishes identical ids (they show up as duplicates in `make stats`).

### Time: event time, availability time and processing time

#### The timestamps

There are only two clocks: the **world clock** (event time, the timeline the data is about) and the **system
clock** (processing time, the wall clock of the machine handling it). On the world clock an event carries three
different timestamps, because they answer different questions, and mixing them up is the classic way to leak
the future into a forecast or a decision. Issue time and availability time are not extra clocks: they say when
the world clock's information became known, which is what lets the EMS stay correct when the system clock is
meaningless (backfill, replay, simulation).

| Timestamp | Question it answers | Where it is | Example |
|---|---|---|---|
| **Event time** (world clock) | When does the data apply? | `interval_start`, `interval_end` and `t` on every event; `valid_start` and `valid_end` on each forecast step. The Kafka record timestamp is also the event time. | A reading for 00:00 to 00:15 has event time `[00:00, 00:15)`. |
| **Issue time** (world clock) | When did the source produce this information? | `issued_at`: the model run time for forecasts, the publication time for prices, null for meter readings. | The ICON-D2 run of 18:00; prices published at 13:41 on the day before. |
| **Availability time** (world clock) | From when could a consumer know it? | **Not a field.** It is derived: meter readings at `interval_end` (plus `meters.availability_delay_s`), forecasts at `issued_at` + 90 minutes, prices at `publication_time`. | The 18:00 run is usable from 19:30. |
| **Processing time** (system clock) | When did the system handle it? | `ingested_at`: the wall-clock time at which the generator handed the record to Kafka. | `2026-10-20T00:15:05Z` |

#### Why event time alone is not enough

A price for tomorrow 14:00 has an event time tomorrow, but it is known
today from its publication at 13:41. A strategy running at 15:00 today may use it; at 10:00 today it may not.
Event time cannot express that, and processing time can't either in a backfill, where every event is produced
within minutes of the others. Issue time is the deterministic stand-in: it travels with the data, so a replay gives the
same answer every time. The same holds for a forecast run, which describes the next 48 hours but is only known from
its run time plus the 90-minute delivery delay.

#### What ingested_at is, and is not

It is the processing time of one stage only: the generator, at the moment it
produces to Kafka. It is stamped once and the event is never changed afterwards, so the EMS engine does **not** add
its own processing time to these events. Later stages (ingestion into Restate, a strategy's predictions, the
decisions) record their own times in their own state and messages. Those are not defined yet; they come with the
actor specs. It is also not Kafka's broker append time, which the data topics don't use (their record timestamp is
the event time; only the dead-letter topic uses append time).

#### Using the times

Things to know when you use the times:

- **In live mode** `ingested_at` is close to the availability time: a meter reading is produced right after its
  interval ends, a forecast about 90 minutes after its run, a price when PSE publishes it.
- **In backfill, accelerated and replay runs `ingested_at` is the real wall clock**, not simulated time. Backfilling
  2025 data in 2026 gives `ingested_at` values in 2026, and a replay refreshes it to the replay moment (the
  `event_id` stays). So `ingested_at` cannot tell you "what was known when" in simulated time. Event time plus
  the availability rules above can.
- **Late events** (fault injection) keep their event time and id, but their `ingested_at` is later than their
  availability time. This is the one case where processing time and availability time visibly differ.
- **Decisions must use event time, never `ingested_at`.** The EMS runs on time carried by the events (a step `t`
  in simulation, wall-clock timers only in live mode), and an experiment has to give bit-identical results when
  replayed. A strategy at step `t` may only use data whose availability time is at most `t`. Using `ingested_at` in a
  decision would make results depend on how fast a particular run happened to be.
- **Measuring latency is a good use of `ingested_at`.** The difference between the moment a downstream component
  handles an event and the event's `ingested_at` (or its availability time) gives the pipeline lag. That fits the
  project's rule to report systems metrics beside energy metrics, but no such measurement exists yet.

### Streams

Four topics carry the data and one is the dead-letter queue. Each is described below with every payload field.

#### Measurements (production and consumption)

Topics `ems.production.v1` and `ems.consumption.v1`.

One event per site and 15-minute interval. The consumption event is always produced; the production event only for
sites that have generation. Values are **energy in the interval (kWh), not power**: multiply by 4 to get the
average kW. Every value is zero or positive; the `direction` carries the sign.

`payload.measurements` is a non-empty list, one entry per asset reading:

| Field | Type | Meaning |
|---|---|---|
| `asset_id` | string | Asset within the site, for example `pv-1`, `hp-1`, `bess-1`, `ev-1`, or the virtual `load` (base load) and `grid` (the grid connection point). |
| `asset_type` | `pv` \| `wind` \| `bess` \| `chp` \| `hydro` \| `biogas` \| `biomass` \| `heat_pump` \| `ev_charger` \| `load` \| `grid_connection` | What kind of asset. |
| `quantity` | `active_energy` \| `active_power` \| `reactive_energy` \| `soc` \| `heat_energy` \| `fuel_gas_volume` | What is measured. The generator emits `active_energy`, `soc` and `heat_energy`. |
| `direction` | `generation` \| `consumption` \| `import` \| `export` \| `charge` \| `discharge` | Which way the energy flows. `generation` and `discharge` belong on the production stream; `consumption`, `charge`, `import` and `export` on the consumption stream. |
| `aggregation` | `interval_sum` \| `interval_mean` \| `instant` \| `register` | How the value relates to the interval: a sum over it, a mean, a point reading, or a cumulative counter. |
| `value` | number, at least 0 | The reading. |
| `unit` | string | `kWh` for everything the generator emits (the contract also allows kW, kvarh and m³). |
| `phase` | string | `total` unless a single phase is reported. |
| `obis` | string or null | OBIS code of the physical register. Null for synthetic data. |
| `quality` | `measured` \| `estimated` \| `substituted` \| `missing` \| `synthetic` | Trust level. Generated data is `synthetic`; fault injection can mark readings `estimated` or `substituted`. |
| `quality_raw` | string or null | The source's own quality code, if it had one. |
| `version` | integer, at least 1 | Revision number of the reading. The generator always emits 1. |

What the generator emits, by asset:

| `asset_id` | `asset_type` | `quantity` | `direction` | `aggregation` | Stream | Present when |
|---|---|---|---|---|---|---|
| `pv-1` | `pv` | `active_energy` | `generation` | `interval_sum` | production | the site has PV |
| `wind-1` | `wind` | `active_energy` | `generation` | `interval_sum` | production | wind farm |
| `chp-1` | `chp` | `active_energy` | `generation` | `interval_sum` | production | CHP plant (electricity) |
| `chp-1` | `chp` | `heat_energy` | `generation` | `interval_sum` | production | CHP plant (heat output, kWh) |
| `biogas-1`, `biomass-1`, `hydro-1` | `biogas`, `biomass`, `hydro` | `active_energy` | `generation` | `interval_sum` | production | that plant type |
| `bess-1` | `bess` | `active_energy` | `discharge` | `interval_sum` | production | site-controlled battery |
| `load` | `load` | `active_energy` | `consumption` | `interval_sum` | consumption | always (base load) |
| `hp-1` | `heat_pump` | `active_energy` | `consumption` | `interval_sum` | consumption | the site has a heat pump |
| `ev-1` | `ev_charger` | `active_energy` | `consumption` | `interval_sum` | consumption | the site has an EV charger |
| `bess-1` | `bess` | `active_energy` | `charge` | `interval_sum` | consumption | site-controlled battery |
| `bess-1` | `bess` | `soc` | `charge` | `instant` | consumption | site-controlled battery (state of charge in kWh; carried with direction `charge` so it fits this stream) |
| `grid` | `grid_connection` | `active_energy` | `import` | `interval_sum` | consumption | sites not controlled by the EMS |
| `grid` | `grid_connection` | `active_energy` | `export` | `interval_sum` | consumption | sites not controlled by the EMS |
| `grid-register` | `grid_connection` | `active_energy` | `import` | `register` | consumption | as above, with `meters.emit_registers: true` (cumulative import counter, for register-reset faults) |

**Who owns the battery and the grid flows.** A site whose battery is controlled by the EMS (`bess.control: ems`, which
includes the test site `PL-LOC-000001`) emits **no `bess` and no `grid_connection` measurements**: those are facts
the EMS will own. Other sites ("background" sites) run a simple battery policy and report the battery and grid
flows, which close the balance:

`generation + discharge + import = consumption + charge + export` (all in kWh, per interval).

Example, a trimmed `ems.consumption.v1` record (full examples in [schemas/examples/](schemas/examples/)):

```json
{
  "schema": "ems.measurement", "schema_version": "1.0.0",
  "event_id": "46fc7784-b7da-5ce6-ba37-a83f37f07093",
  "stream": "consumption", "source_system": "synthetic", "source_record_ref": null,
  "location_id": "PL-LOC-000001", "metering_point_id": "590322400000000010",
  "algorithm_id": null, "run_id": null,
  "interval_start": "2026-10-20T00:00:00Z", "interval_end": "2026-10-20T00:15:00Z",
  "resolution": "PT15M", "t": 1991616, "event_time_kind": "interval",
  "issued_at": null, "ingested_at": "2026-10-20T00:15:05Z", "synthetic": true, "seed": 42,
  "payload": {"measurements": [
    {"asset_id": "load", "asset_type": "load", "quantity": "active_energy", "direction": "consumption",
     "aggregation": "interval_sum", "value": 0.071, "unit": "kWh", "phase": "total", "obis": null,
     "quality": "synthetic", "quality_raw": null, "version": 1},
    {"asset_id": "hp-1", "asset_type": "heat_pump", "quantity": "active_energy", "direction": "consumption",
     "aggregation": "interval_sum", "value": 0.402, "unit": "kWh", "phase": "total", "obis": null,
     "quality": "synthetic", "quality_raw": null, "version": 1}
  ]}
}
```

#### Weather forecasts

Topic `ems.weather.forecast.v1`.

One event per site and model run. Runs are issued every 3 hours (00, 03, ... UTC) and each covers 48 hours as 192
steps of 15 minutes (a live run can be a little shorter, see the Open-Meteo notes below), so for any future moment
up to 16 runs overlap at different lead times. Envelope fields:
`issued_at` is the run's initialisation time and equals the first step's `valid_start`; `interval_start` and
`interval_end` span the first to the last step; `t` is the quarter-hour index of `issued_at`.

| Payload field | Type | Meaning |
|---|---|---|
| `forecast_model` | string | Model that produced the run: `synthetic`, `icon_d2` (DWD ICON-D2, used for Poland) or `dmi_harmonie_arome_europe` (used east of 20.34 °E). |
| `steps` | list, at least 1 | The consecutive 15-minute steps, in order. |

Each step:

| Step field | Type | Meaning |
|---|---|---|
| `valid_start`, `valid_end` | UTC datetime | The 15-minute interval the step describes. Steps are contiguous. |
| `lead_time` | ISO duration | `valid_end - issued_at`, for example `PT15M`, `PT1H15M`. |
| `ghi_wm2` | number, at least 0 | Global horizontal irradiance in W/m², **mean over the preceding 15 minutes**, labelled at `valid_end`. |
| `dni_wm2` | number, at least 0 | Direct normal irradiance in W/m², same averaging. |
| `dhi_wm2` | number, at least 0 | Diffuse horizontal irradiance in W/m², same averaging. |
| `radiation_label` | `preceding_mean` | States the averaging convention above. |
| `temp_air_c` | number | Air temperature at 2 m in °C, an **instant at `valid_end`**. |
| `cloud_cover_pct` | integer 0 to 100 | Total cloud cover in %, an instant at `valid_end`. |
| `wind_speed_ms` | object | Wind speed in m/s at 10, 100 and 120 m above ground: `{"10": 3.1, "100": 6.0, "120": 6.2}`. Instants at `valid_end`. |
| `wind_dir_deg` | integer 0 to 359 | Wind direction at 10 m in degrees, an instant at `valid_end`. |

With the live source, only the radiation is native 15-minute model output. Open-Meteo interpolates temperature, wind
and cloud cover from hourly values, so do not read them as native 15-minute data.

#### Prices

Topic `ems.price.v1`.

One event per market and local (Europe/Warsaw) delivery day, published the day before. Prices are in the native
currency per MWh, **not** per kWh, and negative values are real and allowed.

| Payload field | Type | Meaning |
|---|---|---|
| `market` | `PL_RCE` \| `PL_SDAC` \| `ENTSOE_A44_PL` \| `PL_DAM_TGE` | Which price. Kafka key: `PL:RCE`, `PL:SDAC`, `ENTSOE:PL`, `PL:TGE` (reserved, no free source). |
| `bidding_zone` | `PL` | Bidding zone. |
| `currency` | `PLN` \| `EUR` | Currency; must match the unit. |
| `unit` | `PLN/MWh` \| `EUR/MWh` | Unit of every `price`. |
| `delivery_day` | date | The local calendar day the prices apply to. |
| `publication_time` | UTC datetime | When the price was published; always equal to the envelope's `issued_at`. |
| `resolution` | `PT15M` \| `PT1H` | Slot length. Quarter-hourly since 2025-10-01. |
| `slots` | list | The day's prices in order, each `{"interval_start", "interval_end", "price"}`. **92, 96 or 100 slots** at `PT15M` (spring-forward, normal, fall-back day). |

The markets:
- `PL_RCE`: PSE's RCE, the reference price for net billing under the RES act. It is **not** the day-ahead clearing
  price. The synthetic generator emits only this market.
- `PL_SDAC`: the day-ahead clearing price PSE publishes separately (`csdac-pln`). Live source only.
- `ENTSOE_A44_PL`: ENTSO-E day-ahead price for Poland. Live source only, needs a token.

If a source republishes a day with changed values, the new event has a new `publication_time` and therefore a new
`event_id`, on the same key. Consumers should use the latest `issued_at` per market and delivery day.

#### Dead-letter topic

Topic `ems.gen.dlq.v1`.

Where the generator parks something it refuses to publish to its real topic. It is normally empty. The key is the
original key, the value is the original payload, and the `error` header holds the reason (up to 1,000 characters).
Two things end up here: an event that fails strict validation before sending, and, with live ENTSO-E enabled, a note
about a day whose price series conflict. This topic uses broker append time instead of event time. `make validate` does not
schema-check it and `make stats` skips it. To read it: `uv run ems-gen tail --topic ems.gen.dlq.v1 --from-beginning`.

### Live sources: raw API responses

With live sources enabled (`make gen-live`, `make record`), the generator does not invent the data: adapters call
four external APIs and turn each raw response into the events described above. The adapters live in
[src/ems/ingest/live/](src/ems/ingest/live/). The conversion is declared in YAML files in
[mappings/](mappings/), which name the raw field each canonical field is read from, plus its type, time zone,
unit and what to do with nulls. The envelope fields come from the adapter and the mapping, as described above;
nothing is copied from the raw response except the values listed here.

| Source | Request | Auth | Mapping | Becomes |
|---|---|---|---|---|
| PSE RCE | `GET https://api.raporty.pse.pl/api/rce-pln` | none | [mappings/pse.rce.v2.yaml](mappings/pse.rce.v2.yaml) | `ems.price.v1`, market `PL_RCE` (key `PL:RCE`) |
| PSE SDAC | `GET https://api.raporty.pse.pl/api/csdac-pln` | none | [mappings/pse.csdac.v2.yaml](mappings/pse.csdac.v2.yaml) | `ems.price.v1`, market `PL_SDAC` (key `PL:SDAC`) |
| Open-Meteo | `GET https://api.open-meteo.com/v1/forecast` | none (free tier), or `X-Api-Key` header on `customer-api.open-meteo.com` | [mappings/openmeteo.forecast.v1.yaml](mappings/openmeteo.forecast.v1.yaml) | `ems.weather.forecast.v1` |
| ENTSO-E A44 | `GET https://web-api.tp.entsoe.eu/api` | `SECURITY_TOKEN` header (off until `EMS_ENTSOE_TOKEN` is set) | [mappings/entsoe.a44.v1.yaml](mappings/entsoe.a44.v1.yaml) | `ems.price.v1`, market `ENTSOE_A44_PL` (key `ENTSOE:PL`) |

The samples below are trimmed. The PSE and Open-Meteo ones are real responses recorded on 2026-10-20 and kept in
[tests/fixtures/](tests/fixtures/); the ENTSO-E one is hand-written (see there).

#### PSE RCE and SDAC prices

Both reports have the same shape and differ only in the price field name. The adapter requests one business day
at a time:
`GET /api/csdac-pln?$filter=business_date eq '2026-10-21'&$first=1000`. If more rows exist than fit in a page, the
response contains a `nextLink`, which the adapter follows (`$top` and `$count` are not supported and return
HTTP 400). An empty `value` list means the day has not been published yet.

```json
{"value": [
  {"dtime": "2026-10-21 00:15", "period": "00:00 - 00:15", "csdac_pln": 624.92,
   "dtime_utc": "2026-10-20 22:15:00", "period_utc": "22:00 - 22:15", "business_date": "2026-10-21",
   "publication_ts": "2026-10-20 13:41:12.610", "publication_ts_utc": "2026-10-20 11:41:12.610"},
  {"dtime": "2026-10-21 00:30", "period": "00:15 - 00:30", "csdac_pln": 595.50,
   "dtime_utc": "2026-10-20 22:30:00", "period_utc": "22:15 - 22:30", "business_date": "2026-10-21",
   "publication_ts": "2026-10-20 13:41:12.610", "publication_ts_utc": "2026-10-20 11:41:12.610"}
]}
```

For `rce-pln` the price field is `rce_pln` instead of `csdac_pln`; everything else is the same.

| Raw field | Used as | Note |
|---|---|---|
| `dtime_utc` | slot end; the slot starts 15 minutes earlier | The UTC timestamp labels the **end** of the quarter-hour. |
| `csdac_pln` / `rce_pln` | `slots[].price` | PLN/MWh excluding VAT. Negative values are real. |
| `business_date` | `delivery_day` | The local delivery day. Rows of other days are ignored. |
| `publication_ts_utc` | `publication_time` and `issued_at` | One publication time per day. A new value means PSE republished, which becomes a new event. |
| `period` | kept only as an optional label | Not used for the time. |
| `dtime`, `period_utc`, `publication_ts` | not used | The local labels are unreliable on daylight-saving days (the API writes `02a:15` on fall-back days), so only the UTC fields are trusted. |

Result: one `ems.price.v1` event per report and day, built only when every slot of the local day is present
(92, 96 or 100 rows), with `source_system` `pse-rce` or `pse-csdac` and
`source_record_ref` `csdac-pln:2026-10-21:2026-10-20T11:41:12.610000+00:00`. In this sample slot 1 is
`{"interval_start": "2026-10-20T22:00:00Z", "interval_end": "2026-10-20T22:15:00Z", "price": 624.92}`. Re-polling
an unchanged publication produces the same `event_id` and is suppressed. The adapter polls every 15 minutes between
13:00 and 18:00 local until tomorrow's prices are complete, and hourly otherwise.

Before 2025-10-01 the API has hourly values written as four identical quarter-hour rows (for example
`csdac_pln: 941.06` on both the 00:15 and 00:30 rows of 2024-06-14). The same mapping handles that history.

#### Open-Meteo forecast

Two requests per model run. First a cheap one to see whether a new run exists:
`GET https://api.open-meteo.com/data/dwd_icon_d2/static/meta.json`, polled every 5 minutes. The adapter fetches the
forecast only when `last_run_initialisation_time` has changed and the run is at least 10 minutes past
`last_run_availability_time`.

```json
{"last_run_initialisation_time": 1792519200, "last_run_availability_time": 1792524312,
 "last_run_modification_time": 1792524258, "temporal_resolution_seconds": 3600,
 "update_interval_seconds": 10800, "data_end_time": 1792695600, "chunk_time_length": 121}
```

Then the forecast, anchored to the run so the same run always gives the same window:

```
GET /v1/forecast?latitude=52.41&longitude=16.93&elevation=85&models=icon_d2
    &minutely_15=shortwave_radiation,direct_normal_irradiance,diffuse_radiation,temperature_2m,cloud_cover,
                 wind_speed_10m,wind_speed_100m,wind_speed_120m,wind_direction_10m
    &timezone=UTC&timeformat=unixtime&wind_speed_unit=ms
    &start_minutely_15=2026-10-20T18:15&end_minutely_15=2026-10-22T18:00
```

```json
{"latitude": 52.41825, "longitude": 16.937275, "elevation": 85.0, "utc_offset_seconds": 0, "timezone": "GMT",
 "minutely_15_units": {"time": "unixtime", "shortwave_radiation": "W/m²", "direct_normal_irradiance": "W/m²",
   "diffuse_radiation": "W/m²", "temperature_2m": "°C", "cloud_cover": "%", "wind_speed_10m": "m/s",
   "wind_speed_100m": "m/s", "wind_speed_120m": "m/s", "wind_direction_10m": "°"},
 "minutely_15": {
   "time":                     [1792520100, 1792521000, "..."],
   "shortwave_radiation":      [0.0, 0.0, "..."],
   "direct_normal_irradiance": [0.0, 0.0, "..."],
   "diffuse_radiation":        [0.0, 0.0, "..."],
   "temperature_2m":           [10.9, 10.9, "..."],
   "cloud_cover":              [98, 99, "..."],
   "wind_speed_10m":           [3.58, 3.52, "..."],
   "wind_speed_100m":          [8.37, 7.9, "..."],
   "wind_speed_120m":          [8.54, 8.06, "..."],
   "wind_direction_10m":       [126, 125, "..."]}}
```

The response is columnar: each key under `minutely_15` is an array and index *i* across all arrays is one 15-minute
step. A location outside the model's grid returns HTTP 400 with `{"reason": "No data is available for this
location", "error": true}`.

| Raw field | Becomes (step field) | Note |
|---|---|---|
| `time` | `valid_end` (the step is the 15 minutes before it) | Unix seconds, labels the **end** of the step. The first value, 18:15, is the step 18:00 to 18:15. |
| `shortwave_radiation` | `ghi_wm2` | Mean over the preceding 15 minutes; negatives are clipped to 0. |
| `direct_normal_irradiance` | `dni_wm2` | Same. |
| `diffuse_radiation` | `dhi_wm2` | Same. |
| `temperature_2m` | `temp_air_c` | Instant, interpolated by Open-Meteo from hourly model output. |
| `cloud_cover` | `cloud_cover_pct` | Rounded to an integer 0 to 100; interpolated. |
| `wind_speed_10m`, `_100m`, `_120m` | `wind_speed_ms` `"10"`, `"100"`, `"120"` | m/s, requested with `wind_speed_unit=ms`; interpolated. |
| `wind_direction_10m` | `wind_dir_deg` | Rounded, modulo 360; interpolated. |
| `last_run_initialisation_time` (meta.json) | `issued_at` | The model run time, not the poll time. |

Result: one `ems.weather.forecast.v1` event per site and run, with `forecast_model` `icon_d2` (or
`dmi_harmonie_arome_europe` east of 20.34 °E), `source_system` `open-meteo` and `source_record_ref`
`open-meteo:icon_d2:2026-10-20T18:00:00+00:00`. A live run **can have fewer than 192 steps**: the model's data can end
before the requested window, trailing nulls are dropped, and the event keeps the complete prefix. The recorded
sample above gives 183 steps (ending 2026-10-22T15:45Z).

#### ENTSO-E A44 (day-ahead prices)

The ENTSO-E adapter is off by default and needs a token. It is tested on fixtures and, since 2026-10-29, has also
been run against the real API with a token: the `SECURITY_TOKEN` header was accepted, a day before 2025-10-01
(2025-09-30) came back hourly (`PT1H`, 24 slots, `EUR`), and the fall-back day 2025-10-26 came back quarter-hourly
(`PT15M`, 100 slots). The current day and the next day (already published) also came back as `PT15M` with 96
slots, in EUR. A recording of the real responses (2026-10-29) shows the same structure as the sample below:
schema version 7:3, one `TimeSeries` per document with the same elements, `curveType` A03 and, for these
quarter-hourly days, all 96 positions present. The prices are the same series PSE publishes as SDAC: every slot
matches, with a constant PLN-per-EUR ratio (about 4.4) across the day. The sample below is hand-written to follow
ENTSO-E's implementation guide (the prices are invented): ENTSO-E day-ahead prices are not open data, so no real
response is committed.

```
GET https://web-api.tp.entsoe.eu/api?documentType=A44&in_Domain=10YPL-AREA-----S&out_Domain=10YPL-AREA-----S
    &periodStart=202610202200&periodEnd=202610212200&contract_MarketAgreement.type=A01
SECURITY_TOKEN: <EMS_ENTSOE_TOKEN>
```

```xml
<Publication_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3">
  <mRID>fixture-2026-10-20T22:00Z</mRID>
  <type>A44</type>
  <createdDateTime>2026-10-20T12:07:18Z</createdDateTime>
  <period.timeInterval><start>2026-10-20T22:00Z</start><end>2026-10-21T22:00Z</end></period.timeInterval>
  <TimeSeries>
    <mRID>1</mRID>
    <contract_MarketAgreement.type>A01</contract_MarketAgreement.type>
    <currency_Unit.name>EUR</currency_Unit.name>
    <price_Measure_Unit.name>MWH</price_Measure_Unit.name>
    <curveType>A03</curveType>
    <Period>
      <timeInterval><start>2026-10-20T22:00Z</start><end>2026-10-21T22:00Z</end></timeInterval>
      <resolution>PT15M</resolution>
      <Point><position>1</position><price.amount>95.10</price.amount></Point>
      <Point><position>5</position><price.amount>92.40</price.amount></Point>
      <Point><position>9</position><price.amount>88.00</price.amount></Point>
      <!-- ... -->
    </Period>
  </TimeSeries>
</Publication_MarketDocument>
```

When there is no data, ENTSO-E answers with an acknowledgement document instead
(`<Acknowledgement_MarketDocument>` with `<Reason><code>999</code>`), which the adapter treats as "nothing yet".

The adapter first flattens the XML to one row per slot, which the mapping then reads (element names contain dots,
so the mapping uses JSON pointers):

| Raw element | Becomes | Note |
|---|---|---|
| `Period/Point/position` and `timeInterval/start` | slot start (UTC) | `start + (position − 1) × resolution`. |
| `Point/price.amount` | `slots[].price` | In the currency the document states; **EUR/MWh** for Poland's day-ahead price, not PLN. |
| `currency_Unit.name` | `currency` (and `unit`) | `EUR` or `PLN`. |
| `Period/resolution` | `resolution` | `PT15M` stays; `PT60M` becomes `PT1H` (hourly before 2025-10-01). |
| `contract_MarketAgreement.type` | filter | Only `A01` (day-ahead) series are used. |
| `TimeSeries/mRID` | series id | Used to spot duplicated series. |

Rules the adapter applies:
- **`curveType A03`** sends a point only where the price changes. The value is carried forward to the next
  position, or to the end of the period, so the sample above gives 95.10 for slots 1 to 4, then 92.40 for slots 5 to
  8, and so on.
- **Duplicates.** ENTSO-E sometimes repeats a series; identical copies collapse. If copies disagree, the day is not
  published as an event and goes to the dead-letter topic instead.
- **`issued_at`.** ENTSO-E gives no publication time for the prices (`createdDateTime` is when the document was
  built; the real responses show it equals the request time), so `issued_at` is the first time the adapter saw this
  exact content, kept in recordings so a replay gets the same ids. A restart of a live run may therefore re-emit a
  day once, and a consumer applying the availability rule sees ENTSO-E prices only from that first fetch, not from
  their real publication. PSE prices carry the real publication time.

Result: `source_system` `entsoe`, `source_record_ref` `entsoe:A44:2026-10-21`, market `ENTSOE_A44_PL`, currency
`EUR`, unit `EUR/MWh`. Converting to PLN is the consumer's job.

### When events are emitted

| Stream | Emitted at |
|---|---|
| Measurements | At `interval_end` plus `meters.availability_delay_s` (0 by default). |
| Forecasts | 90 minutes after `issued_at`, which is when ICON-D2 runs become available. |
| Prices | In the early afternoon on the day before the delivery day (mostly 13:00 to 15:00 local, occasionally until 16:15). The days already published when a run starts are emitted at the start. |

In backfill mode the whole window is written as fast as possible; the Kafka record timestamps are still the event
times, which is why backfills use the `sim` topic profile. With fault injection on (`--faults`), some events are
dropped, duplicated (same `event_id`), delivered late (up to 2 hours, same event time and id), repeated (stuck
values) or flagged `estimated` or `substituted`, and consumers must tolerate all of those.

### Rules for consumers

- Parse tolerantly: ignore unknown fields. Within major version 1 changes only add fields; a rename or removal
  means a new major version on new topics (`...v2`).
- Deduplicate on `event_id`; do not assume ordering across topics or between a site's late events and later ones.
- A strategy at time `t` may only use data that was available then: measurements at `interval_end`, forecasts at
  `issued_at` plus 90 minutes, prices at their publication time.
