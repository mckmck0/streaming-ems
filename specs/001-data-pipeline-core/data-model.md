# Data model: 001 data pipeline core

Wire contract v1.0.0. Models live in `src/ems/contracts/`; exported JSON Schemas in `schemas/`. All datetimes are
timezone-aware UTC, serialised RFC 3339 with `Z`. Floats are finite (NaN/inf rejected). Producers validate
strictly (unknown fields rejected); consumers, including `ems-gen validate`, parse with unknown fields ignored, so
an additive minor version never breaks an older reader. Exported schemas are open (`additionalProperties`
allowed); compatibility is checked FULL (backward and forward).

## Envelope (all streams)

| Field | Type | Rule |
|---|---|---|
| `schema` | enum `ems.measurement` \| `ems.weather_forecast` \| `ems.price` | matches `stream` |
| `schema_version` | str semver, `"1.0.0"` | additive within major |
| `event_id` | UUIDv5 (str) | see Identity |
| `stream` | enum `production` \| `consumption` \| `weather_forecast` \| `weather_actual` \| `price` | |
| `source_system` | str | `synthetic`, `open-meteo`, `pse-rce`, `pse-csdac`, `entsoe`, `replay:<orig>` |
| `source_record_ref` | str \| null | never contains secrets |
| `location_id` | str \| null | `PL-LOC-` + 6 digits; required except `price`, null for `price` |
| `metering_point_id` | str \| null | 18-digit GSRN, prefix `590`, valid GS1 check digit |
| `algorithm_id` | null | reserved (always null in v1) |
| `run_id` | str \| null | `^[A-Za-z0-9._-]{1,64}$`; null for live data |
| `interval_start`, `interval_end` | UTC datetime | half-open, `end > start` |
| `resolution` | enum `PT15M` \| `PT1H` \| `PT1M` \| `P1D` | measurements: equals `end − start`; forecasts: step resolution; prices: slot resolution |
| `t` | int | `floor(unix_s(interval_start) / 900)` |
| `event_time_kind` | enum `interval` \| `instant` \| `forecast` | |
| `issued_at` | UTC datetime \| null | required for forecasts and prices |
| `ingested_at` | UTC datetime | wall clock; excluded from identity |
| `synthetic` | bool | |
| `seed` | int \| null | per-location seed for synthetic data |
| `payload` | one of the payloads below | discriminated by `stream` |

IDs match `^[A-Za-z0-9._-]+$`; `:` and `|` are reserved delimiters.

### Identity

`event_id = uuid5(EMS_NAMESPACE, "|".join([stream, location_id or market_key, fmt(interval_start),
fmt(issued_at) or "", origin_source, run_id or "", str(major)]))`, where `fmt` = RFC 3339 UTC with milliseconds
and `Z`, `origin_source` = `source_system` without a leading `replay:`. `EMS_NAMESPACE` is a fixed UUID constant
in `ems.contracts.ids`. `market_key` is the key form without run prefix (table below). A replay re-emits the
recorded envelope unchanged except `source_system = replay:<orig>` and `ingested_at`, so its id is unchanged.

## Measurement payload (`production`, `consumption`)

`payload.measurements: list[Measurement]` (≥ 1), one event per location per interval.

| Field | Values |
|---|---|
| `asset_id` | str id |
| `asset_type` | `pv` `wind` `bess` `chp` `hydro` `biogas` `biomass` `heat_pump` `ev_charger` `load` `grid_connection` |
| `quantity` | `active_energy` `active_power` `reactive_energy` `soc` `heat_energy` `fuel_gas_volume` |
| `direction` | `generation` `consumption` `import` `export` `charge` `discharge` (value ≥ 0; sign by direction) |
| `aggregation` | `interval_sum` `interval_mean` `instant` `register` |
| `value`, `unit` | finite ≥ 0; canonical units kWh, kW, kvarh, kWh (soc), m³ |
| `phase`, `obis` | `total` default; obis str \| null |
| `quality`, `quality_raw` | `measured` `estimated` `substituted` `missing` `synthetic`; raw str \| null |
| `version` | int ≥ 1 |

Routing: production = generation of every generating asset (+ BESS discharge at unmanaged sites); consumption =
load, heat pump, EV (+ BESS charge and grid import/export at unmanaged sites only). **Sites with `bess.control = ems` emit no `bess` and no `grid_connection`
measurements.**

## Weather forecast payload

`payload.forecast_model: str`; `payload.steps: list[Step]` (192 for a 48 h run at PT15M).
Envelope interval = `[first valid_start, last valid_end)`; `issued_at` = model run (initialisation) time = first
`valid_start`; the event is emitted at `issued_at + availability_delay` (PT1H30M). A window counts the runs whose
`issued_at` lies in `[start, end)` (2 days → 16).

| Step field | Rule |
|---|---|
| `valid_start`, `valid_end` | 15-min interval; `lead_time = valid_end − issued_at` (ISO duration, e.g. `PT1H15M`) |
| `ghi_wm2`, `dni_wm2`, `dhi_wm2` | ≥ 0, mean over the interval (`radiation_label = "preceding_mean"`, labelled at `valid_end`) |
| `temp_air_c` | instant at `valid_end` |
| `cloud_cover_pct` | int 0–100, instant |
| `wind_speed_ms` | object `{"10": f, "100": f, "120": f}`, instant |
| `wind_dir_deg` | int 0–359, instant |

## Price payload

One event per market per local delivery day; envelope interval = the delivery day in UTC (local midnights).

| Field | Rule |
|---|---|
| `market` | `PL_RCE` → key `PL:RCE`; `PL_SDAC` → `PL:SDAC`; `ENTSOE_A44_PL` → `ENTSOE:PL`; `PL_DAM_TGE` → `PL:TGE` (reserved). One mapping in `ems.contracts.topics` |
| `bidding_zone`, `currency`, `unit` | `PL`; `PLN`/`EUR`; `PLN/MWh` or `EUR/MWh` |
| `delivery_day` | local date (Europe/Warsaw) |
| `publication_time` | UTC datetime (= `issued_at`) |
| `resolution` | `PT15M` (or `PT1H` for history before 2025-10-01) |
| `slots` | list of `{interval_start, interval_end, price}`; count 92/96/100 at PT15M; negative allowed |

## Site (catalog)

| Field | Rule |
|---|---|
| `location_id`, `site_type` | `household` in 001 (others in 004) |
| `lat`, `lon`, `elevation_m`, `voivodeship` (TERYT) | inside the voivodeship bounding box |
| `metering_point_id`, `dso` | GSRN; DSO prefix (Tauron 3224 real, others synthetic) |
| `tariff_group` | `G11` `G12` (profile H25), `C11` `C21` (profile G25); C11 iff ≤ 40 kW and fuse ≤ 63 A |
| `annual_base_load_kwh` | > 0; excludes heat pump and EV (modelled separately) |
| `assets` | list: `pv {kwp, tilt, azimuth, ac_kw}`, `heat_pump {…}`, `bess {capacity_kwh, p_max_kw, eta, soc_min, soc_max, control: ems\|site}` |
| `seed` | `stable_hash(global_seed, location_id)` |

Test site `PL-LOC-000001`: 52.41 N, 16.93 E, household, G12, PV 10 kWp, BESS 10 kWh / 5 kW (control `ems`), heat
pump (adds ~5–6 MWh/yr), base load 2,500 kWh/yr excluding the heat pump.

## Topic definition (`config/topics.yaml`)

`defaults` (RF, configs), `profiles` (`live`, `sim`, `scale`, `cluster`: partitions per class + config overrides),
`topics` (name, class `location`\|`global`, key rule, enabled, configs). Config keys validated against an
allow-list; values coerced to strings. See [contracts/topics.md](contracts/topics.md).

## State transitions

- Topic: absent → created; existing → ok | partitions-increased (flag) | fail (fewer) | warn (more) |
  drift-reported → drift-applied (flag).
- Generator run: preflight → streaming → flushed (exit 0) | failed (exit ≠ 0 on fatal or undelivered records).
