# Research: 002 live data, record and replay

Facts verified 2026-10-20 (read-only research for the approved plan; sources in `docs/context/13`, `05`, `12`).

## R1 PSE (api.raporty.pse.pl, v2, public)
- `GET /api/rce-pln?$filter=business_date eq 'YYYY-MM-DD'` and `/api/csdac-pln`; 8 fields per record
  (`dtime`, `period`, `rce_pln`|`csdac_pln`, `dtime_utc`, `period_utc`, `business_date`, `publication_ts`,
  `publication_ts_utc`). `dtime_utc` labels the interval end. Default page 100 → follow `nextLink`; `$first`
  works, `$top`/`$count` return 400. Empty `value` = not yet published. csdac `dtime` has no seconds.
- Fall-back day labels `02a:15`; spring-forward labels changed between 2025 and 2026 → use UTC fields only.
- Publication D−1 13:00–16:13 local (77 % 13–14 h); two same-day republications observed → revision = new
  `publication_ts_utc`. Distinct quarter-hour values since 2025-10-01.
- **Decision**: one adapter, parameterised by report (`rce-pln` → `PL_RCE`, `csdac-pln` → `PL_SDAC`).

## R2 Open-Meteo
- `/v1/forecast` with `minutely_15=` all nine variables, `models=icon_d2` (ICON-D2 domain 43.18–58.08 N,
  3.94 W–20.34 E; east of 20.34 E → `dmi_harmonie_arome_europe`), `elevation=`, `timeformat=unixtime`,
  `wind_speed_unit=ms`, `timezone=UTC`, `start_minutely_15`/`end_minutely_15` (run-anchored window).
- Radiation = preceding 15-min mean (label = end); temperature/wind/cloud instant. Only radiation is native
  15-min in ICON-D2; others are interpolated server-side. Response has no model or run time.
- Run time: `https://api.open-meteo.com/data/<model>/static/meta.json` → `last_run_initialisation_time`,
  `last_run_availability_time` (epoch s). Accept a run when now ≥ availability + 600 s.
- Free API non-commercial (public research qualifies); customer API host `customer-api.open-meteo.com`, key in
  `X-Api-Key` header. Errors: 400 (bad params / no data), 429, 503. Data CC BY 4.0, attribution
  "Weather data by Open-Meteo.com".
- **Decision**: poll meta.json every 5 min; fetch forecast only for a new run; issued_at = run init time.

## R3 ENTSO-E A44
- `https://web-api.tp.entsoe.eu/api`, `documentType=A44`, `in_Domain=out_Domain=10YPL-AREA-----S`,
  `contract_MarketAgreement.type=A01`, token header `SECURITY_TOKEN`. Namespace
  `urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3` (also 7:0); parse by local name.
- A03 omits repeated positions (forward-fill to the period end); PT60M before 2025-10-01, PT15M after;
  duplicate series known issue (#9472498); 999 acknowledgement = no data; 400 limit 400 req/min per token, ban
  ~10 min on 429. Day-ahead prices are not CC BY → only hand-written fixtures in the repo.
- **Decision**: own lxml parser (`resolve_entities=False`, `no_network=True`, `load_dtd=False`), no entsoe-py.

## R4 Libraries (resolved under the cutoff)
httpx 0.28.1, tenacity 9.1.4 (`AsyncRetrying`, `wait_exponential_jitter`), lxml 6.1.3, respx 0.23.1 (dev).

## R5 Hybrid weather for live sites
- **Decision**: for a live-weather site, the synthetic P/C models take the "truth" weather from the latest
  live run's steps covering each interval (shortest lead available); before the first run arrives, they fall
  back to synthetic truth.

## R6 Calibration
- Prices: PSE `rce-pln` history from 2025-10-01 (quarter-hour dynamics): monthly base, negative share, noise sd.
- Temperature: Open-Meteo archive (`archive-api.open-meteo.com/v1/archive`, ERA5) annual mean/amplitude.
- **Decision**: the job writes `config/calibration/candidates/*.yaml` with data window and fit stats; adopting
  them is a reviewed change (never automatic).
