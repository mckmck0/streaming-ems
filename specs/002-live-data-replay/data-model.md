# Data model: 002

No contract change: events are v1.0.0 (`source_system` values `open-meteo`, `pse-rce`, `pse-csdac`, `entsoe`,
`replay:<orig>`; markets `PL_RCE`, `PL_SDAC`, `ENTSOE_A44_PL` already exist).

## Mapping file (`mappings/*.yaml`, schema `mappings/_schema/mapping.schema.json`)

```yaml
mapping: pse.rce.v2          # id = file stem
version: 1
source: pse-rce              # source_system of the produced events
layout: rows                 # rows | columns
records: /value              # JSON pointer to the list of records (rows) or the column object (columns)
fields:                      # canonical name -> spec
  interval_end: {path: dtime_utc, type: datetime, tz: utc}
  price:        {path: rce_pln, type: float, unit_in: PLN/MWh, unit_out: PLN/MWh}
  published:    {path: publication_ts_utc, type: datetime, tz: utc}
  raw_label:    {path: period, type: str}
time: {label: end, resolution: PT15M}   # derives interval_start/interval_end from the labelled field
null_policy: skip            # skip | missing (row kept with quality = missing)
```

Field spec keys: `path` (dotted path, or JSON pointer starting with `/`, whose segments may contain dots),
`type` (`float` `int` `str` `datetime` `date`), `tz` (`utc`, `local:Europe/Warsaw`, `epoch_s`, `epoch_ms`),
`unit_in`/`unit_out` (W, kW, MW, Wh, kWh, MWh, 0.1Wh, or identical price units), `transforms` (list of
`negate`, `scale: x`, `enum_map: {...}`, `default: x`, `round: n`), `sign_split: [pos_name, neg_name]`,
`quality_map` (raw → canonical quality).

## Recording (`data/recordings/<name>/`)
`manifest.json` (sources, window, config, versions, commit), `raw/<source>/<n>.json|xml` (+ `.meta.json` with
URL and query without secrets, status, fetched_at), `events.jsonl` (canonical events, one per line, in
emission order).
