# 12 · Domain model: time base, units, battery, settlement

## Current consensus

### Time base

- Storage is always UTC. Europe/Warsaw is used only for calendar logic: delivery days, tariffs, holidays.
- `t` = `floor(unix_seconds(interval_start) / 900)`, the UTC quarter-hour index (epoch 1970-01-01T00:00Z).
- A local delivery day has 96 quarter-hours, 92 on spring-forward days and 100 on fall-back days. 2026:
  29 March and 25 October (Dz.U. 2022 poz. 539 lists 2022–2026; derive later years from the EU rule, last Sunday of
  March and October at 01:00 UTC). Fixtures cover 2026-03-29 and 2026-10-25.
- Intervals are half-open `[start, end)`. Adapters convert the source's label (start, end, snapshot) to that form;
  PSE `dtime_utc` and Open-Meteo radiation timestamps label the interval end.

### Units

| Quantity | Wire (Kafka) | Domain |
|---|---|---|
| Energy | kWh | `e_kwh` |
| Power | kW | `p_kw` (energy only through `dt_h`) |
| Irradiance, temperature, wind | W/m², °C, m/s | same |
| Price | native currency per MWh (PLN/MWh, EUR/MWh) | PLN/kWh (`price_pln_per_kwh`) — ÷ 1000 in one place |

### Prices (verified 2026-10-20, PSE API `api.raporty.pse.pl/api/rce-pln`)

- RCE (PSE) is the RES-act net-billing reference price ("rynkowa cena energii"), PLN/MWh excl. VAT; it is not the
  SDAC day-ahead clearing price (PSE publishes that separately as `csdac-pln`).
- Distinct quarter-hour values since business date 2025-10-01 (SDAC 15-minute MTU go-live); earlier RCE history
  is hourly values in quarter-hour records.
- Over 2025-10-20 … 2026-10-19, 3.8–3.9 % of quarter-hours were negative (0 % Nov–Dec, 13.5 % April).
- **Correction of the brief §4 (energy-domain review 2026-10-20):** RCE is the default for prosumers who first
  generated and fed in from 2024-07-01 (RCEm remains for earlier cohorts unless they opt in) and it is
  quarter-hourly, not hourly, since 2025-10-01.
- **Net-billing facts found by the spec 008 energy review (2026-11-13)**, from the RES act as consolidated in Dz.U.
  2024 poz. 1361 and amended by Dz.U. 2024 poz. 1847 (https://api.sejm.gov.pl/eli/acts/DU/2024/1361,
  https://api.sejm.gov.pl/eli/acts/DU/2024/1847). VERIFY each against the current consolidated text, Dz.U. 2026
  poz. 68, before the settlement spec:
  - cohorts: first generation and feed-in by 2024-06-30 → monthly RCEm (art. 4b ust. 1); from 2024-07-01 → RCE
    per imbalance settlement period (ust. 2); the earlier cohort may opt in by declaration, from the next month,
    without return (ust. 2a–2b);
  - negative RCE counts as 0 for individual prosumers (ust. 2 pkt 2, subject to ust. 17–18); RCEm is built from
    RCEt floored at 0 (ust. 7); art. 4b defines no RCE correction procedure;
  - the deposit offsets only energy purchase, not distribution fees (art. 4c ust. 2), is "zwiększana o współczynnik
    1,23" (ust. 4), is usable for 12 months and then written off, oldest first (ust. 5–7);
  - refunds are capped at 20 % (ust. 1 cohort) and 30 % (ust. 2 cohort) of the month's energy value (art. 4
    ust. 11a), paid by the end of the 13th month (art. 4 ust. 11 pkt 2).

### Battery (brief §4 spike defaults; `src/ems/domain/battery.py`, spec 003)

10 kWh, 5 kW, one-way efficiency 0.95, SoC 1.0–9.5 kWh, initial 5.0 kWh; charge `soc += p·η·Δt`, discharge
`soc += p/η·Δt`, clamped by power and SoC bounds. Implemented as the pure, vectorised
`ems.domain.battery.step` (property-tested); the generator uses it only for site-controlled batteries (ADR 0014),
and the domain-core spec reuses it for the Battery actor.

### Tariff groups

G11 (one zone) and G12 (two zones; zone hours differ per DSO, e.g. Energa night 13–15 and 22–6) for households;
C11 for low voltage ≤ 40 kW **and** fuse ≤ 63 A, otherwise C21. Metering points (PPE) are 18-digit GS1 GSRN codes:
`590` + 4-digit operator number (Tauron 3224 confirmed) + 10 digits + GS1 mod-10 check digit.

## What to build

- Domain-core spec: settlement with RCE/RCEm options, action set, `Policy`, offline reference (battery model exists).

## Prerequisites

- None.

## Open questions

- VERIFY: the net-billing facts above (cohorts, negative RCE as 0, 12-month deposit, factor 1.23, refund caps)
  against the consolidated RES act Dz.U. 2026 poz. 68, which could not be parsed on 2026-11-13.
- VERIFY: typical household consumption (GUS BDL primary figure; secondary sources give a median of about
  2,000–2,500 kWh/yr, plus 5–6 MWh/yr with a heat pump).
- OPEN: battery wear model.
