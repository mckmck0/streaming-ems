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
| Price | native currency per MWh (PLN/MWh, EUR/MWh) | PLN/kWh (`price_pln_per_kwh`); ÷ 1000 only in `ems.domain.units.pln_per_mwh_to_pln_per_kwh` |

### Prices (verified 2026-10-20, PSE API `api.raporty.pse.pl/api/rce-pln`)

- RCE (PSE) is the RES-act net-billing reference price ("rynkowa cena energii"), PLN/MWh excl. VAT; it is not the
  SDAC day-ahead clearing price (PSE publishes that separately as `csdac-pln`).
- Distinct quarter-hour values since business date 2025-10-01 (SDAC 15-minute MTU go-live); earlier RCE history
  is hourly values in quarter-hour records.
- Over 2025-10-20 … 2026-10-19, 3.8–3.9 % of quarter-hours were negative (0 % Nov–Dec, 13.5 % April).
- **Correction of the brief §4 (energy-domain review 2026-10-20):** RCE is the default for prosumers who first
  generated and fed in from 2024-07-01 (RCEm remains for earlier cohorts unless they opt in) and it is
  quarter-hourly, not hourly, since 2025-10-01.

### Net-billing rules (sources checked 2026-11-13, spec 009)

Statute: the RES act (ustawa o OZE), consolidated text Dz.U. 2026 poz. 68
(https://api.sejm.gov.pl/eli/acts/DU/2026/68), amended by poz. 516, 900, 912 and 1347. Art. 4, 4b and 4c are
unchanged through 2026: poz. 516, 900 and 912 do not touch settlement; poz. 1347
(https://api.sejm.gov.pl/eli/acts/DU/2026/1347) changes storage counting from 2026-12-01 and, from 2027-01-17,
replaces the invoice information of art. 4 ust. 6 and adds art. 4 ust. 13b (on a change of seller the old seller
refunds the unused deposit, capped as in ust. 11a); nothing changes 2026 settlement. Statute quotes were read from
the enacting text (Dz.U. 2021 poz. 2376) and the amending acts (Dz.U. 2024 poz. 1847, 2026 poz. 1347); the
consolidated PDF could not be text-extracted (D14). Confidence is "confirmed" unless a
line says otherwise.

| Rule | Value | Source |
|---|---|---|
| Cohort | First generation and feed-in from 2024-07-01: RCE per settlement period. 2022-04-01 … 2024-06-30: monthly RCEm, with one irreversible switch to RCE by declaration, from the next month. First feed-in by 2022-03-31: net-metering, out of scope. Limitation: art. 4d ust. 2 also keeps net-metering for a micro-installation connected (or notified) by 2022-03-31 that first fed in later; the cohort field cannot express that, so such a site must not be configured (no catalog site is) | art. 4b ust. 1, 2, 2a–2b; art. 4d ust. 2 |
| Quantities | The hourly vector-balanced net through the meter, Eb_h = Σ grid_q over the hour; import_h = max(Eb_h, 0), export_h = max(−Eb_h, 0), each split evenly over the hour's quarter-hours. Whether balancing stays hourly while meter data are quarter-hourly is VERIFY (D16) | art. 4 ust. 2b; art. 4b ust. 11 |
| Distribution-fee basis | Every energy-dependent DSO fee (variable network, quality, OZE, cogeneration) is charged on the positive hourly Eb; there is no raw quarter-hour import basis. Ust. 4c applies the Eb⁺ basis to the fees of ust. 4a, those "których wysokość, zgodnie z taryfą operatora systemu dystrybucyjnego … zależą od ilości energii elektrycznej pobranej"; the OZE and cogeneration fees are part of the DSO tariff (Prawo energetyczne art. 45 ust. 7; PGE 7.11–7.12, Enea 7.8–7.9, Energa 9.3–9.4, Tauron 7.5–7.6) | art. 4 ust. 4a, 4c pkt 1 (Dz.U. 2021 poz. 2376 art. 1 pkt 4 lit. g); tariffs: Tauron 3.1.47, Energa 4.1.39, Enea 3.1.40, PGE 3.1.46; Dz.U. 2026 poz. 1182 § 15 ust. 6 |
| Valuation | value = max(P, 0) · export, P = RCE of the quarter-hour (quarter-hourly since 2025-10-01) or the month's RCEm | art. 4b ust. 1–2 |
| Negative prices | Floored at 0 "z zastrzeżeniem ust. 17 i 18": a prosumer who refuses the seller's clause of ust. 12 may be valued at the negative price. **Assumption:** every EMS site accepted the clause (D10) | art. 4b ust. 2 pkt 2, ust. 12, 17–18 |
| RCEm | Σ E_t · max(RCE_t, 0) / Σ E_t, E = feed-in of prosumers at DSOs with ≥ 200,000 customers. PSE OIRE publishes it on the 11th of the next month (https://www.pse.pl/oire/rcem-rynkowa-miesieczna-cena-energii-elektrycznej). A correction that moves it by more than 2 % is re-published until the end of the 12th month, and sellers re-settle (correction rule: likely) | art. 4b ust. 6–7; Prawo energetyczne art. 11y |
| Deposit tranche | 1.23 × the month's export value, for every cohort since 2025-02-01; assigned to the prosumer account in the next month; usable for 12 months from assignment, oldest first. A month's tranche never offsets that month | art. 4c ust. 4–5, 7; 1.23 from 2025-02-01 under Dz.U. 2024 poz. 1847 art. 31 |
| Offset | Only the energy cost: the seller's price with excise and VAT ("ceny … uwzględniającej obowiązkowe obciążenia podatkowe i fiskalne"). Distribution, fixed and capacity charges are always paid | art. 4c ust. 2–3 |
| Refund | At expiry, the unused part up to 20 % (RCEm) or 30 % (RCE) of the value of the energy fed in during the tranche's month, taken **before** the 1.23 factor; paid by the end of the 13th month after the month of feed-in; the rest is extinguished (cap base: likely, from the statute's wording "wartości energii elektrycznej wprowadzonej do sieci", with no seller example) | art. 4 ust. 11 pkt 2, ust. 11a; art. 4c ust. 6 |

### Purchase tariffs 2026 (sources checked 2026-11-13, spec 009)

- URE approved the 2026 tariffs of the four DSOs (Enea Operator, Energa-Operator, PGE Dystrybucja, Tauron
  Dystrybucja) and their incumbent sellers (Enea S.A., Energa Obrót, PGE Obrót, Tauron Sprzedaż) on 2025-12-17
  (https://www.ure.gov.pl/pl/urzad/informacje-ogolne/aktualnosci/13002). No household price cap applies in 2026:
  the cap of Dz.U. 2025 poz. 1302 ended on 2025-12-31.
- **Versions.** Each seller has one version for all of 2026 (URE BIP list to 2026-10-28). Each DSO has two:
  2026-01-01, and 2026-10-01, from which DSOs no longer charge the subscription fee (Dz.U. 2026 poz. 1182 § 56
  ust. 3; Enea's Oct–Dec extract is that version, not a new URE decision). Tauron's 2026-09-01 move of prosumers
  to monthly billing changes no rate. The subscription fee moves to seller tariffs once URE amends them
  (§ 56 ust. 4–5); none had been amended by 2026-11-13 (D13).
- **Components** (net; VAT 23 % on top, VAT act art. 41 ust. 1 with art. 146ef ust. 1):
  - seller energy price by zone; no seller price includes VAT or excise (Enea pkt 1.6, Energa 5.2, PGE Obrót
    1.6, Tauron Sprzedaż 1.6), and no 2026 seller tariff has a trading fee;
  - excise 5.00 PLN/MWh, charged by the seller (excise act art. 89 ust. 3). The model adds it to every imported
    kWh (likely; D15): RES act art. 4 ust. 10 says energy settled under net-billing is not a sale to a final
    buyer for excise purposes, and a seller's sample invoice charges excise on only part of the import;
  - DSO variable network fee by zone;
  - quality fee 0.0331 PLN/kWh in January and 0.0332 PLN/kWh from 2026-02-01, the same for every DSO (URE
    decision DRE.WPR.4211.1.4.2026.BTS of 2026-01-16 on PSE's tariff);
  - OZE fee 7.30 PLN/MWh (Informacja Prezesa URE 61/2025); cogeneration fee 3.00 PLN/MWh (Dz.U. 2025 poz. 1664);
  - no transition fee in 2026 (M.P. 2025 poz. 1211).
- **Fixed monthly charges:** the DSO fixed network fee (1- or 3-phase), the DSO subscription fee (by billing
  period; 0 from 2026-10-01), and the capacity fee by annual consumption in the year to the last meter reading:
  < 500 kWh 4.29; 500–1,200 kWh 10.31; > 1,200–2,800 kWh 17.18; > 2,800 kWh 24.05 PLN/month net (Informacja
  Prezesa URE 58/2025, https://www.ure.gov.pl/download/9/15790/informacja582025PUREstawki2026.pdf; band rule:
  capacity market act art. 89b ust. 3, per the DSO tariffs; qualification year in Tauron 3.1.34, Energa 4.1.29,
  Enea 3.1.30, PGE 3.1.33).
- **Billing period.** Tauron prosumers are billed monthly (new ones from 2026-01-01, all from 2026-09-01);
  Energa offers 1 or 2 months; Enea up to 12 and PGE up to 6 months (PGE 2.3.1); contracts from 2026-09-05 are capped at
  2 months (Dz.U. 2026 poz. 1182 § 39 ust. 3, § 56 ust. 8–9; confirmed by Enea's Oct–Dec extract, footnote *).
- **Zone clock.** Every DSO's 2026 tariff sets meter clocks to winter time all year: "ustawia się według czasu
  zimowego i nie zmienia się w okresie obowiązywania czasu letniego" (Tauron 2.2.9, Energa 3.2.10, Enea 2.2.12,
  PGE 2.2.11). Civil time applies only where a meter keeps zone hours across DST itself.
- **Zone hours** (on winter time, UTC+1):

  | DSO | G12 night (else day) | G12w off-peak (else peak) |
  |---|---|---|
  | Tauron (2.2.6–2.2.7) | 13–15 and 22–6, every day | weekdays 13–15 and 22–6; Saturdays, Sundays and statutory days off all day |
  | Energa (3.2.5–3.2.6) | 13–15 and 22–6, every day | as Tauron |
  | Enea (2.2.5, 2.2.7) | **assumed** 13–15 and 22–6: the tariff lets the operator place 2 h within 13–17 and 8 h within 22–7 ("Godziny zegarowe trwania stref czasowych określa Operator"); the default is the seller's product page, checked again on 2026-11-13 | weekdays 21–6 (no midday window); Saturdays, Sundays and statutory days off all day |
  | PGE (2.2.6, 2.2.8) | summer (1 Apr – 30 Sep) 15–17 and 22–6; winter 13–15 and 22–6 | weekdays with the G12 seasonal hours; Saturdays, Sundays and statutory days off all day |

- Rates per DSO and seller, with their pages, are in `config/tariffs/2026/`.

### Tariff and zone model (spec 009; `src/ems/domain/`, config in `config/tariffs/`)

- **Two calendars.** Zones are read on tariff time, UTC+1 all year (every DSO's meter clocks stay on winter
  time). Version validity, national periods, billing months and RCEm publication dates use the civil
  Europe/Warsaw calendar. Example: 2026-09-30T22:00Z is October (and the 2026-10-01 version) on the civil
  calendar but 2026-09-30 23:00 on tariff time. The 92- and 100-slot days are civil billing days only.
- **Zone scheme** = a default zone plus rules (zone, day types, optional season as a half-open month-day range,
  hour windows `[start, end)`; `end ≤ start` wraps midnight within the date). The first matching rule wins. Day
  type precedence: statutory holiday > Sunday > Saturday > weekday; holidays are listed in `national.yaml`
  (equal to `holidays.Poland(years=2026)`). Zone names: G11 `all`; G12 `day`/`night`; G12w `peak`/`offpeak`.
- **Purchase price per quarter-hour** (gross PLN/kWh; `tariff.buy_price`):
  `energy = (1 + VAT)·(seller price[zone] + excise)`, which the deposit may offset;
  `dist = (1 + VAT)·(variable network fee[zone] + quality fee + OZE fee + cogeneration fee)`, always paid.
  VAT enters once per price and nothing is rounded (invoices round to the grosz; D11).
- **Fixed charges per month** (`tariff.fixed_charges`, gross), only in the bill: the DSO fixed fee (3-phase;
  a 1-phase site fails fast), the DSO subscription fee for a 1-month period (0 from 2026-10-01; every site is
  assumed to have a remote-read DSO meter, so Energa's remote-read rate applies), and the capacity fee by band.
  The band comes from the catalog's `annual_base_load_kwh`, a proxy for the legal quantity (D7).
- **Versions** are picked by the civil month: each DSO has `-2026-01` and `-2026-10` ids, each seller one id, and
  the quality fee two periods (`quality-2026-01`, `quality-2026-02`). Years other than 2026 fail fast.
- **Scope.** EMS-managed sites in the four DSO areas with their incumbent seller, groups G11, G12 and G12w.
  C11/C21, Stoen, net-metering and 1-phase sites, and a window outside 2026, fail before settlement, naming the
  site (`ems.tariffs.site_model`). Every site is assumed to buy from its DSO's incumbent seller (the catalog has
  no seller field). Tauron Sprzedaż's tariff is applied in the Gliwice area too (D8).

### Settlement (spec 009)

- **Cohort** (`Site.net_billing: {first_feed_in, rce_from}`; `netbilling.basis_for`): first feed-in from
  2024-07-01 → RCE (`rce_from` rejected); 2022-04-01 … 2024-06-30 → RCEm, or RCE from `rce_from` (after the month
  of first feed-in); earlier → net-metering, rejected. Generated households default to first feed-in 2024-07-01
  (RCE); the test site is an RCE cohort (2024-09-01).
- **(a) Ledger, additive** (`settlement.settle_hour`): per hour, `Eb_h = Σ grid_q`, import and export split
  evenly over the quarters; energy and distribution costs on the hourly import; export value
  `Σ max(P_q, 0)·export_q` before 1.23, where P is RCE_q or the month's own RCEm (its latest publication). Each
  row depends only on its hour, so rows over [a, c) equal rows over [a, b) and [b, c) for hour-aligned borders.
  "Settlement is additive over steps" (brief) holds for this layer only.
- **(b) Billing, not additive** (`settlement.settle_month`), per civil month, in this order:
  1. totals with `math.fsum`;
  2. offset the energy cost from the tranches with assigned ≤ m ≤ assigned + 11, oldest assignment first (a
     month's own export never offsets it);
  3. expire tranches whose last usable month is m: refund `min(remaining, cap)` (booked in m), extinguish the
     rest;
  4. create m's tranche, `1.23 × value`, assigned in m+1, cap = 20 % (RCEm) or 30 % (RCE) of the value; a month
     without export creates none, and a used-up tranche leaves the account;
  5. `payable = energy − offset + distribution + fixed charges`.

  The month's RCEm affects only m's tranche, never m's payable.
- **RCEm.** `config/tariffs/rcem.yaml` keeps every PSE publication of a month with its date. Billing uses the
  latest; the signal uses the newest month known at q, where a value counts as known from the day after its
  publication date (PSE gives no time of day). A synthetic run uses a proxy for every month,
  `Σ w·max(RCE, 0) / Σ w` over that month with w = the synthetic national PV index
  (`ingest.synthetic.prices.national_pv_index`), "published" on the 11th of the next month and flagged `proxy`
  (D4). The proxy must use the RCE series the run consumes: per local day (`day_prices`), as the generator emits
  it, because a longer `price_curve` call differs in the last digits (AR burn-in). A real-data run uses PSE
  values only and fails on a missing month; the RCEm table must have contiguous months, and a value published
  inside its own month is rejected.

### Price signal and its gaps (`settlement.price_signal`)

Each quarter a policy sees `buy = energy + dist` and `sell = 1.23 · max(P, 0)`, with P = RCE_q for an RCE month
and the latest known RCEm for an RCEm month. While the deposit is not saturated, this is the right marginal
comparison (export now and import later, or store and discharge, both pay distribution on the later import). The
bill differs (D2):
- **hourly netting:** a quarter's export cancels import in the same hour, which the signal prices separately;
- **export inside an hour** is split evenly, so a net-export hour earns the hour's mean of max(RCE_q, 0); moving
  export to the hour's best quarter earns nothing extra;
- **saturation:** once 1.23 · Σ value exceeds the next 12 months' energy cost, extra export earns only the
  refund, at most 0.30 · P (RCE) or 0.20 · P (RCEm) per kWh;
- **monthly billing** stands in for the contract's billing period (D3).

### Actions and `Policy` (`src/ems/domain/actions.py`)

| Action | Command (kW, > 0 charges), before `battery.step` clamps it |
|---|---|
| HOLD | 0 |
| SELF_CONSUME | (pv − load)/Δt: charge the quarter's surplus or cover its deficit |
| CHARGE_FROM_PV | max(pv − load, 0)/Δt |
| CHARGE_FROM_GRID(level) | level · p_max, level in (0, 1] |
| DISCHARGE_TO_LOAD | −max(load − pv, 0)/Δt |
| EXPORT(level) | −level · p_max |
| SETPOINT(p_kw) | p_kw |

- Mode actions use the quarter's realised PV and load: they model the inverter's local control (D6). The decision
  sees only `Observation(t, buy_pln_per_kwh, sell_pln_per_kwh, last_pv_kwh, last_load_kwh)`: the signal for q and
  the last completed quarter's flows. SoC comes in `BatteryState`. Forecasts are added later, additively.
- `Policy.decide(state, observation) -> Action` is pure. Baselines: `NoBattery` (HOLD) and `SelfConsumption`
  (SELF_CONSUME). CHARGE_FROM_PV never imports for the battery (grid_q ≤ max(load − pv, 0)); DISCHARGE_TO_LOAD
  never exports battery energy (grid_q ≥ min(load − pv, 0)).
- CURTAIL and SHIFT_FLEX_LOAD are deferred.

### Offline reference and stepper (`reference.py`, `stepper.py`)

- `reference.run(model, policy, rcem, state, inputs)` replays one site: per quarter the signal, the observation,
  `decide`, `resolve`, `battery.step_params` and `grid_q = load − pv + p·Δt`; then the hour rows; then every month
  whose last hour is inside the window. Windows start and end on UTC hours; the state carries SoC, the last flows,
  the open month's rows and the tranches, so [a, b) then [b, c) equals [a, c) bit for bit.
- `stepper.step` does one quarter-hour: the same physics, then `settlement.settle_quarter`, which closes an hour
  on its fourth quarter and a month on its last hour and rejects an out-of-order `t`. Over ten days across the
  100-slot day and the October close (with an expiring tranche and an RCEm variant) its results equal `run`'s
  with `==`.
- Each quarter keeps the signal price the policy saw and the export price the ledger used, so an RCEm month never
  values export at the lagged price. Results record the operator, seller and quality-fee ids, the RCEm entries
  used (source and publication date) and the cohort (P14).
- Notes for the actor spec: `Tariff` stays keyed by market (ADR 0012) and serves price curves; per-site tariff
  rules come from `config/tariffs/` through `ems.tariffs`. `Meter` owns the real bill and calls only
  `settle_quarter` (`Battery` owns the real SoC, ADR 0014); each `Strategy_*` runs `stepper.step` on its virtual
  battery. A mode action resolves where q's measurement arrives, so the command to `Battery` carries the `Action`.
  State per object: the open hour (≤ 3 quarters), the open month's rows (≤ 745) and the tranches (≤ 13). For an
  RCEm site, month m's payable can close at the boundary but its tranche only once PSE publishes m's RCEm (the 11th
  of m+1); live, close on publication and ignore later corrections, while the offline reference uses the
  corrected table. Live RCEm is a market price on `Tariff` (brief §5.2), not a config edit.

### Battery (brief §4 spike defaults; `src/ems/domain/battery.py`, spec 003)

10 kWh, 5 kW, one-way efficiency 0.95, SoC 1.0–9.5 kWh, initial 5.0 kWh; charge `soc += p·η·Δt`, discharge
`soc += p/η·Δt`, clamped by power and SoC bounds. Implemented as the pure, vectorised
`ems.domain.battery.step` (property-tested); the generator uses it only for site-controlled batteries (ADR 0014),
and the domain-core spec reuses it for the Battery actor.

### Tariff groups

G11 (one zone), G12 (day/night) and G12w (peak/off-peak with weekends and holidays off-peak; hand-defined sites
only, the generator never assigns it) for households, zone hours per DSO above; C11 for low voltage ≤ 40 kW **and**
fuse ≤ 63 A, otherwise C21 (not settled). Metering points (PPE) are 18-digit GS1 GSRN codes:
`590` + 4-digit operator number (Tauron 3224 confirmed) + 10 digits + GS1 mod-10 check digit.

## What to build

- Built by spec 009: tariffs, settlement, price signal, actions, `Policy`, offline reference and stepper.
- Next: the actor skeleton calls `settle_quarter` and `stepper.step` (notes above); the experiment harness reports
  the cost metric of `11-experiments.md`.

## Prerequisites

- None.

## Open questions

- VERIFY: typical household consumption (GUS BDL primary figure; secondary sources give a median of about
  2,000–2,500 kWh/yr, plus 5–6 MWh/yr with a heat pump).
- OPEN: battery wear model.
