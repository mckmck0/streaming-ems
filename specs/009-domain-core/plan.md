# Implementation Plan: Domain core (settlement, actions, Policy, offline reference)

**Branch**: `009-domain-core` | **Date**: 2026-11-13 | **Spec**: [spec.md](spec.md) (approved 2026-11-13)

## Summary

The pure domain core grows from the battery model to everything a strategy needs to be valued like a bill:
- 2026 tariffs of four DSOs and their incumbent sellers, held as sourced config and loaded at the edge;
- a zone calendar;
- hourly net-billing settlement with the deposit;
- a per-quarter-hour price signal;
- the action set and `Policy`;
- an offline reference, checked bit for bit against an incremental stepper that actors will call.

There is no actor, no wire change and no new dependency. The catalog gains `G12w` and a `net_billing` field.

## Resolved VERIFY items (energy-domain-reviewer, 2026-11-13)

Three source reviews were run before this plan was written: the net-billing law, the DSO distribution tariffs,
and the seller tariffs with the national components. Every item checked 2026-11-13. The values, URLs and quotes
go into 12, and each tariff value goes into config with its page.

| Item | Resolution | Source | Confidence |
|---|---|---|---|
| 2026 amendments | Art. 4, 4b and 4c are unchanged through 2026. Poz. 516, 900 and 912 do not touch settlement. Poz. 1347 changes storage counting from 2026-12-01 and invoice information from 2027-01-17. Nothing changes valuation, the 1.23 factor, the windows, the caps or balancing in 2026 | ELI API (`api.sejm.gov.pl/eli/acts/DU/2026/1347`); consolidated text Dz.U. 2026 poz. 68 | confirmed; poz. 1347 art. 1 pkt 3 likely (re-read from the PDF when 12 is written) |
| Same-month offset | **No.** Month m's tranche is assigned in m+1 ("przyporządkowywana do konta prosumenckiego w kolejnym miesiącu kalendarzowym", art. 4c ust. 4) and is usable for 12 months from assignment (ust. 5) | consolidated RES act; Tauron's illustration (secondary) | confirmed for monthly billing |
| Refund-cap base | The art. 4b value **before** 1.23. The cap is "20 % / 30 % wartości energii elektrycznej wprowadzonej do sieci w miesiącu…" (art. 4 ust. 11a), and the statute uses the separate term "wartość depozytu … zwiększana o 1,23" for the deposit. The refund is paid by the end of month m+13 (art. 4 ust. 11 pkt 2), and the rest is extinguished (art. 4c ust. 6) | consolidated RES act; Dz.U. 2024 poz. 1847 art. 10 (in force 2025-02-01) | likely: the wording is clear, but there is no worked seller example. No switch is built |
| Distribution-fee basis | Every energy-dependent DSO fee (variable network, quality, OZE, cogeneration) is charged on Σ of the positive hourly Eb(t). The rule is in art. 4 ust. 4c pkt 1, and in the tariffs at Tauron 3.1.47, Energa 4.1.39, Enea 3.1.40 and Dz.U. 2026 poz. 1182 § 15 ust. 6. There is **no raw-import basis** | statute; tariffs | confirmed: art. 4 ust. 4a and 4c read verbatim in the enacting text (Dz.U. 2021 poz. 2376) during implementation; OZE and cogeneration fees are part of the DSO tariff |
| Capacity band | Set by the energy consumed in the year to the last meter reading (art. 89b ust. 3 of the capacity market act, per the DSO tariffs). Tauron re-qualifies the band after every settlement. Bands: < 500; 500–1,200 inclusive; > 1,200–2,800 inclusive; > 2,800 kWh | Informacja Prezesa URE 58/2025; Tauron | rates confirmed; prosumer quantity likely (Eb⁺) |
| Clock basis | All four DSOs default to **winter time all year** ("ustawia się według czasu zimowego i nie zmienia się w okresie obowiązywania czasu letniego"). Civil time is allowed only for a meter that keeps zone hours across DST | Tauron 2.2.9, Energa 3.2.10, Enea 2.2.12, PGE 2.2.11 | confirmed |
| Enea G12 hours | Not fixed in the tariff: "Godziny zegarowe trwania stref czasowych określa Operator" (2 h within 13–17, 8 h within 22–7). **Assumed** 13–15 and 22–6, from the seller's product page; this default was checked again against the tariff and the product page on 2026-11-13 | Enea Operator tariff 2.2.7 | default confirmed; stays a recorded assumption because the operator sets hours per customer (D12) |
| Versions | DSOs have two versions each:<br>• 2026-01-01;<br>• 2026-10-01, which drops the subscription fee (Dz.U. 2026 poz. 1182 § 56 ust. 3). Enea's Oct–Dec extract is this version, not a new URE decision. Tauron's 09-01 change is a move to monthly billing, not a version.<br>The quality fee is 0.0331 PLN/kWh in January and **0.0332** from 2026-02-01 (URE DRE.WPR.4211.1.4.2026.BTS), identical for every DSO.<br>Sellers have one version each for all of 2026 | URE BIP decisions list (to 2026-10-28); DSO notices | confirmed (PGE: its quality fee follows PSE's change under its tariff 7.10.1) |
| Excise | **Excluded** from every seller's price ("nie zawierają … podatku akcyzowego": Enea pkt 1.6, Energa 5.2, PGE 1.6, Tauron 1.6). 5.00 PLN/MWh is added (excise act art. 89 ust. 3). The deposit offsets price + excise + VAT ("ceny … uwzględniającej obowiązkowe obciążenia podatkowe i fiskalne", art. 4c ust. 3) | seller tariffs; excise act; RES act | prices exclude excise: confirmed. Excise on every imported kWh: likely, VERIFY (risk D15, found during implementation: art. 4 ust. 10) |
| Trading fee | None in any 2026 seller tariff | seller tariffs | confirmed |
| Billing period | Modelled as 1 month (decision below). Facts:<br>• Tauron prosumers are monthly (new ones from 2026-01-01, all from 09-01);<br>• Energa offers 1 or 2 months;<br>• Enea and PGE may keep longer periods;<br>• new contracts are capped at 2 months (poz. 1182 § 39 ust. 3) | tariffs; trade press quoting DSOs | likely |
| RCEm | RCEm = Σ E_t·max(RCE_t, 0) / Σ E_t, where E is the feed-in of prosumers at DSOs with ≥ 200k customers (art. 4b ust. 6). It is published by PSE OIRE on the 11th of the next month. It is re-published if a correction moves it by more than 2 %, until the end of the 12th month (Prawo energetyczne art. 11y), and sellers re-settle. 2026 values Jan–Oct: 551.96, 339.01 (corrected to 331.39 on 2026-06-11), 191.95, 132.92, 191.37, 273.20, 262.88, 294.53, 345.68, 439.78 PLN/MWh | `pse.pl/oire/rcem-rynkowa-miesieczna-cena-energii-elektrycznej` | definition and values confirmed (re-keyed by hand); correction rule likely |
| Zero floor | Negative prices are floored at 0 "z zastrzeżeniem ust. 17 i 18" (art. 4b ust. 2 pkt 2). A prosumer who refuses the seller's clause of ust. 12 may be valued at the negative price. **Assumed:** every EMS site accepted the clause | RES act | confirmed (text); assumption in 12 |
| Citations | The energy-only offset rests on art. 4c ust. 2–3. Ust. 16 covers building managers and is dropped from 12 | RES act | confirmed |
| National components | OZE fee 7.30 PLN/MWh (Informacja 61/2025); cogeneration fee 3.00 PLN/MWh (Dz.U. 2025 poz. 1664); VAT 23 %; excise 5.00 PLN/MWh; no transition fee (M.P. 2025 poz. 1211); no household price cap in 2026 (the cap of Dz.U. 2025 poz. 1302 ended 2025-12-31); URE approved all eight tariffs on 2025-12-17 | URE, ELI | confirmed |

The seller prices, and the network fees of Tauron, Energa and Enea, were transcribed with their pages. They
are entered and re-checked in the config task.

Still open, and none of these blocks a net-billing rule:
- PGE Dystrybucja's numbers were transcribed on 2026-11-13 from URE's reprint of its tariff
  (https://bip.ure.gov.pl/download/3/20345/PGEDystrybucja.pdf, pkt 7.9, p. 59), which also confirms PGE's zone
  hours (2.2.6, 2.2.8) and clock basis (2.2.11). Its billing periods are 1, 2 or 6 months (2.3.1).
- The statute quotes came from an HTML copy (INFORLEX). The final review checks them against the Dz.U. PDF,
  including art. 4 ust. 4c for the non-network fees and the capacity-band article (89a or 89b ust. 3).

## Plan decisions (spec open questions)

| Question | Decision | Why |
|---|---|---|
| First-published or corrected RCEm | `rcem.yaml` keeps every publication of a month (value, date). Billing uses the latest captured one, which is the corrected value. The signal at q uses the newest month known at q, at its latest publication known then; a publication counts as known from the day after its date (PSE gives no time of day) (changed during implementation, 2026-11-13) | Sellers re-settle on a correction, so the bill follows the corrected value. Keeping each publication with its date keeps the signal to data available at q |
| RCEm proxy weights (synthetic months) | The synthetic national PV index, the same series that shapes the synthetic RCE (`ems.ingest.synthetic.prices`, exposed as `national_pv_index`). Month m's proxy uses exactly that month's quarter-hours. A synthetic run uses the proxy for **every** month and never mixes in real PSE values; a real-data run uses PSE values only | RCEm is weighted by prosumer feed-in across the country. The site's own export would make it circular (a strategy would move its own price) and site-specific. Real RCEm over synthetic RCE would value export against prices the run never saw |
| Sell signal for RCEm sites | The newest month known at q, at its latest publication known then; a publication counts as known from the day after its date (PSE gives no time of day). Proxies count as published on the 11th of the next month. A correction of an older month therefore never reaches the signal once a newer month is known | Only data available at q (AC-009-16). Within a month the timing of export does not change an RCEm site's value |
| Fixed-charge defaults | Derived at the edge from the catalog:<br>• 3-phase only. Every generated household needs it (`contracted_kw` > 0.23 · `fuse_a`), and a 1-phase site fails fast. So only the 3-phase fixed fee is sourced;<br>• a 1-month billing period for every site. The config holds only that subscription rate. It matters Jan–Sep only, because the DSO fee is 0 from October;<br>• one global assumption: every EMS site has a remote-read DSO meter, because hourly balancing (art. 4 ust. 2b) and RCE valuation presuppose one. So Energa's remote-read rate applies;<br>• the capacity band from `annual_base_load_kwh`, constant per run. This is a proxy: the legal quantity is the annual Eb⁺, and base load ignores the heat pump, the EV and PV self-consumption (D7) | Fixed charges are identical for every strategy on a site, so they cancel in comparisons (AC-009-05). Only the absolute bill sees them. One month matches the monthly settlement. It is also Tauron's period for prosumers since 2026-09-01, and Dz.U. 2026 poz. 1182 § 39 ust. 3 caps new contracts at 2 months |
| Deposit left at the window's end (cost metric) | 11 reports three numbers:<br>• `bill_pln`: charges minus refunds, the primary cost. A refund is booked in its tranche's expiry month and paid in the following month;<br>• `deposit_closing_pln`, beside it and never netted;<br>• `ledger_net_cost_pln`: additive, energy and distribution cost minus 1.23 × export value. It values export as if the deposit is always used, which is the same unsaturated valuation as the signal.<br>Bill metrics use windows of whole months | The bill is what a household pays. Valuing an unused deposit needs an assumption about the future, so it stays visible instead of being netted. The ledger cost is the additive, per-interval quantity for paired statistics |
| Dynamic tariff (US5, AC-009-21) | **Dropped** from this spec; it can return later as one more tariff kind in config | It needs about four tasks, not two: a sourced seller margin, a PL:SDAC series in the reference inputs, a catalog group and the golden test. No catalog site uses it, and G12 and G12w already give strategies a time-varying purchase price |
| Config layout and loader | `config/tariffs/2026/{national,enea,energa,pge,tauron}.yaml` (each DSO file holds the operator and its incumbent seller) and `config/tariffs/rcem.yaml`. The loader is a new edge package `src/ems/tariffs/` | One file per source pair keeps each value next to its document. The edge package is reusable by the `Meter` and `Strategy_*` actors later |
| Observation fields | `t`, the signal for q (`buy_pln_per_kwh`, `sell_pln_per_kwh`), and the PV and load energy of the last completed quarter. SoC is battery state and comes in `state` | Only data available at q. Forecasts and known future prices are added later, additively |
| Generator default cohort | A constant in the catalog builder, not a YAML key: first feed-in 2024-07-01, no `rce_from` (RCE) | No experiment varies it yet (keep it simple). It is a one-line change if one does |

### Spec amendments (part of this plan's approval)

1. **`SELF_CONSUME` joins the action set** (AC-009-15, -16). It resolves to the quarter's PV surplus if positive,
   else to minus its deficit. `SelfConsumption` returns it every quarter.
   - The decision is made at the start of q, but only the quarter's realised flows tell whether there is a
     surplus. Without this mode, the baseline would have to guess from the last quarter. It would then miss the
     morning and evening transitions, which makes it weaker than the inverter mode it stands for.
   - `CHARGE_FROM_PV` and `DISCHARGE_TO_LOAD` stay as specified (charge only, discharge only).
2. **US5 and AC-009-21 are dropped** (above).
3. **"generator-config default" in the spec's Scope** becomes "a default in the catalog builder" (above).
4. **The source reviews correct the spec** (table above):
   - **Models:**
     - excise is added to the seller price;
     - the quality fee is 0.0331 PLN/kWh in January and 0.0332 from February;
     - there is no trading fee;
     - the offset is cited as art. 4c ust. 2–3;
     - the zero floor assumes the clause of art. 4b ust. 12 was accepted.
   - **Scope** (versions): DSOs change on 2026-01-01 and 2026-10-01; sellers have one version each.
   - **AC-009-03**:
     - zone hours are read on winter time (UTC+1) for every DSO, and no other clock basis is built;
     - PGE G12w is seasonal on weekdays;
     - the "under both clock bases" cases go.
   - **AC-009-04**: excise is always added.
   - **AC-009-10**: no raw quarter-hour import. Distribution fees use the hourly import.
   - **AC-009-12**:
     - the cap base is the value before 1.23, with no switch;
     - a tranche is never used in the month that creates it;
     - "trading" is dropped from the charges paid in full.
   - **Review record and Scope**: "excise is inside the seller's price" and "UTC+1 (PGE, Enea)" are corrected.
5. **Wording fixes from the two plan reviews:**
   - **AC-009-05**: "differ by exactly" becomes "the non-fixed components are identical, and the payables differ
     by the fixed-charge difference within 1e-9". `payable` is an `fsum` of floats.
   - **AC-009-08**: "a synthetic month" means any month of a synthetic run, which always uses the proxy.
   - **AC-009-13**: the "month by month equals one call" check is covered by AC-009-18 with a border on a month
     end, because both are the same fold.
   - **AC-009-15**: the two properties become bounds:
     - CHARGE_FROM_PV keeps grid_q ≤ max(load − pv, 0): it never imports for the battery;
     - DISCHARGE_TO_LOAD keeps grid_q ≥ min(load − pv, 0): it never exports battery energy.

     As first written they were false: charging cuts export, which raises grid_q.
   - **SC-002**: the VERIFY items are resolved in this plan before approval. Writing them into 12 is the first
     task, before any net-billing code (`.claude/rules/domain.md`).
   - **Edge cases**:
     - The 92- and 100-slot days are civil billing days only. A UTC+1 zone calendar never sees them.
     - January 2026's tranche is usable February 2026 – January 2027 and expires at the January 2027 settlement.
       Within 2026, expiry and the cap are exercised only by opening tranches from 2025.
     - New cases:
       - the hour 2026-09-30T22:00Z, which is October and the `-10` version on the civil calendar, but
         2026-09-30 23:00 on UTC+1;
       - a net-export hour with negative quarters;
       - an RCEm correction;
       - an `rce_from` switch inside the window.

On approval of this plan, `spec.md` gets these amendments and a dated note under its review record, before
`tasks.md` is written.

## Principles Check (PASS)

| # | Touched? | How it is satisfied | Shown by |
|---|---|---|---|
| P1 | yes | Spec approved 2026-11-13. This plan and the tasks each get their own approval. Every net-billing rule is resolved above before tasks are written (`.claude/rules/domain.md`) | review |
| P2 | yes | Everything in `src/ems/domain/` is a pure function of its arguments. The YAML loader, catalog resolution and generator inputs sit in `src/ems/tariffs/` and in tests. One accepted input: the civil calendar reads the tz database through `zoneinfo`. That comes from the system TZPATH first, then the locked `tzdata`, and is the same for Europe/Warsaw in 2026. The 92/100-slot tests guard it | AC-009-23 import allow-list test; hook |
| P3 | yes (record only) | No actor and no owner. Notes for the actor spec are under "Actor contract" | review |
| P4, P5, P7, P13 | no | No handler, timer, ingress or tracing | — |
| P6 | yes | The batch reference and the incremental stepper (quarter physics plus `settle_quarter`) are two code paths over the same row and month functions. AC-009-20 compares them with `==`, including the deposit and an RCEm cohort | AC-009-18, -20 tests |
| P8 | yes | Every action resolves to a command that `battery.step` clamps. No action bypasses it | AC-009-15 property tests |
| P9 | yes (guard) | No schema, topic or key change. Catalog, tariff and RCEm config are not exported schemas | AC-009-22 tests, `make ci` |
| P10 | yes | The reference is a library function. Its CLI comes with the experiment harness | review |
| P11 | yes | A missing tariff, cohort, source or RCEm value fails at load or resolution and names the file, field or site. No secret is involved | AC-009-01, -02 tests |
| P12 | yes | No new dependency. The domain uses only the standard library, numpy (already used by `battery.py`) and `ems.contracts.time`. Holidays come from config, not from the `holidays` package (which tests use as a cross-check) | `make lint` (`uv lock --check`) |
| P14 | yes | Reference results carry the tariff version ids, the RCEm entries used (source and publication date) and the cohort | AC-009-20 test |
| P15 | yes | 00, 08, 09, 11, 12 and 13 change with the code (section "Docs and ADRs"). No ADR | review |
| P16 | no | No experiment runs. 11 defines the cost metric | — |

## Contract changes (`08-contracts.md`)

None.
- Topics, keys, payload schemas, `schema_version` and identity are unchanged. `make ci` keeps checking the schema
  export and the 1.0.0 baseline (AC-009-22).
- The catalog is config. Its parquet export gains a `net_billing` column, which no consumer reads yet.
- `ems.contracts.units.price_per_mwh_to_per_kwh` is removed. It has no caller, and the one PLN/MWh → PLN/kWh
  conversion moves to the domain (AC-009-23). This matches `.claude/rules/ingest.md` ("the domain divides
  by 1000").

## Design

### 1. Layout

```
src/ems/domain/
  units.py        DT_H = 0.25, QUARTERS_PER_HOUR = 4, pln_per_mwh_to_pln_per_kwh (the only ÷ 1000)
  calendar.py     tariff time (UTC+1), civil date and month, day types, seasons, zone schemes, zone_at
  tariff.py       distribution, seller and national versions; buy_price; fixed_charges
  netbilling.py   cohort and price basis, export price, RCEm entries and proxy, net-billing rules
  settlement.py   HourRow + settle_hour (ledger), Tranche + MonthSettlement + settle_month, price_signal,
                  settle_quarter (incremental: open hour and month in, row and settlement out)
  actions.py      Action, resolve, Observation, BatteryState, Policy, NoBattery, SelfConsumption
  reference.py    SiteModel, QuarterIn, QuarterOut, RefState, run (batch layers)
  stepper.py      step (incremental: one quarter-hour, carried state), as actors will call it
src/ems/tariffs/  edge: YAML models and loader, RCEm table, catalog site → SiteModel
src/ems/ingest/synthetic/prices.py   national_pv_index factored out of price_curve (same values; the event
                  digests guard it)
config/tariffs/   2026/{national,enea,energa,pge,tauron}.yaml, rcem.yaml
```

All domain values are frozen dataclasses with explicit units in field names. Collections are tuples or frozen
mappings, so nothing mutable is shared and neither `set` order nor `hash()` reaches a result. Settlement
arithmetic is on Python floats. Every sum longer than two terms uses `math.fsum`.

### 2. Time and zones (`calendar.py`; AC-009-03)

- `t` is the UTC quarter-hour index (`ems.contracts.time`). The hour is `h = t // 4`; UTC hours coincide with
  Warsaw hours because the offsets are whole hours.
- The billing month of h is the Europe/Warsaw civil month of its start, `"YYYY-MM"`. A month ends at a local
  midnight, which is always on an hour boundary.
- **Two calendars.**
  - **Tariff time** is `t` plus one hour, all year. Every DSO's meter clocks run on winter time (table below).
    Zone date, hour, day type and season are read on it.
  - **The civil calendar** (Europe/Warsaw through `zoneinfo`; P2 records the tz database as an accepted input)
    gives everything legal dates decide: version validity, national-component periods, billing months and the
    RCEm publication date.
  - Example: 2026-09-30T22:00Z is October, and so the `-10` version, on the civil calendar. On UTC+1 it is
    2026-09-30 23:00, a Wednesday in the night zone. A test pins this hour.

  No civil zone basis is built, because no tariff uses one (D12).
- **Day type** for a date has the precedence holiday > Sunday > Saturday > weekday. Holidays are the statutory
  days off, listed in `national.yaml` with their source (Dz.U. act on days off work, including 24 December
  from 2025). A test compares that list with `holidays.Poland(years=2026)`.
- **Zone scheme** = `(default_zone, rules)`. Each rule has a zone, the day types it applies to, an
  optional season (a half-open month-day range, such as summer `04-01 … 10-01`) and hour windows `[start, end)`.
  A window with `end ≤ start` wraps midnight within the same date: `[22, 6)` means hours 22–23 and 0–5 of
  that date.
- `zone_at(t, scheme, holidays)`: the first matching rule wins, otherwise the default zone. Zone edges are whole
  hours, so the four quarters of an hour share a zone.
- Zone tables. Every DSO's 2026 tariff sets meter clocks to winter time all year: "ustawia się według czasu
  zimowego i nie zmienia się w okresie obowiązywania czasu letniego". G11 is one zone, `all`. G12 zones are
  `day` and `night`, and G12w zones are `peak` and `offpeak`. Each seller's prices map onto these names.

  | DSO | G12 night | G12w off-peak (otherwise peak) |
  |---|---|---|
  | Tauron (pkt 2.2.6–2.2.7) | 13–15 and 22–6, every day | weekdays 13–15 and 22–6; Saturdays, Sundays and holidays all day |
  | Energa (3.2.5–3.2.6) | 13–15 and 22–6, every day | as Tauron |
  | Enea (2.2.5, 2.2.7) | **assumed** 13–15 and 22–6: the tariff only lets the operator place 2 h within 13–17 and 8 h within 22–7. The default comes from the seller's product page (checked again on 2026-11-13) and is marked as an assumption in config and 12 | weekdays 21–6 (peak 6–21, no midday window); Saturdays, Sundays and holidays all day |
  | PGE | summer (1 Apr – 30 Sep) 15–17 and 22–6; winter 13–15 and 22–6 | weekdays with the G12 seasonal hours; Saturdays, Sundays and holidays all day |

  PGE's hours and clock basis are confirmed from its tariff (2.2.6, 2.2.8, 2.2.11). Enea's G12 default needs no
  further check.

### 3. Tariff config and the edge loader (`src/ems/tariffs/`, `config/tariffs/`; AC-009-01, -02)

`national.yaml` holds:
- VAT, excise, the OZE and cogeneration fees and the capacity-fee bands;
- the quality fee: one rate for every DSO, set in each DSO's tariff from the PSE tariff, so it is held once with
  two validity periods and a reference to each DSO's announcement;
- the statutory holidays;
- the net-billing rules: deposit factor 1.23, refund caps 20 % (RCEm) and 30 % (RCE) of the value before
  1.23, and validity of 12 months from assignment in m+1.

Each value has a validity period and a `ref`.

Each DSO file holds:

```yaml
operator:
  name: Enea Operator
  versions:
    - id: enea-op-2026-01            # unique, recorded in results (P14)
      valid_from: 2026-01-01         # civil date, half-open; must be the 1st of a month
      source: {document: "...", url: "...", checked: 2026-11-xx}
      zones: {G11: {...}, G12: {...}, G12w: {...}}         # zone schemes (section 2)
      groups:
        G12:
          variable_fee: {day: {value: 0.0, unit: PLN/kWh, ref: "Tab. 3, p. 14"}, night: {...}}
          fixed_fee: {value: 14.56, unit: PLN/month, ref: "7.2, p. 6 (3-phase)"}
          subscription_fee: {value: 3.84, unit: PLN/month, ref: "7.3, p. 7 (1-month period)"}
    - id: enea-op-2026-10            # Oct–Dec: same rates, subscription fee 0 (Dz.U. 2026 poz. 1182 § 56 ust. 3)
      ...
seller:
  name: Enea S.A.
  versions:
    - id: enea-sa-2026                # one version for all of 2026
      valid_from: 2026-01-01
      source: {document: "Taryfa ... DRE.WRE.4211.51.8.2025.TG", url: "https://bip.ure.gov.pl/...", checked: ...}
      groups:
        G12: {energy: {day: {value: 0.5779, unit: PLN/kWh, ref: "pkt 5, p. 9/7"}, night: {...}}}
```

- Pydantic models with `extra="forbid"` and `frozen=True`, read with `ruamel.yaml` (safe), as the other loaders
  do.
- Loading fails, naming the file and the field path, when:
  - a value has no `ref`, or a version has no `source`;
  - a component or zone name is unknown, or a seller zone does not exist in the operator's scheme for that
    group;
  - versions overlap or leave a gap within 2026, or a `valid_from` is not the first of a month;
  - a unit is not the one the component expects.
- The loader converts every PLN/MWh value through `ems.domain.units.pln_per_mwh_to_pln_per_kwh`, then builds the
  domain objects (`TariffBook`).
- `rcem.yaml` lists every publication of each month: value (PLN/MWh), publication date, URL and the date checked
  (decision above). It starts at October 2025, so the signal has a published value from January 2026.
- `rcem_table(mode, months, published=None, rce=None, weights=None)`:
  - `synthetic`: one proxy entry per month, computed over exactly that month's quarter-hours of the run's input
    series and "published" on the 11th of the next month. The caller generates full months for it;
  - `real`: the PSE entries only. A missing month raises (AC-009-08).
- Opening tranches are validated here: an opening tranche whose last usable month is before the window's first
  month is rejected.
- `site_model(site, book, rules) -> SiteModel` resolves:
  - the operator from `site.dso` and its incumbent seller;
  - the group;
  - the cohort, from `site.net_billing`;
  - the fixed-charge inputs (decision above);
  - `BatteryParams`, from the site's BESS.

  It fails before any settlement and names the site (AC-009-02) when:
  - the site is not EMS-managed;
  - its group is C11 or C21;
  - it would be 1-phase (`contracted_kw` ≤ 0.23 · `fuse_a`);
  - its DSO has no file (Stoen, for example);
  - `net_billing` is missing or out of scope.

  It never falls back to another tariff.

### 4. Purchase price and fixed charges (`tariff.py`; AC-009-04 … -06)

```python
buy_price(tariff: SiteTariff, t: int) -> BuyPrice   # PLN/kWh gross: energy, dist; version ids
  energy = (1 + vat) * (seller_energy[zone] + excise)
  dist   = (1 + vat) * (variable_fee[zone] + quality_fee + oze_fee + cogeneration_fee)
fixed_charges(tariff, month, inputs) -> FixedCharges   # PLN gross per month, per component
  network = fixed_fee (3-phase); subscription = subscription_fee (1-month period);
  capacity = band rate                                            (each × (1 + vat))
```

- No seller price includes excise, and every seller charges it on top: "ceny energii elektrycznej nie zawierają
  podatku od towarów i usług (VAT) oraz podatku akcyzowego" appears in each seller's tariff, pkt 1.6 or 5.2. So
  excise is always added, and the tariff has no `excise_included` switch.
- No 2026 seller tariff has a trading fee, so the fixed charges have no trading component.
- VAT enters once, on the sum of a price's net components. VAT never touches export values or tranches, because
  RCE and RCEm are net prices.
- Invoices apply VAT to the sum of net amounts and round to the grosz. The domain applies (1 + VAT) per price and
  never rounds. These agree up to the invoice's rounding, which is not modelled (risk D11).
- Capacity bands use the annual consumption to the last meter reading: < 500 kWh; 500–1,200 kWh inclusive;
  more than 1,200 up to 2,800 kWh inclusive; more than 2,800 kWh (art. 89b ust. 3 of the capacity market act, per the DSO tariffs).
- `buy_price(t)` picks the versions and national periods for the civil month of t, never for the UTC+1 date
  (section 2). Every version starts on the 1st of a month, and the loader guarantees it.
- `BuyPrice` carries the operator, seller and quality-fee period ids, so results can record them (P14).
- `buy_price` raises when no version covers t: that is a defensive check, since the edge already refuses years
  other than 2026.

### 5. Net-billing (`netbilling.py`; AC-009-07 … -09)

- `Cohort(first_feed_in: date, rce_from: str | None)` validates itself:
  - `first_feed_in` ≤ 2022-03-31 is rejected (net-metering);
  - `rce_from` with `first_feed_in` ≥ 2024-07-01 is rejected;
  - an `rce_from` that is not after the month of `first_feed_in` is rejected. The switch applies from the month
    after the declaration (art. 4b ust. 2a).
- `basis_for(cohort, month)` returns `RCE` or `RCEM` from the cohort table.
- `export_price(basis, rce_q, rcem_entry)` gives the price P; the value is `max(P, 0) · export_q`. The floor
  applies to every quarter-hour, so a negative slot adds exactly 0.0.
- `rcem_proxy(rce, weights) = fsum(w·max(rce, 0)) / fsum(w)`. It raises when `fsum(w) == 0`.
- `RcemTable.signal(day)` gives the newest month known on the civil date `day` at its latest publication known
  then (a publication is known from the day after its date), and raises when there is none.
- `NetBillingRules` holds:
  - the deposit factor 1.23;
  - the cap percentages by basis (RCEm 0.20, RCE 0.30);
  - the validity of 12 months.

  The cap base (before 1.23) and the rule that a tranche is first usable in m+1 are fixed by statute. They are
  code, not fields.
- `RcemEntry(month, publications: tuple[(value_pln_per_kwh, published: date), ...], source: "pse" | "proxy")`.
  Billing uses the last publication; the signal uses `RcemTable.signal`.

### 6. Ledger (a) (`settlement.py`; AC-009-10, -11)

```python
settle_hour(h, quarters: 4 × (grid_kwh, BuyPrice, export_price_pln_per_kwh)) -> HourRow
  eb = fsum(grid);  import_h = max(eb, 0);  export_h = max(-eb, 0)
  import_q = import_h / 4;  export_q = export_h / 4
  energy_cost_pln  = fsum(import_q * buy.energy)
  dist_cost_pln    = fsum(import_q * buy.dist)              # art. 4 ust. 4c: hourly Eb⁺ for every DSO fee
  export_value_pln = fsum(max(P, 0) * export_q)             # before 1.23
HourRow(h, month, import_kwh, export_kwh, energy_cost_pln, dist_cost_pln, export_value_pln)
hour_rows(quarters) -> tuple[HourRow, ...]   # rejects a window that does not start and end on a UTC hour
```

- Each row depends only on its own hour, so the ledger is additive by construction. AC-009-11 checks it on
  arbitrary hour-aligned splits.
- For an RCEm site, the row's export price is the month's own RCEm: its last publication in the input table. That
  is not the lagged value the signal uses (section 8).
  - The offline reference has that table.
  - Month m's payable does not depend on m's RCEm. Only the tranche created from m's export does, and that
    tranche is first used in m+1.
  - A live `Meter` can therefore close m's bill at the month boundary and settle the month once PSE publishes,
    on the 11th of m+1, which is before m+1 closes. This is recorded for the actor spec. The domain functions are
    the same.

### 7. Billing settlement (b) (`settlement.py`; AC-009-12, -13)

```python
settle_month(month, rows, fixed: FixedCharges, basis, rules, opening: tuple[Tranche, ...])
    -> MonthSettlement
Tranche(created: month, assigned: month, amount_pln, remaining_pln, cap_pln, basis)
```

Steps, in this fixed order:
1. Totals with `fsum`: energy cost, distribution cost, export value, import and export kWh.
2. Offset the gross energy cost (price, excise and VAT) from the opening tranches with
   assigned ≤ m ≤ assigned + 11, oldest assignment first. Month m's own export never offsets month m.
3. Expire every tranche with assigned + 11 ≤ m. The rule is ≤ rather than = so that no tranche can outlive its
   window. Refund `min(remaining, cap)` and extinguish the rest. The refund is booked in m and paid by the end of
   m+1, which is the statute's m+13 counted from creation.
4. Create month m's tranche: `amount = 1.23 · value`, assigned in m+1, `cap = pct(basis) · value` (before 1.23).
5. `payable = energy − offset + dist + Σ fixed`.

`MonthSettlement` holds the per-component charges, the offset per tranche, the tranche created, the expired,
refunded and extinguished amounts, `payable_pln`, the closing tranches and the version ids. It is a pure
function of its arguments. Settling month by month with the carried tranches is the same code as settling a
window (AC-009-13). 12 states that this layer is not additive.

Opening tranches whose last usable month is before the window's first month are rejected at the edge.

### 8. Price signal (`settlement.py`; AC-009-14)

`price_signal(buy: BuyPrice, p_pln_per_kwh) -> Signal(buy_pln_per_kwh = buy.energy + buy.dist,
sell_pln_per_kwh = 1.23 · max(P, 0))`.
- P is RCE_q for an RCE site. For an RCEm site it is the latest published RCEm (decision above).
- `1.23 · P` against `energy + dist` is the right marginal comparison while the deposit is not saturated.
  Export-now-import-later and store-and-discharge both pay distribution on the later import.
- 12 documents the gaps between the signal and the bill:
  - **Hourly netting.** A quarter-hour's export can cancel import in the same hour, which the signal prices
    separately.
  - **Export inside an hour.** It is split evenly over the hour's quarters, so a net-export hour earns the hour's
    mean of max(RCE_q, 0). Moving export to the hour's best quarter earns nothing extra.
  - **Saturation.** Once 1.23 · Σ value exceeds the next 12 months' gross energy cost, the extra export earns only
    the refund cap: 0.30 · P (RCE) or 0.20 · P (RCEm).
  - **Monthly billing** stands in for the contract's period.

  `ledger_net_cost_pln` uses the same unsaturated valuation as the signal.

### 9. Actions and Policy (`actions.py`; AC-009-15, -16)

```python
class ActionKind(StrEnum): HOLD, SELF_CONSUME, CHARGE_FROM_PV, CHARGE_FROM_GRID, DISCHARGE_TO_LOAD,
                          EXPORT, SETPOINT
@dataclass(frozen=True) class Action: kind; level: float | None = None; p_kw: float | None = None
resolve(action, pv_kwh, load_kwh, p_max_kw, dt_h=DT_H) -> p_cmd_kw
  HOLD 0 | SELF_CONSUME (pv − load)/dt | CHARGE_FROM_PV max(pv − load, 0)/dt
  CHARGE_FROM_GRID level·p_max | DISCHARGE_TO_LOAD −max(load − pv, 0)/dt | EXPORT −level·p_max | SETPOINT p_kw
```

- `Action.__post_init__` rejects:
  - a `level` outside (0, 1], or one given to a kind that takes none;
  - a missing or non-finite `p_kw` for `SETPOINT`.
- `BatteryState(soc_kwh)`.
- `Observation(t, buy_pln_per_kwh, sell_pln_per_kwh, last_pv_kwh, last_load_kwh)`. Before the first quarter of a
  run, the last flows are 0.0.
- `Policy` is a `Protocol` with `decide(state: BatteryState, obs: Observation) -> Action`. Policies are frozen
  dataclasses and keep no internal state in this spec. A stateful policy needs its own spec.
- `NoBattery` returns HOLD; `SelfConsumption` returns SELF_CONSUME.
- Mode actions use the quarter's realised flows, which model the inverter's local control (documented in 12).
  The decision itself sees only the observation.

### 10. Offline reference and stepper (`reference.py`, `stepper.py`; AC-009-17 … -20)

Inputs:
- `SiteModel(tariff, cohort, fixed_inputs, battery, rules)`;
- `QuarterIn(t, pv_kwh, load_kwh, rce_pln_per_kwh)`;
- the RCEm table;
- `RefState(soc_kwh, last_pv_kwh, last_load_kwh, acc)`, where the settlement accumulator is
  `SettleAcc(t, hour: tuple[SettleIn, ...], month_rows: tuple[HourRow, ...], tranches)`, with
  `SettleIn(t, grid_kwh, buy, export_price_pln_per_kwh)`.

For each q, the physics is the same in both paths:
1. Build the signal and the observation.
2. `policy.decide`.
3. `resolve`.
4. `battery.step_params`.
5. Compute `grid_q = load − pv + p·DT_H`.

The result is `QuarterOut(t, action, p_cmd_kw, p_kw, soc_kwh, grid_kwh, buy, sell_signal_pln_per_kwh,
export_price_pln_per_kwh)`.
- The sell signal is what the policy saw. For an RCEm site that is the lagged published value.
- The export price is what the ledger uses: RCE_q, or the month's own RCEm.
- The two are separate fields, so the ledger can never value export at the lagged price.

The two paths group the work differently:
- **`reference.run(model, policy, rcem, state, inputs)`** (batch):
  - It runs the quarter loop over the window.
  - It then calls `hour_rows(quarters)`.
  - It then groups rows by civil month and calls `settle_month` for every month whose last hour lies in the
    window.
  - Borders must be on UTC hours. The open month's rows and the tranches come back in the state.
- **`settlement.settle_quarter(acc, s: SettleIn, fixed, basis, rules)`** (incremental settlement only):
  - It rejects `s.t != acc.t`. Grouping by count would silently shift hours after a missing record.
  - It appends the quarter to the open hour.
  - When `t % 4 == 3`, it calls `settle_hour` and appends the row to the open month.
  - In the same quarter, when the civil month of hour h+1 differs from that of h, it calls `settle_month` and
    resets the month. A window that ends on a month border is therefore settled in both paths.
  - It returns `(acc′, HourRow | None, MonthSettlement | None)`.

  The live `Meter` calls exactly this, never the policy or the physics: `Battery` is the single writer of real
  SoC (ADR 0014).
- **`stepper.step(model, policy, rcem, state, q_in)`** = the quarter physics followed by `settle_quarter`. It is
  what a `Strategy_*` runs on its virtual battery.

Results record the version ids (operator, seller and quality-fee period), the RCEm entries used and the cohort
(P14).

AC-009-20 runs the test site PL-LOC-000001 (Enea G12) with seed 4242 over the local days 2026-10-24 …
2026-11-02 (ten days, 964 quarter-hours):
- **Calendar.** The window covers the civil 100-slot day 2026-10-25 and the October close.
- **Inputs.** They come from `SyntheticModel.chunk` and `price_curve` in-process, without Kafka. September and
  October are generated whole for the proxies.
- **Opening tranches.** There are two:
  - one assigned in 2025-11, which is used first and expires at October's close with a capped refund;
  - one assigned in 2026-10, which is used after it.

  So offset, expiry, refund and carry-over all go through the stepper's month change.
- **Variants.** Two sites run: the RCE site as configured, and the same site as an RCEm cohort (first feed-in
  2023-05-01, proxies). The RCEm variant makes the signal price and the ledger price differ.
- **Check.** `run` is compared with a loop of `step` using `==` on every quarter result, ledger row, settlement
  and the final state, for NoBattery and SelfConsumption.

### 11. Catalog (`ingest/catalog/`, `config/sites.yaml`; AC-009-09)

- `TariffGroup.G12W = "G12w"`, with profile H25. The generator never assigns it; hand-defined sites may.
- `NetBilling(first_feed_in: date, rce_from: str | None)` is a pydantic model (pattern `^\d{4}-\d{2}$`), and the
  site gets `Site.net_billing: NetBilling | None = None`.
- `generate_household` sets `DEFAULT_NET_BILLING` (2024-07-01, null) on **every** generated household, without a
  condition and without a new random draw.
  - Every other value stays the same.
  - The field does not follow `site_controlled_bess_share`, so AC-003-03 ("changing the share changes nothing
    else", `tests/property/test_fleet.py`) still holds.
  - Plants get None.
- The test site in `config/sites.yaml` gets `net_billing: {first_feed_in: 2024-09-01, rce_from: null}`.
- Proof that existing values are unchanged:
  - A household catalog digest (100 generated households, with `net_billing` dropped) is captured on this branch
    **before** any catalog change, then pinned.
  - `PLANT_CATALOG_DIGEST` stays pinned, with `net_billing` dropped.
  - Separate assertions check that every household has the default and every plant has None.

## Actor contract

Not applicable: no actor, handler, message, timer or state owner changes. Recorded for the actor-skeleton spec
(12 and 00):
- `Tariff` stays keyed by market (ADR 0012) and serves price curves through a shared read. Per-site tariff rules
  come from `src/ems/tariffs/` config through the domain functions; no actor holds them.
- `Meter` owns the real bill and calls `settle_quarter` only. Each `Strategy_*` owns its shadow settlement and
  calls `stepper.step` on its virtual battery.
- A mode action resolves with the quarter's realised flows. So the command sent to `Battery` carries the
  `Action`, and resolution happens where q's measurement arrives.
- State per object: the open hour (at most 3 quarters), the open month's rows (at most 745) and the tranches (at
  most 13). The exact size is checked against Restate's state limits in that spec.
- **RCEm sites.**
  - Month m's payable closes at the month boundary. Its tranche, and so the settlement, can close only once PSE
    publishes m's RCEm (around the 11th of m+1).
  - Corrections can reopen months for up to 12 months and cascade through the tranches.
  - Choice to record: close on publication and ignore later corrections live; the offline reference uses the
    corrected table.
  - Live RCEm is a market price on `Tariff` (brief §5.2), not a config edit.
- The 00 entry names the brief §5.2 row it changes: the per-site tariff rules move out of `Tariff`.

## Risks (`09-risks-and-tech-debt.md`)

| Id | Change |
|---|---|
| D1 | Resolved: RCE by first feed-in from 2024-07-01, quarter-hourly since 2025-10-01; rules in 12 with sources |
| D2 (new) | The strategy signal is per quarter-hour and assumes the deposit is not saturated. The bill does three things the signal does not:<br>• it nets hourly;<br>• it values export at the hour's mean price;<br>• past saturation it pays only the refund cap.<br>Accepted: the gaps are documented in 12, and both the bill and the ledger cost are reported |
| D3 (new) | Monthly billing stands in for the contract's billing period. Some older contracts keep longer periods (Enea up to 12 months, PGE up to 6) until 2028 (Dz.U. 2026 poz. 1182 § 56 ust. 8). Accepted |
| D4 (new) | Synthetic months use an RCEm proxy weighted by the synthetic national PV index; results flag it. Accepted until the historical backfill supplies real months |
| D5 (new) | One aggregate flow stands in for the vector sum over phases. Accepted (meters report the balanced value) |
| D6 (new) | Mode actions use the quarter's realised flows (inverter local control). Accepted and documented |
| D7 (new) | The capacity band and the billing period are constant per run. The band comes from base load, a proxy for the legal quantity, annual Eb⁺; base load ignores the heat pump, the EV and self-consumption. DSOs re-qualify the band after every settlement (Tauron). Accepted: fixed charges cancel between strategies |
| D8 (new) | DSOs are assigned by voivodeship. Stoen and the Gliwice-area exemption are ignored, and other sellers are out of scope. Accepted |
| D9 (new) | Tariffs are re-sourced every year, with several versions a year; only 2026 exists. Years other than 2026 fail fast |
| D10 (new) | The negative-price floor assumes every EMS site accepted the seller's clause of art. 4b ust. 12. Without it, ust. 17–18 allow valuation at the negative price. Accepted and stated in 12 |
| D11 (new) | Invoices apply VAT to the sum of net amounts and round to the grosz; the domain applies (1 + VAT) per price and never rounds. Accepted: differences are below one grosz per invoice line |
| D12 (new) | Zone hours run on winter time (UTC+1) for every DSO, but a meter that keeps zone hours across DST may use civil time. That is a per-meter exception, and no site override is built. Enea G12 hours are an assumption: the operator sets them per customer. Every EMS site is assumed to have a remote-read DSO meter, so Tauron's 2.2.11 exception (an old holiday list on meters that are not remote-read) does not apply. Accepted |
| D13 (new) | The subscription fee moved from DSOs to sellers on 2026-10-01 (Dz.U. 2026 poz. 1182 § 56 ust. 3–5). No amended seller tariff had appeared by 2026-11-13, so Oct–Dec carry no subscription fee. A later seller amendment becomes a new seller version |
| D14 (new) | Art. 1 pkt 3 of Dz.U. 2026 poz. 1347 (invoice information, from 2027-01-17) is known only from secondary reading. It is re-read from the PDF when 12 is written. Nothing in 2026 depends on it. (Implementation: read; it also adds art. 4 ust. 13b from 2027-01-17. D14 now tracks the unread consolidated text) |
| D15 (implementation) | Excise on deposit-settled imports (RES act art. 4 ust. 10). VERIFY; at most 6.15 PLN/MWh gross |
| D16 (implementation) | Hourly vs quarter-hourly balancing (art. 41 ust. 21). VERIFY; trade press says hourly in 2026 |

## Experiment impact

No experiment result exists yet, so no reported number changes. 11 gains the cost-metric definitions, and the
first experiment harness computes them from the reference and actor state.

## Test strategy

Unit and property tiers, written first by `test-engineer`. There is no network (AC-002-14). Fixtures for broken
config live under `tests/fixtures/tariffs/`.

| File | Covers |
|---|---|
| `tests/unit/tariffs/test_config.py` | **AC-009-01**: the real 2026 config loads. Broken fixtures fail and name the file and field: missing `ref`, missing `source`, unknown component, unknown seller zone, overlap, gap, a version not on the 1st, wrong unit.<br>**AC-009-02**: Stoen DSO, C11, no `net_billing`, net-metering cohort, a site that is not EMS-managed, and a window outside 2026 all fail and name the site |
| `tests/unit/domain/test_calendar.py` | **AC-009-03**: for each DSO and group:<br>• a weekday, Saturday, Sunday, 2026-11-11 and 2026-12-24;<br>• the slot counts of the civil days 2026-03-29 (92) and 2026-10-25 (100);<br>• summer weekdays at 06:30, 21:30 and 22:30 CEST, where 21:30 CEST (20:30 tariff time) is peak for Enea G12w;<br>• PGE summer weekdays at 15:30 and 17:30 CEST, and a PGE winter weekday;<br>• the hour 2026-09-30T22:00Z (civil October, tariff-time September).<br>The Friday → Saturday edge on tariff time is tested on a synthetic scheme without a window across midnight, because every real scheme hides that edge. The holiday list equals `holidays.Poland(years=2026)` |
| `tests/unit/domain/test_tariff_prices.py` | **AC-009-04**: golden table `tests/fixtures/tariffs/golden_2026.csv`, transcribed from the PDFs separately from the config. It has every DSO, group, zone and version, with energy and dist within 1e-9.<br>**AC-009-05**: two sites that differ only in fixed charges: identical ledgers, signals and non-fixed components; payables differ by the fixed-charge difference within 1e-9.<br>**AC-009-06**: VAT v → v′ scales buy prices by (1 + v′)/(1 + v) (relative 1e-12), and export values and tranches are unchanged |
| `tests/unit/domain/test_netbilling.py` | **AC-009-07**:<br>• a negative slot adds 0.0;<br>• a net-export hour with some negative quarters earns the hour's mean of max(P, 0);<br>• charge plus export in one hour settle the net.<br>**AC-009-08**: the proxy against its definition; zero weights raise; real mode with a missing month raises; latest-published lookup on civil dates, including before and after a correction.<br>**AC-009-09**: the cohort table and its rejections, including an `rce_from` that is not after the first feed-in month |
| `tests/unit/domain/test_settlement.py` | **AC-009-10**: row fields and invariants.<br>**AC-009-12** scenarios:<br>• carry-over;<br>• oldest first;<br>• energy-only offset;<br>• expiry 12 months after assignment;<br>• caps of 20 % and 30 % of the value before 1.23;<br>• month m's tranche is untouched in m and first usable in m+1, after older tranches;<br>• a correction of month m's RCEm changes m's tranche, not m's payable;<br>• an opening tranche that has already expired is rejected |
| `tests/unit/domain/test_signal.py` | **AC-009-14**:<br>• buy = energy + dist;<br>• RCE sell = 1.23 · max(RCE_q, 0), including a negative slot;<br>• an RCEm site's sell changes the day after publication (the 11th vs the 12th);<br>• February's correction (published with May's value on 2026-06-11) never reaches the signal; a fixture shows a correction reaching it once known |
| `tests/property/test_ledger.py` | **AC-009-11**: hour-aligned splits give equal rows; a non-hour border raises; totals via `fsum` are order-independent |
| `tests/unit/domain/test_actions.py`, `tests/property/test_actions.py` | **AC-009-15**:<br>• each kind resolves as specified;<br>• CHARGE_FROM_PV keeps grid_q ≤ max(load − pv, 0);<br>• DISCHARGE_TO_LOAD keeps grid_q ≥ min(load − pv, 0);<br>• SELF_CONSUME satisfies both;<br>• levels outside (0, 1] are rejected.<br>**AC-009-16**: identical inputs give identical actions; the observation has exactly the fields listed |
| `tests/property/test_reference.py` | **AC-009-18** and **AC-009-13**: [a, c) equals [a, b) + [b, c) with carried state, bit for bit.<br>• Inputs are generated once and sliced.<br>• One drawn border always falls on a civil month end.<br>• Generated sites include an RCEm cohort with an `rce_from` switch inside the window and with export, so the ledger price and the signal price differ.<br>**AC-009-19**: per quarter, load = pv + grid − p·Δt; per hour, Σ grid = import − export; ΔSoC = p·η·Δt or p/η·Δt (1e-9) |
| `tests/unit/domain/test_reference.py` | **AC-009-17**: output shape and P14 metadata.<br>**AC-009-20**: the ten-day window with the two opening tranches. `run` == the `step` loop for both baselines and both cohort variants. `settle_quarter` rejects an out-of-order t |
| `tests/unit/domain/test_purity.py` | **AC-009-23**: every import in `src/ems/domain/` is on the allow-list:<br>• `__future__`, `dataclasses`, `enum`, `math`, `datetime`, `zoneinfo`, `typing`, `collections.abc`, `functools`, `itertools`;<br>• `numpy`;<br>• `ems.domain.*`, `ems.contracts.time`.<br>No `now()` or `today()` call appears. The literal division by 1000 (or multiplication by 1e-3) appears only in `units.py` |
| `tests/unit/contracts/test_topics_schemas.py` | **AC-009-22**: the existing staleness and baseline tests stay, and the set of exported schema names is unchanged |
| `tests/unit/ingest/test_narrow_assets.py`, `test_catalog.py` | **AC-009-09**:<br>• a household catalog digest, pinned before the change, and `PLANT_CATALOG_DIGEST`, both without `net_billing`;<br>• every household has the default cohort and every plant has None;<br>• the test site loads its cohort;<br>• `tests/property/test_fleet.py` (AC-003-03) still passes |

Existing tests: `tests/property/test_battery.py` is unchanged. The event digests are unchanged, because no event
value changes.

## Docs and ADRs

No ADR. These go to "Decisions so far" in `00-overview.md`:
- the narrowing of `Tariff` and the per-site config path;
- the cost metric;
- the dropped dynamic tariff;
- `SELF_CONSUME`.

- `12`: every VERIFY item resolved with value, URL and date. Also:
  - "first fed in from 2024-07-01" replaces "connected from";
  - the tariff and zone model;
  - the two layers, the signal and its gaps;
  - actions and `Policy`;
  - the updated RES-act citation;
  - "settlement is additive over steps" is restricted to the ledger.
- `11`: `bill_pln`, `deposit_closing_pln`, `ledger_net_cost_pln`, whole-month windows, savings against NoBattery
  and SelfConsumption on both.
- `09`: the table above.
- `00`: status row and backlog row 009; the domain-core "next" row is replaced.
- `13`: the catalog gains `G12w` and `net_billing`.
- `08`: its observation line (`{t, pv_kw, load_kw}`) points to 12's `Observation`, which is a domain type and not
  a wire message.
- `config/tariffs/*`: a header comment in each file with the documents it cites.

## Review

Five reviews ran on 2026-11-13:
- three energy-domain source reviews before the draft (net-billing law, DSO tariffs, seller tariffs and national
  components), folded into "Resolved VERIFY items";
- `energy-domain-reviewer` on the draft: no blockers, 12 findings;
- `architect-reviewer` on the draft: no blockers, 13 findings.

Every finding is fixed above, except where the table says otherwise.

| Finding | Resolution |
|---|---|
| One `P` field lets the ledger value RCEm export at the lagged signal price (energy 1) | `QuarterOut` has `sell_signal_pln_per_kwh` and `export_price_pln_per_kwh`. An RCEm cohort with a switch is added to AC-009-18 and -20 |
| Signal gaps misstated; within-hour averaging missing (energy 2) | The gaps list saturation (cap-only value) and the hourly mean. The signal stays per quarter as specified; making it an hourly mean would need the hour's later prices in every step |
| Versions on civil vs tariff time implicit (energy 3) | The two-calendar rule is in section 2, with a pinned test hour; the quality-fee period id is recorded |
| AC-009-03 cases cannot separate the bases (energy 4) | 21:30 CEST for Enea G12w, 15:30 and 17:30 CEST for PGE; the Friday → Saturday edge on a synthetic scheme; PGE hours flagged for the config task; Enea's G12 default confirmed on 2026-11-13 |
| Remote-read rate justified wrongly (energy 5) | One global assumption: a remote-read DSO meter, which balancing and RCE valuation presuppose |
| Capacity band from base load (energy 6) | Called a proxy in the decision and D7 |
| Refund booking month; expired opening tranches (energy 7; architect 5) | Booked in the expiry month. Offset window assigned ≤ m ≤ assigned + 11, expiry at assigned + 11 ≤ m; expired opening tranches are rejected |
| `rce_from` before first feed-in (energy 8) | Rejected |
| Spec edge cases and stale spec wording (energy 9) | Spec amendments 4 and 5 |
| Test wording, D11 order, poz. 1347 row, actor note (energy 10–12) | Fixed; D14 added; the actor note says close the bill at the boundary and create the tranche on publication |
| `net_billing` tied to BESS control breaks AC-003-03; no household digest (architect 1) | Default on every generated household; household digest pinned before the change |
| AC-009-14 had no test (architect 2) | `tests/unit/domain/test_signal.py` |
| SC-002 requires 12 before approval (architect 3) | SC-002 amended: VERIFY resolved here, and 12 is the first task, before any net-billing code |
| AC-009-20 never exercises the deposit (architect 4) | Two opening tranches (one expiring at October's close) and an RCEm variant |
| Stepper triggers ambiguous; missing records (architect 6) | `t % 4 == 3`, month close in the last hour's fourth quarter, reject `t != acc.t` |
| Proxy over the run window is a hidden input; refactor missing (architect 7) | Proxy per whole month; inputs generated once and sliced; `national_pv_index` in Layout, guarded by the event digests |
| Simplicity cuts (architect 8) | Civil zone basis and 1-phase fee dropped (fail fast); AC-009-13 covered by AC-009-18 |
| `zoneinfo` not pinned by `tzdata` (architect 9) | Recorded as an accepted input under P2 |
| 08 defines the observation differently (architect 10) | 08 added to the docs list |
| AC-009-15 properties false as written (architect 11) | Restated as bounds (spec amendment 5) |
| AC-009-05 "exactly" (architect 12) | Within 1e-9, with the non-fixed components identical |
| Actor notes: Meter must not run physics; RCEm months; Action in the command; brief §5.2 (architect 13) | `settle_quarter` split out for `Meter`; the notes are rewritten |
| Real RCEm mixed into synthetic runs (architect, open question) | A synthetic run uses the proxy for every month |
| PGE PDFs unreachable (architect, open question) | Asked of the user with this plan |

## Complexity Tracking

Two paths compute the same settlement: the batch reference and the incremental stepper. That is intended. The
stepper is what actors run, and P6 needs an independent path to compare it against. Both share `settle_hour`,
`settle_month` and the quarter physics, so they differ only in how they group the work.
