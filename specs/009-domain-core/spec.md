# Feature Specification: Domain core (settlement, actions, Policy, offline reference)

**Feature Branch**: `009-domain-core` · **Created**: 2026-11-13 · **Status**: Approved 2026-11-13, amended by the
approved plan (see "Amendments" at the end)

**Input**:
- The "Domain core" backlog row: battery model (exists), settlement with RCE/RCEm options, action set, `Policy`,
  offline reference.
- A review of the data pipeline (2026-11) found that the purchase price is not modelled and the RCE/RCEm choice is
  not planned. The brief (§4) settles with a synthetic `sell = max(0, 0.6 · buy − 0.1)`.
- User decisions of 2026-11-13:
  - the spec is written now as 009 (008 is reserved for the historical backfill);
  - the tariffs of all four DSOs the catalog assigns are fully sourced;
  - the action set is battery actions plus a setpoint.
- Legal facts were checked by the energy-domain-reviewer on 2026-11-13 against the consolidated RES act (Dz.U. 2026
  poz. 68, 516, 900, 912 and 1347), Prawo energetyczne, URE and PSE. Items still open are marked VERIFY.

## Goal

A deterministic, pure domain core that values a battery the way a Polish net-billing prosumer's bill does, plus the
offline reference that every actor and strategy is later checked against, bit for bit. A battery's value is
avoided purchase minus forgone export, so a realistic purchase side is what makes strategy comparisons meaningful.

1. **Purchase tariffs G11, G12 and G12w** for Enea, Energa, PGE and Tauron with their incumbent sellers, for every
   2026 tariff version. Each value is sourced and split into marginal per-kWh components and fixed monthly charges.
2. **Net-billing**: hourly vector-balanced quantities, export valued at RCE or RCEm by cohort, negative prices
   floored at 0, and the prosumer deposit (1.23 factor, energy-only offset, 12-month validity, refund cap).
3. **Two settlement layers**: (a) an additive hourly ledger; (b) a non-additive monthly billing settlement, which is
   a pure function of the ledger.
4. **A per-quarter-hour price signal** for strategies, with its gap to the bill documented.
5. **The action set and the `Policy` interface**, with NoBattery and SelfConsumption baselines.
6. **The offline reference**: observations and prices replayed through a policy, `ems.domain.battery.step` and
   both settlement layers.

### Models

**Physics per quarter-hour q** (Δt = 0.25 h; battery p > 0 charges):

    grid_q = load_q − pv_q + p_batt_q·Δt

The load (base load, heat pump, EV) is uncontrolled.

**Settlement quantities per hour h.** The hourly vector-balanced net goes through the meter (RES act art. 4
ust. 2b) and is split evenly over the hour's four quarter-hours (art. 4b ust. 11):

    Eb_h = Σ_{q∈h} grid_q        import_h = max(Eb_h, 0)        export_h = max(−Eb_h, 0)
    import_q = import_h / 4      export_q = export_h / 4

- One aggregate flow per quarter-hour stands in for the vector sum over phases.
- UTC hours align with local hours, so settlement chunks start and end on hours.

**Marginal purchase price** (gross PLN/kWh), split into what the deposit may offset and what it may not:

    buy_q        = buy_energy_q + buy_dist_q
    buy_energy_q = (1 + VAT) · (energy_price(seller version, zone) + excise)
    buy_dist_q   = (1 + VAT) · (variable network fee(zone) + quality fee + OZE fee + cogeneration fee)

- No seller price includes excise (each seller's tariff, pkt 1.6 or 5.2), so excise (5.00 PLN/MWh, excise act
  art. 89 ust. 3) is always added.
- Every energy-dependent distribution fee is charged on the hourly balanced import (art. 4 ust. 4c pkt 1). There
  is no raw quarter-hour import basis.
- 2026 values: OZE fee 7.30 PLN/MWh, cogeneration fee 3.00 PLN/MWh, quality fee 0.0331 PLN/kWh in January and
  0.0332 PLN/kWh from 2026-02-01, VAT 23 %.

**Fixed charges** (gross, per month) appear only in the absolute bill:
- fixed network fee, 3-phase (a 1-phase site fails fast);
- subscription fee, for a 1-month billing period (0 from 2026-10-01);
- capacity fee: a monthly lump sum by annual-consumption band (URE Informacja 58/2025).

No 2026 seller tariff has a trading fee. The transition fee ended on 2026-01-01.

**Export value**: value_q = max(P_q, 0) · export_q, where P_q is RCE_q (quarter-hourly since 2025-10-01) or the
month's RCEm. The negative-price floor comes from art. 4b ust. 2; it assumes the site accepted the seller's clause
of art. 4b ust. 12. RCEm is PSE's export-weighted monthly price (art. 4b ust. 6–7), published on the 11th of the
next month.

**Deposit** (art. 4c, art. 4 ust. 11–11a; monthly billing simplifies the contract's 1/2/6/12-month period):
- Month m creates a tranche of 1.23 × Σ value_q (art. 4c ust. 4; every cohort, since 2025-02-01). It is assigned in
  m+1 and usable for 12 months from assignment. It never offsets month m itself.
- Tranches offset only the gross energy cost Σ import_q · buy_energy_q, which includes excise and VAT
  (art. 4c ust. 2–3), oldest first (ust. 7).
- At expiry, the unused part is refunded up to 20 % (RCEm) or 30 % (RCE) of the value of the energy fed in during
  the tranche's month, before the 1.23 factor, and paid by the end of month m+13. The rest is extinguished.
- The opening deposit is a list of tranches (month created, month assigned, amount, remaining, cap, basis).

**Cohort** `net_billing: {first_feed_in: date, rce_from: YYYY-MM | null}`:

| `first_feed_in` | Settlement |
|---|---|
| ≤ 2022-03-31 | Net-metering: out of scope, fails fast |
| 2022-04-01 … 2024-06-30 | RCEm; RCE from `rce_from` if it is set (one irreversible switch, art. 4b ust. 2a–2b) |
| ≥ 2024-07-01 | RCE; `rce_from` must be null |

## Scope

In scope:
- **Tariff config**: versions per (DSO or seller, group, validity period). Every value has a source (document, URL,
  table or page, date checked). The config covers:
  - Enea, Energa, PGE and Tauron with their incumbent sellers;
  - groups G11, G12 and G12w;
  - every 2026 version: each DSO changes on 2026-01-01 and 2026-10-01 (subscription fee 0); the quality fee has
    two periods (January, then from 2026-02-01); each seller has one version for all of 2026.

  National components and VAT are sourced once. RCEm is a table of PSE values with their publication dates.
- **Zone calendar**: one schema that expresses all four DSOs:
  - zone hours read on winter time (UTC+1) all year, which every DSO's tariff prescribes; no civil-time basis is
    built;
  - seasons by date (PGE summer 1 Apr–30 Sep);
  - day types: weekday, Saturday, Sunday, statutory holiday (the `holidays` list, which includes 24 Dec from 2025);
  - hour ranges that cross midnight;
  - operator-set hours, stored as the operator's published default.
- **Pure domain functions** in `src/ems/domain/`:
  - zones, purchase prices and fixed charges;
  - hourly quantities and export valuation;
  - cohort resolution, the RCEm lookup and the synthetic RCEm proxy;
  - the ledger, the billing settlement and the price signal;
  - actions, `Policy`, the baselines and the offline reference;
  - the one PLN/MWh → PLN/kWh conversion.
- **Edge loader** for the tariff and RCEm config, outside the domain, failing fast (P11).
- **Catalog** (config, not wire contract):
  - `G12w` becomes a tariff group;
  - EMS-managed sites get a `net_billing` setting, in `config/sites.yaml` for hand-defined sites and from a
    default in the catalog builder for generated ones;
  - every existing catalog value stays the same.
- **Docs**:
  - 12: the VERIFY items resolved with value, URL and date; "first fed in from 2024-07-01" in place of "connected
    from"; the tariff model, the two layers, actions and `Policy`; the updated citation;
  - 11: the cost metric;
  - 09: D1 resolved, new risks;
  - 00: backlog row 009 and the decisions.

Out of scope:
- Actors (`Tariff`, `Meter`, `Strategy_*`, `Battery`) and the mapping from events to observations (the
  actor-skeleton and ingestion specs).
- Forecasts in the observation (added later, additively).
- CURTAIL and SHIFT_FLEX_LOAD; battery wear (OPEN); unmanaged background sites.
- Any wire contract change (ADR 0016); the experiment harness.
- Settlement outside the scope above. An EMS-managed site in any of these cases fails fast:
  - C11 or C21;
  - the Stoen area;
  - a non-incumbent seller;
  - net-metering and prosumers who switched from it;
  - years other than 2026.

  Sites in śląskie use Tauron Sprzedaż's approved tariff, which ignores the Gliwice-area seller exemption.

## Affected

- `src/ems/domain/` grows from the battery model to the full core. Generator output and event ids are unchanged.
- The catalog gains one enum value and one field. Generated sites keep every existing value. The golden catalog
  digest is refreshed once if the new field enters it (the plan decides).
- `config/` gains the tariff and RCEm files.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - A household is settled as its bill would be (Priority: P1)
An EMS-managed household in any of the four DSO areas is settled hourly and monthly with its 2026 tariff versions
and its net-billing cohort. **Acceptance**: AC-009-01 … -13.

### User Story 2 - Strategies see the right objective (Priority: P1)
In each quarter-hour, a strategy gets the marginal purchase price and the export value its site faces. The gap
between that signal and the bill is documented. **Acceptance**: AC-009-14.

### User Story 3 - One action set (Priority: P1)
Rule, optimisation and (later) LLM strategies decide with named battery actions or a setpoint. Each resolves to one
battery command, which the battery model clamps. **Acceptance**: AC-009-15, -16.

### User Story 4 - One offline reference (Priority: P1)
A pure replay gives per-interval results and the bill. It is chunk-independent and bit-identical to stepping the
same domain functions one interval at a time, as actors will. **Acceptance**: AC-009-17 … -20.

### User Story 5 - Dynamic tariff (dropped by the plan)
Dropped: it needs about four tasks, and no catalog site uses it. It can return later as one more tariff kind in
config. Legal basis, kept for that spec: Prawo energetyczne art. 3 pkt 6d and art. 5 ust. 4f–4i, in force since
2024-08-24.

### Edge Cases
- 2026-03-29 (92 slots) and 2026-10-25 (100 slots) are civil billing days only. The zone calendar runs on UTC+1
  and never sees them; they test slot counts of billing days.
- On UTC+1, a summer weekday's zone edges and the Friday→Saturday boundary fall one hour later in local time.
- The hour 2026-09-30T22:00Z is October (and the 2026-10-01 version) on the civil calendar, but 2026-09-30 23:00 on
  UTC+1.
- Negative RCE slots: 3.8–3.9 % of quarter-hours, 13.5 % in April.
- An hour with grid charging in one quarter and export in another: the two net within the hour.
- A net-export hour with some negative quarters: it earns the hour's mean of max(RCE_q, 0).
- A month without export.
- A month whose tranches exceed its energy cost: the rest carries over, and distribution and fixed charges are
  still paid.
- An opening tranche that expires inside the window. January 2026's tranche is usable February 2026 – January 2027
  and expires at the January 2027 settlement, so within 2026 expiry and the cap are exercised only by opening
  tranches from 2025.
- An RCEm correction, and an `rce_from` switch inside the window.
- A tariff version that changes mid-year.
- A date with no tariff.
- A synthetic run: every month uses the RCEm proxy, flagged.
- A mode action with no surplus or no deficit: the command is zero.

## Requirements *(mandatory)*

- **FR-001**: Tariffs MUST be versioned, sourced config validated at the edge. → AC-009-01, -02
- **FR-002**: Zones MUST follow each tariff's rules, read on winter time (UTC+1), and its calendar. → AC-009-03
- **FR-003**: Purchase prices MUST separate the energy part, the distribution part and the fixed charges, with VAT
  applied once. → AC-009-04 … -06
- **FR-004**: Export MUST be settled on hourly balanced quantities at the cohort's price. → AC-009-07 … -09
- **FR-005**: The ledger MUST be additive and chunk-independent on hour boundaries. → AC-009-10, -11
- **FR-006**: The billing settlement MUST be a deterministic pure function of the ledger. → AC-009-12, -13
- **FR-007**: Strategies MUST get a per-quarter-hour price signal. → AC-009-14
- **FR-008**: Actions and `Policy` MUST be pure and resolve to clamped battery commands. → AC-009-15, -16
- **FR-009**: The offline reference MUST match step-wise execution bit for bit. → AC-009-17 … -20
- **FR-010**: Wire contract v1 MUST NOT change, and the domain MUST stay pure. → AC-009-22, -23

### Acceptance criteria (EARS)

Tariff config and calendar
- **AC-009-01**: WHEN the tariff config is loaded, THE SYSTEM SHALL validate every 2026 version of the distribution,
  seller and national components for Enea, Energa, PGE and Tauron (groups G11, G12 and G12w). IF a value lacks a
  source, a component is unknown, or validity periods overlap or leave a gap within 2026, THEN loading SHALL fail
  with an error naming the file and field. (Unit tests on broken fixtures.)
- **AC-009-02**: IF an EMS-managed site has no tariff for its (DSO, group, interval), no valid `net_billing`, or a
  cohort out of scope, THEN THE SYSTEM SHALL fail before settling, naming the site, and SHALL never fall back to
  another tariff.
- **AC-009-03**: THE SYSTEM SHALL assign every quarter-hour to exactly one zone using its tariff's zone rules,
  seasons, day types and statutory holidays, read on winter time (UTC+1) for every DSO. For each DSO and group,
  tests cover:
  - a weekday, a Saturday, a Sunday, 2026-11-11 and 2026-12-24;
  - 2026-03-29 and 2026-10-25 (slot counts of civil billing days);
  - summer weekdays at 06:30, 21:30 and 22:30 local time (21:30 CEST is peak for Enea G12w on UTC+1);
  - PGE summer weekdays at 15:30 and 17:30 local time, and a PGE winter weekday (PGE G12w is seasonal on
    weekdays);
  - the hour 2026-09-30T22:00Z (civil October, tariff-time September);
  - the Friday→Saturday boundary on tariff time.

Purchase price
- **AC-009-04**: THE SYSTEM SHALL compute buy_energy_q and buy_dist_q from the site's tariff version and zone at q,
  with VAT applied once. For every DSO, group, zone and 2026 version, they SHALL equal the hand-computed values
  from the sourced tables within 1e-9 PLN/kWh (golden tests). Excise SHALL always be added to the seller's price.
- **AC-009-05**: THE SYSTEM SHALL compute fixed charges per month, in the billing settlement only. Two sites that
  differ only in fixed charges SHALL have identical ledgers, signals and non-fixed bill components, and their
  payables SHALL differ by the difference in fixed charges within 1e-9 PLN.
- **AC-009-06**: THE SYSTEM SHALL apply VAT once, to purchase components. Changing the VAT rate from v to v′ SHALL
  scale every purchase price by (1 + v′)/(1 + v) and leave every export value and tranche unchanged.

Net-billing
- **AC-009-07**: THE SYSTEM SHALL settle each hour on Eb_h = Σ grid_q, split import_h and export_h evenly over the
  hour's quarter-hours, value export as max(P_q, 0) · export_q, and create month m's tranche as 1.23 × Σ value_q.
  - A negative RCE slot SHALL add exactly 0.
  - An hour with grid charging in one quarter and export in another SHALL settle only the hour's net.
- **AC-009-08**: THE SYSTEM SHALL take RCEm from the PSE-published table in config. The table keeps every
  publication of a month; billing uses the latest one and the signal the newest month known at q, at its latest publication known then; a publication counts as known from the day after its date (PSE gives no time of day).
  - WHERE a month belongs to a synthetic run (every month of it), THE SYSTEM SHALL use the proxy
    Σ w_q·max(RCE_q, 0) / Σ w_q weighted by the synthetic national PV index over that month, flag the proxy in the
    results, and unit-test it against its definition.
  - IF a real-data month has no published RCEm, THEN THE SYSTEM SHALL fail.
- **AC-009-09**: THE SYSTEM SHALL resolve each EMS-managed site's price basis per month from `net_billing` using the
  cohort table.
  - It SHALL reject `rce_from` when `first_feed_in` ≥ 2024-07-01, and any `first_feed_in` ≤ 2022-03-31.
  - The test site and the generator default SHALL be RCE cohorts.
  - Every existing catalog value of generated sites SHALL stay the same (catalog test).

Ledger (a)
- **AC-009-10**: THE SYSTEM SHALL produce one ledger row per site and hour, with:
  - import_h and export_h (both ≥ 0, at most one positive);
  - the energy cost and the distribution cost, both on the hourly import, and the export value (before 1.23).
- **AC-009-11**: THE SYSTEM SHALL make the ledger additive and chunk-independent (property tests):
  - rows for [a, c) SHALL equal rows for [a, b) followed by [b, c), for hour-aligned a, b and c;
  - totals SHALL use one exactly rounded summation (`math.fsum`);
  - IF a chunk border is not on a UTC hour, THEN THE SYSTEM SHALL reject it.

Billing settlement (b)
- **AC-009-12**: THE SYSTEM SHALL settle each Europe/Warsaw calendar month as a pure function of (that month's
  ledger rows, fixed charges, cohort, opening tranches). It returns the charges per component, the offsets, the
  tranches created, expired and refunded, and the closing tranches. Scenario tests:
  - carry-over to the next month;
  - tranches used oldest first;
  - offsets limited to the gross energy cost, so distribution, fixed and capacity charges are paid in full;
  - an opening tranche expiring 12 months after assignment;
  - the refund capped at 20 % (RCEm) or 30 % (RCE) of the value before 1.23;
  - month m's tranche is never used in m and is first usable in m+1, after older tranches.
- **AC-009-13**: THE SYSTEM SHALL settle deterministically: settling month by month with carried tranches SHALL
  equal settling the window in one call (covered by AC-009-18 with a border on a month end). 12 SHALL state that
  this layer is not additive, replacing the brief's "settlement additive over steps" for it.

Price signal
- **AC-009-14**: THE SYSTEM SHALL give a policy, for each quarter-hour q, the signal buy_q and
  sell_q = 1.23 · max(P_q, 0) in PLN/kWh.
  - For RCE sites, P_q is RCE_q.
  - For RCEm sites, P_q follows the rule the plan sets (leaning: the latest published RCEm).
  - 12 SHALL document the gaps between the signal and the bill: hourly netting, the energy-only offset, the refund
    cap and expiry, and monthly billing.

Actions and Policy
- **AC-009-15**: THE SYSTEM SHALL resolve each action to one battery command with one pure function, and
  `battery.step` SHALL clamp it:
  - HOLD → 0;
  - SELF_CONSUME → the quarter's PV surplus if positive, else minus its deficit;
  - CHARGE_FROM_PV → the quarter's PV surplus (0 without one);
  - CHARGE_FROM_GRID(level) → level · p_max;
  - DISCHARGE_TO_LOAD → minus the quarter's deficit (0 without one);
  - EXPORT(level) → −level · p_max;
  - SETPOINT(p_kw) → p_kw.

  Property tests: CHARGE_FROM_PV keeps grid_q ≤ max(load − pv, 0) (it never imports for the battery);
  DISCHARGE_TO_LOAD keeps grid_q ≥ min(load − pv, 0) (it never exports battery energy); SELF_CONSUME satisfies
  both; levels outside (0, 1] are rejected. Mode actions use the quarter's realised PV and load; this models the
  inverter's local control and is documented in 12.
- **AC-009-16**: THE SYSTEM SHALL define `Policy.decide(state, observation) -> Action` as a pure function, with the
  baselines `NoBattery` (HOLD) and `SelfConsumption` (SELF_CONSUME every quarter). Identical inputs SHALL give
  identical actions (property test). The observation SHALL carry only data available at q: `t`, the signal for q
  and the PV and load energy of the last completed quarter.

Offline reference
- **AC-009-17**: WHEN the reference replays a policy over [a, c) for one site (observations, prices, battery
  parameters, initial SoC, opening tranches), THE SYSTEM SHALL return:
  - per quarter-hour: the action, the command, the actual power, the SoC and grid_q;
  - the hourly ledger;
  - the monthly settlements.
- **AC-009-18**: THE SYSTEM SHALL make the reference chunk-independent for hour-aligned borders: [a, c) SHALL equal
  [a, b) followed by [b, c) with carried SoC, policy state and tranches, bit for bit (property test).
- **AC-009-19**: THE SYSTEM SHALL conserve energy (property test):
  - in each quarter-hour, load = pv + grid_q − battery AC energy;
  - in each hour, Σ grid_q = import_h − export_h;
  - every SoC change equals charge · η or discharge / η.
- **AC-009-20**: WHEN at least one week of the test site that spans 2026-10-25 and the 1 November month boundary
  (fixed seed, generator output) is stepped one quarter-hour at a time through the domain functions with carried
  state, THE SYSTEM SHALL produce results, ledger and settlements equal (`==`) to the offline reference, for both
  NoBattery and SelfConsumption. The results SHALL record the tariff version ids, the RCEm source and the cohort
  (P14).

Optional
- **AC-009-21**: dropped with User Story 5 (plan decision).

Contract and purity
- **AC-009-22**: THE SYSTEM SHALL leave wire contract v1.0.0 unchanged: the schema export equals `schemas/` and the
  baseline check passes. The catalog, tariff and RCEm config are not part of the exported schemas (test).
- **AC-009-23**: THE SYSTEM SHALL keep `src/ems/domain/` free of I/O, clock, environment and runtime imports (an
  import test). It SHALL convert PLN/MWh to PLN/kWh in exactly one function.

## Constraints (from the checklists)

- **P1**: spec, plan and tasks are each approved before code. Per `.claude/rules/domain.md`, no net-billing rule
  still marked VERIFY in 12 is implemented, so the plan phase resolves the remaining VERIFY items first.
- **P2**: everything new in `src/ems/domain/` is pure, and config loading sits at the edge.
- **P3**: no actor or owner is added. Recorded for the actor spec:
  - `Tariff` stays keyed by market (ADR 0012) and holds only price curves, served as a shared read.
  - Per-site tariff rules come from config through the domain functions.
  - `Meter` owns the real bill; each `Strategy_*` owns its shadow settlement.
  - Ledger rows are hourly, so a row is final at the hour's fourth quarter.
  - Any per-household tariff state in an actor needs a justification against R1–R7.
- **P4, P5, P7, P8, P13**: no actors, handlers, timers or tracing are touched. The battery model still clamps every
  command (AC-009-15).
- **P6**: the reference and the step-wise check (AC-009-20) are the bit-exact baseline for the actor spec.
- **P9**: no wire contract change (AC-009-22).
- **P10**: the reference is a pure function. Its CLI comes with the experiment harness.
- **P11**: missing tariff, cohort or RCEm data fails fast.
- **P12**: no new dependency is expected (`holidays` 0.105 and numpy are already locked).
- **P14**: results record the tariff versions, the RCEm source and the cohort.
- **P15**: docs land with the change. No ADR: the narrowing of the Tariff actor and the backlog order go to
  "Decisions so far" in 00 and to 12.
- **P16**: no experiment runs in this spec. 11 defines the cost metric.
- **Risks** (09):
  - D1 is resolved.
  - New risks:
    - the quarter-hour signal versus hourly netting and the deposit;
    - monthly billing versus the contract's billing period;
    - the RCEm proxy in synthetic months;
    - one aggregate flow standing in for the phase vector sum;
    - mode actions using realised flows;
    - a capacity-fee band that is constant per run;
    - DSOs assigned by voivodeship, ignoring Stoen and the Gliwice exemption;
    - tariffs re-sourced every year, with several versions per year;
    - the 2026 amendments (poz. 1347, mostly in force from 2026-12-01) not read article by article.

## Success Criteria

- **SC-001**: `make ci` is green, and every AC has at least one new test, written first.
- **SC-002**: every VERIFY item is resolved in the plan by the energy-domain-reviewer before the plan is approved,
  and writing them into 12 with value, URL and date is the first task, before any net-billing code.
- **SC-003** (review): 11 defines the cost metric; 09 resolves D1 and adds the new risks; 00 has row 009 and the
  decisions.

## Assumptions

- The modelling year is 2026.
- EMS-managed households are net-billing prosumers supplied by the incumbent seller.
- The test site is an RCE cohort (`first_feed_in` ≥ 2024-07-01).
- Fixed-charge inputs (phases, billing period, consumption band) come from defaults derived from the catalog (the
  plan confirms them).

## Open questions (resolved in the plan unless marked)

- **VERIFY** (energy-domain-reviewer, plan phase):
  - the article-level text of the 2026 amendments (art. 4 ust. 2b, art. 4b, art. 4c);
  - whether month m's tranche may offset month m;
  - the base of the refund cap;
  - the import basis for the distribution fees and the capacity band, per DSO;
  - Tauron's clock basis;
  - Enea's G12 hours and whether a 2026-10-01 version exists;
  - whether each seller's price includes excise, and the trading fees;
  - the billing period per DSO.
- **Plan decisions**:
  - first-published or corrected RCEm;
  - the RCEm proxy weights;
  - the sell signal for RCEm sites;
  - the fixed-charge defaults;
  - how the cost metric in 11 treats a deposit left at the end of the window (leaning: its own column beside the
    bill, plus the ledger net cost);
  - whether to keep or drop the dynamic tariff;
  - the config layout and the location of the loader;
  - the observation fields.

## Brief → spec coverage

| Brief item | Where |
|---|---|
| 1. G11/G12/G12w, sourced versions, zone calendar, marginal vs fixed, VAT, four DSOs | AC-009-01 … -06 |
| 2. Dynamic tariff (optional), legal basis | US5 (dropped by the plan; legal basis kept) |
| 3. Net-billing: RCE/RCEm source, coefficient, negative → 0, deposit, measurement | Models, AC-009-07 … -09, -12 |
| 4. Ledger (a), billing (b), not additive, reference covers both, signal gap | AC-009-10 … -14, -17 … -20 |
| 5. Purity, thin `Tariff`, ownership, no new actor | P2, P3, AC-009-23 |
| 6. Units, ÷ 1000 in one place, where VAT enters | Models, AC-009-06, -23 |
| 7. Config, not contract | AC-009-09, -22 |
| Tests to plan | AC-009-03, -04, -07, -11, -12, -13, -20 |
| Experiment and docs impact | P14, SC-002, SC-003, Scope (docs) |

## Review record (energy-domain-reviewer, 2026-11-13)

Changes from the first draft:
- **Settlement quantities are hourly**: the hourly vector-balanced net (art. 4 ust. 2b) is split over the
  quarter-hours (art. 4b ust. 11).
- **The 1.23 factor applies to every cohort's tranche** (art. 4c ust. 4, since 2025-02-01).
- **The refund cap is 20 % for RCEm and 30 % for RCE** (art. 4 ust. 11a).
- **The deposit offsets only the gross energy cost**, oldest first, and is valid for 12 months from its assignment
  in month m+1.
- **RCEm is export-weighted and published by PSE** (with later corrections), so it cannot be recomputed from RCE.
- **The cohort follows the first feed-in date.**
- **Net-metering is excluded.**
- **The zone schema needs a clock basis** (every DSO runs zone hours on UTC+1 all year; corrected by the plan),
  PGE's seasons, and Enea's G12w without a midday window.
- **Tariff components**: the transition fee is gone in 2026; excise is added on top of every seller's price
  (corrected by the plan); each DSO has a 2026-01-01 and a 2026-10-01 version, and the quality fee changes on
  2026-02-01.
- **No household price freeze applies in 2026**: URE approved the tariffs on 2025-12-17.

## Amendments (2026-11-13, with the approved plan)

Applied from `plan.md` "Spec amendments", which the plan's approval covers:
1. `SELF_CONSUME` joins the action set, and `SelfConsumption` returns it (AC-009-15, -16).
2. User Story 5 and AC-009-21 (dynamic tariff) are dropped.
3. The generator's default cohort is a constant in the catalog builder.
4. Source corrections: excise added to the seller price; quality fee 0.0331 then 0.0332; no trading fee; the offset
   cited as art. 4c ust. 2–3; the zero floor assumes the clause of art. 4b ust. 12; DSO versions on 2026-01-01 and
   2026-10-01; zone hours on UTC+1 for every DSO; no raw-import basis; refund cap before 1.23 with no switch; no
   same-month offset.
5. Wording: AC-009-05 within 1e-9; AC-009-08 proxy for every month of a synthetic run; AC-009-13 covered by
   AC-009-18; AC-009-15 properties restated as bounds; SC-002 and the edge cases updated.

Amended during implementation (2026-11-13): AC-009-08 and AC-009-14's RCEm signal uses the newest month known
at q, and a PSE publication counts as known from the day after its date, because PSE publishes a date without a
time. "On or before q's date" could look a day ahead, and corrections share the 11th with first publications.
