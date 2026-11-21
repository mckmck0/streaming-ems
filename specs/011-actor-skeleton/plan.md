# Implementation Plan: Actor skeleton

**Branch**: `011-actor-skeleton` | **Date**: 2026-11-20 | **Spec**: [spec.md](spec.md) | Approved 2026-11-20,
after the architecture and energy-domain reviews (section Review)

## Summary

Stage 3 builds the actors of `01-architecture.md` on the runtime of spec 010. A new pure package,
`src/ems/assembler/`, turns a household's four streams into ordered steps; the offline side runs the same code.
`src/ems/actors/` holds one file per actor: a pure transition `(params, state, message) -> (state, sends)` and
short handlers around it. The domain gains the pieces of `quarter()` and a ledger that corrects late readings by
an allowed-lateness model. A run is configured by control messages. The test tiers then compare actor state with
the offline side through Kafka in five runs, one of which closes October.

No wire contract, topic or generator change. The internal messages are new at version 1.0.0. `Probe` and the two
state-cost objects are deleted.

## Facts checked for this plan

Checked on 2026-11-20. "Task" means the value is confirmed on the pinned server when its task runs. The two
prototypes are scratch scripts outside the repo; their results are reproduced by the tests of this spec.

| Item | Value | Source | Confidence |
|---|---|---|---|
| A clean window releases every step | 10 households × 1 day from 2026-10-20T00:00Z: 96 steps for each managed site (000001, 000002, 000004), no missing price, 8 steps without a visible forecast, 2 price curves. 30 h from 2026-10-31T00:00Z: 120 steps each; October ends at 23:00Z and, at `L` = 8, closes after the step starting 2026-11-01T02:45Z | Prototype of the rules below over `ems.ingest.generator.emissions` | confirmed |
| Fault counters at the fixture rates | Late 15 %, missing 3 %, duplicate 3 %, 60 random run ids, summed over the three managed sites (min / median / max). `L` = 8: estimated 5 / 13 / 24, duplicates 7 / 14 / 21, no late reading, steps 270 / 284 / 288 of 288. `L` = 2: estimated 51 / 64 / 79, late 11 / 18 / 26, corrections applied 10 / 17 / 25, `too_old` 20 / 32 / 43 | Same prototype with `apply_faults` | confirmed |
| Correction bound | Every late reading had `at_step` within `[t + L + 1, t + 2L]` (asserted in the prototype); exhaustive over arrival orders of up to 8 slots at `L` = 2 and 3 in the design review | Prototype; design review | confirmed |
| Ledger design (§2) | On the spec 009 test window (964 quarter-hours across the October close, with opening tranches): lag 0 equals `settle_quarter` at every step (accumulator, row, month); lag 16 differs from lag 0 only at the 16 steps from October's last quarter-hour on and gives the same settlement; with about 100 random corrections (`L` = 2 and `L` = 8) the October settlement equals `settle_month` over the final rows and the open November rows equal `hour_rows` over the final flows; at most `lag + 3` flows are kept | Prototype of `add_quarter`, `correct`, `due_month`, `close_month` and `settle_step` over the existing domain code | confirmed |
| Late faults | A late record is delayed in time by up to `late_max` (2 h) from its own emission, so it is overtaken by at most 7 newer slots of its stream; missing and late hit meters and forecasts; duplicates hit every stream, prices included; value mutations keep the `event_id` | `src/ems/ingest/faults.py` 135–161 | confirmed |
| Generator schedule | Forecast runs issued where `t % 12 == 0` (`FORECAST_CADENCE_Q`), emitted at issue + 120 min; only runs issued at or after the start; price days published on D−1 in bands 13:00–16:15 local, days already published emitted at the start | `generator.py` 81–82, 616–649; `config/calibration/prices.yaml` | confirmed |
| Measurement identity | `event_id` = UUIDv5 of stream, subject, interval start, `issued_at` (none for meters), source, run id, major: no content | `src/ems/contracts/ids.py` 26–51 | confirmed |
| Catalog, 10 households | Managed: 000001 (PV, heat pump, G12 Enea), 000002 (no PV, G11 PGE), 000004 (PV, G11 PGE). A site without PV emits no production record. Counts for one day: 960 consumption, 768 production, 80 forecast, 2 price records | `ems.ingest.catalog`, `emissions` (read-only run) | confirmed |
| Wire values without bounds | Measurement `value` (≥ 0) and price `price` have no upper bound; a `PL:RCE` curve may be PT1H or EUR; a measurement may have another resolution | `src/ems/contracts/envelope.py` 163, 198, 211–227, 299 | confirmed |
| Dataclass state | The SDK's default serde cannot store a dataclass (dacite is not installed) or a list of models, and cannot return a dataclass from a handler; a custom `Serde` is passed with `serde=` and `output_serde=` | `restate/serde.py` 264–317; `server_context.py` 812–836 | confirmed |
| `TypeAdapter` round trip | `RefState` with 745 `HourRow`s: 156 kB, 0.32 ms encode, 0.42 ms decode, equal with every float bit-identical; 300,000 random doubles: 0 mismatches; a PEP 695 generic dataclass (`Inbox[Step]`) decodes to its member type; `field(compare=False)` is stored and ignored by `==`; unknown fields are ignored on read | Measured with pydantic 2.13.5 | confirmed |
| Non-finite floats | `TypeAdapter` refuses a `config` for a dataclass; NaN is written as `null`, which the next read rejects. The mapping's bounds keep every value finite (§4) | Measured | confirmed |
| Unstable bytes | `TypeAdapter(SiteModel).dump_json` gives 1,646 bytes with a different SHA-256 under `PYTHONHASHSEED` 1, 2 and 3; the only set-typed field is `National.holidays` (14 dates). `RcemTable` is stable (1,628 bytes) | Measured | confirmed |
| Journal check on replay | The SDK enables payload checks unless a codec is configured; a retried handler whose commands differ from the journal fails with RT0016 | `restate/server.py` 328; <https://docs.restate.dev/references/errors> | read from source and docs; the fault test exercises a retry |
| Sends by name | `ctx.generic_send(service, handler, arg: bytes, key=...)`; typed sends need the handler function. A handler's own sends are deduplicated on retry | `restate/server_context.py` 1266–1325; <https://docs.restate.dev/develop/python/service-communication> | confirmed |
| Errors | A plain exception is retried and holds its key; `TerminalError` fails the invocation once; an input the serde cannot decode becomes a `TerminalError` before the handler runs | `restate/server_context.py` 551–607; `handler.py` 403–406 | confirmed |
| Registration | `POST /deployments` with `force: true` "overrides … any deployment using the same `uri`" and implies `breaking`; `GET /services/{service}/handlers/{handler}` answers 404 when missing; removing a service means removing its deployment, and only forced deletion exists | <https://docs.restate.dev/admin-api/deployment/register-deployment>, `…/service_handler/get-service-handler`, `…/deployment/delete-deployment`, <https://docs.restate.dev/services/versioning> (the reference shows API version 1.7.14) | task: confirmed on 1.8.1-hotfix.2, including whether `Probe` stays listed after the override |
| Introspection | `sys_invocation`: `target_service_name`, `target_service_key`, `status` (`pending`, `scheduled`, `ready`, `running`, `paused`, `backing-off`, `suspended`, `completed`), `completion_result`, `last_failure`, `retry_count`. `state`: `service_name`, `service_key`, `key`, `value_utf8`, `value_length` | <https://docs.restate.dev/references/sql-introspection> | task: columns confirmed on the pinned server, and the status an interrupted invocation shows after a SIGKILL of the SDK service (spec 010 saw `ready` at the head of each queue while the service was down) |
| Domain purity test | `tests/unit/domain/test_purity.py` allows the standard library, numpy and `ems.contracts.time` only, and globs `*.py` without recursion | The file | confirmed |
| Allowed lateness | Flink: a late element inside the allowed lateness is added to the window and may fire it again; the result is an update of the earlier one; state is kept until the lateness expires | <https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/windows/> | read 2026-11-20 |
| Forecast cadence | ICON-D2: eight runs a day (00 … 21 UTC). DMI HARMONIE (DINI): the deterministic run every third hour. Open-Meteo: "every 3 hours" for both, `update_interval_seconds` 10800. DWD's notice for the 2026-11-30 cutover keeps forecast range and time resolution and says nothing about run times | <https://www.dwd.de/EN/ourservices/nwp_forecast_data/nwp_forecast_data.html>, <https://www.dmi.dk/friedata/dokumentation/data/forecast-data-weather-model-harmonie-for-dini-and-ig>, <https://open-meteo.com/en/docs/dwd-api>, Open-Meteo `meta.json` of both models, <https://www.dwd.de/DE/leistungen/opendata/neuigkeiten/opendata_september2026_1.html> | read 2026-11-20 (energy-domain review); re-probe after the cutover (L1) |
| Price window | Over 417 delivery days (2025-10-01 … 2026-11-21) every D−1 publication of `rce-pln` fell between 13:17:12 and 16:13:17 local; two days had only a same-day publication (2025-10-23 at 12:34 local, 2026-03-04 at 08:05 local). SDAC results are not published before 12:55 CET/CEST. No statute sets an earliest time | PSE API `rce-pln` (`publication_ts`); Nord Pool SDAC FAQ | read 2026-11-20 (energy-domain review); the publication terms of IRiESP-OIRE are unread (A2) |
| DST | PSE returns 92 slots for 2026-03-29 (first starts 2026-03-28T23:00Z) and 100 for 2026-10-25 (first starts 2026-10-24T22:00Z). 13:00 Europe/Warsaw is 11:00Z on 2026-03-29 and 12:00Z on 2026-10-25 | PSE API; `zoneinfo` | confirmed 2026-11-20 |
| No RCE for a period | RES act art. 4b ust. 10: when RCE cannot be determined for a period t, the value determined for period t on the nearest preceding day for which it was determined applies | <https://api.sejm.gov.pl/eli/acts/DU/2021/2376/text.html> (wording unchanged by Dz.U. 2024 poz. 1847) | read 2026-11-20 (energy-domain review); the consolidated text Dz.U. 2026 poz. 68 is unread (D14) |

## Plan decisions (the spec's open questions)

| Question | Decision |
|---|---|
| Field lists | In the Actor contract below and in `08-contracts.md` |
| Stale deployment | Harness `ensure_registered`: register with `force: false`; if the handlers Restate lists for the eight services differ from those of `ems.actors.app`, register again with `force: true` (the module has closed the stack's ingestion, so nothing is in flight) and fail unless they are equal. `make register`: after the POST, `curl -fsS` on the three ingress target handlers; a 404 stops the target before any ingress container starts. A task confirms both on the pinned server |
| Waiting for a run | Deterministic end marker: the offline fold gives the last released step of every managed household before the wait; `wait_run` returns when every Kafka record is handled and every managed strategy, `Ems`, `Battery` and `Meter` expects the step after it. It fails at once on a failed or paused invocation of the run, and in the default (strict) mode also on an invocation still failing after four attempts (`STRICT_ATTEMPTS`; revised 2026-11-21: single attempts fail on HTTP/2 transport errors and are retried with success); the fault tier uses the lenient mode |
| Fault fixture | `tests/fixtures/generator.faults.yaml`: `config/generator.yaml` with `late_rate` 0.15, `missing_rate` 0.03, `duplicate_rate` 0.03; a unit test checks that nothing else differs |
| Fault-tier run | 30 households × 2 days (about 9 managed sites, 10,900 records, 10,400 downstream invocations). The SDK service is killed when 30 % of the records are handled. Proof of the window, from data that survives the kill: fewer records handled than published, and for one managed household a strategy that expects a step strictly between `first_step` and its offline end |
| Arrival-order data | Fields with `compare=False`: `Inbox.reordered`, `Inbox.stale`, and the stream leads in `HouseholdState.leads`. Nothing else depends on arrival order |
| Hypothesis sizes | The `ci` profile (60 examples, derandomised) except: interleaving 200 examples, streams of up to 24 slots, `L` in 1…3; network 50 examples, up to 16 slots |
| ADR files | `docs/adr/0018-late-and-missing-inputs.md`, `docs/adr/0019-run-parameters-as-control-messages.md` |
| `grid_kwh` | Lives in `settlement.py`, which `reference.py` already imports (AC-011-35) |
| Shell names | `shell.configure`, `shell.handle` (an exclusive handler) and `shell.ingest` (a Kafka-fed handler), so nothing is called `apply` except the domain function and `Battery.apply` |
| Mapping bounds | A reading entry above 10⁴ kWh or a price beyond ±10⁵ PLN/MWh is refused: far above any physical or market value, low enough that no sum overflows |
| A second, different `configure` | Fails with `TerminalError` and changes nothing (visible to the operator); an equal one changes nothing |

## Principles Check (PASS)

| # | Touched | How it holds | Shown by |
|---|---|---|---|
| P1 | yes | Spec 011, this plan and the tasks; commits cite task ids | review |
| P2 | yes | The new domain functions stay inside the domain allow-list. `src/ems/assembler/` gets its own allow-list test and the hook pattern. Every handler goes through `shell.py`; logic sits in pure transitions | AC-011-06 (purity tests, hook test) |
| P3 | yes | Owners as in 01. The ingress log moves to the Kafka-fed actors. New fact: a run's parameters, written once per object by the operator | Actor contract |
| P4 | yes | Every edge is a one-way send by service name. Request-response only from tests and jobs to shared handlers | Actor contract; `test_app.py` |
| P5 | yes | Handlers parse a record (up to about 60 kB) or a state (up to about 160 kB) and run pure code; no external call, no clock, no wait | ruff `ASYNC`; `test_app.py` scan |
| P6 | yes | Offset dedup at the ingress; the slot table; `Idempotency-Key` on `configure`; deterministic bytes of values and transitions; `==` against the offline side in five runs and after a kill | AC-011-21, -33, -38 … -43 |
| P7 | yes | No timer. Watermarks come from content and schedule; no handler reads a clock | `test_app.py` scan (`sleep`, `send_delay`, `ctx.time`, `ctx.run`, `object_call`, `service_call`, `generic_call` absent from `src/ems/actors/`) |
| P8 | yes | `Battery` executes through `reference.apply`, which calls `battery.step_params`; clamps are counted | AC-011-35; `tests/property/test_battery.py` |
| P9 | yes | Internal messages new at 1.0.0 with `schema_version`; field names of messages and nested types pinned by a test; R7 reworded; no wire change | AC-011-32 |
| P10 | yes | `jobs/ingress_measure.py` stays a CLI; the offline fold and the parameter builder are library functions | review |
| P11 | no | No new setting | |
| P12 | no change | No new dependency or pin | `uv lock --check` |
| P13 | no change | Tracing stays off | |
| P14 | yes | Each object stores its `RunParams`, so a config edit or a later deployment cannot change a run; the `make measure` manifest records `late_slots`, the strategies and the live strategy | AC-011-27, -46 |
| P15 | yes | Docs and ADRs 0018 and 0019 land with the change | SC-001 |
| P16 | yes | `make measure` reports the skew, records per second, memory and the re-order counters | AC-011-46 |

## Contract changes (`08-contracts.md`)

None on the wire. New internal messages at 1.0.0: `Step`, `PriceUpdate`, `ForecastUpdate`, `Subscribe`, `Apply`,
`Ack`, `Flows`, each carrying `schema_version` and `t`; R7 is reworded so that `t` is the sim timestamp and stream
updates carry `seq`. The control record `RunParams` (the body of `configure`) carries `schema_version`; its time
is `first_step`. A test pins the field names of every message and of the types nested in it (`Curve`, `Run`,
`Correction`, `Action`); a later field must be optional with a default. 08 also records the schedule constants,
which are code, not contract.

## Design

### 1. Layout

```
src/ems/domain/reference.py      + Prices, prices, decide, apply; quarter() composes them
src/ems/domain/settlement.py     + grid_kwh, Correction, Flow, add_quarter, correct, due_month, close_month
src/ems/domain/stepper.py        + settle_step; step(…, recent=(), corrections=(), lag=0); StepOut.recent
src/ems/domain/actions.py        + POLICIES
src/ems/domain/tariff.py, calendar.py; src/ems/tariffs/config.py     holidays: sorted tuple
src/ems/contracts/availability.py   + forecast_cadence, price_window_opens
src/ems/assembler/               params.py, mapping.py, meter_stream.py, sparse_streams.py, join.py, offline.py
src/ems/actors/                  messages.py, reorder.py, serde.py, shell.py, household.py, tariff.py,
                                 weather.py, strategy.py, ems.py, battery.py, meter.py, app.py
src/ems/actors/ingress_check.py  + public parse, log_step, lead_step(leads, stream, slot); log_apply, fold_log unchanged
src/ems/actors/probe.py, statecost.py     deleted
src/ems/experiments/runs.py      managed_params
src/ems/experiments/stack.py     generalised (§7)
infra/docker-compose.yml, Makefile, jobs/ingress_measure.py     (§8, §9)
tests/fixtures/generator.faults.yaml; tests/network.py (the in-memory network of the property test)
.claude/rules/assembler.md, .claude/hooks/post-edit.sh
```

### 2. Domain (AC-011-01 … -05)

**The split** (`reference.py`). `quarter()` keeps its signature and becomes:

```python
pr = prices(model, rcem, q.t, q.rce_pln_per_kwh)  # buy, basis, signal, export price
action = decide(policy, pr, q.t, state.soc_kwh, state.last_pv_kwh, state.last_load_kwh)
cmd, p, soc = apply(model.battery, state.soc_kwh, action, q.pv_kwh, q.load_kwh)
grid = grid_kwh(q.pv_kwh, q.load_kwh, p)  # (load − pv) + p·Δt, as today
```

`Ems` calls `prices` and `decide`, `Battery` calls `apply`, `Meter` calls `prices` and `grid_kwh`. A test keeps
the spec 009 body verbatim and compares it with `quarter()` for every action kind and both bases.

**The ledger** (`settlement.py`):

```python
@dataclass(frozen=True)
class Correction: stream: str; t: int; kwh: float          # stream: "production" or "consumption"

@dataclass(frozen=True)
class Flow:                                                 # kept while its hour can still be corrected
    pv_kwh: float; load_kwh: float; p_kw: float; settle: SettleIn

def grid_kwh(pv_kwh: float, load_kwh: float, p_kw: float) -> float
def add_quarter(acc: SettleAcc, s: SettleIn) -> tuple[SettleAcc, HourRow | None]
def correct(acc, recent, c) -> tuple[SettleAcc, tuple[Flow, ...], HourRow | None]
def due_month(acc: SettleAcc, lag: int) -> str | None
def close_month(acc, fixed, basis, rules) -> tuple[SettleAcc, MonthSettlement]
```

- `add_quarter` is today's hour part of `settle_quarter`: it appends the quarter and writes the hour's row on
  its fourth quarter.
- `correct` replaces PV or load in the kept flow of `c.t` and recomputes its grid energy with `grid_kwh`. If
  `c.t` lies in the open hour it replaces that entry; otherwise it recomputes the hour's row with `settle_hour`
  over the hour's four kept quarters and replaces it at index `h − month_rows[0].h` (rows are contiguous and
  months start on whole hours). The hour's row is always still in `month_rows` (its month closes only after the
  step that ends the lateness window). A flow that is not kept raises `ValueError`; the bound
  `at_step ≤ t + 2L` makes that unreachable, and a property test shows it.
- `SettleAcc.month_rows` holds the rows not yet settled into a month: the open month, and an ended month until
  its lag has passed. `due_month` returns the oldest month when its last hour is in the rows and `acc.t` is more
  than `lag` past that hour's last quarter. `close_month` settles that month's rows with `settle_month` and
  keeps the rest.
- `settle_quarter(acc, s, fixed, basis, rules)` keeps its signature and results: `add_quarter`, then
  `close_month` when `due_month(acc, 0)`.

**The stepper** (`stepper.py`):

```python
def settle_step(model, acc, recent, flow, corrections=(), lag=0
                ) -> tuple[SettleAcc, tuple[Flow, ...], HourRow | None, MonthSettlement | None]:
    # 1 correct(...) for each correction, in order   2 add_quarter(flow.settle); keep the flow
    # 3 a due month closes with fixed_charges and basis_for of that month (not of the current step)
    # 4 keep the flows from the hour of (acc.t − lag) on
def step(model, policy, rcem, state, q, recent=(), corrections=(), lag=0) -> StepOut
    # quarter(), then settle_step; StepOut gains recent
```

`RefState` does not change, so a strategy's `RefState` still equals `reference.run`'s; the kept flows travel
beside it (`StepOut.recent`, `StrategyState.recent`, `MeterState.recent`). `reference.run` is unchanged.

**No sets.** `National.holidays` becomes a sorted `tuple[date, ...]` (checked in `__post_init__`); `zone_at` and
`day_type` take it as it is. `tests/unit/domain/test_calendar.py` compares with a tuple.

### 3. Schedule constants (`ems.contracts.availability`; AC-011-14 … -16)

`FORECAST_CADENCE_MIN = {"synthetic": 180, "icon_d2": 180, "dmi_harmonie_arome_europe": 180}` with
`forecast_cadence(model)`, and `price_window_opens(market_key, delivery_day)` (13:00 Europe/Warsaw on that day,
in UTC; defined for `PL:RCE` only). A unit test checks the generator against them: every synthetic issue time
is a multiple of the cadence, and no publication of a day falls before the window opens on D−1. It also pins the
window on the DST days: 11:00Z on 2026-03-29 and 12:00Z on 2026-10-25 (a fixed slot offset would open an hour
late on 2026-03-29).

### 4. Assembler (AC-011-06 … -23)

Time is an integer past the mapping. `visible_step = ceil(available_at / 900 s)`. `covered = floor(watermark /
900 s)`: steps below it are covered, and 0 means nothing is.

**`mapping.py`.** Every function is total: it returns None on any exception, and the caller counts it.
- `parse(raw) -> Event | None`.
- `reading(event) -> Reading(stream, t, event_id, kwh, skipped) | None`. None for a resolution other than PT15M
  or an entry above the bound. An entry counts only if it is `active_energy`, `interval_sum`, in `kWh` and not
  `quality: missing`; `skipped` is the number of other entries of a wanted asset type.
- `curve(event) -> CurveIn(event_id, t0, day, rce_pln_per_kwh, issued_s, issued_step, opens_step) | None`. None
  for a market without schedule constants, or a curve that is not `PLN/MWh` at PT15M, or a price beyond the
  bound.
- `run(event) -> RunIn(issue_s, usable_step, next_covered) | None`. None for an unknown model.
- Today's producers emit only the accepted forms.

**`meter_stream.py`.** `MeterStream(closed, newest, last_kwh, slots, counters…)`; `slots` is a sorted tuple of
`(t, event_id, kwh)`. `Closed(t, kwh, estimated)` and `Late(t, kwh, at_step)` are its outputs.

```python
def meter_apply(stream, first_step, end_step, late_slots, r) -> tuple[MeterStream, tuple[Closed, ...], Late | None]:
    # 1 t < first_step: before_start.  t >= end_step: after_end.
    # 2 t <= newest − 2L: too_old.
    # 3 slot filled: duplicate (same id) or conflict.
    # 4 store; if t < closed: Late(t, kwh, at_step = newest + 1), newest taken before step 5.
    # 5 newest = max(newest, t).
    # 6 while slot `closed` is filled or newest >= closed + L:
    #       Closed(closed, reading or last_kwh flagged estimated); last_kwh = that value; closed += 1
    # 7 prune slots <= newest − 2L.
```

**`sparse_streams.py`.**

```python
def forecast_apply(s: ForecastStream, r: RunIn) -> tuple[ForecastStream, Run | None]:
    # issue not newer than the newest accepted: stale, no output
    # visible_step = max(r.usable_step, s.covered), counted `clamped` when the watermark moved it
    # covered = max(covered, r.next_covered); seq += 1
def price_apply(s: PriceStream, c: CurveIn) -> tuple[PriceStream, Curve | None]:
    # day older than the newest: old_day.  same day and id already seen: duplicate.
    # visible_step = max(c.issued_step, s.covered), counted `clamped` when the watermark moved it
    # a second curve of the newest day is counted `revised`
    # a new newest day: covered = max(covered, c.opens_step); its ids start again.  seq += 1
```

**`join.py`.** `Join(next, consumption, production | None, price_covered, curves, forecast_covered, runs,
counters…)`; each `MeterSide` holds the stream, the closed values not yet released (`ready`) and its `Late`
readings. `start(params)`, `on_reading`, `on_curves` and `on_run` each end in the release loop:

```python
while every needed side has closed `next` and price_covered > next and forecast_covered > next:
    price = the price of `next` (below)
    Step(next, pv, load, price, flags, issue of the last run with visible_step <= next,
         corrections = production's Late with at_step == next, then consumption's; both removed)
    next += 1
# then drop curves of delivery days more than two days before the day of `next`,
# and runs older than the last one visible at `next`
```

The price of step `q`, among curves with `visible_step ≤ q`:
1. a curve of `q`'s delivery day: slot `q − t0` of the one with the greatest `issued_s` (ties: lowest `seq`);
2. otherwise the statutory replacement (RES act art. 4b ust. 10): the nearest earlier delivery day, at most two
   days back, whose curve has `q`'s local clock quarter-hour (the first of the two copies on a 100-slot day;
   the 92-slot day lacks 02:00–02:59, so the search goes one day further), flagged `price_replaced`;
3. otherwise 0.0, flagged `price_replaced`.

The two-day limit keeps the state bounded and covers both DST days; beyond it the statute's "nearest preceding
day" is not followed (ADR 0018). Pruning keeps every curve a later step can need: `next ≤ q`, so the days kept
around `next` include the two before `q`'s day. A curve applied later than a step's release has `visible_step`
past that step, so the lookup does not depend on arrival order. A production reading for a site without PV is
counted and ignored.

**`offline.py`.** `fold(params, production, consumption, forecasts, prices) -> Offline` takes each stream's raw
records in partition order, folds prices, forecasts, production and consumption in that order through the
functions above, and runs `stepper.step` per strategy with the step's corrections and `lag = 2L`. It returns the
steps, each strategy's `RefState`, kept flows and closed months, and the final `Join`.

### 5. Actors (AC-011-24 … -36)

**`shell.py`.**

```python
@dataclass(frozen=True)
class Send: service: str; handler: str; key: str; message: object

async def configure(ctx, params, start, state_type):
    # params.household_key != ctx.key(), or stored params differ: TerminalError
    # stored and equal: nothing
    # else: store params, `n_at_configure` (the record counter found, Kafka-fed objects), state = start(params); send
async def handle(ctx, fn, state_type, message, default=None):
    # no params and no default: TerminalError.  state, sends = fn(params, state, message); store; send
async def ingest(ctx, raw, to_input, fn, state_type, default=None):
    # log_step; if configured (or default) and the record maps: transition; n last
```

`send` encodes each message with its `DataclassSerde` and calls `ctx.generic_send`. `Tariff` passes
`default=TariffState()`: it is keyed by market and never configured. Every shared handler returns the stored
`state` through its `DataclassSerde` (`output_serde`).

**`serde.py`.** `DataclassSerde(tp)`: `TypeAdapter(tp).dump_json` and `validate_json`; empty bytes mean None.

**`reorder.py`.**

```python
@dataclass(frozen=True)
class Inbox[M]:
    expected: int | None = None                # None until the start is known (the price snapshot)
    held: tuple[tuple[int, M], ...] = ()       # early messages, sorted by number
    reordered: int = field(default=0, compare=False)
    stale: int = field(default=0, compare=False)

def in_order(inbox, number, message) -> tuple[Inbox[M], tuple[M, ...]]   # the messages now ready, in order
def begin(inbox, expected) -> tuple[Inbox[M], tuple[M, ...]]             # set the start, release what is held
```

**Start states** (`start(params)` of each actor; `Tariff` starts from its default):

| Actor | Start |
|---|---|
| `Household` | `Join(next=first_step)` with a consumption side and, if `has_pv`, a production side, each `closed=first_step`, `last_kwh=0.0`; both watermarks 0; price inbox `expected=None` (the snapshot sets it); forecast inbox `expected=1`; sends `Subscribe(t=first_step, household_key)` to `Tariff` at `market_key` |
| `Tariff` | `TariffState()`: empty stream (`covered` 0, `seq` 0), no curves, no subscribers |
| `Weather` | `ForecastStream(covered=0, newest_issue_s=None, seq=0)`; its first accepted run gets `seq` 1 |
| strategy | `ref = initial_state(model, first_step, initial_soc_kwh)`; `recent=()`; `months=()`; step inbox `expected=first_step` |
| `Ems` | `soc_kwh=initial_soc_kwh`; `last_pv_kwh = last_load_kwh = 0.0`; `sent = first_step − 1`; step and acknowledgement inboxes `expected=first_step`; nothing waiting |
| `Battery` | `soc_kwh=initial_soc_kwh`; command inbox `expected=first_step`; `clamped=0` |
| `Meter` | `acc=SettleAcc(first_step, (), (), ())`; `recent=()`; `months=()`; flow inbox `expected=first_step` |

**Transitions**, all `fn(params, state, message) -> (state, sends)`:

| File | Transition | What it does |
|---|---|---|
| `household.py` | `on_reading`, `on_prices`, `on_forecast` | Apply to the join (updates through their inbox; a snapshot calls `begin`); one `Step` per released step to each strategy and to `Ems`; update `leads` with the record's slot (a reading: `t + 1`; a curve or run: its `visible_step`) |
| `tariff.py` | `on_curve` | `price_apply`; an accepted curve goes to every subscriber as `PriceUpdate`; an unusable curve is counted |
| | `subscribe` | A new subscriber is added and gets the snapshot (every accepted curve, `seq` = the stream's); a known subscriber gets nothing |
| `weather.py` | `on_run` | `forecast_apply`; an accepted run goes to the household as `ForecastUpdate`; an unusable run is counted |
| `strategy.py` | `on_step` | Inbox by `t`; `stepper.step` with `POLICIES[name]`, the kept flows, the corrections and `lag = 2L`; closed months appended |
| `ems.py` | `on_step`, `on_ack` | Steps and acknowledgements through their inboxes (an `Ack` sets `soc_kwh`). While the first waiting step has `t == acks.expected` and `t > sent`: `prices`, `decide` with `POLICIES[live]`, send `Apply`, `sent = t`, keep its PV and load for the next observation |
| `battery.py` | `on_apply` | Inbox by `t`; `reference.apply`; a command that `apply` changed is counted in `clamped`; `Ack` to `Ems`, `Flows` to `Meter` |
| `meter.py` | `on_flows` | Inbox by `t`; `prices`, `grid_kwh`, `stepper.settle_step` with the corrections and `lag = 2L`; closed months appended |

**Kafka-fed handlers.** `on_measurement`, `on_forecast` and `on_price` take raw bytes (`BytesSerde`,
`accept="*/*"`). `shell.ingest` reads the Kafka coordinates, parses once, writes `log`, maps and runs the
transition when the object is configured (or has a default) and the record maps, and writes `n` last.

### 6. Run parameters (AC-011-27, -37)

```python
@dataclass(frozen=True)
class RunParams:
    run_id: str | None
    location_id: str
    household_key: str
    market_key: str  # [run_id:]location_id, [run_id:]PL:RCE
    first_step: int
    end_step: int
    late_slots: int
    strategies: tuple[str, ...]
    live: str
    has_pv: bool
    initial_soc_kwh: float
    model: SiteModel
    rcem: RcemTable
    schema_version: str = "1.0.0"
```

`__post_init__` checks: `first_step` on a UTC hour, `end_step` after it, `late_slots` in 1…48, `live` among
`strategies`, every strategy in `POLICIES`, `initial_soc_kwh` within the battery's SoC bounds, and
`location_id` equal to `model.location_id` and to the end of `household_key`.

`runs.managed_params(run_id, start, days, locations, late_slots, strategies, live, generator_config)` loads the
generator config and catalog as `ems-gen run` does, keeps the EMS-managed sites, and builds `SiteModel` with
`ems.tariffs.site_model` and the RCEm table with `load_rcem`. It then evaluates `reference.prices` for every step
of `[first_step, end_step)` and `fixed_charges` for every civil month of it, so a missing tariff version or RCEm
entry fails before anything is sent. It returns `{household_key: RunParams}`.

### 7. Harness (`src/ems/experiments/stack.py`; AC-011-44, -45, -47)

| Today | Change |
|---|---|
| `PROBE_TOPICS` | `INGRESS_TARGETS = {"ingress-meters": ("Household", "on_measurement"), "ingress-forecasts": ("Weather", "on_forecast"), "ingress-prices": ("Tariff", "on_price")}` and the topic → service map derived from it |
| `handled`, `wait_handled` | Sum `n` over the three Kafka-fed services; an optional `services=` for the gated runs |
| `probe_states`, `probe_logs` | `states(run_id, service) -> {key: {name: text}}`, `ingress_logs(run_id) -> {(service, key): log}` |
| `invocations` | All services of the run, with `status`, `completion_result`, `last_failure` |
| `offline_logs` | Grouped by (service of the topic, key): `Household` and `Weather` share keys |
| `publish_backfill` | `config=` passes `--config` to `ems-gen run`; `days` takes fractions (30 h = 1.25) |
| `IngressSet.up` | `services=` to start a subset (the gated arrival orders) |
| `register(force=False)` in `StackEnv.open` | `ensure_registered()` (Plan decisions) |
| new | `configure(restate, params)`: one `send` per object with the `Idempotency-Key` `<household_key>:<service>:configure`; `wait_run(run_id, records, ends, strict=True)` (Plan decisions) |

`tests/stack_fixtures.py` gains a helper that configures a run, publishes, waits and returns the decoded states
with the offline result; on a failed comparison it writes the records and parameters under `logs/tests/`.

### 8. Compose, Makefile, registration (AC-011-25, -44)

- `infra/docker-compose.yml`: `RESTATE_RECORD_MAPPER_SERVICE` moves from the shared anchor into the three
  services (`Household`, `Weather`, `Tariff`); the handler names stay.
- `make register`: the POST as today, then three `curl -fsS …/services/<Service>/handlers/<handler>` checks, then
  the ingress containers. `FORCE=true` remains the development way to replace a deployment.
- The stale "actor skeleton" lines of the Makefile's planned block are removed.

### 9. Jobs (`jobs/ingress_measure.py`; AC-011-46)

`skew` configures the managed sites, runs the unpaced backfill and reports from each managed `Household`: the
largest lead per stream pair, prices included, and per receiving service the summed `reordered` and `stale`. Its
manifest records `late_slots`, the strategies and the live strategy. `backlog` uses the generic `handled` and
`invocations`. `state-cost` and its objects are removed; its result stays under `experiments/results/` and needs
the spec 010 commit to reproduce.

## Actor contract

### Actors

Keys are `[run_id:]location_id`, except `Tariff`: `[run_id:]market_key`. Every configured object also holds
`params: RunParams`; Kafka-fed ones also `log`, `n` and, once configured, `n_at_configure`.

| Actor (Virtual Object) | Key | State (field: type, unit) | Owner of facts | New / changed |
|---|---|---|---|---|
| `Household` | location | `state: HouseholdState{join: Join, prices: Inbox[PriceUpdate], forecasts: Inbox[ForecastUpdate], leads (compare=False)}`; `Join{next: int [t], consumption: MeterSide, production: MeterSide \| None, price_covered: int [t], curves: (Curve…), forecast_covered: int [t], runs: (Run…), price_replaced, unneeded, unusable: int}`; `MeterSide{stream: MeterStream, ready: (Closed{t, kwh [kWh], estimated}…), late: (Late{t, kwh [kWh], at_step}…)}`; `MeterStream{closed: int [t], newest: int \| None, last_kwh: float [kWh], slots: ((t, event_id, kWh)…), before_start, after_end, too_old, duplicate, conflict, estimated, late, skipped: int}` | The household's step sequence, `estimated` flags and what each step saw | new |
| `Tariff` | market | `state: TariffState{stream: PriceStream{covered: int [t], newest_t0: int \| None, newest_ids: (str…), seq: int, duplicate, old_day, clamped, revised: int}, curves: (Curve…), subscribers: (str…), unusable: int}` | Price curves, their visibility, the price watermark | new |
| `Weather` | location | `state: WeatherState{stream: ForecastStream{covered: int [t], newest_issue_s: int \| None [s], seq: int, stale, clamped: int}, unusable: int}` | Forecast runs, their visibility, the forecast watermark | new |
| `NoBattery`, `SelfConsumption` | location | `state: StrategyState{result: StrategyResult{state: RefState{soc_kwh, last_pv_kwh, last_load_kwh: float [kWh], acc: SettleAcc}, recent: (Flow…), months: (MonthSettlement…)}, steps: Inbox[Step]}` (`StrategyResult` is shared with the offline fold, so one function steps both) | Virtual SoC, shadow ledger and bills | new |
| `Ems` | location | `state: EmsState{soc_kwh: float [kWh], last_pv_kwh, last_load_kwh: float [kWh], sent: int [t], steps: Inbox[Step], acks: Inbox[Ack], waiting: (Step…)}` | The action commanded at each step; the live strategy in force | new |
| `Battery` | location | `state: BatteryState{soc_kwh: float [kWh], commands: Inbox[Apply], clamped: int}` | Real SoC; the power executed at each step | new |
| `Meter` | location | `state: MeterState{acc: SettleAcc, recent: (Flow…), months: (MonthSettlement…), flows: Inbox[Flows]}` | Grid energy per step, the real ledger, bills and deposit | new |
| `Probe`, `StateCostEager`, `StateCostLazy` | | | | removed |

### Handlers

| Actor.handler | Kind | Input message (version) | Writes | Sends (one-way) | Calls (request-response, shared only) |
|---|---|---|---|---|---|
| `Household.configure` | exclusive | `RunParams` 1.0.0, from the operator through the HTTP ingress | `params`, `n_at_configure`, `state` | `Tariff.subscribe: Subscribe` | none |
| `Household.on_measurement` | exclusive | raw bytes of a measurement event (wire v1), from `ingress-meters` | `log`, `state`, `n` | `on_step: Step` to each strategy and to `Ems` | none |
| `Household.on_prices` | exclusive | `PriceUpdate` 1.0.0 | `state` | `on_step: Step` as above | none |
| `Household.on_forecast` | exclusive | `ForecastUpdate` 1.0.0 | `state` | `on_step: Step` as above | none |
| `Household.status` | shared | none; returns `HouseholdState` | none | none | none |
| `Tariff.on_price` | exclusive | raw bytes of a price event (wire v1), from `ingress-prices` | `log`, `state`, `n` | `Household.on_prices: PriceUpdate` to every subscriber | none |
| `Tariff.subscribe` | exclusive | `Subscribe` 1.0.0 | `state` | `Household.on_prices: PriceUpdate` (snapshot) | none |
| `Tariff.status` | shared | none; returns `TariffState` | none | none | none |
| `Weather.configure` | exclusive | `RunParams` 1.0.0 | `params`, `n_at_configure`, `state` | none | none |
| `Weather.on_forecast` | exclusive | raw bytes of a forecast event (wire v1), from `ingress-forecasts` | `log`, `state`, `n` | `Household.on_forecast: ForecastUpdate` | none |
| `Weather.status` | shared | none; returns `WeatherState` | none | none | none |
| `<Strategy>.configure` | exclusive | `RunParams` 1.0.0 | `params`, `state` | none | none |
| `<Strategy>.on_step` | exclusive | `Step` 1.0.0 | `state` | none | none |
| `<Strategy>.summary` | shared | none; returns `StrategyState` | none | none | none |
| `Ems.configure` | exclusive | `RunParams` 1.0.0 | `params`, `state` | none | none |
| `Ems.on_step` | exclusive | `Step` 1.0.0 | `state` | `Battery.apply: Apply` | none |
| `Ems.on_ack` | exclusive | `Ack` 1.0.0 | `state` | `Battery.apply: Apply` | none |
| `Ems.status` | shared | none; returns `EmsState` | none | none | none |
| `Battery.configure` | exclusive | `RunParams` 1.0.0 | `params`, `state` | none | none |
| `Battery.apply` | exclusive | `Apply` 1.0.0 | `state` | `Ems.on_ack: Ack`, `Meter.on_flows: Flows` | none |
| `Battery.state` | shared | none; returns `BatteryState` | none | none | none |
| `Meter.configure` | exclusive | `RunParams` 1.0.0 | `params`, `state` | none | none |
| `Meter.on_flows` | exclusive | `Flows` 1.0.0 | `state` | none | none |
| `Meter.bill` | shared | none; returns `MeterState` | none | none | none |

### Messages

All new at 1.0.0; every one ends with `schema_version: str = "1.0.0"`. `t` is the step a message belongs to,
or, for an update, its stream's watermark (`covered`).

| Message | Version | Producer → consumer | Fields added or changed | Additive? | Golden examples updated? |
|---|---|---|---|---|---|
| `Step` | 1.0.0 | Household → each strategy, Ems | `t: int`, `pv_kwh`, `load_kwh: float [kWh]`, `rce_pln_per_kwh: float [PLN/kWh]`, `pv_estimated`, `load_estimated`, `price_replaced: bool`, `forecast_issue_s: int \| None [s]`, `corrections: (Correction{stream: str, t: int, kwh: float [kWh]}…)` | new | n/a: field names pinned by a test |
| `PriceUpdate` | 1.0.0 | Tariff → Household | `t: int`, `seq: int`, `snapshot: bool`, `curves: (Curve{seq: int, t0: int, day: date, rce_pln_per_kwh: (float…) [PLN/kWh], issued_s: int [s], visible_step: int}…)` | new | n/a |
| `ForecastUpdate` | 1.0.0 | Weather → Household | `t: int`, `seq: int`, `run: Run{seq: int, issue_s: int [s], visible_step: int}` | new | n/a |
| `Subscribe` | 1.0.0 | Household → Tariff | `t: int` (the household's first step), `household_key: str` | new | n/a |
| `Apply` | 1.0.0 | Ems → Battery | `t: int`, `action: Action`, `pv_kwh`, `load_kwh: float [kWh]`, `rce_pln_per_kwh: float [PLN/kWh]`, `corrections` | new | n/a |
| `Ack` | 1.0.0 | Battery → Ems | `t: int`, `soc_kwh: float [kWh]` | new | n/a |
| `Flows` | 1.0.0 | Battery → Meter | `t: int`, `pv_kwh`, `load_kwh: float [kWh]`, `p_kw: float [kW]`, `rce_pln_per_kwh: float [PLN/kWh]`, `corrections` | new | n/a |
| `RunParams` | 1.0.0 | operator → each object of a household | §6; no `t` (its time is `first_step`) | new | n/a |

### Timers

| Timer | Owner | Sim mode (data in state, fired at its step) | Live mode (`send_delay`) | Generation guard |
|---|---|---|---|---|
| none | | none | none (the live deadline is stage 4) | n/a |

### Failure modes

| Failure | Expected behaviour | Test (tier) |
|---|---|---|
| SDK service `kill -9` mid-replay | Restate retries the interrupted invocations; replayed writes and sends match the journal because the bytes of values and transitions are deterministic; states end equal to the offline side | AC-011-43 (fault); AC-011-33 (unit) |
| Restate server `kill -9` mid-replay | As in spec 010 for the ingress; the actors' state is journaled. Not repeated with configured households in this stage (stage 4's kill matrix) | AC-010-13 via AC-011-47 (fault) |
| Duplicate delivery | A redelivered Kafka record never reaches the handler. An event published twice is dropped by the slot table, the newest issue time or the day's ids. A handler's own sends are deduplicated by Restate. An equal second `configure` changes nothing | AC-011-13, -14, -16 (unit, property); AC-011-27 (integration); AC-011-40 (e2e) |
| Out-of-order or late message | Applied in `t` or `seq` order; early ones held, stale ones dropped and counted, re-orderings counted | AC-011-30, -36 (property); AC-011-39 (e2e) |
| Late or missing reading | Waited for `L` slots, then estimated; a late reading corrects the ledger; older than `2L` is counted | AC-011-08 … -12 (unit, property); AC-011-40, -41 (e2e) |
| A record before `configure` | A Kafka record at an unconfigured `Household` or `Weather` is only logged and counted; `configure` stores the count it finds (`n_at_configure`), and the harness asserts 0, because it configures before it publishes | AC-011-28, -42 (e2e) |
| Internal message at an unconfigured object, or a different second `configure` | `TerminalError`: the invocation fails once and later invocations of the key still run. The lost `t` or `seq` is never filled, so the receiver holds every later message and the run cannot finish; `wait_run` fails at once on the failed invocation | AC-011-27, -29 (integration); AC-011-45 (unit) |
| A transition raises (a defect) | Restate retries and holds the key; `wait_run` fails at once on the recorded failure | AC-011-45 (unit on canned rows); AC-011-21 (transitions never raise) |
| Stale view | `Ems` decides `t` only from the acknowledgement of `t − 1`, so its SoC view is never stale | AC-011-35, -36 |
| Bad or unmappable record | Counted; the handler returns | AC-011-26 (property over mutated events; integration via AC-011-47) |
| Registration of a new deployment while traffic flows | Not done in this stage: `make register` and `ensure_registered` run before ingestion opens. Adding a strategy at run time is stage 4 | n/a |
| Stale registration | `make register` stops before ingestion; the harness replaces the deployment | AC-011-44 (unit; by hand on the pinned server) |

### Rule check

| Rule | Holds? | Evidence |
|---|---|---|
| R1/R2: exclusive handlers reached only by one-way messages; request-response only to shared handlers | yes | Every send is `ctx.generic_send`. `configure` arrives as a one-way send from the operator. `test_app.py` checks that every `Send` target exists and that `src/ems/actors/` contains no `object_call`, `service_call` or `generic_call` |
| R3: non-determinism journaled | yes | Handlers make no external call and read no clock, random source or file (`test_app.py` scan) |
| R4: idempotent ingress | yes | Offset dedup; first reading wins in the slot table; `Idempotency-Key` on `configure` |
| R5: no waiting in exclusive handlers | yes | Waiting is `ready` values, held messages and `Ems.waiting` in state; no `sleep` (`test_app.py` scan) |
| R6: generation numbers on self-rescheduling jobs | n/a | No such job in this stage |
| R7: envelope fields; messages applied in `t` or sequence order, early ones held back, reorderings counted | yes, reworded | `schema_version` and `t` on every message, `seq` on updates; one `in_order` function at every receiver |
| Ordering (ADR 0017): steps released only when every input watermark covers them; lateness and dedup judged inside the record's own stream | yes | `join` release loop; `meter_apply` sees one stream only; AC-011-21 |
| Single writer per fact (ownership table updated) | yes | The table in 01 keeps its owners; the ingress log row names the Kafka-fed actors; one new row for run parameters |

## Risks (`09-risks-and-tech-debt.md`)

| Id | Change |
|---|---|
| K7 | Built: the join of the two measurement topics |
| A1 | Exercised by the arrival-order runs (a whole day of one stream buffered); stays accepted; `make measure MODE=skew` reports the leads |
| A2 | The constants are in `ems.contracts.availability` with a generator test. Observed publications are never before 13:17 local, but "never before 13:00" stays VERIFY: the publication terms of IRiESP-OIRE are unread |
| A3 | Rule decided: the statutory replacement (art. 4b ust. 10), flagged and counted |
| K12 | Tightened: `make register` checks the three target handlers before ingestion opens |
| L3 (reworded) | "The RES act defines no RCE correction procedure" is wrong in effect: Prawo energetyczne art. 11y ust. 1 pkt 7–8 obliges PSE to publish corrected RCE and RCEm when inputs change the result by more than 2 % (Dz.U. 2024 poz. 1847). The rest of L3 stands |
| A8 (new) | A month's bill and tranche appear `2L` quarter-hours after the month ends; a run must extend that far past a month it wants closed. Three consequences: (1) the spring-2026 recording ends 8 quarter-hours after June, so at `L` = 8 June never closes (run it with `late_slots` ≤ 4, or rebuild with END ≥ 2026-07-02 while archived ICON-D2 runs are retrievable, L1); (2) December 2026 cannot close at a lag above 0, because the tariffs end with 2026 (D9); (3) an RCEm-basis site needs the RCEm entry of m+1 for the lag steps. Accepted (ADR 0018) |
| A9 (new) | An object's whole `state` is rewritten every step (up to about 160 kB at a month's end, 0.7 ms of codec time), and closed months accumulate in strategies and `Meter` (one entry a month). Accepted; stage 4 measures it and may split the value |
| A10 (new) | `Tariff` keeps every accepted curve of a run (about 1.5 kB a day) for late subscribers. Accepted |
| A11 (new) | Bytes that differ between processes would fail a retry with RT0016. Mitigated: no set in any stored type, byte-determinism tests of values and of a transition trace, one kill test |
| A12 (new) | A quarter-hour without a visible price uses the statutory replacement for the decision and the bill alike, while the legal bill would use the RCE published later; the replacement looks back at most two days. Accepted (ADR 0018); results report `price_replaced` |
| K13 (new) | Removing a service means removing its deployment; a stale registration with re-pointed ingress would skip records. Mitigated by the guards of AC-011-44 |

## Experiment impact

No reported number changes. `make measure MODE=skew` gains the price leads and the re-order counters; one run
after the tiers pass resolves V5 in 01 with its result file.

## Test strategy

Tests are written first by `test-engineer`. The Restate tiers use the fixtures of spec 010.

| Tier | Tests | Criteria |
|---|---|---|
| unit | `domain/test_quarter_split.py`, `domain/test_corrections.py` (the three cases of `correct`, the lagged month close across the October close, lag 0 equal to spec 009); `domain/test_calendar.py` with holidays as a tuple; `contracts/test_availability.py` (constants against the generator, the DST instants) | AC-011-01 … -05, -14, -16 |
| unit | `assembler/test_purity.py`, `test_params.py`, `test_mapping.py` (units, resolution, bounds), `test_meter_stream.py`, `test_sparse_streams.py`, `test_join.py` (DST days, replacement price across both DST days, no PV), `test_offline.py` (a clean generated day against `reference.run`); `tests/unit/test_post_edit_hook.py` (a Restate import under `src/ems/assembler/` is flagged) | AC-011-06 … -20, -22, -23 |
| unit | `actors/test_reorder.py`, `test_messages.py` (field names, nested types included), `test_serde.py`, `test_byte_determinism.py` (two subprocesses with different hash seeds: values, and a fixed trace of transitions with their sends), `test_app.py` (services, handler kinds, `Send` targets, the source scan), `test_transitions.py` (start states, `Ems` waits for the acknowledgement, `Battery` clamps, the snapshot, a known subscriber, unusable runs and curves, the `configure` checks), `test_ingress_check.py` | AC-011-24, -27, -30 … -35 |
| unit | `experiments/test_runs.py`; `test_stack_harness.py` (wait and registration predicates on canned rows); `infra/test_compose_restate.py`, `infra/test_makefile_targets.py`, the fault fixture against `config/generator.yaml`; `test_ingress_measure_job.py` | AC-011-25, -37, -44 … -46 |
| property | `test_meter_stream.py` (closed once and in order, on-time readings never estimated, `Late.at_step` in range, a duplicate changes nothing, at most `2L` slots, pruning changes no output); `test_corrections.py` (rows equal `hour_rows` over final flows; months equal `settle_month`; lag only delays; `correct` never misses a flow); `test_reorder.py`; `test_serde.py`; `test_ingress_check.py` extended: parse, mapping and one transition per Kafka-fed actor never raise on arbitrary bytes or mutated events | AC-011-03 … -05, -08 … -13, -26, -30, -33 |
| property | `test_interleaving.py`: any order-preserving merge of the four streams, any delivery order of updates and any subscription point give the steps and the final `Join` of `offline.fold`; prefixes release prefixes; no step reads the future; nothing raises | AC-011-21, -22 |
| property | `test_network.py`: all transitions as an in-memory network (`tests/network.py`) with an arbitrary delivery order: strategy states equal the offline side; `Battery` and `Meter` equal SelfConsumption | AC-011-36 |
| integration | `test_ingress_delivery.py`, `test_ingress_bad_records.py`, `test_ingress_prepublish.py`, `test_stack_lifecycle.py` (re-pointed, no household configured); `test_actors.py`: `configure` twice and with other parameters, an internal message at an unconfigured object, one site × 6 h through every edge, the shared handlers | AC-011-25 … -29, -31, -47 |
| e2e | `test_ingress_backfill.py` (AC-010-09 on the real services); `test_actor_backfill.py`: clean 2026-10-31 × 30 h (October closes); meters first and price first (2026-10-20 × 1 day); faults at `L` = 8 and at `L` = 2. Each asserts the offline counters first, then household, strategy, `Battery` and `Meter` states, parity, ingress logs, unmanaged keys under 1 kB, `n_at_configure` 0, no failed invocation | AC-011-38 … -42, -47 |
| fault | `test_ingress_kills.py` (spec 010's four tests, re-pointed); `test_actor_kills.py` (SDK `kill -9` during a configured run, `wait_run` in lenient mode) | AC-011-43, -47 |
| by hand, output in the pull request | `make register` on a volume that still holds the spec 010 deployment; `make measure MODE=skew` and `MODE=backlog` | AC-011-44, -46 |

## Docs and ADRs

| File | Change |
|---|---|
| `docs/adr/0018-late-and-missing-inputs.md` (new) | Hour rows re-fire within the allowed lateness and the month closes after it; the estimate; the statutory replacement price (art. 4b ust. 10) and its two-day limit, and that the legal bill would use a later-published price; unknown models; first reading wins (narrows ADR 0017's revision sentence to prices). Alternatives: settle only when final; drop a correction after its month closed; the other estimates; price 0 and why it was dropped |
| `docs/adr/0019-run-parameters-as-control-messages.md` (new) | `configure` before data, stored once per object, a different second one refused. Alternatives: a run file read by the service; a config actor |
| `docs/adr/README.md` | Index rows |
| `00-overview.md` | Status; a dated entry linking both ADRs and recording the R7 rewording and one sequence number per `Tariff`; stage table (stage 3 done, "arriving in different orders"); repo map (`assembler/`) |
| `01-architecture.md` | Rules as built (integers, first reading wins, older sparse records ignored, corrections and month close, the replacement price, one sequence number per `Tariff`); the state, handlers and messages tables; the ownership rows; R7 wording; OPEN items closed; V5 with its value; the stage 3 done-when wording; the renamed test files (stage 2 notes, V1, V2); the stage 4 brief (bill windows need `2L` more steps); the stage 5 brief (export valuation of a site without PV, which is not a prosumer) |
| `02-subsystem-actors.md` | The handlers; the shell and why; serde and the no-sets rule; state keys; registration and removal of services; `make measure` modes; "How R1–R7 hold" for stage 3 |
| `05-deploy-and-infra.md` | The registration guard |
| `06-data-flows.md` | Faults also hit forecasts and duplicate prices; corrections and the month close |
| `07-design-principles.md` | P2 names `src/ems/assembler/` beside the domain; R1–R7 now specified in 02 |
| `08-contracts.md` | The internal messages and envelope; the schedule constants; first reading wins (the revision sentence of the handoff); the open question closed |
| `09-risks-and-tech-debt.md` | The rows above |
| `11-experiments.md` | The first comparison through Kafka; the V5 measurement; bill windows are whole civil months plus `2L` quarter-hours; every energy result reports its estimate, correction and `price_replaced` counts |
| `12-domain-model.md` | The split; corrections; the lagged month close; the estimate; holidays as a tuple; the replacement price with art. 4b ust. 10 in the valuation rules; under D6, that a corrected quarter keeps the battery power resolved on the estimate, so the mode-action invariants hold for the step's inputs, not for the corrected flows |
| `13-subsystem-ingest.md` | "What to build": stage 3 is built |
| `docs/thesis/kafka-ingress.md` | The renamed test files; the state-cost result needs the spec 010 commit |
| `README.md`, `CLAUDE.md` | Commands (`measure` modes), the area list and rules table (assembler), the "Do not" line and the hook sentence (the assembler is pure too), the status example |
| `.claude/rules/actors.md`, `.claude/rules/assembler.md` (new), `.claude/hooks/post-edit.sh` | R7 wording and the file layout; the assembler's purity rules; the hook pattern |
| `.claude/skills/ems-actor-contract/SKILL.md`, `.claude/skills/ems-plan-change/checklists.md`, `.claude/skills/ems-context/SKILL.md` | R7 wording; the ems-context row for 02 (handlers are built) |

## Review

Both reviews read the spec and the first draft of this plan on 2026-11-20.

**`energy-domain-reviewer`**: no blocker; four should-fix findings, all folded in.

| Finding | Change |
|---|---|
| Price 0 has no source and is not neutral between strategies; RES act art. 4b ust. 10 sets a replacement; PSE published two days only on the day itself | User decision: the statutory replacement (spec Decisions, AC-011-19, §4, ADR 0018, risk A12) |
| The lag's cost is larger than stated: June in the spring-2026 recording, December 2026, RCEm m+1 | Risk A8; 11 and the stage 4 brief |
| No counter for a record made visible later by the watermark, or a second curve of a day | `clamped`, `revised` (AC-011-14, -16) |
| The mapping has no unit or resolution guards | AC-011-07; §4 |
| Notes: SoC bounds unchecked in `RunParams`; edge-case wording; units in two messages; L3; D6 for corrected quarters; A2 still VERIFY; site 000002 is not a prosumer | Folded in (§6, spec Edge Cases, Messages, Risks, Docs) |
| Confirmed: the lagged close changes no result; corrections fit hourly balancing; the estimate is an admissible substitute; cadences of 180 min; 13:00 as a lower bound; DST instants; month parameters of the closed month; holidays as a tuple | Facts |

**`architect-reviewer`**: approve with changes; one blocker, eight should-fix findings, twelve notes.

| Finding | Change |
|---|---|
| Blocker: `recent` inside `RefState` breaks `==` with `reference.run` (AC-011-23) and with lag 0 (AC-011-04) | `recent` beside `RefState` (§2); AC-011-04 compares the accumulator; AC-011-23 the `RefState` and quarter results |
| Contract-valid inputs could raise or produce non-finite state; SoC and RCEm unchecked before sending | Total mapping with bounds (§4); `RunParams` and `managed_params` checks (§6); the mutated-event property covers mapping and transitions |
| `wait_idle` would fail on the retries a kill causes | `wait_run` with a deterministic end marker and a lenient mode for the fault tier (Plan decisions; AC-011-45) |
| `leads` on updates had no definition | Leads live in `HouseholdState` (outside `==`), updated by the transitions with a defined slot |
| `Tariff` has no params; start values, shared outputs, `settle_step` return, `correct` on a missing flow, `Closed`/`Late` fields unspecified | Shell default state; the start table; shared handlers return the state; signatures and field lists |
| AC-011-47 left AC-010-01, -03 … -07 uncited; AC-011-02 contradicted the holidays change | Both criteria reworded |
| Missing failure rows: records before `configure`, registration while traffic flows; the unconfigured-message row was inaccurate | Rows added and corrected; `n_at_configure` |
| Docs list incomplete | Rows added (00, 01, 02, 07, 08, 11, `docs/thesis`, `CLAUDE.md`, skills) |
| `RunParams` has no `t` | Exempted (Contract changes; AC-011-32) |
| Notes: five runs, not four; hook and write-order criteria untested; household state comparison; transition byte determinism; nested field names; P7 and P14 evidence; harness gaps; revision wording; repeated subscribe; leads placement; growth; key check | Folded in. "Writes `n` last" stays a design rule (§5), not a criterion |
| Open questions: quiescence (now the end marker); `force: true` instead of delete and register (adopted); status after SIGKILL (a task); a different second `configure` (refused); a month close through Kafka (user decision: the clean run crosses October) | Folded in |

Proposed removals, and what was kept:
- `settle_quarter` stays: spec 009's tests call it, and it is the lag-0 anchor of AC-011-02.
- The shared handlers stay: 01 lists them and AC-011-24 requires them; stage 4 decides how the export reads state.
- `make measure MODE=backlog` stays: re-pointing it is a few lines and keeps a recorded measurement reproducible.
- The kill test (AC-011-43) stays: it is the only test that exercises a replay against the journal (A11).
- Redundant `Ems` fields and the buffer peaks are removed (`EmsState` keeps `sent`; `Inbox` has no `peak`).

## Complexity Tracking

| Addition | Why it is needed | Simpler option not taken |
|---|---|---|
| A new package `src/ems/assembler/` | The offline side must run the ordering code, and that code needs `ems.contracts`, which the domain's purity test forbids | Putting it in `src/ems/domain/`: the purity test would have to admit the wire models |
| A month close with a lag | The chosen model loses no correction inside the allowed lateness | Closing the month on time: a late reading from a month's last hours would be dropped |
| Two entry points to the ledger (`settle_quarter`, `settle_step`) | `settle_quarter` is spec 009's tested function and the lag-0 anchor; `settle_step` adds corrections and the lag | Removing `settle_quarter`: spec 009's tests would change with it |
| The replacement price | The statute's rule; zero would bias the strategy comparison | Price 0 (first decision) |
| An in-memory network test | On the real stack messages almost never arrive out of order, so the hold-back code would run untested | Relying on the e2e runs alone |
| A fault fixture with raised rates | At the configured rates a small run may contain no correction | A run twelve times larger for the same evidence |
| A registration guard | A stale deployment with re-pointed ingress loses records silently (K12) | Documenting `FORCE=true` only |
