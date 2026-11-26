"""Export a run's object state to Parquet (spec 012, AC-012-26 … -28; P10: a CLI, no HTTP surface).

State is read only through SQL on Restate's `state` table and decoded with the actors' serde. Per
household, per strategy window (and ``real`` for `Battery` and `Meter`) and per closed month the
tables hold what the objects hold. Per-step series come from the offline fold (``offline.trace``),
written only once the fold's final state equals the actors' (the exactly-once check). Every table
carries ``ems.schema_version`` in its Parquet metadata.

    python -m ems.experiments.export --run-id <run> --out <dir>
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ems.actors.battery import BatteryState
from ems.actors.household import HouseholdState
from ems.actors.meter import MeterState
from ems.actors.strategy import StrategyState
from ems.assembler.offline import Offline, trace
from ems.assembler.params import RunParams
from ems.assembler.schedule import windows
from ems.domain.calendar import civil_month
from ems.domain.settlement import costs
from ems.experiments.metrics import Due, Line, releases

SCHEMA_VERSION = "1.0.0"
TABLES = ("households", "strategies", "months", "steps", "timings", "lags")
REAL = "real"  # the strategy name of the real battery and meter in the tables


@dataclass(frozen=True)
class RunStates:
    """A run's decoded state, by household key."""

    params: dict[str, RunParams]  # each managed household's parameters (from `Household`)
    households: dict[str, HouseholdState]
    strategies: dict[str, dict[str, StrategyState]]  # strategy name -> key -> state
    strategy_params: dict[str, dict[str, RunParams]]  # strategy name -> key -> its parameters
    batteries: dict[str, BatteryState]
    meters: dict[str, MeterState]
    records: dict[str, int]  # `Household` key -> records handled (`n`)


def read_states(restate: Any, run_id: str) -> RunStates:
    """Every managed object of the run, from the SQL `state` table (one query per service)."""
    from ems.actors.serde import serde_for
    from ems.domain.actions import POLICIES

    params_serde = serde_for(RunParams)

    def decoded(service: str, tp: type[Any]) -> tuple[dict[str, Any], dict[str, RunParams]]:
        states: dict[str, Any] = {}
        params: dict[str, RunParams] = {}
        for key, values in restate.states(run_id, service).items():
            if "params" not in values or "state" not in values:
                continue  # an unmanaged site keeps only its log and counter
            p = params_serde.deserialize(values["params"].encode())
            assert p is not None
            params[key] = p
            states[key] = serde_for(tp).deserialize(values["state"].encode())
        return states, params

    households, params = decoded("Household", HouseholdState)
    records = {
        key: int(values["n"])
        for key, values in restate.states(run_id, "Household").items()
        if "n" in values
    }
    batteries, _ = decoded("Battery", BatteryState)
    meters, _ = decoded("Meter", MeterState)
    strategies: dict[str, dict[str, StrategyState]] = {}
    strategy_params: dict[str, dict[str, RunParams]] = {}
    for name in sorted(POLICIES):
        states, sp = decoded(name, StrategyState)
        if states:
            strategies[name], strategy_params[name] = states, sp
    return RunStates(params, households, strategies, strategy_params, batteries, meters, records)


def household_rows(run_id: str, states: RunStates) -> list[dict[str, Any]]:
    """One row per managed household: window, next step, records, the meter counters per side,
    ``after_end_late``, ``price_replaced``, the clamp counters, changes and refusals, and in live
    mode the deadline counters.
    """
    rows: list[dict[str, Any]] = []
    for key in sorted(states.params):
        p, h = states.params[key], states.households[key]
        j = h.join
        row: dict[str, Any] = {
            "run_id": run_id,
            "household_key": key,
            "location_id": p.location_id,
            "has_pv": p.has_pv,
            "mode": p.mode,
            "first_step": p.first_step,
            "end_step": p.end_step,
            "next_step": j.next,
            "records": states.records.get(key, 0),
        }
        for side_name, side in (("consumption", j.consumption), ("production", j.production)):
            for counter in ("estimated", "late", "too_old", "duplicate", "conflict"):
                row[f"{side_name}_{counter}"] = (
                    None if side is None else getattr(side.stream, counter)
                )
        row |= {
            "after_end_late": j.after_end_late,
            "price_replaced": j.price_replaced,
            "clamped_price": j.clamped_price,
            "clamped_forecast": j.clamped_forecast,
            "changes": len(h.schedule.changes),
            "refused": len(h.schedule.refused),
            "deadline_firings": h.clock.firings,
            "deadline_early": h.clock.early,
            "deadline_stale": h.clock.stale,
        }
        rows.append(row)
    return rows


def _partial(start: int, stop: int) -> bool:
    """A window that does not run whole civil months."""
    return civil_month(start - 1) == civil_month(start) or civil_month(stop - 1) == civil_month(
        stop
    )


def strategy_rows(run_id: str, states: RunStates) -> list[dict[str, Any]]:
    """One row per household and strategy window, plus ``real`` (Battery and Meter):
    ``start_step``, ``stop_step``, ``compare_until_step`` (a removed strategy:
    ``4 · ⌊(stop - 2L) / 4⌋``), ``next_step``, ``start_soc_kwh``, ``end_soc_kwh``,
    ``months_closed``, ``bill_pln``, ``deposit_closing_pln``, ``ledger_net_cost_pln`` (from
    ``settlement.costs``), ``partial``, ``dropped_corrections`` and ``clamped`` (real only).
    """
    rows: list[dict[str, Any]] = []
    for key in sorted(states.params):
        p = states.params[key]
        for name, (start, stop) in sorted(windows(p, states.households[key].schedule).items()):
            st = states.strategies.get(name, {}).get(key)
            sp = states.strategy_params.get(name, {}).get(key)
            if st is None or sp is None:
                continue
            r = st.result
            c = costs(sp.model.rules, r.months, r.state.acc)
            until = None if stop is None else 4 * ((stop - p.lag) // 4)
            rows.append(
                {
                    "run_id": run_id,
                    "household_key": key,
                    "strategy": name,
                    "start_step": start,
                    "stop_step": stop,
                    "compare_until_step": until,
                    "next_step": st.steps.expected,
                    "start_soc_kwh": sp.initial_soc_kwh,
                    "end_soc_kwh": r.state.soc_kwh,
                    "months_closed": len(r.months),
                    "bill_pln": c.bill_pln,
                    "deposit_closing_pln": c.deposit_closing_pln,
                    "ledger_net_cost_pln": c.ledger_net_cost_pln,
                    "partial": _partial(start, p.end_step if stop is None else stop),
                    "dropped_corrections": r.dropped_corrections,
                    "clamped": None,
                }
            )
        bat, mtr = states.batteries.get(key), states.meters.get(key)
        if bat is None or mtr is None:
            continue
        c = costs(p.model.rules, mtr.months, mtr.acc)
        rows.append(
            {
                "run_id": run_id,
                "household_key": key,
                "strategy": REAL,
                "start_step": p.first_step,
                "stop_step": None,
                "compare_until_step": None,
                "next_step": mtr.flows.expected,
                "start_soc_kwh": p.initial_soc_kwh,
                "end_soc_kwh": bat.soc_kwh,
                "months_closed": len(mtr.months),
                "bill_pln": c.bill_pln,
                "deposit_closing_pln": c.deposit_closing_pln,
                "ledger_net_cost_pln": c.ledger_net_cost_pln,
                "partial": _partial(p.first_step, p.end_step),
                "dropped_corrections": 0,
                "clamped": bat.clamped,
            }
        )
    return rows


MONTH_SCALARS = (
    "import_kwh",
    "export_kwh",
    "energy_cost_pln",
    "dist_cost_pln",
    "export_value_pln",
    "offset_pln",
    "expired_pln",
    "refund_pln",
    "extinguished_pln",
    "payable_pln",
)


def month_rows(run_id: str, states: RunStates) -> list[dict[str, Any]]:
    """One row per household, strategy (and ``real``) and closed month: the `MonthSettlement`
    scalars.
    """
    rows: list[dict[str, Any]] = []

    def add(key: str, name: str, months: Sequence[Any]) -> None:
        for m in months:
            row = {"run_id": run_id, "household_key": key, "strategy": name, "month": m.month}
            row["basis"] = str(m.basis)
            row |= {col: getattr(m, col) for col in MONTH_SCALARS}
            rows.append(row)

    for key in sorted(states.params):
        for name in sorted(states.strategies):
            st = states.strategies[name].get(key)
            if st is not None:
                add(key, name, st.result.months)
        mtr = states.meters.get(key)
        if mtr is not None:
            add(key, REAL, mtr.months)
    return rows


def step_rows(
    run_id: str, offline: Mapping[str, Offline], states: RunStates
) -> list[dict[str, Any]]:
    """One row per household, strategy and step, from ``offline.trace`` with the household's
    recorded schedule.
    """
    rows: list[dict[str, Any]] = []
    for key in sorted(states.params):
        p, twin = states.params[key], offline[key]
        by_t = {s.t: s for s in twin.steps}
        traced = trace(p, twin.steps, states.households[key].schedule)
        for name in sorted(traced):
            for out in traced[name]:
                q, s = out.quarter, by_t[out.quarter.t]
                rows.append(
                    {
                        "run_id": run_id,
                        "household_key": key,
                        "strategy": name,
                        "t": q.t,
                        "pv_kwh": s.pv_kwh,
                        "load_kwh": s.load_kwh,
                        "rce_pln_per_kwh": s.rce_pln_per_kwh,
                        "pv_estimated": s.pv_estimated,
                        "load_estimated": s.load_estimated,
                        "price_replaced": s.price_replaced,
                        "forecast_issue_s": s.forecast_issue_s,
                        "action": str(q.action.kind),
                        "p_kw": q.p_kw,
                        "soc_kwh": q.soc_kwh,
                        "grid_kwh": q.grid_kwh,
                    }
                )
    return rows


def timing_rows(lines: Sequence[Line]) -> list[dict[str, Any]]:
    """The deduplicated timing lines."""
    return [
        {
            "ns": x.ns,
            "svc": x.svc,
            "h": x.h,
            "key": x.key,
            "inv": x.inv,
            "in_t": x.in_t,
            "sent": [{"service": svc, "t": t} for svc, t in x.sent],
            "lines": x.lines,
        }
        for x in lines
    ]


def lag_rows(
    run_id: str, due: Mapping[tuple[str, int], Due], lines: Sequence[Line]
) -> list[dict[str, Any]]:
    """Per released household-step (sim runs): when its releasing record was published, which
    stream it came from, when the `Household` released the step, and the lag between the two.
    """
    rows: list[dict[str, Any]] = []
    for (key, t), ns in sorted(releases(lines).items()):
        d = due.get((key, t))
        if d is None:
            continue
        released = ns / 1e9
        rows.append(
            {
                "run_id": run_id,
                "household_key": key,
                "t": t,
                "stream": d.stream,
                "due_unix": d.unix,
                "released_unix": released,
                "lag_s": released - d.unix,
            }
        )
    return rows


def write_table(
    rows: Sequence[Mapping[str, Any]], path: Path, *, schema_version: str = SCHEMA_VERSION
) -> Path:
    """Rows as one Parquet file with ``ems.schema_version`` in its metadata; NaN and infinity
    refused.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    for row in rows:
        for column, value in row.items():
            if isinstance(value, float) and not math.isfinite(value):
                raise ValueError(f"{path.name}: {column} is {value}")
    table = pa.Table.from_pylist([dict(r) for r in rows])
    meta = dict(table.schema.metadata or {})
    meta[b"ems.schema_version"] = schema_version.encode()
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table.replace_schema_metadata(meta), path)
    return path


def export(
    states: RunStates,
    run_id: str,
    out_dir: Path,
    *,
    offline: Mapping[str, Offline] | None = None,
    lines: Sequence[Line] = (),
    due: Mapping[tuple[str, int], Due] | None = None,
) -> dict[str, Path]:
    """Write the tables of a run; ``steps`` only when ``offline`` is given (sim runs whose
    exactly-once check passed), ``lags`` only when ``due`` is given (sim runs).
    """
    tables: dict[str, list[dict[str, Any]]] = {
        "households": household_rows(run_id, states),
        "strategies": strategy_rows(run_id, states),
        "months": month_rows(run_id, states),
        "timings": timing_rows(lines),
    }
    if offline is not None:
        tables["steps"] = step_rows(run_id, offline, states)
    if due is not None:
        tables["lags"] = lag_rows(run_id, due, lines)
    return {name: write_table(rows, out_dir / f"{name}.parquet") for name, rows in tables.items()}


def main(argv: Sequence[str] | None = None) -> int:
    """The CLI: read the run's state from the stack and write the tables without ``steps``."""
    import argparse

    from ems.experiments.stack import Restate

    ap = argparse.ArgumentParser(prog="python -m ems.experiments.export")
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--out", required=True, type=Path)
    args = ap.parse_args(argv)
    restate = Restate()
    try:
        written = export(read_states(restate, args.run_id), args.run_id, args.out)
    finally:
        restate.close()
    for name, path in written.items():
        print(f"{name}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
