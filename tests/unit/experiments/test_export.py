"""AC-012-26 … -28 (plan §5, §7, Export): a run's object state as Parquet tables.

The states are real: the actors' transitions run as the in-memory network of ``tests/network.py``
over a generated run of the 10-household catalog (3 managed households), in sim, with ``L`` = 2.
Two windows:
- ``CLOSES``: 2026-10-31T21:00Z for 6 h. October ends at 23:00Z (Warsaw time) and closes after
  ``2L`` quarters, so every strategy and every ``Meter`` holds one closed month (checked below);
- ``OPEN``: 2026-11-02T00:00Z for 3 h, which closes no month (bill and deposit null).

The strategy-change cases put a recorded ``Schedule`` on those states: the export reads the windows
from the household's schedule (``schedule.windows``) and the numbers from the objects. The cost
columns are ``settlement.costs`` of what each object holds (AC-012-27). Tables carry
``ems.schema_version`` = 1.0.0 in their Parquet metadata.
"""

from __future__ import annotations

import dataclasses
import math
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from ems.actors import strategy
from ems.actors.serde import serde_for
from ems.assembler import offline
from ems.assembler.params import RunParams
from ems.domain import settlement
from ems.experiments import export
from ems.experiments.metrics import Due, Line
from tests.network import CONSUMPTION, PRODUCTION, Generated, Network, Order, generate

RUN_ID = "net"
CLOSES = (datetime(2026, 10, 31, 21, tzinfo=UTC), 6)
OPEN = (datetime(2026, 11, 2, tzinfo=UTC), 3)
L = 2
BOTH = ("NoBattery", "SelfConsumption")
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
STEP_COLUMNS = {
    "run_id",
    "household_key",
    "strategy",
    "t",
    "pv_kwh",
    "load_kwh",
    "rce_pln_per_kwh",
    "forecast_issue_s",
    "action",
    "p_kw",
    "soc_kwh",
    "grid_kwh",
}


# ── real states ──────────────────────────────────────────────────────────────────────────────────


def _run(window: tuple[datetime, int]) -> tuple[Generated, Network]:
    gen = generate(window[0], window[1], L, False, RUN_ID)
    net = Network(gen)
    net.run(Order(seed=0, weights=(1, 1, 1, 1, 1)))
    assert not net.pending()
    return gen, net


def _states(gen: Generated, net: Network) -> export.RunStates:
    keys = sorted(gen.params)
    params = {k: net.params[("Household", k)] for k in keys}
    names = sorted({name for p in params.values() for name in p.strategies})
    return export.RunStates(
        params=params,
        households={k: net.state("Household", k) for k in keys},
        strategies={n: {k: net.state(n, k) for k in keys} for n in names},
        strategy_params={n: {k: net.params[(n, k)] for k in keys} for n in names},
        batteries={k: net.state("Battery", k) for k in keys},
        meters={k: net.state("Meter", k) for k in keys},
        records={k: len(gen.of(PRODUCTION, k)) + len(gen.of(CONSUMPTION, k)) for k in keys},
    )


@pytest.fixture(scope="module")
def closes() -> tuple[Generated, export.RunStates]:
    gen, net = _run(CLOSES)
    return gen, _states(gen, net)


@pytest.fixture(scope="module")
def open_month() -> export.RunStates:
    gen, net = _run(OPEN)
    return _states(gen, net)


def _first(states: export.RunStates) -> int:
    (first,) = {p.first_step for p in states.params.values()}
    return first


def _with_schedule(states: export.RunStates, schedule: Any) -> export.RunStates:
    households = {
        k: dataclasses.replace(h, schedule=schedule) for k, h in states.households.items()
    }
    return dataclasses.replace(states, households=households)


def _removed(states: export.RunStates) -> tuple[export.RunStates, int]:
    """NoBattery removed at ``first + 12``; one more change refused as released."""
    from ems.assembler.schedule import Change, Refusal, Schedule

    first = _first(states)
    stop = first + 12
    schedule = Schedule(
        changes=(Change(stop, ("SelfConsumption",)),),
        refused=(Refusal(first, ("SelfConsumption",), "released"),),
    )
    return _with_schedule(states, schedule), stop


def _added_never_started(states: export.RunStates) -> tuple[export.RunStates, int]:
    """The run starts with SelfConsumption only; NoBattery is added at ``first + 20``, a step the
    run never released to it: its object holds the start state of ``strategy_params``.
    """
    from ems.assembler.schedule import Change, Schedule, strategy_params

    first = _first(states)
    t = first + 20
    change = Change(t, BOTH)
    params = {
        k: dataclasses.replace(p, strategies=("SelfConsumption",)) for k, p in states.params.items()
    }
    added = {k: strategy_params(p, change) for k, p in params.items()}
    started = {k: strategy.start(p)[0] for k, p in added.items()}
    with_change = dataclasses.replace(
        states,
        params=params,
        strategies={**states.strategies, "NoBattery": started},
        strategy_params={**states.strategy_params, "NoBattery": added},
    )
    return _with_schedule(with_change, Schedule(changes=(change,))), t


def _by(
    rows: Sequence[Mapping[str, Any]], *columns: str
) -> dict[tuple[Any, ...], Mapping[str, Any]]:
    out = {tuple(r[c] for c in columns): r for r in rows}
    assert len(out) == len(rows), f"rows are not unique by {columns}"
    return out


# ── households ───────────────────────────────────────────────────────────────────────────────────


def test_ac_012_26_one_household_row_per_managed_household(
    closes: tuple[Generated, export.RunStates],
) -> None:
    _, states = closes
    rows = _by(export.household_rows(RUN_ID, states), "household_key")
    assert set(rows) == {(k,) for k in states.params}
    for key, p in states.params.items():
        row, h = rows[(key,)], states.households[key]
        assert row["run_id"] == RUN_ID
        assert row["location_id"] == p.location_id
        assert row["has_pv"] == p.has_pv
        assert (row["first_step"], row["end_step"]) == (p.first_step, p.end_step)
        assert row["next_step"] == h.join.next
        assert row["records"] == states.records[key]
        assert row["after_end_late"] == h.join.after_end_late
        assert row["price_replaced"] == h.join.price_replaced
        assert row["clamped_price"] == h.join.clamped_price
        assert row["clamped_forecast"] == h.join.clamped_forecast
        assert (row["changes"], row["refused"]) == (0, 0)


def test_ac_012_27_household_rows_count_changes_and_refusals(
    closes: tuple[Generated, export.RunStates],
) -> None:
    states, _ = _removed(closes[1])
    for row in export.household_rows(RUN_ID, states):
        assert (row["changes"], row["refused"]) == (1, 1)


# ── strategies ───────────────────────────────────────────────────────────────────────────────────


def test_ac_012_27_strategy_rows_hold_the_costs_of_each_strategy_object(
    closes: tuple[Generated, export.RunStates],
) -> None:
    _, states = closes
    rows = _by(export.strategy_rows(RUN_ID, states), "household_key", "strategy")
    assert set(rows) == {(k, n) for k in states.params for n in (*BOTH, export.REAL)}
    for key, p in states.params.items():
        for name in BOTH:
            row, st = rows[(key, name)], states.strategies[name][key]
            sp = states.strategy_params[name][key]
            result = st.result
            assert result.months, "the CLOSES window must close October"
            want = settlement.costs(sp.model.rules, result.months, result.state.acc)
            assert row["run_id"] == RUN_ID
            assert (row["start_step"], row["stop_step"]) == (p.first_step, None)
            assert row["compare_until_step"] is None
            assert row["next_step"] == st.steps.expected == states.households[key].join.next
            assert row["start_soc_kwh"] == sp.initial_soc_kwh
            assert row["end_soc_kwh"] == result.state.soc_kwh
            assert row["months_closed"] == len(result.months)
            assert row["bill_pln"] == want.bill_pln
            assert row["deposit_closing_pln"] == want.deposit_closing_pln
            assert row["ledger_net_cost_pln"] == want.ledger_net_cost_pln
            assert row["dropped_corrections"] == result.dropped_corrections


def test_ac_012_27_the_real_row_is_battery_and_meter(
    closes: tuple[Generated, export.RunStates],
) -> None:
    _, states = closes
    assert export.REAL == "real"
    rows = _by(export.strategy_rows(RUN_ID, states), "household_key", "strategy")
    for key, p in states.params.items():
        row, bat, mtr = rows[(key, "real")], states.batteries[key], states.meters[key]
        assert mtr.months, "the CLOSES window must close October"
        want = settlement.costs(p.model.rules, mtr.months, mtr.acc)
        assert (row["start_step"], row["stop_step"]) == (p.first_step, None)
        assert row["next_step"] == mtr.flows.expected
        assert row["start_soc_kwh"] == p.initial_soc_kwh
        assert row["end_soc_kwh"] == bat.soc_kwh
        assert row["months_closed"] == len(mtr.months)
        assert row["bill_pln"] == want.bill_pln
        assert row["deposit_closing_pln"] == want.deposit_closing_pln
        assert row["ledger_net_cost_pln"] == want.ledger_net_cost_pln
        assert row["clamped"] == bat.clamped


def test_ac_012_27_bill_and_deposit_are_null_while_no_month_has_closed(
    open_month: export.RunStates,
) -> None:
    states = open_month
    rows = export.strategy_rows(RUN_ID, states)
    assert rows
    for row in rows:
        assert row["months_closed"] == 0
        assert row["bill_pln"] is None
        assert row["deposit_closing_pln"] is None
        # Hour rows are held, so the ledger has a value (not 0 by default, not null).
        assert isinstance(row["ledger_net_cost_pln"], float)
        assert not math.isnan(row["ledger_net_cost_pln"])


def test_ac_012_27_a_removed_strategy_is_comparable_until_4_floor_stop_minus_2l_over_4(
    closes: tuple[Generated, export.RunStates],
) -> None:
    states, stop = _removed(closes[1])
    rows = _by(export.strategy_rows(RUN_ID, states), "household_key", "strategy")
    first = _first(states)
    for key in states.params:
        row = rows[(key, "NoBattery")]
        assert (row["start_step"], row["stop_step"]) == (first, stop)
        assert row["compare_until_step"] == 4 * ((stop - 2 * L) // 4) == first + 8
        assert row["partial"] is True
        kept = rows[(key, "SelfConsumption")]
        assert (kept["stop_step"], kept["compare_until_step"]) == (None, None)


def test_ac_012_27_a_strategy_window_that_never_started_has_a_null_ledger(
    closes: tuple[Generated, export.RunStates],
) -> None:
    states, t = _added_never_started(closes[1])
    rows = _by(export.strategy_rows(RUN_ID, states), "household_key", "strategy")
    for key in states.params:
        row = rows[(key, "NoBattery")]
        sp = states.strategy_params["NoBattery"][key]
        assert (row["start_step"], row["stop_step"], row["next_step"]) == (t, None, t)
        assert row["compare_until_step"] is None
        assert row["months_closed"] == 0
        assert row["bill_pln"] is None
        assert row["deposit_closing_pln"] is None
        assert row["ledger_net_cost_pln"] is None
        assert row["start_soc_kwh"] == row["end_soc_kwh"] == sp.initial_soc_kwh
        assert row["partial"] is True
        assert row["dropped_corrections"] == 0


# ── months ───────────────────────────────────────────────────────────────────────────────────────


def test_ac_012_27_one_month_row_per_household_strategy_and_closed_month(
    closes: tuple[Generated, export.RunStates],
) -> None:
    _, states = closes
    rows = _by(export.month_rows(RUN_ID, states), "household_key", "strategy", "month")
    expected: dict[tuple[str, str, str], settlement.MonthSettlement] = {}
    for key in states.params:
        for name in BOTH:
            for m in states.strategies[name][key].result.months:
                expected[(key, name, m.month)] = m
        for m in states.meters[key].months:
            expected[(key, export.REAL, m.month)] = m
    assert expected, "the CLOSES window must close October"
    assert set(rows) == set(expected)
    for at, m in expected.items():
        row = rows[at]
        assert row["run_id"] == RUN_ID
        assert row["basis"] == m.basis
        for column in MONTH_SCALARS:
            assert row[column] == getattr(m, column), (at, column)


def test_ac_012_27_no_month_rows_without_a_closed_month(open_month: export.RunStates) -> None:
    assert export.month_rows(RUN_ID, open_month) == []


# ── steps (from the offline fold) ────────────────────────────────────────────────────────────────


def test_ac_012_27_step_rows_come_from_offline_trace(
    closes: tuple[Generated, export.RunStates],
) -> None:
    gen, states = closes
    twins = {k: gen.offline(k) for k in states.params}
    rows = export.step_rows(RUN_ID, twins, states)
    assert rows
    assert set(rows[0]) >= STEP_COLUMNS
    for key, p in states.params.items():
        traced = offline.trace(p, twins[key].steps, states.households[key].schedule)
        for name in BOTH:
            mine = [r for r in rows if r["household_key"] == key and r["strategy"] == name]
            assert [r["t"] for r in mine] == [s.t for s in twins[key].steps]
            assert [r["soc_kwh"] for r in mine] == [o.state.soc_kwh for o in traced[name]]
            assert [r["pv_kwh"] for r in mine] == [s.pv_kwh for s in twins[key].steps]
            assert mine[-1]["soc_kwh"] == states.strategies[name][key].result.state.soc_kwh


# ── timings ──────────────────────────────────────────────────────────────────────────────────────

LINES = [
    Line(10, "Household", "on_measurement", "net:PL-LOC-000001", "inv_1", None,
         (("SelfConsumption", 5), ("Ems", 5)), lines=1),
    Line(20, "Battery", "apply", "net:PL-LOC-000001", "inv_2", 5,
         (("Ems", 5), ("Meter", 5)), lines=2),
    Line(30, "Meter", "on_flows", "net:PL-LOC-000001", "inv_3", 5, (), lines=1),
]  # fmt: skip


DUE = {
    ("net:PL-LOC-000001", 5): Due(4e-9, "production"),
    ("net:PL-LOC-000001", 6): Due(5e-9, "price"),  # not released: no row
}


def test_ac_012_26_lags_pair_each_release_with_its_releasing_record() -> None:
    rows = export.lag_rows(RUN_ID, DUE, LINES)
    assert rows == [
        {
            "run_id": RUN_ID,
            "household_key": "net:PL-LOC-000001",
            "t": 5,
            "stream": "production",
            "due_unix": 4e-9,
            "released_unix": 10e-9,
            "lag_s": 10e-9 - 4e-9,
        }
    ]


def _sent(value: Any) -> list[tuple[str, int]]:
    return [tuple(x.values()) if isinstance(x, Mapping) else tuple(x) for x in value]


def test_ac_012_29_timing_rows_are_the_deduplicated_lines() -> None:
    rows = export.timing_rows(LINES)
    assert len(rows) == len(LINES)
    for row, line in zip(rows, LINES, strict=True):
        assert {k: row[k] for k in ("ns", "svc", "h", "key", "inv", "in_t", "lines")} == {
            "ns": line.ns,
            "svc": line.svc,
            "h": line.h,
            "key": line.key,
            "inv": line.inv,
            "in_t": line.in_t,
            "lines": line.lines,
        }
        assert _sent(row["sent"]) == list(line.sent)


# ── write_table and export ───────────────────────────────────────────────────────────────────────

ROWS = [
    {"household_key": "k1", "t": 1, "bill_pln": 12.5, "partial": False, "note": None},
    {"household_key": "k2", "t": 2, "bill_pln": None, "partial": True, "note": "x"},
]


def _version(path: Path) -> bytes | None:
    metadata = pq.read_schema(path).metadata or {}
    return metadata.get(b"ems.schema_version")


def test_ac_012_26_write_table_round_trips_with_its_schema_version(tmp_path: Path) -> None:
    assert export.SCHEMA_VERSION == "1.0.0"
    path = export.write_table(ROWS, tmp_path / "t.parquet")
    assert path == tmp_path / "t.parquet"
    assert _version(path) == b"1.0.0"
    assert pq.read_table(path).to_pylist() == ROWS


def test_ac_012_26_write_table_writes_the_given_schema_version(tmp_path: Path) -> None:
    path = export.write_table(ROWS, tmp_path / "t.parquet", schema_version="2.1.0")
    assert _version(path) == b"2.1.0"


@pytest.mark.parametrize("bad", [math.nan, math.inf, -math.inf])
def test_ac_012_26_write_table_refuses_nan_and_infinity(tmp_path: Path, bad: float) -> None:
    rows = [{**ROWS[0], "bill_pln": bad}]
    with pytest.raises(ValueError):
        export.write_table(rows, tmp_path / "t.parquet")


def test_ac_012_27_states_round_trip_through_parquet(
    closes: tuple[Generated, export.RunStates], tmp_path: Path
) -> None:
    _, states = closes
    rows = export.strategy_rows(RUN_ID, states)
    path = export.write_table(rows, tmp_path / "strategies.parquet")
    assert pq.read_table(path).to_pylist() == rows


def test_ac_012_27_export_writes_no_steps_without_the_offline_fold(
    closes: tuple[Generated, export.RunStates], tmp_path: Path
) -> None:
    _, states = closes
    written = export.export(states, RUN_ID, tmp_path, lines=LINES)
    assert set(written) == {"households", "strategies", "months", "timings"}
    assert not list(tmp_path.glob("steps*"))
    for name, path in written.items():
        assert path.is_file(), name
        assert path.parent == tmp_path
        assert _version(path) == b"1.0.0", name


def test_ac_012_27_export_writes_steps_with_the_offline_fold(
    closes: tuple[Generated, export.RunStates], tmp_path: Path
) -> None:
    gen, states = closes
    twins = {k: gen.offline(k) for k in states.params}
    written = export.export(states, RUN_ID, tmp_path, offline=twins, lines=LINES, due=DUE)
    assert set(written) == set(export.TABLES)
    assert pq.read_table(written["steps"]).num_rows == len(export.step_rows(RUN_ID, twins, states))
    assert _version(written["steps"]) == b"1.0.0"


# ── read_states: SQL rows decoded with the actors' serde ─────────────────────────────────────────


class StateTable:
    """Restate's `state` table for a run, as ``Restate.states`` and ``Restate.query`` return it:
    one row per object and state key, the value as the serde's JSON text."""

    def __init__(self, table: Mapping[str, Mapping[str, Mapping[str, str]]]) -> None:
        self.table = table
        self.sql: list[str] = []

    def states(self, run_id: str, service: str) -> dict[str, dict[str, str]]:
        return {
            k: dict(v) for k, v in self.table.get(service, {}).items() if k.startswith(f"{run_id}:")
        }

    def query(self, sql: str) -> list[dict[str, Any]]:
        self.sql.append(sql)
        named = re.findall(r"'([A-Za-z]+)'", sql.split("WHERE", 1)[-1])
        services = [s for s in named if s in self.table] or list(self.table)
        return [
            {"service_name": svc, "service_key": key, "key": sk, "value_utf8": value}
            for svc in services
            for key, values in self.table[svc].items()
            for sk, value in values.items()
        ]


def _json(value: Any) -> str:
    return serde_for(type(value)).serialize(value).decode()


def test_ac_012_27_read_states_decodes_the_state_table_with_the_actors_serde(
    closes: tuple[Generated, export.RunStates],
) -> None:
    _, states = closes
    unmanaged = f"{RUN_ID}:PL-LOC-000003"
    table: dict[str, dict[str, dict[str, str]]] = {"Household": {}, "Battery": {}, "Meter": {}}
    for key, p in states.params.items():
        table["Household"][key] = {
            "params": _json(p),
            "state": _json(states.households[key]),
            "n": str(states.records[key]),
            "n_at_configure": "0",
        }
        table["Battery"][key] = {"params": _json(p), "state": _json(states.batteries[key])}
        table["Meter"][key] = {"params": _json(p), "state": _json(states.meters[key])}
        for name in BOTH:
            sp: RunParams = states.strategy_params[name][key]
            table.setdefault(name, {})[key] = {
                "params": _json(sp),
                "state": _json(states.strategies[name][key]),
            }
    table["Household"][unmanaged] = {"n": "17", "log": "{}"}  # a site the EMS does not manage
    read = export.read_states(StateTable(table), RUN_ID)
    assert read.params == states.params
    assert read.households == states.households
    assert read.strategies == states.strategies
    assert read.strategy_params == states.strategy_params
    assert read.batteries == states.batteries
    assert read.meters == states.meters
    assert {k: read.records[k] for k in states.params} == states.records
