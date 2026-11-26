"""AC-011-37 (the record's own checks): ``RunParams`` refuses parameters a run cannot use, before
anything is sent, and survives the JSON round trip of the actors' serde (``pydantic.TypeAdapter``)
equal to the original.

The record is a real one: site PL-LOC-000001 of the 10-household catalog, 2026-10-20 x 1 day, its
``SiteModel`` from ``ems.tariffs.site_model`` and the RCEm table of ``config/tariffs/rcem.yaml``.

AC-012-07, -24 (spec 012): ``RunParams`` 1.1.0 adds ``mode`` ("sim" or "live", default "sim") and
``grace_s`` (0 … 3600 s, default 120) after ``rcem``. A record stored by stage 3 (1.0.0, without
them; ``tests/fixtures/state/1.0.0/RunParams.jsonl``) decodes with the defaults and keeps its
version.
"""

from __future__ import annotations

import dataclasses
import json
import typing
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal

import pytest
from pydantic import TypeAdapter

from ems.assembler.params import RunParams
from tests.unit.assembler.cases import NO_PV_SITE, PV_SITE, run_params


@pytest.fixture(scope="module")
def params() -> RunParams:
    return run_params(PV_SITE)


def test_ac_011_37_a_valid_record(params: RunParams) -> None:
    assert params.first_step % 4 == 0 and params.end_step == params.first_step + 96
    assert params.household_key == "r1:PL-LOC-000001" and params.market_key == "r1:PL:RCE"
    assert params.has_pv and params.initial_soc_kwh == 5.0
    assert params.schema_version == "1.1.0"  # spec 012: 1.0.0 + mode, grace_s


@pytest.mark.parametrize(("late_slots", "lag"), [(1, 2), (2, 4), (8, 16), (48, 96)])
def test_ac_011_37_lag_is_twice_the_allowed_lateness(
    params: RunParams, late_slots: int, lag: int
) -> None:
    assert dataclasses.replace(params, late_slots=late_slots).lag == lag


def test_a_run_without_run_id_uses_bare_keys() -> None:
    p = run_params(NO_PV_SITE, run_id=None)
    assert (p.household_key, p.market_key, p.has_pv) == ("PL-LOC-000002", "PL:RCE", False)


type Change = Callable[[RunParams], dict[str, Any]]

BAD: list[Any] = [
    pytest.param(lambda p: {"first_step": p.first_step + 1}, id="first step off the hour"),
    pytest.param(lambda p: {"first_step": p.first_step + 3}, id="first step off the hour (3)"),
    pytest.param(lambda p: {"end_step": p.first_step}, id="empty window"),
    pytest.param(lambda p: {"end_step": p.first_step - 4}, id="window ends before it starts"),
    pytest.param(lambda p: {"late_slots": 0}, id="late_slots 0"),
    pytest.param(lambda p: {"late_slots": -1}, id="late_slots negative"),
    pytest.param(lambda p: {"late_slots": 49}, id="late_slots 49"),
    pytest.param(lambda p: {"strategies": ("NoBattery",)}, id="live not among the strategies"),
    pytest.param(
        lambda p: {"strategies": ("NoBattery", "SelfConsumption", "Mpc")}, id="unknown strategy"
    ),
    pytest.param(lambda p: {"strategies": ("Mpc",), "live": "Mpc"}, id="unknown live strategy"),
    pytest.param(
        lambda p: {"initial_soc_kwh": p.model.battery.soc_min_kwh - 0.01},
        id="SoC below its minimum",
    ),
    pytest.param(
        lambda p: {"initial_soc_kwh": p.model.battery.soc_max_kwh + 0.01},
        id="SoC above its maximum",
    ),
    pytest.param(
        lambda p: {"location_id": "PL-LOC-000004", "household_key": "r1:PL-LOC-000004"},
        id="location of another model",
    ),
    pytest.param(
        lambda p: {"household_key": "r1:PL-LOC-000004"}, id="household key of another site"
    ),
    pytest.param(
        lambda p: {"household_key": "r1:PL-LOC-000001:x"}, id="household key with a suffix"
    ),
]


@pytest.mark.parametrize("change", BAD)
def test_ac_011_37_post_init_refuses(params: RunParams, change: Change) -> None:
    with pytest.raises(ValueError):
        dataclasses.replace(params, **change(params))


GOOD: list[Any] = [
    pytest.param(lambda p: {"late_slots": 1}, id="late_slots 1"),
    pytest.param(lambda p: {"late_slots": 48}, id="late_slots 48"),
    pytest.param(lambda p: {"end_step": p.first_step + 1}, id="one step"),
    pytest.param(lambda p: {"initial_soc_kwh": p.model.battery.soc_min_kwh}, id="SoC minimum"),
    pytest.param(lambda p: {"initial_soc_kwh": p.model.battery.soc_max_kwh}, id="SoC maximum"),
    pytest.param(lambda p: {"strategies": ("SelfConsumption",)}, id="live strategy alone"),
    pytest.param(
        lambda p: {"strategies": ("NoBattery",), "live": "NoBattery"}, id="NoBattery live"
    ),
]


@pytest.mark.parametrize("change", GOOD)
def test_ac_011_37_post_init_accepts_the_bounds(params: RunParams, change: Change) -> None:
    changed = dataclasses.replace(params, **change(params))
    assert changed != params


@pytest.mark.parametrize("location", [PV_SITE, NO_PV_SITE])
def test_ac_011_37_json_round_trip_is_equal(location: str) -> None:
    params = run_params(location)
    adapter = TypeAdapter(RunParams)
    raw = adapter.dump_json(params)
    back = adapter.validate_json(raw)
    assert back == params
    assert float(back.initial_soc_kwh).hex() == float(params.initial_soc_kwh).hex()
    assert adapter.dump_json(back) == raw


# ── spec 012: mode and grace ──────────────────────────────────────────────────────────────────────

STORED_1_0_0 = Path(__file__).resolve().parents[2] / "fixtures/state/1.0.0/RunParams.jsonl"


def test_ac_012_07_mode_and_grace_default_to_sim_and_120_s(params: RunParams) -> None:
    assert (params.mode, params.grace_s, params.schema_version) == ("sim", 120, "1.1.0")
    hints = typing.get_type_hints(RunParams)
    assert hints["mode"] == Literal["sim", "live"] and hints["grace_s"] is int
    names = [f.name for f in dataclasses.fields(RunParams)]
    assert names[-4:] == ["rcem", "mode", "grace_s", "schema_version"]


MODE_GRACE_BAD: list[Any] = [
    pytest.param({"mode": "replay"}, id="unknown mode"),
    pytest.param({"mode": ""}, id="empty mode"),
    pytest.param({"mode": "Live"}, id="mode with a capital"),
    pytest.param({"grace_s": -1}, id="grace negative"),
    pytest.param({"grace_s": 3601}, id="grace above an hour"),
    pytest.param({"mode": "live", "grace_s": 7200}, id="live with grace above an hour"),
]


@pytest.mark.parametrize("change", MODE_GRACE_BAD)
def test_ac_012_07_post_init_refuses_mode_and_grace(
    params: RunParams, change: dict[str, Any]
) -> None:
    with pytest.raises(ValueError):
        dataclasses.replace(params, **change)


MODE_GRACE_GOOD: list[Any] = [
    pytest.param({"mode": "live"}, id="live"),
    pytest.param({"mode": "sim", "grace_s": 0}, id="grace 0"),
    pytest.param({"grace_s": 3600}, id="grace 3600"),
    pytest.param({"mode": "live", "grace_s": 300}, id="live with 5 min"),
]


@pytest.mark.parametrize("change", MODE_GRACE_GOOD)
def test_ac_012_07_post_init_accepts_mode_and_grace_bounds(
    params: RunParams, change: dict[str, Any]
) -> None:
    changed = dataclasses.replace(params, **change)
    assert {k: getattr(changed, k) for k in change} == change
    assert dataclasses.replace(changed, mode="sim", grace_s=120) == params


def test_ac_012_07_a_1_0_0_record_decodes_with_the_defaults() -> None:
    """The run parameters stage 3 stored, as it encoded them."""
    adapter = TypeAdapter(RunParams)
    lines = STORED_1_0_0.read_bytes().splitlines()
    assert lines
    for line in lines:
        stored = json.loads(line)
        assert "mode" not in stored and "grace_s" not in stored
        assert stored["schema_version"] == "1.0.0"
        back = adapter.validate_json(line)
        assert (back.mode, back.grace_s, back.schema_version) == ("sim", 120, "1.0.0")


def test_ac_012_07_a_record_without_the_new_fields_equals_the_sim_default(
    params: RunParams,
) -> None:
    adapter = TypeAdapter(RunParams)
    raw = json.loads(adapter.dump_json(params))
    del raw["mode"], raw["grace_s"]
    raw["schema_version"] = "1.0.0"
    back = adapter.validate_json(json.dumps(raw))
    assert back == dataclasses.replace(params, schema_version="1.0.0")


def test_ac_012_07_live_params_survive_the_json_round_trip(params: RunParams) -> None:
    live = dataclasses.replace(params, mode="live", grace_s=300)
    adapter = TypeAdapter(RunParams)
    raw = adapter.dump_json(live)
    assert json.loads(raw)["mode"] == "live" and json.loads(raw)["grace_s"] == 300
    assert adapter.validate_json(raw) == live
