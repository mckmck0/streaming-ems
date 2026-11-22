"""AC-011-37 (the record's own checks): ``RunParams`` refuses parameters a run cannot use, before
anything is sent, and survives the JSON round trip of the actors' serde (``pydantic.TypeAdapter``)
equal to the original.

The record is a real one: site PL-LOC-000001 of the 10-household catalog, 2026-10-20 x 1 day, its
``SiteModel`` from ``ems.tariffs.site_model`` and the RCEm table of ``config/tariffs/rcem.yaml``.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable
from typing import Any

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
    assert params.schema_version == "1.0.0"


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
