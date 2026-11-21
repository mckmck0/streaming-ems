"""AC-011-37: the parameters of a run, one record per EMS-managed site of the generator's catalog.

The 10-household catalog has three managed sites: PL-LOC-000001 (PV), PL-LOC-000002 (no PV) and
PL-LOC-000004 (PV) (plan.md, Facts). The clean e2e window starts 2026-10-31T00:00Z and lasts 30 h
(1.25 days, 120 quarter-hours). Anything that would make a handler raise later fails here, before a
single ``configure`` is sent: the tariffs end with 2026, so a window in 2027 cannot be settled.
"""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime, timedelta

import pytest

from ems.assembler.params import RunParams
from ems.contracts.time import quarter_index
from ems.domain.netbilling import RcemTable
from ems.experiments.runs import CONFIG, managed_params, scoped
from ems.ingest.catalog.sites import Site
from ems.ingest.generator import load_generator_config, load_sites
from ems.tariffs import load_book, load_rcem, site_model

RUN_ID = "r1"
START = datetime(2026, 10, 31, tzinfo=UTC)
DAYS = 1.25
LOCATIONS = 10
MANAGED = ("PL-LOC-000001", "PL-LOC-000002", "PL-LOC-000004")
WITHOUT_PV = "PL-LOC-000002"
QUARTERS_IN_30_H = 120


@pytest.fixture(scope="module")
def params() -> dict[str, RunParams]:
    return managed_params(run_id=RUN_ID, start=START, days=DAYS, locations=LOCATIONS)


def _sites() -> dict[str, Site]:
    cfg = dataclasses.replace(
        load_generator_config(CONFIG / "generator.yaml"), synthetic_locations=LOCATIONS
    )
    return {s.location_id: s for s in load_sites(CONFIG, cfg)}


def test_ac_011_37_one_record_per_managed_site_of_the_10_household_catalog(
    params: dict[str, RunParams],
) -> None:
    assert list(params) == [f"{RUN_ID}:{loc}" for loc in MANAGED]
    assert [p.location_id for p in params.values()] == list(MANAGED)


def test_ac_011_37_keys_are_scoped_by_the_run_id(params: dict[str, RunParams]) -> None:
    for key, p in params.items():
        assert key == p.household_key == scoped(RUN_ID, p.location_id)
        assert p.run_id == RUN_ID
        assert p.market_key == f"{RUN_ID}:PL:RCE"


def test_ac_011_37_without_run_id_the_keys_are_unscoped() -> None:
    params = managed_params(run_id=None, start=START, days=DAYS, locations=LOCATIONS)
    assert list(params) == list(MANAGED)
    for key, p in params.items():
        assert (p.run_id, p.household_key, p.market_key) == (None, key, "PL:RCE")


def test_ac_011_37_has_pv_follows_the_catalog(params: dict[str, RunParams]) -> None:
    assert {p.location_id: p.has_pv for p in params.values()} == {
        loc: loc != WITHOUT_PV for loc in MANAGED
    }


def test_ac_011_37_initial_soc_is_half_the_battery_capacity(params: dict[str, RunParams]) -> None:
    sites = _sites()
    for p in params.values():
        bess = sites[p.location_id].bess
        assert bess is not None
        capacity_kwh = bess.capacity_kwh
        assert p.initial_soc_kwh == capacity_kwh / 2
        assert p.initial_soc_kwh == p.model.battery.capacity_kwh / 2


def test_ac_011_37_first_and_end_step_come_from_the_window(params: dict[str, RunParams]) -> None:
    first = quarter_index(START)
    end = quarter_index(START + timedelta(days=DAYS))
    assert end - first == QUARTERS_IN_30_H
    for p in params.values():
        assert (p.first_step, p.end_step) == (first, end)


def test_ac_011_37_run_settings_are_the_defaults(params: dict[str, RunParams]) -> None:
    for p in params.values():
        assert p.late_slots == 8
        assert p.strategies == ("NoBattery", "SelfConsumption")
        assert p.live == "SelfConsumption"
        assert p.schema_version == "1.0.0"


def test_ac_011_37_late_slots_and_strategies_are_passed_through() -> None:
    params = managed_params(
        run_id=RUN_ID,
        start=START,
        days=DAYS,
        locations=LOCATIONS,
        late_slots=2,
        strategies=("SelfConsumption",),
        live="SelfConsumption",
    )
    assert {(p.late_slots, p.strategies, p.live) for p in params.values()} == {
        (2, ("SelfConsumption",), "SelfConsumption")
    }


def test_ac_011_37_site_model_and_rcem_table_are_those_of_ems_tariffs(
    params: dict[str, RunParams],
) -> None:
    sites, book = _sites(), load_book(CONFIG / "tariffs")
    rcem = RcemTable(tuple(load_rcem(CONFIG / "tariffs" / "rcem.yaml")))
    for p in params.values():
        window = (p.first_step, p.end_step)
        assert p.model == site_model(sites[p.location_id], book, window)
        assert p.rcem == rcem


@pytest.mark.parametrize(
    ("start", "days"),
    [
        (datetime(2027, 1, 1, tzinfo=UTC), 1.0),  # no tariff version for 2027
        (datetime(2026, 12, 31, 12, tzinfo=UTC), 1.0),  # the window's end lies in 2027
        (datetime(2025, 12, 31, tzinfo=UTC), 2.0),  # starts before the tariffs
    ],
)
def test_ac_011_37_a_window_outside_the_tariffs_fails_before_anything_is_built(
    start: datetime, days: float
) -> None:
    with pytest.raises(ValueError):
        managed_params(run_id=RUN_ID, start=start, days=days, locations=LOCATIONS)


def test_ac_011_37_a_first_step_off_the_utc_hour_fails() -> None:
    with pytest.raises(ValueError, match="UTC hour"):
        managed_params(
            run_id=RUN_ID, start=START + timedelta(minutes=15), days=DAYS, locations=LOCATIONS
        )
