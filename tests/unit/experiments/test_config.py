"""AC-012-25 (plan §7, §8): experiment configs, validated before the runner touches the stack.

``ems.experiments.config``: ``load`` reads a YAML config and runs every check of ``validate``;
``expand`` gives the full product of the matrix times the repetitions, or one run per sweep speed;
``stall_s`` and ``window`` give a run's stall limit and its steps; ``check_households`` holds a
config to the managed sites with a battery that its catalog has.

The five validation configs of plan §8 live in ``experiments/configs/`` (T29). Until they exist, the
tests that read them fail on the missing file, before anything else.

Catalog facts used here (``config/generator.yaml``, read through ``load_sites``): 1400 locations
hold 403 managed sites with a battery, 350 locations hold 104.
"""

from __future__ import annotations

import copy
import dataclasses
import itertools
import re
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from ruamel.yaml import YAML

from ems.contracts.time import quarter_index
from ems.experiments import config
from ems.ingest.generator import load_generator_config, load_sites

REPO = Path(__file__).resolve().parents[3]
CONFIGS = REPO / "experiments" / "configs"
CONFIG_DIR = REPO / "config"
GENERATOR = CONFIG_DIR / "generator.yaml"
VALIDATION = ("sys-scale", "sys-speed", "sys-recovery", "sys-strategy-change", "sys-live")
BOTH = ("NoBattery", "SelfConsumption")
JUNE_3 = datetime(2026, 6, 3, tzinfo=UTC)
NOW = datetime(2026, 11, 25, 10, 17, 3, tzinfo=UTC)  # a fixed "now" for the live window


def _config_path(name: str) -> Path:
    path = CONFIGS / f"{name}.yaml"
    assert path.is_file(), f"{path} does not exist (T29 adds the validation configs)"
    return path


def _load(name: str) -> config.Experiment:
    return config.load(_config_path(name))


def _strategies(spec: config.RunSpec) -> tuple[str, ...]:
    return tuple(spec.run.strategies)


# ── the validation configs (plan §8) ─────────────────────────────────────────────────────────────


def test_ac_012_25_the_configs_directory_holds_the_five_validation_configs() -> None:
    assert CONFIGS.is_dir(), f"{CONFIGS} does not exist (T29)"
    assert sorted(p.stem for p in CONFIGS.glob("*.yaml")) == sorted(VALIDATION)


@pytest.mark.parametrize("name", VALIDATION)
def test_ac_012_25_every_validation_config_loads(name: str) -> None:
    exp = _load(name)
    assert exp.id == name
    assert exp.hypothesis.strip()
    # Every config uses the generator's own seed: the default generator config (plan §8).
    assert exp.data.generator_config == Path("config/generator.yaml")
    assert config.validate(exp) == exp


@pytest.mark.parametrize("name", VALIDATION)
def test_ac_012_25_every_run_of_a_validation_config_fits_its_catalog(name: str) -> None:
    for spec in config.expand(_load(name)):
        managed = config.check_households(spec, CONFIG_DIR)
        assert spec.data.households is not None
        assert managed >= spec.data.households, spec.name


def test_ac_012_36_sys_scale_is_households_by_strategies_at_speed_96() -> None:
    exp = _load("sys-scale")
    assert exp.data.kind == "synthetic"
    assert (exp.data.start, exp.data.days, exp.data.locations) == (JUNE_3, 1, 1400)
    assert exp.run.mode == "sim"
    assert exp.run.speed == 96
    specs = config.expand(exp)
    assert len(specs) == 4
    assert {(s.data.households, _strategies(s)) for s in specs} == {
        (100, ("SelfConsumption",)),
        (100, BOTH),
        (400, ("SelfConsumption",)),
        (400, BOTH),
    }
    assert {s.run.speed for s in specs} == {96}


def test_ac_012_36_sys_speed_doubles_from_96_to_1536() -> None:
    exp = _load("sys-speed")
    assert (exp.data.start, exp.data.days, exp.data.households) == (JUNE_3, 1, 100)
    assert exp.sweep is not None
    assert (exp.sweep.speed.start, exp.sweep.speed.factor, exp.sweep.speed.max) == (96, 2, 1536)
    assert [s.run.speed for s in config.expand(exp)] == [96, 192, 384, 768, 1536]


def test_ac_012_36_sys_recovery_kills_the_sdk_and_the_server_twice() -> None:
    exp = _load("sys-recovery")
    assert (exp.data.start, exp.data.days, exp.data.households) == (JUNE_3, 1, 100)
    assert exp.repetitions == 2
    assert [(k.target, k.at) for k in exp.kills] == [("sdk", 0.3), ("server", 0.7)]
    specs = config.expand(exp)
    assert len(specs) == 2
    assert len({s.repetition for s in specs}) == 2
    assert all(
        [(k.target, k.at) for k in s.kills] == [("sdk", 0.3), ("server", 0.7)] for s in specs
    )


def test_ac_012_36_sys_strategy_change_adds_on_day_1_and_removes_on_day_2() -> None:
    exp = _load("sys-strategy-change")
    start = datetime(2026, 6, 29, 4, tzinfo=UTC)
    assert (exp.data.start, exp.data.days, exp.data.households) == (start, 2, 100)
    assert tuple(exp.run.strategies) == ("SelfConsumption",)
    assert exp.run.live == "SelfConsumption"
    assert [(c.at_hour, tuple(c.strategies)) for c in exp.changes] == [
        (12, BOTH),
        (36, ("SelfConsumption",)),
    ]


def test_ac_012_36_sys_live_runs_two_hours_of_ten_households_in_real_time() -> None:
    exp = _load("sys-live")
    assert exp.run.mode == "live"
    assert exp.run.grace_s == 120
    assert (exp.data.kind, exp.data.hours, exp.data.households) == ("synthetic", 2, 10)
    assert exp.data.faults is True
    (spec,) = config.expand(exp)
    first, end = config.window(spec, NOW)
    assert first == quarter_index(datetime(2026, 11, 25, 11, tzinfo=UTC))
    assert end - first == 8


# ── extra="forbid" and the refusals of validate ──────────────────────────────────────────────────

BASE: dict[str, Any] = {
    "id": "t-base",
    "hypothesis": "a test config",
    "data": {
        "kind": "synthetic",
        "start": "2026-06-03T00:00:00Z",
        "days": 1,
        "locations": 350,
        "households": 100,
    },
    "run": {
        "late_slots": 8,
        "strategies": list(BOTH),
        "live": "SelfConsumption",
        "mode": "sim",
        "speed": 96,
    },
}


def _doc(**changes: Any) -> dict[str, Any]:
    """BASE with sections replaced or merged: ``data={...}`` merges, ``data_drop=[...]`` removes."""
    doc = copy.deepcopy(BASE)
    for name, value in changes.items():
        if name.endswith("_drop"):
            for key in value:
                doc[name.removesuffix("_drop")].pop(key, None)
        elif isinstance(value, dict) and isinstance(doc.get(name), dict):
            doc[name] = {**doc[name], **value}
        else:
            doc[name] = value
    return doc


def _write(tmp_path: Path, doc: dict[str, Any], name: str = "t.yaml") -> Path:
    path = tmp_path / name
    with path.open("w") as fh:
        YAML(typ="safe").dump(doc, fh)
    return path


def test_ac_012_25_the_base_config_loads_and_validates(tmp_path: Path) -> None:
    exp = config.load(_write(tmp_path, BASE))
    assert exp.id == "t-base"
    assert exp.data.start == JUNE_3
    assert tuple(exp.run.strategies) == BOTH
    assert config.validate(exp) == exp


@pytest.mark.parametrize(
    "doc",
    [
        _doc(unknown=1),
        _doc(data={"bogus": 1}),
        _doc(run={"bogus": 1}),
        _doc(kills=[{"target": "sdk", "at": 0.3, "bogus": 1}]),
        _doc(changes=[{"at_hour": 2, "strategies": list(BOTH), "bogus": 1}]),
    ],
    ids=["top", "data", "run", "kill", "change"],
)
def test_ac_012_25_an_unknown_field_is_refused(tmp_path: Path, doc: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        config.load(_write(tmp_path, doc))


KILL_IN_SENDING = _doc(
    changes=[{"at_hour": 12, "strategies": list(BOTH)}],
    run={"strategies": ["SelfConsumption"]},
    # 1 day = 96 steps: 0.45 is step 43.2, in [48 - CHANGE_SEND_STEPS, 48) of the change at 12 h.
    kills=[{"target": "sdk", "at": 0.45}],
)

REFUSED = {
    "sim-start-off-the-hour": _doc(data={"start": "2026-06-03T00:30:00Z"}),
    "sim-without-days": _doc(data_drop=["days"]),
    "sim-without-start": _doc(data_drop=["start"]),
    "live-without-hours": _doc(run={"mode": "live"}, data_drop=["start", "days"]),
    "history-without-recording": _doc(data={"kind": "history"}, data_drop=["locations"]),
    "matrix-field-not-allowed": _doc(matrix={"late_slots": [2, 8]}),
    "live-missing-from-strategies": _doc(run={"strategies": ["NoBattery"]}),
    "live-missing-from-a-matrix-set": _doc(matrix={"strategies": [["NoBattery"], list(BOTH)]}),
    "change-in-the-first-hour": _doc(changes=[{"at_hour": 0, "strategies": list(BOTH)}]),
    "change-unknown-policy": _doc(
        changes=[{"at_hour": 2, "strategies": ["Bogus", "SelfConsumption"]}]
    ),
    "kill-inside-a-change-sending": KILL_IN_SENDING,
}


@pytest.mark.parametrize("doc", REFUSED.values(), ids=REFUSED.keys())
def test_ac_012_25_load_refuses_a_config_that_could_not_run(
    tmp_path: Path, doc: dict[str, Any]
) -> None:
    with pytest.raises(ValueError):
        config.load(_write(tmp_path, doc))


# The refusals that are not field constraints, through ``validate`` on a parsed model.
SEMANTIC = {k: v for k, v in REFUSED.items() if k != "change-in-the-first-hour"}


@pytest.mark.parametrize("doc", SEMANTIC.values(), ids=SEMANTIC.keys())
def test_ac_012_25_validate_refuses_a_config_that_could_not_run(doc: dict[str, Any]) -> None:
    exp = config.Experiment.model_validate(doc)
    with pytest.raises(ValueError):
        config.validate(exp)


def test_ac_012_25_a_kill_outside_the_sending_of_a_change_is_accepted(tmp_path: Path) -> None:
    doc = copy.deepcopy(KILL_IN_SENDING)
    doc["kills"] = [
        {"target": "sdk", "at": 0.3},
        {"target": "server", "at": 0.7},
    ]  # steps 28.8, 67.2
    exp = config.load(_write(tmp_path, doc))
    assert [k.at for k in exp.kills] == [0.3, 0.7]


def test_ac_012_25_change_send_steps_is_two_hours() -> None:
    assert config.CHANGE_SEND_STEPS == 8


# ── expand ───────────────────────────────────────────────────────────────────────────────────────

SAFE_NAME = re.compile(r"^[A-Za-z0-9._-]+$")


def test_ac_012_25_expand_without_matrix_gives_the_config_itself() -> None:
    exp = config.Experiment.model_validate(BASE)
    (spec,) = config.expand(exp)
    assert spec.data == exp.data
    assert spec.run == exp.run
    assert tuple(spec.kills) == exp.kills
    assert tuple(spec.changes) == exp.changes
    assert SAFE_NAME.match(spec.name)


def test_ac_012_25_expand_is_the_full_product_of_the_matrix_times_the_repetitions() -> None:
    matrix = {
        "households": [100, 300],
        "strategies": [["SelfConsumption"], list(BOTH)],
        "locations": [350, 1400],
    }
    exp = config.Experiment.model_validate(_doc(matrix=matrix, repetitions=2))
    specs = config.expand(exp)
    assert len(specs) == 2 * 2 * 2 * 2
    points = [(s.data.households, _strategies(s), s.data.locations, s.repetition) for s in specs]
    assert len(set(points)) == len(points)
    product = itertools.product([100, 300], [("SelfConsumption",), BOTH], [350, 1400])
    for households, strategies, locations in product:
        reps = [p[3] for p in points if p[:3] == (households, strategies, locations)]
        assert len(reps) == 2, (households, strategies, locations)
    names = [s.name for s in specs]
    assert len(set(names)) == len(names)
    assert all(SAFE_NAME.match(n) for n in names), names
    # What the matrix does not vary stays as configured.
    assert {s.run.speed for s in specs} == {96}
    assert {s.data.start for s in specs} == {JUNE_3}


def test_ac_012_25_expand_varies_speed_from_the_matrix() -> None:
    exp = config.Experiment.model_validate(_doc(matrix={"speed": [96, 192]}))
    assert sorted(s.run.speed for s in config.expand(exp)) == [96, 192]


@pytest.mark.parametrize(
    ("sweep", "speeds"),
    [
        ({"start": 96, "factor": 2, "max": 1536}, [96, 192, 384, 768, 1536]),
        ({"start": 96, "factor": 2, "max": 1000}, [96, 192, 384, 768]),
        ({"start": 100, "factor": 3, "max": 900}, [100, 300, 900]),
    ],
)
def test_ac_012_31_expand_gives_one_run_per_sweep_speed_in_order(
    sweep: dict[str, float], speeds: list[float]
) -> None:
    exp = config.Experiment.model_validate(_doc(sweep={"speed": sweep}))
    specs = config.expand(exp)
    assert [s.run.speed for s in specs] == speeds
    assert len({s.name for s in specs}) == len(speeds)


# ── stall_s (AC-012-32) ──────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("speed", "expected_s"),
    [(96.0, 300.0), (30.0, 300.0), (10.0, 900.0), (1.0, 9000.0), (1536.0, 300.0)],
)
def test_ac_012_32_stall_s_in_sim_is_max_of_300_and_ten_quarters_of_wall_time(
    speed: float, expected_s: float
) -> None:
    assert config.stall_s(config.RunConfig(mode="sim", speed=speed)) == pytest.approx(expected_s)


@pytest.mark.parametrize(("grace_s", "expected_s"), [(120, 1920.0), (0, 1800.0), (600, 2400.0)])
def test_ac_012_32_stall_s_in_live_is_1800_plus_grace(grace_s: int, expected_s: float) -> None:
    run = config.RunConfig(mode="live", grace_s=grace_s, speed=96.0)
    assert config.stall_s(run) == expected_s


@pytest.mark.parametrize("mode", ["sim", "live"])
def test_ac_012_32_a_configured_stall_s_wins(mode: str) -> None:
    run = config.RunConfig.model_validate({"mode": mode, "stall_s": 42.0})
    assert config.stall_s(run) == 42.0


# ── window ───────────────────────────────────────────────────────────────────────────────────────


def _spec(data: dict[str, Any], run: dict[str, Any] | None = None) -> config.RunSpec:
    return config.RunSpec(
        name="r0",
        data=config.DataConfig(**data),
        run=config.RunConfig(**(run or {})),
        kills=(),
        changes=(),
        repetition=0,
    )


@pytest.mark.parametrize(
    ("start", "days", "steps"),
    [(JUNE_3, 1.0, 96), (datetime(2026, 6, 29, 4, tzinfo=UTC), 2.0, 192), (JUNE_3, 0.25, 24)],
)
def test_ac_012_25_the_sim_window_runs_from_start_for_days(
    start: datetime, days: float, steps: int
) -> None:
    spec = _spec({"start": start, "days": days, "locations": 350})
    first = quarter_index(start)
    assert config.window(spec, NOW) == (first, first + steps)


@pytest.mark.parametrize(
    ("now", "first_hour"),
    [
        (NOW, datetime(2026, 11, 25, 11, tzinfo=UTC)),
        (
            datetime(2026, 11, 25, 10, 59, 59, 900000, tzinfo=UTC),
            datetime(2026, 11, 25, 11, tzinfo=UTC),
        ),
        (datetime(2026, 11, 25, 23, 0, 1, tzinfo=UTC), datetime(2026, 11, 26, 0, tzinfo=UTC)),
        # 11:30 at UTC+1 is 10:30 UTC: the window starts on the next full UTC hour.
        (
            datetime(2026, 11, 25, 11, 30, tzinfo=timezone(timedelta(hours=1))),
            datetime(2026, 11, 25, 11, tzinfo=UTC),
        ),
    ],
)
def test_ac_012_25_the_live_window_starts_on_the_next_full_utc_hour(
    now: datetime, first_hour: datetime
) -> None:
    spec = _spec({"hours": 2, "locations": 10}, {"mode": "live"})
    first = quarter_index(first_hour)
    assert config.window(spec, now) == (first, first + 8)


# ── check_households ─────────────────────────────────────────────────────────────────────────────


def _managed_with_battery(locations: int) -> int:
    cfg = dataclasses.replace(load_generator_config(GENERATOR), synthetic_locations=locations)
    return sum(1 for s in load_sites(CONFIG_DIR, cfg) if s.ems_managed and s.bess is not None)


def _catalog_spec(locations: int, households: int | None) -> config.RunSpec:
    return _spec(
        {
            "start": JUNE_3,
            "days": 1,
            "locations": locations,
            "households": households,
            "generator_config": GENERATOR,
        }
    )


def test_ac_012_25_1400_locations_hold_400_managed_households() -> None:
    managed = _managed_with_battery(1400)
    assert managed >= 400
    assert config.check_households(_catalog_spec(1400, 400), CONFIG_DIR) == managed


def test_ac_012_25_more_households_than_the_catalog_manages_are_refused() -> None:
    managed = _managed_with_battery(350)
    assert config.check_households(_catalog_spec(350, managed), CONFIG_DIR) == managed
    with pytest.raises(ValueError):
        config.check_households(_catalog_spec(350, managed + 1), CONFIG_DIR)


def test_ac_012_25_without_households_every_managed_site_runs() -> None:
    assert config.check_households(_catalog_spec(350, None), CONFIG_DIR) == _managed_with_battery(
        350
    )
