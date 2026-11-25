"""Experiment configs (`experiments/configs/<id>.yaml`), validated and expanded into runs.

Spec 012.

One config describes an experiment: its data (a synthetic catalog window or a history recording),
the run parameters, the kills and strategy changes to carry out, and a matrix, sweep and repetitions
that expand it into runs. Everything is checked before the runner touches the stack, so a config
that cannot run fails at once instead of after a stack reset.
"""

from __future__ import annotations

import dataclasses
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from ems.contracts.time import quarter_index
from ems.domain.units import QUARTERS_PER_HOUR

QUARTER_S = 900

ID_PATTERN = r"^[a-z0-9][a-z0-9-]*$"
# Fields a matrix may vary; each names a field of `data` or `run`.
MATRIX_FIELDS = {"households": "data", "locations": "data", "strategies": "run", "speed": "run"}
CHANGE_SEND_STEPS = (
    8  # a change is sent while publication is at least this many steps (2 h) before `t`
)


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class DataConfig(_Strict):
    kind: Literal["synthetic", "history"] = "synthetic"
    start: datetime | None = None  # sim: the window's first step, on a UTC hour
    days: float | None = None  # sim: the window's length
    hours: float | None = None  # live: the window after the next full UTC hour
    locations: int | None = None  # synthetic: catalog size (managed and unmanaged sites)
    households: int | None = None  # managed households kept: the first N keys, sorted
    faults: bool = False
    recording: str | None = None  # history: the recording's name under data/recordings
    generator_config: Path = Path("config/generator.yaml")


class RunConfig(_Strict):
    late_slots: int = 8
    strategies: tuple[str, ...] = ("NoBattery", "SelfConsumption")
    live: str = "SelfConsumption"
    mode: Literal["sim", "live"] = "sim"
    grace_s: int = 120
    speed: float = 96.0  # simulated seconds per wall second (sim); live runs in real time
    stall_s: float | None = None  # None: `stall_s(run)`


class KillConfig(_Strict):
    target: Literal["sdk", "server"]
    at: float = Field(gt=0.0, lt=1.0)  # a fraction of the window in data time
    downtime_s: float = Field(default=0.0, ge=0.0)


class ChangeConfig(_Strict):
    at_hour: int = Field(gt=0)  # hours after the window's first step
    strategies: tuple[str, ...]


class SpeedSweep(_Strict):
    start: float = Field(gt=0.0)
    factor: float = Field(default=2.0, gt=1.0)
    max: float = Field(gt=0.0)


class SweepConfig(_Strict):
    speed: SpeedSweep


class Experiment(_Strict):
    id: str = Field(pattern=ID_PATTERN)
    hypothesis: str
    data: DataConfig
    run: RunConfig = RunConfig()
    kills: tuple[KillConfig, ...] = ()
    changes: tuple[ChangeConfig, ...] = ()
    matrix: dict[str, list[Any]] = {}
    sweep: SweepConfig | None = None
    repetitions: int = Field(default=1, ge=1)


@dataclass(frozen=True)
class RunSpec:
    """One run of an experiment: the config with its matrix point, sweep speed and repetition."""

    name: str  # unique within the experiment, safe as a directory name
    data: DataConfig
    run: RunConfig
    kills: tuple[KillConfig, ...]
    changes: tuple[ChangeConfig, ...]
    repetition: int


def load(path: Path) -> Experiment:
    """Read and validate a config; every check of ``validate`` runs."""
    from ruamel.yaml import YAML

    raw = YAML(typ="safe").load(Path(path).read_text())
    return validate(Experiment.model_validate(raw))


def validate(exp: Experiment) -> Experiment:
    """Refuse what could not run: a sim window not on a UTC hour or without days, a live run
    without hours, history without a recording, a matrix field outside ``MATRIX_FIELDS``, a live
    strategy missing from a strategy set, a change not after the first hour or naming an unknown
    policy, and a kill that falls inside a change's sending (``CHANGE_SEND_STEPS`` before its
    ``t`` up to ``t``).
    """
    from ems.domain.actions import POLICIES

    d, r = exp.data, exp.run
    if d.kind == "history":
        if not d.recording:
            raise ValueError(f"{exp.id}: a history run needs a recording")
    elif r.mode == "live":
        if not d.hours:
            raise ValueError(f"{exp.id}: a live run needs hours")
    else:
        if d.start is None or d.days is None:
            raise ValueError(f"{exp.id}: a sim run needs start and days")
        if d.start.tzinfo is None or d.start.utcoffset() is None:
            raise ValueError(f"{exp.id}: start must carry a time zone")
        if quarter_index(d.start) % QUARTERS_PER_HOUR:
            raise ValueError(f"{exp.id}: start must be on a UTC hour")
    unknown = sorted(set(exp.matrix) - set(MATRIX_FIELDS))
    if unknown:
        raise ValueError(f"{exp.id}: the matrix cannot vary {unknown}")
    sets = [tuple(r.strategies), *(tuple(x) for x in exp.matrix.get("strategies", []))]
    for names in sets:
        if r.live not in names:
            raise ValueError(f"{exp.id}: the live strategy {r.live!r} is missing from {names}")
        if any(n not in POLICIES for n in names):
            raise ValueError(f"{exp.id}: unknown policy in {names}")
    for change in exp.changes:
        if any(n not in POLICIES for n in change.strategies):
            raise ValueError(f"{exp.id}: unknown policy in change {change.strategies}")
        if r.live not in change.strategies:
            raise ValueError(f"{exp.id}: a change must keep the live strategy")
    if exp.kills and exp.changes and r.mode == "sim" and d.days:
        steps = d.days * 24 * QUARTERS_PER_HOUR
        for kill in exp.kills:
            at = kill.at * steps
            for change in exp.changes:
                t = change.at_hour * QUARTERS_PER_HOUR
                if t - CHANGE_SEND_STEPS <= at < t:
                    raise ValueError(
                        f"{exp.id}: the {kill.target} kill at {kill.at} falls inside the sending "
                        f"of the change at hour {change.at_hour}"
                    )
    return exp


def expand(exp: Experiment) -> list[RunSpec]:
    """The full product of the matrix, times the repetitions; with a sweep, one run per speed from
    ``start`` by ``factor`` up to ``max`` (the runner stops after the first that is not sustained).
    """
    axes = sorted(exp.matrix.items())
    points: list[list[tuple[str, Any]]] = [[]]
    for name, values in axes:
        points = [[*p, (name, v)] for p in points for v in values]
    if exp.sweep is not None:
        speeds: list[float] = []
        speed = exp.sweep.speed.start
        while speed <= exp.sweep.speed.max * (1 + 1e-9):
            speeds.append(speed)
            speed *= exp.sweep.speed.factor
        points = [[*p, ("speed", v)] for p in points for v in speeds]
    out: list[RunSpec] = []
    for point in points:
        data_changes = {k: v for k, v in point if MATRIX_FIELDS.get(k) == "data"}
        run_changes = {
            k: tuple(v) if isinstance(v, list) else v
            for k, v in point
            if MATRIX_FIELDS.get(k) == "run"
        }
        data = exp.data.model_copy(update=data_changes)
        run = exp.run.model_copy(update=run_changes)
        label = "-".join(_label(k, v) for k, v in point) or "run"
        for rep in range(exp.repetitions):
            name = label if exp.repetitions == 1 else f"{label}-r{rep}"
            out.append(RunSpec(name, data, run, exp.kills, exp.changes, rep))
    return out


def _label(name: str, value: Any) -> str:
    if isinstance(value, (list, tuple)):
        value = "+".join(str(v) for v in value)
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    return re.sub(r"[^A-Za-z0-9._+-]", "_", f"{name}{value}").replace("+", "_")


def stall_s(run: RunConfig) -> float:
    """How long no managed `Meter` may advance before a run is stalled: ``run.stall_s`` if set,
    else ``max(300, 10 x 900 / speed)`` in sim and ``1800 + grace_s`` in live (AC-012-32).
    """
    if run.stall_s is not None:
        return run.stall_s
    if run.mode == "live":
        return 1800.0 + run.grace_s
    return max(300.0, 10 * QUARTER_S / run.speed)


def window(spec: RunSpec, now: datetime) -> tuple[int, int]:
    """``(first_step, end_step)``: from ``start`` and ``days`` in sim; in live, from the first full
    UTC hour after ``now`` for ``hours``.
    """
    d = spec.data
    if spec.run.mode == "live":
        assert d.hours is not None
        hour = now.astimezone(UTC).replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        first = quarter_index(hour)
        return first, first + round(d.hours * QUARTERS_PER_HOUR)
    assert d.start is not None and d.days is not None
    first = quarter_index(d.start)
    return first, quarter_index(d.start + timedelta(days=d.days))


def check_households(spec: RunSpec, config_dir: Path) -> int:
    """The number of managed households with a battery in the catalog of ``locations`` sites;
    raises if it is below ``households``.
    """
    from ems.ingest.generator import load_generator_config, load_sites

    gen = spec.data.generator_config
    if not gen.is_absolute():
        gen = Path(config_dir).resolve().parent / gen
    cfg = load_generator_config(gen)
    if spec.data.locations is not None:
        cfg = dataclasses.replace(cfg, synthetic_locations=spec.data.locations)
    managed = sum(1 for s in load_sites(config_dir, cfg) if s.ems_managed and s.bess is not None)
    if spec.data.households is not None and managed < spec.data.households:
        raise ValueError(
            f"{spec.name}: {spec.data.households} households asked, the catalog of "
            f"{spec.data.locations} locations manages {managed}"
        )
    return managed
