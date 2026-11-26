"""AC-012-41: state stored by an earlier release still decodes, and nothing it held is lost.

``tests/fixtures/state/<version>/<Type>.jsonl`` holds every stored and sent type (state, run
parameters and the messages that can wait in Restate's queues) as that release encoded it, one value
per line (``tests/fixtures/state/generate.py``; stage 3 is 1.0.0). Every line decodes with the
current serde of its type, and the decoded value, encoded again, keeps every key and value of the
line, recursively: the stored JSON is a recursive subset of the re-encoded JSON. pydantic ignores
unknown keys, so decoding alone would let a removed or renamed field pass; the subset check makes it
fail, as it does a changed value or type, while an added field with a default passes.

File stems map to types here, by hand: a type that is renamed or moved keeps reading its old file.
"""

from __future__ import annotations

import dataclasses
import json
import struct
from pathlib import Path
from typing import Any

import pytest

from ems.actors.battery import BatteryState
from ems.actors.ems import EmsState
from ems.actors.household import HouseholdState
from ems.actors.messages import Ack, Apply, Flows, ForecastUpdate, PriceUpdate, Subscribe
from ems.actors.meter import MeterState
from ems.actors.serde import DataclassSerde, serde_for
from ems.actors.strategy import StrategyState
from ems.actors.tariff import TariffState
from ems.actors.weather import WeatherState
from ems.assembler.join import Step
from ems.assembler.params import RunParams

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "state"
VERSIONS = ("1.0.0",)
TYPES: dict[str, type[Any]] = {
    "HouseholdState": HouseholdState,
    "TariffState": TariffState,
    "WeatherState": WeatherState,
    "StrategyState": StrategyState,
    "EmsState": EmsState,
    "BatteryState": BatteryState,
    "MeterState": MeterState,
    "RunParams": RunParams,
    "Step": Step,
    "PriceUpdate": PriceUpdate,
    "ForecastUpdate": ForecastUpdate,
    "Subscribe": Subscribe,
    "Apply": Apply,
    "Ack": Ack,
    "Flows": Flows,
}
CASES = [(version, stem) for version in VERSIONS for stem in TYPES]


def lost(stored: Any, fresh: Any, path: str = "$") -> list[str]:
    """Where ``fresh`` does not keep ``stored``: a key missing, a list of another length, or a
    scalar of another type or value (floats compared bit for bit, so -0.0 is not 0.0). Keys only
    ``fresh`` has are allowed."""
    if isinstance(stored, dict):
        if not isinstance(fresh, dict):
            return [f"{path}: an object became {fresh!r}"]
        out: list[str] = []
        for key, value in stored.items():
            if key not in fresh:
                out.append(f"{path}.{key}: missing")
            else:
                out += lost(value, fresh[key], f"{path}.{key}")
        return out
    if isinstance(stored, list):
        if not isinstance(fresh, list) or len(fresh) != len(stored):
            return [f"{path}: a list of {len(stored)} became {fresh!r}"]
        return [
            where
            for i, (a, b) in enumerate(zip(stored, fresh, strict=True))
            for where in lost(a, b, f"{path}[{i}]")
        ]
    same = type(stored) is type(fresh) and stored == fresh
    if same and isinstance(stored, float):
        same = struct.pack(">d", stored) == struct.pack(">d", fresh)
    return [] if same else [f"{path}: {stored!r} became {fresh!r}"]


def _lines(version: str, stem: str) -> list[bytes]:
    path = FIXTURES / version / f"{stem}.jsonl"
    assert path.is_file(), (
        f"{path} is missing: run `uv run python -m tests.fixtures.state.generate`"
    )
    return path.read_bytes().splitlines()


@pytest.mark.parametrize("version", VERSIONS)
def test_ac_012_41_every_fixture_file_maps_to_a_type(version: str) -> None:
    stems = {p.stem for p in (FIXTURES / version).glob("*.jsonl")}
    assert stems == set(TYPES)


@pytest.mark.parametrize(("version", "stem"), CASES, ids=[f"{v}-{s}" for v, s in CASES])
def test_ac_012_41_stored_values_decode_and_keep_every_key_and_value(
    version: str, stem: str
) -> None:
    tp = TYPES[stem]
    serde = serde_for(tp)
    lines = _lines(version, stem)
    assert lines, f"{stem}.jsonl holds no value"
    for n, line in enumerate(lines, start=1):
        value = serde.deserialize(line)
        assert type(value) is tp, (stem, n, type(value))
        changes = lost(json.loads(line), json.loads(serde.serialize(value)))
        assert changes == [], f"{version}/{stem}.jsonl line {n}: {changes[:10]}"


# ── the subset check is not vacuous ─────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("stored", "fresh", "kept"),
    [
        (
            {"a": 1, "b": [1.5, {"c": "x"}]},
            {"a": 1, "b": [1.5, {"c": "x", "d": None}], "e": 0},
            True,
        ),
        ({"a": 1, "b": 2}, {"a": 1}, False),  # removed
        ({"a": 1}, {"b": 1}, False),  # renamed
        ({"a": {"b": 1}}, {"a": {"b": 2}}, False),  # changed, nested
        ({"a": True}, {"a": 1}, False),  # bool to int (equal in Python)
        ({"a": 1}, {"a": 1.0}, False),  # int to float
        ({"a": 0.0}, {"a": -0.0}, False),  # the sign of zero
        ({"a": None}, {"a": 0}, False),
        ({"a": [1, 2]}, {"a": [1, 2, 3]}, False),
        ({"a": [1, 2]}, {"a": {"0": 1, "1": 2}}, False),
    ],
    ids=[
        "added",
        "removed",
        "renamed",
        "changed",
        "bool-int",
        "int-float",
        "minus-zero",
        "none",
        "longer-list",
        "list-object",
    ],
)
def test_ac_012_41_lost_finds_removed_renamed_and_changed_values(
    stored: Any, fresh: Any, kept: bool
) -> None:
    assert (lost(stored, fresh) == []) is kept


@dataclasses.dataclass(frozen=True)
class _V1:
    t: int
    soc_kwh: float


@dataclasses.dataclass(frozen=True)
class _Removed:
    t: int


@dataclasses.dataclass(frozen=True)
class _Added:
    t: int
    soc_kwh: float
    clamped: int = 0


def test_ac_012_41_a_removed_field_fails_and_an_added_one_passes() -> None:
    """pydantic reads a line whose field was removed without complaint; the re-encoding loses it."""
    line = DataclassSerde(_V1).serialize(_V1(7, 2.5))
    removed = DataclassSerde(_Removed)
    assert removed.deserialize(line) == _Removed(7)
    assert lost(json.loads(line), json.loads(removed.serialize(removed.deserialize(line)))) == [
        "$.soc_kwh: missing"
    ]
    added = DataclassSerde(_Added)
    assert lost(json.loads(line), json.loads(added.serialize(added.deserialize(line)))) == []
