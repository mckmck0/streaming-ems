"""AC-002-01/-02: the mapping engine (schema validation, paths, layouts, time, units,
transforms).
"""

from __future__ import annotations

import math
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from ems.ingest.mapping.engine import MappingError, apply, load_mapping, parse_mapping, resolve

REPO = Path(__file__).resolve().parents[3]
MAPPINGS = REPO / "mappings"


def m(**overrides: Any) -> Any:
    base: dict[str, Any] = {
        "mapping": "test.thing.v1",
        "version": 1,
        "source": "test",
        "layout": "rows",
        "records": "/value",
        "fields": {"x": {"path": "x", "type": "float"}},
    }
    base.update(overrides)
    return parse_mapping(base, MAPPINGS)


def test_ac_002_01_schema_rejects_unknown_keys_and_ops() -> None:
    with pytest.raises(MappingError):
        m(unexpected=True)
    with pytest.raises(MappingError):
        m(fields={"x": {"path": "x", "transforms": ["explode"]}})
    with pytest.raises(MappingError):
        m(fields={"x": {"path": "x", "unit_in": "kWh", "unit_out": "W"}})
    with pytest.raises(MappingError, match=r"time\.field"):
        m(time={"field": "nope", "label": "end", "resolution": "PT15M"})


@pytest.mark.parametrize("name", sorted(p.stem for p in MAPPINGS.glob("*.yaml")))
def test_ac_002_01_repo_mappings_validate(name: str) -> None:
    assert load_mapping(MAPPINGS / f"{name}.yaml").id == name


def test_paths_dotted_pointer_and_star() -> None:
    doc = {"a": {"b": 1, "price.amount": 7}, "list": [{"v": 1}, {"v": 2}]}
    assert resolve(doc, "a.b") == 1
    assert resolve(doc, "/a/price.amount") == 7  # JSON pointer keeps dots inside a segment
    assert resolve(doc, "/list/*/v") == [1, 2]
    assert resolve(doc, "/list/1/v") == 2
    assert resolve(doc, "a.missing") is None


def test_ac_002_02_rows_time_units_transforms() -> None:
    mapping = m(
        fields={
            "end": {"path": "t", "type": "datetime", "tz": "utc"},
            "energy_kwh": {"path": "e", "type": "float", "unit_in": "Wh", "unit_out": "kWh"},
            "load_kw": {
                "path": "p",
                "type": "float",
                "unit_in": "W",
                "unit_out": "kW",
                "transforms": ["negate"],
            },
            "label": {
                "path": "q",
                "type": "str",
                "quality_map": {"0": "measured", "1": "estimated"},
            },
            "n": {
                "path": "n",
                "type": "float",
                "transforms": [{"default": 0.0}, {"scale": 2}, {"round": 1}],
            },
        },
        time={"field": "end", "label": "end", "resolution": "PT15M"},
    )
    rows = apply(
        mapping,
        {"value": [{"t": "2026-10-20 22:15:00", "e": 1500, "p": -2000, "q": "0", "n": None}]},
    )
    assert rows == [
        {
            "end": datetime(2026, 10, 20, 22, 15, tzinfo=UTC),
            "energy_kwh": 1.5,
            "load_kw": 2.0,
            "label": "measured",
            "n": 0.0,
            "interval_start": datetime(2026, 10, 20, 22, 0, tzinfo=UTC),
            "interval_end": datetime(2026, 10, 20, 22, 15, tzinfo=UTC),
        }
    ]


def test_sign_split_and_null_policies() -> None:
    split = m(
        fields={"grid": {"path": "g", "type": "float", "sign_split": ["import_kwh", "export_kwh"]}}
    )
    assert apply(split, {"value": [{"g": -3.0}, {"g": 2.0}]}) == [
        {"import_kwh": 0.0, "export_kwh": 3.0},
        {"import_kwh": 2.0, "export_kwh": 0.0},
    ]
    skip = m()
    assert apply(skip, {"value": [{"x": None}, {"x": 1}]}) == [{"x": 1.0}]
    keep = m(null_policy="missing")
    assert apply(keep, {"value": [{"x": None}]}) == [{"x": None, "quality": "missing"}]
    assert apply(skip, {"value": [{"x": float("nan")}]}) == []


def test_columns_layout_zips_and_checks_lengths() -> None:
    mapping = m(
        layout="columns",
        records="/cols",
        fields={
            "time": {"path": "time", "type": "datetime", "tz": "epoch_s"},
            "v": {"path": "v", "type": "float"},
        },
    )
    rows = apply(mapping, {"cols": {"time": [1792519200, 1792520100], "v": [1, 2]}})
    assert [r["v"] for r in rows] == [1.0, 2.0]
    assert rows[1]["time"] == datetime(2026, 10, 20, 18, 15, tzinfo=UTC)
    with pytest.raises(MappingError, match="lengths differ"):
        apply(mapping, {"cols": {"time": [1, 2], "v": [1]}})
    with pytest.raises(MappingError, match="missing"):
        apply(mapping, {"cols": {"time": [1]}})


def test_local_time_and_ambiguity() -> None:
    mapping = m(fields={"t": {"path": "t", "type": "datetime", "tz": "local:Europe/Warsaw"}})
    assert apply(mapping, {"value": [{"t": "2026-10-21 00:15:00"}]})[0]["t"] == datetime(
        2026, 10, 20, 22, 15, tzinfo=UTC
    )
    with pytest.raises(MappingError, match="ambiguous"):
        apply(mapping, {"value": [{"t": "2026-10-25 02:30:00"}]})


def test_epoch_ms_and_enum_map() -> None:
    mapping = m(
        fields={
            "t": {"path": "t", "type": "datetime", "tz": "epoch_ms"},
            "c": {"path": "c", "type": "str", "transforms": [{"enum_map": {"A01": "day_ahead"}}]},
        }
    )
    row = apply(mapping, {"value": [{"t": 1792519200000, "c": "A01"}]})[0]
    assert row == {"t": datetime(2026, 10, 20, 18, tzinfo=UTC), "c": "day_ahead"}
    with pytest.raises(MappingError, match="enum_map"):
        apply(mapping, {"value": [{"t": 0, "c": "A07"}]})
    assert math.isclose(1.0, 1.0)
