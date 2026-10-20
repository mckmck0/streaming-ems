"""Minimal mapping engine: raw source payloads → canonical rows, driven by versioned YAML mappings.

A mapping names where the records are (a JSON pointer), whether they are rows (a list of objects) or
columns (an object of equal-length arrays, zipped by index), and how each canonical field is read:
path, type, time zone, unit conversion, transforms, sign split and quality map. Adapters assemble
contract events from the rows.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from functools import cache
from pathlib import Path
from typing import Any

import jsonschema
from ruamel.yaml import YAML

from ems.contracts.time import RESOLUTIONS, WARSAW

ENERGY = {"Wh": 1e-3, "0.1Wh": 1e-4, "kWh": 1.0, "MWh": 1e3}
POWER = {"W": 1e-3, "kW": 1.0, "MW": 1e3}

Row = dict[str, Any]


class MappingError(ValueError):
    """A mapping file is invalid, or a payload does not fit its mapping."""


@dataclass(frozen=True)
class FieldSpec:
    name: str
    path: str
    type: str = "str"
    tz: str | None = None
    unit_in: str | None = None
    unit_out: str | None = None
    required: bool = True
    transforms: tuple[Any, ...] = ()
    sign_split: tuple[str, str] | None = None
    quality_map: dict[str, str] | None = None


@dataclass(frozen=True)
class TimeSpec:
    field: str
    label: str
    resolution: timedelta


@dataclass(frozen=True)
class Mapping:
    id: str
    version: int
    source: str
    layout: str
    records: str
    fields: tuple[FieldSpec, ...]
    null_policy: str = "skip"
    time: TimeSpec | None = None
    raw: dict[str, Any] = field(default_factory=dict, compare=False, repr=False)


@cache
def _schema(mappings_dir: Path) -> dict[str, Any]:
    return json.loads((mappings_dir / "_schema" / "mapping.schema.json").read_text())


def parse_mapping(data: dict[str, Any], mappings_dir: Path, origin: str = "<mapping>") -> Mapping:
    try:
        jsonschema.Draft202012Validator(_schema(mappings_dir.resolve())).validate(data)
    except jsonschema.ValidationError as exc:
        raise MappingError(f"{origin}: {exc.message} at {list(exc.absolute_path)}") from exc
    fields = []
    for name, spec in data["fields"].items():
        split = spec.get("sign_split")
        fields.append(
            FieldSpec(
                name=str(name),
                path=str(spec["path"]),
                type=spec.get("type", "str"),
                tz=spec.get("tz"),
                unit_in=spec.get("unit_in"),
                unit_out=spec.get("unit_out"),
                required=bool(spec.get("required", True)),
                transforms=tuple(spec.get("transforms", [])),
                sign_split=(str(split[0]), str(split[1])) if split else None,
                quality_map=dict(spec["quality_map"]) if "quality_map" in spec else None,
            )
        )
        _check_units(fields[-1], origin)
    time = None
    if "time" in data:
        t = data["time"]
        if t["field"] not in data["fields"]:
            raise MappingError(f"{origin}: time.field {t['field']!r} is not a mapped field")
        time = TimeSpec(field=t["field"], label=t["label"], resolution=RESOLUTIONS[t["resolution"]])
    return Mapping(
        id=data["mapping"],
        version=int(data["version"]),
        source=data["source"],
        layout=data["layout"],
        records=data["records"],
        fields=tuple(fields),
        null_policy=data.get("null_policy", "skip"),
        time=time,
        raw=data,
    )


def load_mapping(path: Path) -> Mapping:
    data = YAML(typ="safe").load(path.read_text())
    if not isinstance(data, dict):
        raise MappingError(f"{path}: not a mapping document")
    if data.get("mapping") != path.stem:
        raise MappingError(f"{path}: 'mapping' must equal the file name ({path.stem})")
    return parse_mapping(data, path.parent, str(path))


def _check_units(spec: FieldSpec, origin: str) -> None:
    if (spec.unit_in is None) != (spec.unit_out is None):
        raise MappingError(f"{origin}: {spec.name} needs both unit_in and unit_out")
    if spec.unit_in is None or spec.unit_in == spec.unit_out:
        return
    for table in (ENERGY, POWER):
        if spec.unit_in in table and spec.unit_out in table:
            return
    raise MappingError(f"{origin}: {spec.name}: cannot convert {spec.unit_in} to {spec.unit_out}")


# ── paths ─────────────────────────────────────────────────────────────────────────────────────────


def _pointer_tokens(path: str) -> list[str]:
    """JSON pointer (``/a/b.c``: segments may contain dots) or dotted path (``a.b``)."""
    if path.startswith("/"):
        return (
            [t.replace("~1", "/").replace("~0", "~") for t in path[1:].split("/")]
            if path != "/"
            else []
        )
    return path.split(".")


def resolve(obj: Any, path: str) -> Any:
    """Value at ``path``; a ``*`` segment iterates a list and returns a list of results."""
    tokens = _pointer_tokens(path)

    def walk(cur: Any, rest: list[str]) -> Any:
        if not rest:
            return cur
        head, tail = rest[0], rest[1:]
        if head == "*":
            if not isinstance(cur, list):
                raise MappingError(f"'*' needs a list at {path!r}")
            return [walk(item, tail) for item in cur]
        if isinstance(cur, dict):
            return walk(cur.get(head), tail) if head in cur else None
        if isinstance(cur, list) and head.isdigit():
            idx = int(head)
            return walk(cur[idx], tail) if idx < len(cur) else None
        return None

    return walk(obj, tokens)


# ── values ────────────────────────────────────────────────────────────────────────────────────────


def _datetime(value: Any, tz: str | None) -> datetime:
    if tz == "epoch_s":
        return datetime.fromtimestamp(float(value), tz=UTC)
    if tz == "epoch_ms":
        return datetime.fromtimestamp(float(value) / 1000.0, tz=UTC)
    text = str(value).strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is not None:
        return parsed.astimezone(UTC)
    if tz in (None, "utc"):
        return parsed.replace(tzinfo=UTC)
    if tz == "local:Europe/Warsaw":
        local = parsed.replace(tzinfo=WARSAW)
        other = parsed.replace(tzinfo=WARSAW, fold=1)
        if local.utcoffset() != other.utcoffset():
            raise MappingError(f"ambiguous local time {text} (DST fall-back); use a UTC field")
        return local.astimezone(UTC)
    raise MappingError(f"unknown tz {tz!r}")


def _convert(spec: FieldSpec, raw: Any) -> Any:
    if raw is None:
        return None
    if spec.type == "float":
        value: Any = float(raw)
        if not math.isfinite(value):
            return None
    elif spec.type == "int":
        value = int(raw)
    elif spec.type == "datetime":
        return _datetime(raw, spec.tz)
    elif spec.type == "date":
        return date.fromisoformat(str(raw)[:10])
    else:
        value = str(raw)
    if spec.unit_in and spec.unit_out and spec.unit_in != spec.unit_out:
        table = ENERGY if spec.unit_in in ENERGY else POWER
        value = float(value) * table[spec.unit_in] / table[spec.unit_out]
    return value


def _transform(spec: FieldSpec, value: Any) -> Any:
    for op in spec.transforms:
        if op == "negate":
            value = None if value is None else -value
        elif "scale" in op:
            value = None if value is None else value * float(op["scale"])
        elif "enum_map" in op:
            if value is not None:
                if str(value) not in op["enum_map"]:
                    raise MappingError(f"{spec.name}: value {value!r} not in enum_map")
                value = op["enum_map"][str(value)]
        elif "default" in op:
            value = op["default"] if value is None else value
        elif "round" in op:
            value = None if value is None else round(value, int(op["round"]))
    if spec.quality_map is not None and value is not None:
        value = spec.quality_map.get(str(value), "estimated")
    return value


# ── apply ─────────────────────────────────────────────────────────────────────────────────────────


def _records(mapping: Mapping, payload: Any) -> list[dict[str, Any]]:
    found = resolve(payload, mapping.records)
    if mapping.layout == "rows":
        if found is None:
            return []
        if not isinstance(found, list):
            raise MappingError(f"{mapping.id}: records at {mapping.records} must be a list")
        return [r for r in found if isinstance(r, dict)]
    if not isinstance(found, dict):
        raise MappingError(
            f"{mapping.id}: columns at {mapping.records} must be an object of arrays"
        )
    columns = {f.path: resolve(found, f.path) for f in mapping.fields}
    lengths = {k: len(v) for k, v in columns.items() if isinstance(v, list)}
    missing = [k for k, v in columns.items() if v is None]
    if missing:
        raise MappingError(f"{mapping.id}: columns missing from payload: {missing}")
    if len(set(lengths.values())) > 1:
        raise MappingError(f"{mapping.id}: column lengths differ: {lengths}")
    n = next(iter(lengths.values()), 0)
    return [{k: v[i] for k, v in columns.items()} for i in range(n)]


def apply(mapping: Mapping, payload: Any) -> list[Row]:
    """Canonical rows for ``payload``. Rows keep input order."""
    rows: list[Row] = []
    for record in _records(mapping, payload):
        row: Row = {}
        incomplete = False
        for spec in mapping.fields:
            raw = (
                record.get(spec.path) if mapping.layout == "columns" else resolve(record, spec.path)
            )
            value = _transform(spec, _convert(spec, raw))
            if value is None and spec.required:
                incomplete = True
            if spec.sign_split is not None:
                pos, neg = spec.sign_split
                row[pos] = None if value is None else max(value, 0.0)
                row[neg] = None if value is None else max(-value, 0.0)
            else:
                row[spec.name] = value
        if incomplete:
            if mapping.null_policy == "skip":
                continue
            row["quality"] = "missing"
        if mapping.time is not None:
            stamp = row.get(mapping.time.field)
            if isinstance(stamp, datetime):
                res = mapping.time.resolution
                if mapping.time.label == "end":
                    row["interval_start"], row["interval_end"] = stamp - res, stamp
                elif mapping.time.label == "start":
                    row["interval_start"], row["interval_end"] = stamp, stamp + res
                else:
                    row["instant"] = stamp
        rows.append(row)
    return rows


@cache
def builtin(name: str, mappings_dir: Path) -> Mapping:
    """A mapping by id (``pse.rce.v2``) from ``mappings_dir`` (settings: ``EMS_MAPPINGS_DIR``)."""
    return load_mapping(mappings_dir.resolve() / f"{name}.yaml")
