"""Export JSON Schemas from the contract models and check them for staleness and compatibility.

Schemas are open (unknown properties allowed) so v1.x readers accept later additive fields.
Compatibility is FULL: a new schema may add optional properties and must not remove properties,
change types, add required
properties or tighten enums.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ems.contracts.envelope import EVENT_TYPES

SCHEMA_DIALECT = "https://json-schema.org/draft/2020-12/schema"


def _open(node: Any) -> Any:
    if isinstance(node, dict):
        out = {
            k: _open(v) for k, v in node.items() if k != "additionalProperties" or v is not False
        }
        return out
    if isinstance(node, list):
        return [_open(v) for v in node]
    return node


def build_schemas() -> dict[str, dict[str, Any]]:
    """File name → schema, for every event type."""
    out: dict[str, dict[str, Any]] = {}
    for name, model in EVENT_TYPES.items():
        schema = model.model_json_schema(by_alias=True, mode="serialization")
        schema = {"$schema": SCHEMA_DIALECT, "$id": f"urn:ems:schema:{name}:v1", **_open(schema)}
        out[f"{name}.v1.json"] = schema
    return out


def render(schema: dict[str, Any]) -> str:
    return json.dumps(schema, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def export(out_dir: Path) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for fname, schema in build_schemas().items():
        path = out_dir / fname
        path.write_text(render(schema))
        written.append(path)
    return written


def stale(out_dir: Path) -> list[str]:
    """Files that are missing or differ from what the models export."""
    problems = []
    for fname, schema in build_schemas().items():
        path = out_dir / fname
        if not path.exists() or path.read_text() != render(schema):
            problems.append(fname)
    return problems


def _resolve(node: dict[str, Any], root: dict[str, Any]) -> dict[str, Any]:
    ref = node.get("$ref")
    if isinstance(ref, str) and ref.startswith("#/$defs/"):
        return root["$defs"][ref.split("/")[-1]]
    return node


def incompatibilities(
    old: dict[str, Any],
    new: dict[str, Any],
    path: str = "$",
    old_root: Any = None,
    new_root: Any = None,
) -> list[str]:
    """Breaking differences between two schema versions (empty list = FULL compatible)."""
    old_root = old_root or old
    new_root = new_root or new
    old = _resolve(old, old_root)
    new = _resolve(new, new_root)
    issues: list[str] = []
    if old.get("type") != new.get("type"):
        issues.append(f"{path}: type {old.get('type')} -> {new.get('type')}")
    if "enum" in old and "enum" in new and not set(old["enum"]) <= set(new["enum"]):
        issues.append(f"{path}: enum values removed")
    if "const" in old and old.get("const") != new.get("const"):
        issues.append(f"{path}: const changed")
    added_required = set(new.get("required", [])) - set(old.get("required", []))
    if added_required:
        issues.append(f"{path}: new required {sorted(added_required)}")
    old_props = old.get("properties", {})
    new_props = new.get("properties", {})
    for name, sub in old_props.items():
        if name not in new_props:
            issues.append(f"{path}.{name}: removed")
        else:
            issues += incompatibilities(sub, new_props[name], f"{path}.{name}", old_root, new_root)
    if new.get("additionalProperties") is False:
        issues.append(f"{path}: closed schema (readers of later versions would reject additions)")
    return issues


def render_examples() -> dict[str, str]:
    from ems.contracts.examples import golden_examples

    return {
        name: json.dumps(json.loads(event.model_dump_json(by_alias=True)), indent=2, sort_keys=True)
        + "\n"
        for name, event in golden_examples().items()
    }


def export_examples(out_dir: Path) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for name, text in render_examples().items():
        path = out_dir / name
        path.write_text(text)
        paths.append(path)
    return paths
