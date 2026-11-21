"""One serde for actor state, run parameters and messages: frozen dataclasses as JSON.

The SDK's default serde cannot store a dataclass, and every domain and assembler type is one, so a
``pydantic.TypeAdapter`` does the encoding: floats round-trip bit for bit, and the field order is
fixed by the class. No stored type may contain a set, whose order depends on the hash seed: Restate
compares a retried handler's writes with its journal, and different bytes would fail the retry.
"""

from __future__ import annotations

from functools import cache
from typing import Any

from pydantic import TypeAdapter
from restate.serde import Serde


class DataclassSerde[T](Serde[T]):
    def __init__(self, tp: type[T]) -> None:
        self._adapter: TypeAdapter[T] = TypeAdapter(tp)

    def serialize(self, obj: T | None) -> bytes:
        return b"" if obj is None else self._adapter.dump_json(obj)

    def deserialize(self, buf: bytes) -> T | None:
        return None if not buf else self._adapter.validate_json(buf)


@cache
def serde_for(tp: type[Any]) -> DataclassSerde[Any]:
    """The serde of one type, built once."""
    return DataclassSerde(tp)
