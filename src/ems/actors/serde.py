"""One serde for actor state, run parameters and messages: frozen dataclasses as JSON.

The SDK's default serde cannot store a dataclass, and every domain and assembler type is one, so a
``pydantic.TypeAdapter`` does the encoding: floats round-trip bit for bit, and the field order is
fixed by the class. No stored type may contain a set, whose order depends on the hash seed: Restate
compares a retried handler's writes with its journal, and different bytes would fail the retry.

JSON has no NaN or infinity: pydantic writes them as ``null``, which a ``float`` field then
refuses to read and an optional one reads as ``None``. So a value that does not decode back equal
to itself is refused before it is stored or sent (spec 012, AC-012-40). The error is a plain
``ValueError``: the invocation retries and pauses where the value was produced, and nothing is
written.
"""

from __future__ import annotations

from functools import cache
from typing import Any

from pydantic import TypeAdapter, ValidationError
from restate.serde import Serde


class DataclassSerde[T](Serde[T]):
    def __init__(self, tp: type[T]) -> None:
        self._adapter: TypeAdapter[T] = TypeAdapter(tp)

    def serialize(self, obj: T | None) -> bytes:
        if obj is None:
            return b""
        out = self._adapter.dump_json(obj)
        refused = ValueError(f"{type(obj).__name__} does not survive JSON: a NaN or an infinity?")
        try:
            back = self._adapter.validate_json(out)
        except ValidationError as exc:  # a float field written as null
            raise refused from exc
        if back != obj:  # an optional field read back as None, or NaN (never equal to itself)
            raise refused
        return out

    def deserialize(self, buf: bytes) -> T | None:
        return None if not buf else self._adapter.validate_json(buf)


@cache
def serde_for(tp: type[Any]) -> DataclassSerde[Any]:
    """The serde of one type, built once."""
    return DataclassSerde(tp)
