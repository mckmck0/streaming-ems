"""Deterministic fault injection over the emission stream (off by default).

Every decision is a keyed uniform of (seed, event id, fault kind), so the same faults hit the same
events on every run (AC-003-05). Faults keep the event id: a duplicate is the identical event
published twice, a late event keeps its event time and id but is emitted up to ``late_max`` later
(AC-003-06). Stuck values, register resets and quality flags change meter payloads only.
"""

from __future__ import annotations

import heapq
import itertools
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import numpy as np

from ems.contracts.envelope import Aggregation, MeasurementEvent, Quality
from ems.contracts.ids import stable_hash64
from ems.ingest import rng

Builder = Callable[[datetime], Any]
Emission = tuple[datetime, str, Builder]
METER_STREAMS = {"production", "consumption"}


def _duration(value: Any) -> timedelta:
    if isinstance(value, timedelta):
        return value
    text = str(value).upper()
    if not text.startswith("PT"):
        raise ValueError(f"late_max must be an ISO duration like PT2H, got {value!r}")
    hours = minutes = 0.0
    num = ""
    for ch in text[2:]:
        if ch.isdigit() or ch == ".":
            num += ch
        elif ch == "H":
            hours, num = float(num), ""
        elif ch == "M":
            minutes, num = float(num), ""
        else:
            raise ValueError(f"unsupported duration {value!r}")
    return timedelta(hours=hours, minutes=minutes)


@dataclass(frozen=True)
class FaultConfig:
    enabled: bool = False
    missing_rate: float = 0.0
    duplicate_rate: float = 0.0
    late_rate: float = 0.0
    late_max: timedelta = timedelta(hours=2)
    stuck_value_rate: float = 0.0
    register_reset_rate: float = 0.0
    estimated_rate: float = 0.0
    substituted_rate: float = 0.0

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> FaultConfig:
        if not raw:
            return cls()

        def rate(name: str) -> float:
            return float(raw.get(name, 0.0))

        return cls(
            enabled=bool(raw.get("enabled", False)),
            missing_rate=rate("missing_rate"),
            duplicate_rate=rate("duplicate_rate"),
            late_rate=rate("late_rate"),
            late_max=_duration(raw.get("late_max", "PT2H")),
            stuck_value_rate=rate("stuck_value_rate"),
            register_reset_rate=rate("register_reset_rate"),
            estimated_rate=rate("estimated_rate"),
            substituted_rate=rate("substituted_rate"),
        )


@dataclass
class _State:
    last_values: dict[tuple[str, str], list[float]] = field(default_factory=dict)
    register_offset: dict[str, float] = field(default_factory=dict)


def _u(seed: int, event_id: str, kind: str) -> float:
    key = stable_hash64("fault", event_id)
    return float(rng.uniform(seed, [key], f"fault.{kind}", np.array([0]))[0, 0])


def _mutate(event: Any, cfg: FaultConfig, seed: int, state: _State) -> Any:
    """Stuck values, register resets and quality flags for one meter event."""
    if not isinstance(event, MeasurementEvent):
        return event
    eid = str(event.event_id)
    key = (event.subject, event.stream.value)
    ms = list(event.payload.measurements)
    previous = state.last_values.get(key)
    if (
        previous is not None
        and len(previous) == len(ms)
        and _u(seed, eid, "stuck") < cfg.stuck_value_rate
    ):
        ms = [m.model_copy(update={"value": v}) for m, v in zip(ms, previous, strict=True)]
    loc = event.subject
    if _u(seed, eid, "register_reset") < cfg.register_reset_rate:
        regs = [m.value for m in ms if m.aggregation is Aggregation.REGISTER]
        if regs:
            state.register_offset[loc] = regs[0]
    offset = state.register_offset.get(loc, 0.0)
    if offset:
        ms = [
            m.model_copy(update={"value": max(0.0, m.value - offset)})
            if m.aggregation is Aggregation.REGISTER
            else m
            for m in ms
        ]
    q = _u(seed, eid, "quality")
    flag = None
    if q < cfg.estimated_rate:
        flag = Quality.ESTIMATED
    elif q < cfg.estimated_rate + cfg.substituted_rate:
        flag = Quality.SUBSTITUTED
    if flag is not None:
        ms = [m.model_copy(update={"quality": flag}) for m in ms]
    state.last_values[key] = [m.value for m in ms]
    payload = event.payload.model_copy(update={"measurements": ms})
    return event.model_copy(update={"payload": payload})


def apply_faults(emissions: Iterator[Emission], cfg: FaultConfig, seed: int) -> Iterator[Emission]:
    """The same emissions with faults injected, still in emission-time order."""
    if not cfg.enabled:
        yield from emissions
        return
    state = _State()
    heap: list[tuple[datetime, int, str, Builder]] = []
    seq = itertools.count()

    def restamp(event: Any) -> Builder:
        return lambda ingested_at: event.model_copy(update={"ingested_at": ingested_at})

    for emit_at, stream, build in emissions:
        while heap and heap[0][0] <= emit_at:
            at, _, s, b = heapq.heappop(heap)
            yield at, s, b
        event = build(emit_at)  # ingested_at is restamped at produce time
        eid = str(event.event_id)
        location_stream = stream in METER_STREAMS or stream == "weather_forecast"
        if location_stream and _u(seed, eid, "missing") < cfg.missing_rate:
            continue
        if stream in METER_STREAMS:
            event = _mutate(event, cfg, seed, state)
        at = emit_at
        if location_stream and _u(seed, eid, "late") < cfg.late_rate:
            at = emit_at + cfg.late_max * _u(seed, eid, "late.delay")
        heapq.heappush(heap, (at, next(seq), stream, restamp(event)))
        if _u(seed, eid, "duplicate") < cfg.duplicate_rate:
            heapq.heappush(heap, (at, next(seq), stream, restamp(event)))
    while heap:
        at, _, s, b = heapq.heappop(heap)
        yield at, s, b
