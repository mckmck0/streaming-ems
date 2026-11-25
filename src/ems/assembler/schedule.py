"""A household's strategy schedule: strategies added and removed while a run goes (spec 012, ADR
0021).

The operator sends ``set_strategies(t, strategies)``: from step ``t`` on, the household feeds
exactly that set. A change takes effect exactly at ``t`` or not at all; one that cannot is refused,
with its reason, and counted. ``t`` starts a UTC hour, because settlement nets per hour and a
strategy's first step must start one (``RunParams.first_step``).

Pure: the household's transitions and the offline twin both read the schedule through these
functions, so they route and fold the same steps.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass

from ems.assembler.params import RunParams
from ems.domain.actions import POLICIES
from ems.domain.units import QUARTERS_PER_HOUR


@dataclass(frozen=True)
class Change:
    t: int  # the step from which the set is in force, on a UTC hour
    strategies: tuple[str, ...]  # sorted, without duplicates


@dataclass(frozen=True)
class Refusal:
    t: int
    strategies: tuple[str, ...]
    reason: str


@dataclass(frozen=True)
class Schedule:
    changes: tuple[Change, ...] = ()  # accepted, ascending by t
    refused: tuple[Refusal, ...] = ()


def accept(
    params: RunParams,
    schedule: Schedule,
    next_step: int,
    t: int,
    strategies: tuple[str, ...] | list[str],
) -> Schedule:
    """Add the change, or a refusal with the first reason that applies, in this order."""
    names = tuple(sorted(dict.fromkeys(strategies)))
    reason = _reason(params, schedule, next_step, t, names)
    if reason is not None:
        return dataclasses.replace(schedule, refused=(*schedule.refused, Refusal(t, names, reason)))
    return dataclasses.replace(schedule, changes=(*schedule.changes, Change(t, names)))


def _reason(
    params: RunParams, schedule: Schedule, next_step: int, t: int, names: tuple[str, ...]
) -> str | None:
    if t % QUARTERS_PER_HOUR:
        return "not_on_hour"
    if t < next_step:
        return "released"
    if t >= params.end_step:
        return "after_end"
    if schedule.changes and t <= schedule.changes[-1].t:
        return "not_after_pending"
    if any(name not in POLICIES for name in names):
        return "unknown_policy"
    if params.live not in names:
        return "live_removed"
    if set(names) & _dropped(params, schedule):
        return "re_added"
    return None


def _dropped(params: RunParams, schedule: Schedule) -> set[str]:
    """Names that were in force at some point and left a later set."""
    dropped: set[str] = set()
    current = set(params.strategies)
    for change in schedule.changes:
        dropped |= current - set(change.strategies)
        current = set(change.strategies)
    return dropped


def strategies_at(params: RunParams, schedule: Schedule, t: int) -> tuple[str, ...]:
    """The set in force at step ``t``."""
    current = params.strategies
    for change in schedule.changes:
        if change.t > t:
            break
        current = change.strategies
    return current


def windows(params: RunParams, schedule: Schedule) -> dict[str, tuple[int, int | None]]:
    """Each strategy that is ever in force: its first step and the step it stops at (None if never
    removed). A removed strategy cannot come back, so each has one window.
    """
    out: dict[str, tuple[int, int | None]] = {
        name: (params.first_step, None) for name in params.strategies
    }
    current = set(params.strategies)
    for change in schedule.changes:
        new = set(change.strategies)
        for name in sorted(new - current):
            out[name] = (change.t, None)
        for name in sorted(current - new):
            out[name] = (out[name][0], change.t)
        current = new
    return out


def strategy_params(params: RunParams, change: Change) -> RunParams:
    """The parameters of a strategy that joins at ``change.t``: a fresh virtual battery there, with
    the run's initial SoC.
    """
    return dataclasses.replace(params, first_step=change.t, strategies=change.strategies)
