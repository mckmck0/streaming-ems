"""One meter stream of one household: slots, duplicates, lateness and estimates (ADR 0017, 0018).

Everything is judged inside the stream itself, in slots of its own quarter-hours, so the outcome
does not depend on how the household's other streams interleave with it. With ``L`` =
``late_slots``:
- a reading that arrives before the stream has delivered slot ``q + L`` is used normally;
- when the stream reaches ``q + L`` without it, slot ``q`` closes on an estimate: the value the
  stream used for the slot before (0 before the first reading);
- a reading that arrives after that, while the stream is less than ``2L`` past ``q``, becomes a
  ``Late`` correction, attached to the first step after the stream's newest slot;
- a reading older still is counted (``too_old``) and dropped.
The first reading of a slot wins; a second one is counted (``duplicate`` or ``conflict``).

Live mode adds the wall clock (spec 012, ADR 0020): ``meter_close`` closes every slot below a clock
step, on its reading or on the estimate. The stream's *horizon*, the higher of its newest reading
and its last closed slot, then stands in for the newest reading wherever lateness is judged. In sim
the two are always equal (a slot closes only on its reading or ``L`` newer ones), so nothing
changes.
"""

from __future__ import annotations

from dataclasses import dataclass, replace

from ems.assembler.mapping import Reading


@dataclass(frozen=True)
class Closed:
    """A slot whose value is final for the decision: a reading or an estimate."""

    t: int
    kwh: float
    estimated: bool


@dataclass(frozen=True)
class Late:
    """A reading for a slot that already closed on an estimate."""

    t: int
    kwh: float
    at_step: int  # the step that carries it as a correction


@dataclass(frozen=True)
class MeterStream:
    closed: int  # every slot below it is closed
    newest: int | None = None  # the newest slot with a reading
    last_kwh: float = 0.0  # the value used for slot ``closed - 1``
    # (t, event_id, kWh) of the slots above newest - 2L, sorted by t
    slots: tuple[tuple[int, str, float], ...] = ()
    before_start: int = 0
    after_end: int = 0
    too_old: int = 0
    duplicate: int = 0
    conflict: int = 0
    estimated: int = 0
    late: int = 0
    skipped: int = 0  # unusable entries inside accepted readings


def horizon(s: MeterStream) -> int:
    """The higher of the newest reading and the last closed slot: lateness is judged against it."""
    return s.closed - 1 if s.newest is None else max(s.newest, s.closed - 1)


def meter_apply(
    s: MeterStream, first_step: int, end_step: int, late_slots: int, r: Reading
) -> tuple[MeterStream, tuple[Closed, ...], Late | None]:
    """Add one reading: the slots it closes, in order, and a correction if it came late."""
    t = r.t
    if t < first_step:
        return replace(s, before_start=s.before_start + 1), (), None
    if t >= end_step:
        return replace(s, after_end=s.after_end + 1), (), None
    before = horizon(s)
    if t <= before - 2 * late_slots:
        return replace(s, too_old=s.too_old + 1), (), None
    for slot_t, event_id, _ in s.slots:
        if slot_t == t:
            if event_id == r.event_id:
                return replace(s, duplicate=s.duplicate + 1), (), None
            return replace(s, conflict=s.conflict + 1), (), None

    slots = {slot_t: (eid, kwh) for slot_t, eid, kwh in s.slots}
    slots[t] = (r.event_id, r.kwh)
    late = None
    late_count = s.late
    # Below `closed` and not stored: the slot closed on an estimate (after `L` newer readings, or
    # on the clock in live mode, when the stream may have no reading at all).
    if t < s.closed:
        late = Late(t, r.kwh, before + 1)
        late_count += 1
    newest = t if s.newest is None else max(s.newest, t)

    out: list[Closed] = []
    closed, last_kwh, estimated = s.closed, s.last_kwh, s.estimated
    while closed in slots or newest >= closed + late_slots:
        if closed in slots:
            item = Closed(closed, slots[closed][1], False)
        else:
            item = Closed(closed, last_kwh, True)
            estimated += 1
        out.append(item)
        last_kwh = item.kwh
        closed += 1

    after = max(newest, closed - 1)
    keep = tuple(
        (slot_t, eid, kwh)
        for slot_t, (eid, kwh) in sorted(slots.items())
        if slot_t > after - 2 * late_slots
    )
    stream = MeterStream(
        closed=closed,
        newest=newest,
        last_kwh=last_kwh,
        slots=keep,
        before_start=s.before_start,
        after_end=s.after_end,
        too_old=s.too_old,
        duplicate=s.duplicate,
        conflict=s.conflict,
        estimated=estimated,
        late=late_count,
        skipped=s.skipped + r.skipped,
    )
    return stream, tuple(out), late


def meter_close(
    s: MeterStream, until: int, late_slots: int
) -> tuple[MeterStream, tuple[Closed, ...]]:
    """Close every slot below ``until`` on its reading or on the estimate, then every slot after it
    that already holds a reading (live mode's clock, spec 012). Pruning follows the horizon.
    """
    slots = {slot_t: kwh for slot_t, _, kwh in s.slots}
    out: list[Closed] = []
    closed, last_kwh, estimated = s.closed, s.last_kwh, s.estimated
    while closed < until or closed in slots:
        if closed in slots:
            item = Closed(closed, slots[closed], False)
        else:
            item = Closed(closed, last_kwh, True)
            estimated += 1
        out.append(item)
        last_kwh = item.kwh
        closed += 1
    if not out:
        return s, ()
    stream = replace(s, closed=closed, last_kwh=last_kwh, estimated=estimated)
    edge = horizon(stream) - 2 * late_slots
    stream = replace(stream, slots=tuple(x for x in s.slots if x[0] > edge))
    return stream, tuple(out)
