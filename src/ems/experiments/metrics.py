"""Systems metrics of a run, from the timing log and the runner's events (spec 012, P16).

Pure functions. The timing log (``ems.actors.timing``) holds one line per handler commit: the wall
time in ns, the service, handler and key, the invocation id, the ``t`` of the message handled and
the ``t`` of each message sent. A retried invocation writes a second line; the first one is kept,
and an invocation with more than one line counts as retried (K14). Steps inside a recovery window
are reported with the recovery, not in the latency percentiles.
"""

from __future__ import annotations

import json
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace

QUARTER_S = 900


@dataclass(frozen=True)
class Line:
    ns: int
    svc: str
    h: str
    key: str
    inv: str
    in_t: int | None
    sent: tuple[tuple[str, int], ...]
    lines: int = 1  # lines seen for this invocation (after ``dedup``)


@dataclass(frozen=True)
class Stats:
    count: int
    p50: float | None
    p99: float | None
    max: float | None
    missing: int = 0


@dataclass(frozen=True)
class Sustained:
    speed: float
    sustained: bool
    p99_lag_s: float | None
    drain_s: float | None


@dataclass(frozen=True)
class Due:
    """When a household-step became releasable: the publication time (unix s, ``ingested_at``) and
    the stream of the record that released it.
    """

    unix: float
    stream: str


@dataclass(frozen=True)
class KillEvent:
    target: str  # "sdk" or "server"
    kill_unix: float
    healthy_unix: float


@dataclass(frozen=True)
class Recovery:
    target: str
    healthy_s: float
    first_commit_s: float | None
    caught_up_s: float | None


FOREVER_NS = 2**63 - 1


def parse(text: Iterable[str]) -> list[Line]:
    """Timing lines from the log's JSON lines; blank lines are skipped."""
    out: list[Line] = []
    for raw in text:
        if not raw.strip():
            continue
        d = json.loads(raw)
        sent = tuple((str(svc), int(t)) for svc, t in d["sent"])
        out.append(Line(int(d["ns"]), d["svc"], d["h"], d["key"], d["inv"], d["in_t"], sent))
    return out


def dedup(lines: Sequence[Line]) -> tuple[list[Line], int]:
    """The first line per invocation id (with ``lines`` set), in time order, and the number of
    invocations with more than one line (retries).
    """
    first: dict[str, Line] = {}
    seen: dict[str, int] = {}
    for line in sorted(lines, key=lambda x: x.ns):
        seen[line.inv] = seen.get(line.inv, 0) + 1
        first.setdefault(line.inv, line)
    kept = [replace(line, lines=seen[inv]) for inv, line in first.items()]
    kept.sort(key=lambda x: x.ns)
    return kept, sum(1 for n in seen.values() if n > 1)


def stats(values: Sequence[float], missing: int = 0) -> Stats:
    """Count, p50, p99 (nearest rank) and max; None for an empty sequence."""
    if not values:
        return Stats(0, None, None, None, missing)
    ordered = sorted(values)
    return Stats(len(ordered), _rank(ordered, 0.50), _rank(ordered, 0.99), ordered[-1], missing)


def _rank(ordered: Sequence[float], p: float) -> float:
    return ordered[max(1, math.ceil(p * len(ordered))) - 1]


def _first_sending(lines: Sequence[Line], svc: str) -> dict[tuple[str, int], int]:
    out: dict[tuple[str, int], int] = {}
    for line in sorted(lines, key=lambda x: x.ns):
        if line.svc != svc:
            continue
        for target, t in line.sent:
            if target == "Ems":
                out.setdefault((line.key, t), line.ns)
    return out


def releases(lines: Sequence[Line]) -> dict[tuple[str, int], int]:
    """``(key, t) -> ns`` of the first `Household` line that sends ``("Ems", t)``."""
    return _first_sending(lines, "Household")


def applies(lines: Sequence[Line]) -> dict[tuple[str, int], int]:
    """``(key, t) -> ns`` of the first `Battery` line that sends ``("Ems", t)`` (the ``Ack``)."""
    return _first_sending(lines, "Battery")


def _inside(ns: int, windows: Sequence[tuple[int, int]]) -> bool:
    return any(a <= ns <= b for a, b in windows)


def decision_latency_ms(lines: Sequence[Line], windows: Sequence[tuple[int, int]] = ()) -> Stats:
    """Per household-step, the apply minus the release, in ms. A step released inside one of the
    ``windows`` (ns ranges) is left out; a step with only one of the two lines counts as missing.
    """
    rel, app = releases(lines), applies(lines)
    values: list[float] = []
    missing = 0
    for step in sorted(set(rel) | set(app)):
        at = rel.get(step, app.get(step))
        assert at is not None
        if _inside(at, windows):
            continue
        if step in rel and step in app:
            values.append((app[step] - rel[step]) / 1e6)
        else:
            missing += 1
    return stats(values, missing)


def release_lags_s(
    lines: Sequence[Line], due: Mapping[tuple[str, int], float]
) -> dict[tuple[str, int], float]:
    """Per released household-step, the release time minus ``due[(key, t)]``: the publication
    time (``ingested_at``) of the record that made the step releasable (``offline.released_by``),
    so a step that waited for a forecast or a price counts from that record, not from its reading.
    """
    return {step: ns / 1e9 - due[step] for step, ns in releases(lines).items() if step in due}


def sustained(
    lags: Mapping[tuple[str, int], float],
    *,
    speed: float,
    last_publish_unix: float,
    last_release_unix: float,
) -> Sustained:
    """Speed ``S`` is sustained when 99 % of steps have a lag of at most ``900 / S`` s and the last
    step is released within ``2 x 900 / S`` s after the last record is published (AC-012-31).
    """
    step_s = QUARTER_S / speed
    p99 = stats(list(lags.values())).p99
    drain = last_release_unix - last_publish_unix
    ok = p99 is not None and p99 <= step_s and drain <= 2 * step_s
    return Sustained(speed, ok, p99, drain)


def recovery(
    kill: KillEvent, lines: Sequence[Line], lags: Mapping[tuple[str, int], float]
) -> Recovery:
    """The phases of one kill: kill → healthy, healthy → first commit after it, and → the first
    release whose lag is back under the p99 of the lags released before the kill. A line written
    between the kill and healthy belongs to a handler whose commit may have died with the server,
    so the first commit counts only from healthy.
    """
    kill_ns = round(kill.kill_unix * 1e9)
    healthy_ns = round(kill.healthy_unix * 1e9)
    healthy_s = kill.healthy_unix - kill.kill_unix
    after = sorted(line.ns for line in lines if line.ns > healthy_ns)
    if not after:
        return Recovery(kill.target, healthy_s, None, None)
    first_ns = after[0]
    first_commit_s = first_ns / 1e9 - kill.healthy_unix
    rel = releases(lines)
    before = [lag for step, lag in lags.items() if step in rel and rel[step] <= kill_ns]
    bound = stats(before).p99
    caught_up_s = None
    if bound is not None:
        for step, ns in sorted(rel.items(), key=lambda x: x[1]):
            if ns >= first_ns and step in lags and lags[step] <= bound:
                caught_up_s = (ns - first_ns) / 1e9
                break
    return Recovery(kill.target, healthy_s, first_commit_s, caught_up_s)


def recovery_window(kill: KillEvent, rec: Recovery) -> tuple[int, int]:
    """The ns range from the kill to caught up (or to the end of time if it never caught up)."""
    start = round(kill.kill_unix * 1e9)
    if rec.first_commit_s is None or rec.caught_up_s is None:
        return start, FOREVER_NS
    first = kill.healthy_unix + rec.first_commit_s
    return start, round((first + rec.caught_up_s) * 1e9)


def meter_last(lines: Sequence[Line]) -> dict[str, int]:
    """The highest ``in_t`` per key over the `Meter` lines."""
    out: dict[str, int] = {}
    for line in lines:
        if line.svc == "Meter" and line.in_t is not None:
            out[line.key] = max(out.get(line.key, line.in_t), line.in_t)
    return out


def ended(lines: Sequence[Line], ends: Mapping[str, int]) -> bool:
    """Every key in ``ends`` (key -> the Meter's next step at the end) has a `Meter` line with
    ``in_t`` at least ``end - 1``.
    """
    last = meter_last(lines)
    return all(key in last and last[key] >= end - 1 for key, end in ends.items())


def stalled(lines: Sequence[Line], *, now_ns: int, stall_s: float, since_ns: int) -> bool:
    """No `Meter` line advanced the highest ``in_t`` of its key for ``stall_s`` (counting from
    ``since_ns`` when there is none yet).
    """
    best: dict[str, int] = {}
    advanced = since_ns
    for line in sorted(lines, key=lambda x: x.ns):
        if line.svc != "Meter" or line.in_t is None:
            continue
        if line.key not in best or line.in_t > best[line.key]:
            best[line.key] = line.in_t
            advanced = max(advanced, line.ns)
    return (now_ns - advanced) / 1e9 > stall_s


def last_t(lines: Sequence[Line]) -> dict[str, int]:
    """The highest ``in_t`` per service, for a stall's diagnosis."""
    out: dict[str, int] = {}
    for line in lines:
        if line.in_t is not None:
            out[line.svc] = max(out.get(line.svc, line.in_t), line.in_t)
    return out


def throughput(lines: Sequence[Line], *, records: int, start_unix: float, end_unix: float) -> dict:
    """``records_per_s`` (records handled over the wall span) and ``invocations_per_s`` (kept
    timing lines over the same span).
    """
    span = end_unix - start_unix
    if span <= 0:
        return {"records_per_s": None, "invocations_per_s": None, "wall_s": span}
    return {
        "records_per_s": records / span,
        "invocations_per_s": len(lines) / span,
        "wall_s": span,
    }


def forecast_age_h(steps: Iterable[tuple[int, int | None]]) -> Stats:
    """``(t, forecast_issue_s)`` per released step: ``(900·t - issue_s) / 3600``; a step without a
    visible run counts as missing.
    """
    values: list[float] = []
    missing = 0
    for t, issue in steps:
        if issue is None:
            missing += 1
        else:
            values.append((QUARTER_S * t - issue) / 3600)
    return stats(values, missing)


def live_release_delay_s(lines: Sequence[Line]) -> Stats:
    """Live runs: per released step, the release time minus the end of its quarter
    (``900·(t+1)``).
    """
    return stats([ns / 1e9 - QUARTER_S * (t + 1) for (_k, t), ns in releases(lines).items()])
