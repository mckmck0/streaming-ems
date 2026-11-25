"""The experiment runner: `make exp NAME=<id>` (spec 012, AC-012-25 … -32; P10: a CLI).

    python -m ems.experiments.run experiments/configs/<id>.yaml [--yes]

For each run of the config: a fresh stack with journal retention 0, the run configured, published
(paced in sim, in real time from ``first_step`` in live), the configured kills and strategy changes
carried out, its end detected from the timing log, the strategies' ends confirmed, the exactly-once
check (sim) or the invariant checks (live), the export, the metrics and ``summary.json``. A run that
fails at any point is written with ``status: failed`` and its reason, and the next run goes on.

The stack is reached through ``Ops``; ``StackOps`` is the real one (``ems.experiments.ops``), and
the tests pass a fake that records the calls.
"""

from __future__ import annotations

import contextlib
import dataclasses
import json
import math
import subprocess
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import httpx

from ems.actors.messages import StrategyChange
from ems.assembler.offline import NO_CHANGES, Offline, fold, released_by
from ems.assembler.params import RunParams
from ems.assembler.schedule import Change, Schedule, strategy_params
from ems.domain.units import QUARTERS_PER_HOUR
from ems.experiments import config, export, metrics, stack
from ems.experiments.config import CHANGE_SEND_STEPS, Experiment, RunSpec
from ems.experiments.export import RunStates
from ems.experiments.metrics import KillEvent, Line

SUMMARY_SCHEMA = "1.0.0"
MANIFEST_SCHEMA = "1.0.0"
TIMING_LOG_VERSION = 1
STDERR_LINES = 10  # of a failed command, in the reason of a failed run
POLL_S = 5.0  # the state table is read at most this often while a run is in progress
QUARTER_S = 900
CONFIG_DIR = stack.REPO / "config"
RESULTS = stack.REPO / "experiments" / "results"


class Publication(Protocol):
    """A running publisher (``stack.Publisher``)."""

    started_unix: float

    @property
    def running(self) -> bool: ...

    def wait(self, timeout: float | None = None) -> int: ...

    def kill(self) -> None: ...

    def manifest(self) -> dict[str, Any]: ...


class Ops(Protocol):
    """What the runner needs from the stack."""

    def reset(self) -> None: ...

    def open(self, run_dir: Path, *, live: bool) -> None: ...

    def close(self) -> None: ...

    def configure(
        self, params: Mapping[str, RunParams], services: tuple[str, ...] | None = None
    ) -> None: ...

    def stored_params(self, run_id: str, service: str) -> dict[str, RunParams]: ...

    def set_strategies(self, key: str, change: StrategyChange) -> None: ...

    def publish(
        self, spec: RunSpec, run_id: str, first_step: int, end_step: int
    ) -> Publication: ...

    def kill(self, target: str, downtime_s: float) -> KillEvent: ...

    def records(self, run_id: str) -> list[Any]: ...

    def timing_lines(self) -> list[Line]: ...

    def schedules(self, run_id: str) -> dict[str, Schedule]: ...

    def wait_run(
        self, run_id: str, records: int, ends: Mapping[str, Mapping[str, int]]
    ) -> None: ...

    def pending(self, run_id: str) -> int: ...

    def stuck(self, run_id: str) -> list[dict[str, Any]]: ...

    def held(self, run_id: str) -> list[dict[str, Any]]: ...

    def states(self, run_id: str) -> RunStates: ...

    def samples(self) -> dict[str, Any]: ...

    def now(self) -> float: ...

    def sleep(self, seconds: float) -> None: ...


@dataclass
class RunResult:
    """What one run produced; ``summary`` is what ``summary.json`` holds."""

    name: str
    status: str  # "ok" or "failed"
    reason: str | None = None
    summary: dict[str, Any] = field(default_factory=dict)


def kill_times(spec: RunSpec, first_step: int, end_step: int, start_unix: float) -> list[float]:
    """When each kill falls on the publication schedule: its data-time fraction of the window,
    divided by the speed, after ``start_unix``.
    """
    span_s = QUARTER_S * (end_step - first_step)
    return [start_unix + k.at * span_s / spec.run.speed for k in spec.kills]


def change_deadlines(spec: RunSpec, first_step: int, start_unix: float) -> list[float]:
    """The latest wall time to send each change: while the schedule is still
    ``config.CHANGE_SEND_STEPS`` steps of data before its ``t``.
    """
    return [
        start_unix
        + (QUARTERS_PER_HOUR * c.at_hour - CHANGE_SEND_STEPS) * QUARTER_S / spec.run.speed
        for c in spec.changes
    ]


def live_change_step(now_unix: float) -> int:
    """The first full UTC hour after ``now_unix``, as a step: a live change's ``t``."""
    return (math.floor(now_unix / 3600) + 1) * QUARTERS_PER_HOUR


class _Failed(Exception):
    """A run that ends early with a reason (and, for a stall, its diagnosis)."""

    def __init__(self, reason: str, diagnosis: dict[str, Any] | None = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.diagnosis = diagnosis or {}


def run_one(
    exp: Experiment, spec: RunSpec, ops: Ops, out_dir: Path, *, config_dir: Path, now: datetime
) -> RunResult:
    """One run, in this order: reset; open; parameters and configure; changes due before
    publishing (configure the added objects, wait until each stores equal parameters, send);
    publish; kills and later changes on the schedule; wait for the publisher; read the records and
    fold the joins; end detection from the timing log (or ``stalled``); read the schedules; wait for
    the schedule-aware ends; fold the strategies; exactly-once check (sim) or invariants (live);
    the empty-inbox check once nothing is pending; export; metrics; ``summary.json``; close.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    live = spec.run.mode == "live"
    first, end = config.window(spec, now)
    run_id = f"x{uuid.uuid4().hex[:10]}"
    summary: dict[str, Any] = {"schema_version": SUMMARY_SCHEMA, "run": spec.name, "run_id": run_id}
    manifest: dict[str, Any] = {}
    status, reason = "ok", None
    try:
        ops.reset()
        ops.open(out_dir, live=live)
        params, rcem_table = _params(spec, run_id, first, end, config_dir)
        manifest = _manifest(exp, spec, run_id, first, end, params, rcem_table, config_dir)
        _write_yaml(out_dir / "config.yaml", spec)
        ops.configure(params)
        current = {k: p.strategies for k, p in params.items()}
        changes = list(zip(spec.changes, _change_steps(spec, first, ops), strict=True))
        if live:
            _sleep_until(ops, QUARTER_S * first)
        estimated = change_deadlines(spec, first, ops.now()) if not live else []
        early = [i for i, d in enumerate(estimated) if d <= ops.now() + 60.0] if not live else []
        for i in early:
            _send_change(ops, run_id, params, current, *changes[i])
        pub = ops.publish(spec, run_id, first, end)
        later = [i for i in range(len(changes)) if i not in early]
        for i in later:
            _send_change(ops, run_id, params, current, *changes[i])
        kills: list[KillEvent] = []
        for at, kill in zip(
            kill_times(spec, first, end, pub.started_unix), spec.kills, strict=True
        ):
            _sleep_until(ops, at)
            kills.append(ops.kill(kill.target, kill.downtime_s))
        if live:
            _sleep_until(ops, QUARTER_S * end + spec.run.grace_s + 1.0)
            if pub.running:
                pub.kill()
        else:
            pub.wait()
        records = ops.records(run_id)
        joins = _fold(params, records, {})
        ends = dict.fromkeys(params, end) if live else {k: o.join.next for k, o in joins.items()}
        _wait_end(ops, spec, ends, since_ns=round(pub.started_unix * 1e9), run_id=run_id)
        schedules = ops.schedules(run_id)
        if live:
            run_ends = {s: dict.fromkeys(params, end) for s in ("Ems", "Battery", "Meter")}
        else:
            run_ends = stack.run_ends(params, joins, schedules)
        ops.wait_run(run_id, len(records), run_ends)
        twins = _fold(params, records, schedules)
        states = ops.states(run_id)
        problems = _invariants(params, states) if live else _mismatches(params, twins, states)
        summary["exactly_once"] = {"mode": spec.run.mode, "mismatches": problems}
        _wait_nothing_pending(ops, run_id)
        held = ops.held(run_id)
        summary["held"] = held
        written_twins = None if (live or problems) else twins
        lines_all = ops.timing_lines()
        kept, retries = metrics.dedup(lines_all)
        export.export(states, run_id, out_dir, offline=written_twins, lines=kept)
        pacing = pub.manifest().get("pacing") or {}
        samples = ops.samples()
        manifest["clock_offset_s"] = samples.get("clock_offset_s")
        summary |= _metrics(spec, params, first, end, kept, retries, records, twins, kills, pacing)
        summary["samples"] = samples
        summary["kills"] = [dataclasses.asdict(k) for k in kills]
        if problems:
            raise _Failed(f"{len(problems)} mismatches against the offline side: {problems[:5]}")
        if held:
            raise _Failed(f"{len(held)} objects still hold messages after the end")
        if _has_nan(summary):
            raise _Failed("a metric is NaN or infinite")
    except _Failed as exc:
        status, reason = "failed", exc.reason
        summary |= exc.diagnosis
    except Exception as exc:
        status, reason = "failed", _reason(exc)
    finally:
        with contextlib.suppress(Exception):
            ops.close()
    summary = _finite(summary)
    summary["status"], summary["reason"] = status, reason
    _write_json(out_dir / "summary.json", summary)
    if manifest:
        _write_json(out_dir / "manifest.json", _finite(manifest))
    return RunResult(spec.name, status, reason, summary)


def _params(
    spec: RunSpec, run_id: str, first: int, end: int, config_dir: Path
) -> tuple[dict[str, RunParams], Any]:
    from ems.experiments.rcem import proxy_table
    from ems.experiments.runs import managed_params, pse_table
    from ems.ingest.generator import load_generator_config

    gen = _generator_path(spec, config_dir)
    if spec.data.kind == "history":
        table = pse_table(config_dir)
    else:
        cfg = load_generator_config(gen)
        if spec.data.locations is not None:
            cfg = dataclasses.replace(cfg, synthetic_locations=spec.data.locations)
        table = proxy_table(cfg, config_dir, first, end)
    params = managed_params(
        run_id=run_id,
        start=datetime.fromtimestamp(first * QUARTER_S, UTC),
        days=(end - first) / (24 * QUARTERS_PER_HOUR),
        locations=spec.data.locations or 1,
        rcem=table,
        late_slots=spec.run.late_slots,
        strategies=spec.run.strategies,
        live=spec.run.live,
        mode=spec.run.mode,
        grace_s=spec.run.grace_s,
        generator_config=gen,
        config_dir=config_dir,
    )
    keys = sorted(params)
    if spec.data.households is not None:
        if len(keys) < spec.data.households:
            raise _Failed(f"{len(keys)} managed households, {spec.data.households} asked")
        keys = keys[: spec.data.households]
    return {k: params[k] for k in keys}, table


def _generator_path(spec: RunSpec, config_dir: Path) -> Path:
    gen = spec.data.generator_config
    return gen if gen.is_absolute() else Path(config_dir).resolve().parent / gen


def _change_steps(spec: RunSpec, first: int, ops: Ops) -> list[int]:
    if spec.run.mode == "live":
        return [live_change_step(ops.now()) for _ in spec.changes]
    return [first + QUARTERS_PER_HOUR * c.at_hour for c in spec.changes]


def _send_change(
    ops: Ops,
    run_id: str,
    params: Mapping[str, RunParams],
    current: dict[str, tuple[str, ...]],
    change: Any,
    t: int,
) -> None:
    """Configure the strategies a change adds, wait until each stores equal parameters, then send
    the change to every household (a transport error retried with the same change, so the same
    ``Idempotency-Key``).
    """
    names = tuple(sorted(dict.fromkeys(change.strategies)))
    added = sorted({n for k in params for n in names if n not in current[k]})
    for name in added:
        sent = {k: strategy_params(p, Change(t, names)) for k, p in params.items()}
        ops.configure(sent, (name,))
        deadline = ops.now() + 600.0
        while ops.stored_params(run_id, name) != sent:
            if ops.now() > deadline:
                raise _Failed(f"{name}: the added strategy's parameters were not stored")
            ops.sleep(POLL_S)
    message = StrategyChange(t, names)
    for key in sorted(params):
        for attempt in range(5):
            try:
                ops.set_strategies(key, message)
                break
            except httpx.TransportError:
                if attempt == 4:
                    raise
                ops.sleep(1.0)
        current[key] = names


def _sleep_until(ops: Ops, at: float) -> None:
    while ops.now() < at:
        ops.sleep(min(POLL_S, at - ops.now()))


def _fold(
    params: Mapping[str, RunParams], records: Sequence[Any], schedules: Mapping[str, Schedule]
) -> dict[str, Offline]:
    from ems.contracts.envelope import Stream

    out: dict[str, Offline] = {}
    for key, p in params.items():
        own = stack.records_by_stream(records, key)
        market = stack.records_by_stream(records, p.market_key)
        out[key] = fold(
            p,
            own.get(Stream.PRODUCTION.value, []),
            own.get(Stream.CONSUMPTION.value, []),
            own.get(Stream.WEATHER_FORECAST.value, []),
            market.get(Stream.PRICE.value, []),
            schedules.get(key, NO_CHANGES),
        )
    return out


def _wait_end(
    ops: Ops, spec: RunSpec, ends: Mapping[str, int], *, since_ns: int, run_id: str
) -> list[Line]:
    """Tail the timing log until every managed `Meter` has handled its last step; a stall fails
    the run with its diagnosis (AC-012-32).
    """
    limit = config.stall_s(spec.run)
    while True:
        lines = ops.timing_lines()
        if metrics.ended(lines, ends):
            return lines
        if metrics.stalled(lines, now_ns=round(ops.now() * 1e9), stall_s=limit, since_ns=since_ns):
            diagnosis = {
                "stall": {
                    "stall_s": limit,
                    "stuck": ops.stuck(run_id),
                    "held": ops.held(run_id),
                    "last_t": metrics.last_t(lines),
                }
            }
            raise _Failed("stalled", diagnosis)
        ops.sleep(POLL_S)


def _wait_nothing_pending(ops: Ops, run_id: str, timeout_s: float = 600.0) -> None:
    deadline = ops.now() + timeout_s
    while ops.pending(run_id) > 0:
        if ops.now() > deadline:
            raise _Failed("invocations still pending after the end")
        ops.sleep(POLL_S)


def _mismatches(
    params: Mapping[str, RunParams], twins: Mapping[str, Offline], states: RunStates
) -> list[str]:
    """Every household, strategy, `Battery` and `Meter` state that differs from the offline twin."""
    out: list[str] = []
    for key, p in params.items():
        twin = twins[key]
        if states.households[key].join != twin.join:
            out.append(f"Household {key}")
        for name, result in twin.strategies.items():
            st = states.strategies.get(name, {}).get(key)
            if st is None or st.result != result:
                out.append(f"{name} {key}")
        live = twin.strategies[p.live]
        if states.batteries[key].soc_kwh != live.state.soc_kwh:
            out.append(f"Battery {key}")
        m = states.meters[key]
        if (m.acc, m.recent, m.months) != (live.state.acc, live.recent, live.months):
            out.append(f"Meter {key}")
    return out


def _invariants(params: Mapping[str, RunParams], states: RunStates) -> list[str]:
    """Live runs have no offline twin (risk A4): the real battery and meter must equal the live
    strategy's shadow, and every step of the window must be released.
    """
    out: list[str] = []
    for key, p in params.items():
        shadow = states.strategies.get(p.live, {}).get(key)
        if shadow is None:
            out.append(f"{p.live} {key} missing")
            continue
        r = shadow.result
        if states.batteries[key].soc_kwh != r.state.soc_kwh:
            out.append(f"Battery {key}")
        m = states.meters[key]
        if (m.acc, m.recent, m.months) != (r.state.acc, r.recent, r.months):
            out.append(f"Meter {key}")
        if states.households[key].join.next != p.end_step:
            out.append(f"Household {key} released up to {states.households[key].join.next}")
    return out


def _metrics(
    spec: RunSpec,
    params: Mapping[str, RunParams],
    first: int,
    end: int,
    lines: Sequence[Line],
    retries: int,
    records: Sequence[Any],
    twins: Mapping[str, Offline],
    kills: Sequence[KillEvent],
    pacing: Mapping[str, Any],
) -> dict[str, Any]:
    out: dict[str, Any] = {"retries": retries}
    start_unix = float(pacing.get("paced_from_unix") or 0.0)
    end_ns = max((x.ns for x in lines), default=0)
    out |= metrics.throughput(
        lines, records=len(records), start_unix=start_unix, end_unix=end_ns / 1e9
    )
    out["publisher_lateness_s"] = pacing.get("lateness")
    steps = [(s.t, s.forecast_issue_s) for twin in twins.values() for s in twin.steps]
    out["forecast_age_h"] = dataclasses.asdict(metrics.forecast_age_h(steps))
    if spec.run.mode == "live":
        out["decision_latency_ms"] = dataclasses.asdict(metrics.decision_latency_ms(lines))
        out["release_lag_s"] = None
        out["live_release_delay_s"] = dataclasses.asdict(metrics.live_release_delay_s(lines))
        return out
    due = release_due(params, records)
    lags = metrics.release_lags_s(lines, due)
    recoveries = [metrics.recovery(k, lines, lags) for k in kills]
    windows = [metrics.recovery_window(k, r) for k, r in zip(kills, recoveries, strict=True)]
    out["decision_latency_ms"] = dataclasses.asdict(metrics.decision_latency_ms(lines, windows))
    out["release_lag_s"] = dataclasses.asdict(metrics.stats(list(lags.values())))
    out["recovery"] = [dataclasses.asdict(r) for r in recoveries]
    last_publish = max(due.values(), default=start_unix)
    rel = metrics.releases(lines)
    last_release = max(rel.values(), default=0) / 1e9
    out["sustained"] = dataclasses.asdict(
        metrics.sustained(
            lags,
            speed=spec.run.speed,
            last_publish_unix=last_publish,
            last_release_unix=last_release,
        )
    )
    return out


def release_due(
    params: Mapping[str, RunParams], records: Sequence[stack.Record]
) -> dict[tuple[str, int], float]:
    """``(key, t)`` -> the publication time (unix s, the record's ``ingested_at``) of the record
    that made step ``t`` of household ``key`` releasable: the household's records and its market's
    prices, merged in publication order, through ``offline.released_by``.
    """
    by_key: dict[str, list[stack.Record]] = {}
    for rec in records:
        by_key.setdefault(rec.key, []).append(rec)
    out: dict[tuple[str, int], float] = {}
    for key, p in params.items():
        events: list[tuple[float, str, bytes]] = []
        for rec in (*by_key.get(key, ()), *by_key.get(p.market_key, ())):
            stream = stack.stream_of(rec)
            published = _ingested_unix(rec.value)
            if stream is not None and published is not None:
                events.append((published, stream, rec.value))
        events.sort(key=lambda e: e[0])  # stable: partition order within a tie
        released = released_by(p, [(stream, raw) for _, stream, raw in events])
        out |= {(key, t): events[i][0] for t, i in released.items()}
    return out


def _ingested_unix(raw: bytes) -> float | None:
    """A record's ``ingested_at`` (the generator's wall clock when it published it); None for a
    record that does not parse (the actors count it and skip it, and so does the fold).
    """
    try:
        return datetime.fromisoformat(json.loads(raw)["ingested_at"]).timestamp()
    except (ValueError, KeyError, TypeError):
        return None


def _reason(exc: Exception) -> str:
    """The exception, and for a failed command the last lines of its stderr (which say why)."""
    text = f"{type(exc).__name__}: {exc}"
    if isinstance(exc, subprocess.CalledProcessError) and exc.stderr:
        err = exc.stderr if isinstance(exc.stderr, str) else exc.stderr.decode(errors="replace")
        text += " stderr: " + " | ".join(err.strip().splitlines()[-STDERR_LINES:])
    return text


def _manifest(
    exp: Experiment,
    spec: RunSpec,
    run_id: str,
    first: int,
    end: int,
    params: Mapping[str, RunParams],
    table: Any,
    config_dir: Path,
) -> dict[str, Any]:
    from ems.experiments.runs import rcem_used
    from ems.ingest.generator import load_generator_config

    base = stack.manifest(exp.model_dump(mode="json"))
    base["config"] = exp.model_dump(mode="json")
    seed = load_generator_config(_generator_path(spec, config_dir)).seed
    return {
        "schema_version": MANIFEST_SCHEMA,
        **base,
        "run": spec.name,
        "run_id": run_id,
        "repetition": spec.repetition,
        "seed": seed,
        "window": {
            "first_step": first,
            "end_step": end,
            "start": datetime.fromtimestamp(first * QUARTER_S, UTC).isoformat(),
            "end": datetime.fromtimestamp(end * QUARTER_S, UTC).isoformat(),
        },
        "households": sorted(params),
        "rcem": [
            {"month": e.month, "value": e.billing_value, "source": e.source} for e in table.entries
        ],
        "rcem_used": rcem_used(params),
        "timing_log_version": TIMING_LOG_VERSION,
    }


def _has_nan(value: Any) -> bool:
    if isinstance(value, float):
        return not math.isfinite(value)
    if isinstance(value, Mapping):
        return any(_has_nan(v) for v in value.values())
    if isinstance(value, (list, tuple)):
        return any(_has_nan(v) for v in value)
    return False


def _finite(value: Any) -> Any:
    """The value with every NaN or infinity replaced by None (the summary is written strictly)."""
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Mapping):
        return {str(k): _finite(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_finite(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    return value


def _write_json(path: Path, obj: dict[str, Any]) -> None:
    with path.open("w") as fh:
        json.dump(obj, fh, indent=2, sort_keys=True, allow_nan=False, default=str)
        fh.write("\n")


def _write_yaml(path: Path, spec: RunSpec) -> None:
    from ruamel.yaml import YAML

    doc = {
        "name": spec.name,
        "repetition": spec.repetition,
        "data": spec.data.model_dump(mode="json"),
        "run": spec.run.model_dump(mode="json"),
        "kills": [k.model_dump(mode="json") for k in spec.kills],
        "changes": [c.model_dump(mode="json") for c in spec.changes],
    }
    with path.open("w") as fh:
        YAML(typ="safe").dump(doc, fh)


def run_experiment(
    exp: Experiment, ops: Ops, results_root: Path, *, config_dir: Path, now: datetime
) -> list[RunResult]:
    """Every run of the expanded config under ``results_root/<id>-<UTC>/<run>/``; a speed sweep
    stops     after the first speed that is not sustained.
    """
    root = results_root / f"{exp.id}-{now.astimezone(UTC):%Y%m%dT%H%M%SZ}"
    results: list[RunResult] = []
    for spec in config.expand(exp):
        result = run_one(exp, spec, ops, root / spec.name, config_dir=config_dir, now=now)
        results.append(result)
        sustained = (result.summary.get("sustained") or {}).get("sustained")
        if exp.sweep is not None and sustained is False:
            break
    return results


def main(argv: Sequence[str] | None = None) -> int:
    """The CLI: load the config, ask for confirmation unless ``--yes``, run, print where the results
    are.
    """
    import argparse

    from ems.experiments.ops import StackOps

    ap = argparse.ArgumentParser(prog="python -m ems.experiments.run")
    ap.add_argument("config", type=Path)
    ap.add_argument("--yes", action="store_true", help="do not ask before wiping the stack")
    args = ap.parse_args(argv)
    exp = config.load(args.config)
    for spec in config.expand(exp):
        config.check_households(spec, CONFIG_DIR)
    if not args.yes:
        answer = input("This wipes the Kafka and Restate volumes for every run. Type yes: ")
        if answer.strip() != "yes":
            print("aborted")
            return 1
    results = run_experiment(exp, StackOps(), RESULTS, config_dir=CONFIG_DIR, now=datetime.now(UTC))
    for r in results:
        print(f"{r.name}: {r.status}" + (f" ({r.reason})" if r.reason else ""))
    return 0 if all(r.status == "ok" for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
