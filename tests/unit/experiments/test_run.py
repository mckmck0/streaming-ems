"""AC-012-17, -25, -26, -28, -32 (plan §7, Runner): ``ems.experiments.run`` against a fake stack.

``FakeOps`` implements ``run.Ops``. It records every call with the time of its fake clock, where
``now()`` reads the clock and ``sleep()`` moves it on; nothing waits on the wall clock. What the
stack would hold is real: the records are a generated run of the 10-household catalog (3 managed
households, the run's own ``run_id``), and the object states come from the actors' transitions run
as the in-memory network of ``tests/network.py`` with the parameters the runner configured and the
strategy changes it sent. So a correct runner finds no mismatch against the offline twin, and a
state the fake perturbs is one.

The fake's timing log: per household and released step, a `Household` line sending ``("Ems", t)``,
a `Battery` line (the Ack) and a `Meter` line with ``in_t = t``, each visible once the fake clock
passes its time. In sim a step is released ``RELEASE_S`` after its reading is due on the publication
schedule; in live at the end of its quarter plus ``G`` + 1 s.

Sim window: 2026-10-31T21:00Z for 6 h (24 steps), ``L`` = 2, speed 96, so the publication takes
225 s of fake time. Live: ``now`` = 2026-11-25T10:17Z, so the window runs from 11:00Z to 13:00Z.
"""

from __future__ import annotations

import dataclasses
import json
import math
import socket
import subprocess
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pyarrow.parquet as pq
import pytest

from ems.actors.messages import StrategyChange
from ems.assembler.offline import released_by
from ems.assembler.params import RunParams
from ems.assembler.schedule import Change, Schedule, strategy_params, windows
from ems.contracts.time import quarter_index
from ems.experiments import config, export, run, stack
from ems.experiments.config import Experiment, RunSpec
from ems.experiments.metrics import KillEvent, Line
from ems.ingest.generator import load_generator_config
from tests.network import CONSUMPTION, PRODUCTION, Generated, Network, Order, generate

REPO = Path(__file__).resolve().parents[3]
CONFIG_DIR = REPO / "config"
GENERATOR = CONFIG_DIR / "generator.yaml"
BOTH = ("NoBattery", "SelfConsumption")
ONLY = ("SelfConsumption",)
CONFIGURED = ("Household", "Weather", "Ems", "Battery", "Meter")
SIM_START = datetime(2026, 10, 31, 21, tzinfo=UTC)
SIM_HOURS = 6
L = 2
T0 = 1_790_000_000.0  # the fake wall clock when a sim run starts
LIVE_NOW = datetime(2026, 11, 25, 10, 17, tzinfo=UTC)
LIVE_FIRST = datetime(2026, 11, 25, 11, tzinfo=UTC)
LIVE_HOURS = 2
GRACE_S = 120
STORED_AFTER_S = 7.0  # configured parameters show in the state table this long after configure
RELEASE_S = 0.5
OFFSET_S = 0.25  # the fake host-VM clock offset
TABLES = ("households", "strategies", "months", "timings")


# ── the fake stack ───────────────────────────────────────────────────────────────────────────────


class FakePublication:
    """``stack.Publisher`` as the runner sees it, on the fake clock."""

    def __init__(self, ops: FakeOps, spec: RunSpec, first: int, end: int) -> None:
        self.ops = ops
        self.spec = spec
        self.started_unix = ops.t
        self.speed = spec.run.speed
        if spec.run.mode == "live":
            self.end_unix = 900.0 * end + 30.0
        else:
            self.end_unix = ops.t + 900.0 * (end - first) / self.speed + 1.0
        self.killed = False

    @property
    def running(self) -> bool:
        return not self.killed and self.ops.t < self.end_unix

    def wait(self, timeout: float | None = None) -> int:
        self.ops.log("publication.wait", timeout)
        if self.killed or self.ops.t >= self.end_unix:
            return 0
        if timeout is not None and self.ops.t + timeout < self.end_unix:
            self.ops.t += timeout
            raise subprocess.TimeoutExpired(["ems-gen"], timeout)
        self.ops.t = self.end_unix
        return 0

    def kill(self) -> None:
        self.ops.log("publication.kill")
        self.killed = True

    def manifest(self) -> dict[str, Any]:
        speed = None if self.spec.run.mode == "live" else self.speed
        pacing = {
            "paced_from_unix": self.started_unix,
            "speed": speed,
            "lateness": {"p99_s": 0.01, "max_s": 0.05},
        }
        return {"run_id": self.ops.run_id, "pacing": pacing}


@dataclasses.dataclass
class FakeOps:
    """``run.Ops`` over the in-memory network, with a fake clock (see the module docstring)."""

    start: datetime = SIM_START
    hours: int = SIM_HOURS
    clock: float = T0
    stall_after: int | None = None  # Battery and Meter lines stop at first_step + this
    held_rows: list[dict[str, Any]] = dataclasses.field(default_factory=list)
    stuck_rows: list[dict[str, Any]] = dataclasses.field(default_factory=list)
    pending_answers: list[int] = dataclasses.field(default_factory=lambda: [2, 0])
    fail_set_first: bool = False
    fail_configure_runs: tuple[int, ...] = ()  # runs (counted by reset) whose configure raises
    fail_reset_runs: tuple[int, ...] = ()  # runs whose reset fails as a command with stderr
    perturb: bool = False
    release_delay_s: Mapping[float, float] = dataclasses.field(default_factory=dict)
    nan_samples: bool = False

    def __post_init__(self) -> None:
        self.t = self.clock
        self.calls: list[tuple[str, float, Any]] = []
        self.publications: list[FakePublication] = []
        self.runs = 0
        self._new_run()

    def _new_run(self) -> None:
        self.run_id: str | None = None
        self.configured: dict[tuple[str, str], RunParams] = {}
        self.configured_at: dict[tuple[str, str], float] = {}
        self.conflicts: list[tuple[str, str]] = []
        self.set_attempts: list[tuple[str, StrategyChange, bool]] = []  # (key, change, raised)
        self.sent: list[tuple[str, StrategyChange]] = []
        self.pending_left = list(self.pending_answers)
        self.pending_seen: list[int] = []
        self._net: Network | None = None
        self._gen: Generated | None = None
        self.states_running: list[bool] = []

    # -- the record --

    def log(self, name: str, detail: Any = None) -> None:
        self.calls.append((name, self.t, detail))

    def names(self) -> list[str]:
        return [name for name, _, _ in self.calls]

    def at(self, name: str) -> list[tuple[float, Any]]:
        return [(t, d) for n, t, d in self.calls if n == name]

    def first(self, name: str) -> int:
        names = self.names()
        assert name in names, f"{name} was never called: {names}"
        return names.index(name)

    def last(self, name: str) -> int:
        names = self.names()
        assert name in names, f"{name} was never called: {names}"
        return len(names) - 1 - names[::-1].index(name)

    # -- the clock --

    def now(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.t += max(0.0, seconds)
        if self.t - self.clock > 30 * 86400:
            raise RuntimeError("the fake clock ran 30 days: the runner does not stop")

    # -- the stack --

    def reset(self) -> None:
        self.runs += 1
        self._new_run()
        self.log("reset")
        if self.runs in self.fail_reset_runs:
            raise subprocess.CalledProcessError(
                1, ["docker", "compose", "up"], output="", stderr="line 1\ncontainer exited (0)\n"
            )

    def open(self, run_dir: Path, *, live: bool) -> None:
        self.log("open", (run_dir, live))

    def close(self) -> None:
        self.log("close")

    def configure(
        self, params: Mapping[str, RunParams], services: tuple[str, ...] | None = None
    ) -> None:
        self.log("configure", (dict(params), services))
        if self.runs in self.fail_configure_runs:
            raise RuntimeError("boom: configure failed")
        for key, p in params.items():
            self.run_id = p.run_id
            for service in services or (*CONFIGURED, *p.strategies):
                at = (service, key)
                if at not in self.configured:
                    self.configured[at] = p
                    self.configured_at[at] = self.t
                elif self.configured[at] != p:
                    self.conflicts.append(at)  # shell.configure would fail this terminally
        self._net = None

    def stored_params(self, run_id: str, service: str) -> dict[str, RunParams]:
        out = {
            key: p
            for (svc, key), p in self.configured.items()
            if svc == service and self.configured_at[(svc, key)] + STORED_AFTER_S <= self.t
        }
        self.log("stored_params", (service, out))
        return out

    def set_strategies(self, key: str, change: StrategyChange) -> None:
        raised = self.fail_set_first and not self.set_attempts
        self.set_attempts.append((key, change, raised))
        self.log("set_strategies", (key, change, raised))
        if raised:
            request = httpx.Request("POST", "http://127.0.0.1:8080/Household/x/set_strategies/send")
            raise httpx.ConnectError("connection reset", request=request)
        self.sent.append((key, change))
        self._net = None

    def publish(
        self, spec: RunSpec, run_id: str, first_step: int, end_step: int
    ) -> FakePublication:
        self.log("publish", (spec, run_id, first_step, end_step))
        self.run_id = run_id
        pub = FakePublication(self, spec, first_step, end_step)
        self.publications.append(pub)
        return pub

    def kill(self, target: str, downtime_s: float) -> KillEvent:
        self.log("kill", (target, downtime_s))
        kill_unix = self.t
        self.t += downtime_s + 2.0
        return KillEvent(target, kill_unix, self.t)

    def gen(self) -> Generated:
        assert self.run_id is not None, "nothing was configured or published yet"
        if self._gen is None:
            self._gen = generate(self.start, self.hours, L, False, self.run_id)
        return self._gen

    def records(self, run_id: str) -> list[stack.Record]:
        self.log("records", run_id)
        return self.record_list()

    def record_list(self) -> list[stack.Record]:
        """The records without a log entry (for the tests' own checks). In sim each carries as
        ``ingested_at`` the fake time it was published at: its emission time on the schedule.
        """
        gen = self.gen()
        out = []
        for (topic, key), values in sorted(gen.records.items()):
            times = gen.emitted[(topic, key)]
            out += [
                stack.Record(key, (topic, 0, i), self._stamped(v, times[i]))
                for i, v in enumerate(values)
            ]
        return out

    def _published_unix(self, emit_at: datetime) -> float | None:
        if not self.publications or self.publications[-1].spec.run.mode == "live":
            return None
        pub = self.publications[-1]
        return pub.started_unix + (emit_at - self.start).total_seconds() / pub.speed

    def _stamped(self, value: bytes, emit_at: datetime) -> bytes:
        at = self._published_unix(emit_at)
        if at is None:
            return value
        doc = json.loads(value)
        doc["ingested_at"] = datetime.fromtimestamp(at, UTC).isoformat()
        return json.dumps(doc).encode()

    def release_due(self, key: str, p: RunParams) -> dict[int, float]:
        """Step -> the fake publication time of the record that made it releasable (the household's
        and the market's records merged by emission time, through ``offline.released_by``).
        """
        gen = self.gen()
        events: list[tuple[datetime, str, bytes]] = []
        for (topic, k), values in gen.records.items():
            if k in (key, p.market_key):
                stream = stack.stream_of(stack.Record(k, (topic, 0, 0), b""))
                if stream is not None:
                    times = gen.emitted[(topic, k)]
                    events += [(e, stream, v) for e, v in zip(times, values, strict=True)]
        events.sort(key=lambda e: e[0])
        by = released_by(p, [(stream, v) for _, stream, v in events])
        return {t: self._published_unix(events[i][0]) or 0.0 for t, i in by.items()}

    def household_params(self) -> dict[str, RunParams]:
        return {k: p for (svc, k), p in self.configured.items() if svc == "Household"}

    def net(self) -> Network:
        if self._net is None:
            net = Network(self.gen(), params=self.household_params())
            for key, change in self.sent:
                net.change(key, change.t, change.strategies)
            while net.controls:
                net.deliver_control()
            net.run(Order(seed=0, weights=(1, 1, 1, 1, 1)))
            assert not net.pending()
            self._net = net
        return self._net

    def timing_lines(self) -> list[Line]:
        self.log("timing_lines")
        return self.lines()

    def lines(self) -> list[Line]:
        """The timing lines visible now, without a log entry (for the tests' own checks)."""
        if not self.publications:
            return []
        pub = self.publications[-1]
        lines: list[Line] = []
        for key, p in sorted(self.household_params().items()):
            end = self.net().state("Household", key).join.next
            due = {} if p.mode == "live" else self.release_due(key, p)
            for t in range(p.first_step, end):
                if p.mode == "live":
                    rel = 900.0 * (t + 1) + p.grace_s + 1.0
                else:
                    rel = due[t] + self.release_delay_s.get(pub.speed, RELEASE_S)
                sent = ((p.live, t), ("Ems", t))
                lines.append(
                    Line(_ns(rel), "Household", "on_measurement", key, f"h{key}{t}", None, sent)
                )
                if self.stall_after is not None and t >= p.first_step + self.stall_after:
                    continue
                ack = (("Ems", t), ("Meter", t))
                lines.append(Line(_ns(rel + 0.002), "Battery", "apply", key, f"b{key}{t}", t, ack))
                lines.append(Line(_ns(rel + 0.004), "Meter", "on_flows", key, f"m{key}{t}", t, ()))
        return sorted((x for x in lines if x.ns <= _ns(self.t)), key=lambda x: x.ns)

    def schedules(self, run_id: str) -> dict[str, Schedule]:
        self.log("schedules", run_id)
        return {k: self.net().state("Household", k).schedule for k in self.household_params()}

    def wait_run(self, run_id: str, records: int, ends: Mapping[str, Mapping[str, int]]) -> None:
        self.log("wait_run", (run_id, records, {s: dict(e) for s, e in ends.items()}))

    def pending(self, run_id: str) -> int:
        n = self.pending_left.pop(0) if self.pending_left else 0
        self.pending_seen.append(n)
        self.log("pending", n)
        return n

    def stuck(self, run_id: str) -> list[dict[str, Any]]:
        self.log("stuck", run_id)
        return list(self.stuck_rows)

    def held(self, run_id: str) -> list[dict[str, Any]]:
        self.log("held", run_id)
        return list(self.held_rows)

    def states(self, run_id: str) -> export.RunStates:
        self.log("states", run_id)
        self.states_running.append(any(p.running for p in self.publications))
        net = self.net()
        params = self.household_params()
        keys = sorted(params)
        names = sorted(
            {n for k in keys for n in windows(params[k], net.state("Household", k).schedule)}
        )
        batteries = {k: net.state("Battery", k) for k in keys}
        if self.perturb:
            k0 = keys[0]
            batteries[k0] = dataclasses.replace(batteries[k0], soc_kwh=batteries[k0].soc_kwh + 0.25)
        gen = self.gen()
        return export.RunStates(
            params=params,
            households={k: net.state("Household", k) for k in keys},
            strategies={n: {k: net.state(n, k) for k in keys} for n in names},
            strategy_params={n: {k: net.params[(n, k)] for k in keys} for n in names},
            batteries=batteries,
            meters={k: net.state("Meter", k) for k in keys},
            records={k: len(gen.of(PRODUCTION, k)) + len(gen.of(CONSUMPTION, k)) for k in keys},
        )

    def samples(self) -> dict[str, Any]:
        self.log("samples")
        cpu = math.nan if self.nan_samples else 120.5
        return {
            "memory_peaks": {stack.RESTATE_CONTAINER: 1_200_000_000},
            "cpu_peaks": {stack.RESTATE_CONTAINER: cpu},
            "anon_peaks": {stack.RESTATE_CONTAINER: 900_000_000},
            "oom_killed": {stack.RESTATE_CONTAINER: False},
            "host": {"cpu_peak": 80.0, "rss_peak": 200_000_000},
            "lag_peaks": {"ems-ingress-meters": 12},
            "clock_offset_s": OFFSET_S,
        }


def _ns(unix: float) -> int:
    return round(unix * 1e9)


def _block_stack(mp: pytest.MonkeyPatch) -> None:
    """The runner reaches the stack only through ``Ops``, and this tier has no network."""

    def refuse(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError(f"the runner reached docker, compose or the network: {args!r}")

    mp.setattr(stack, "docker", refuse)
    mp.setattr(stack, "compose", refuse)
    mp.setattr(stack, "docker_info", lambda: {})
    mp.setattr(stack, "clock_offset_s", lambda *a, **k: OFFSET_S)
    mp.setattr(socket.socket, "connect", refuse)
    mp.setattr(socket, "create_connection", refuse)


@pytest.fixture(autouse=True)
def _no_stack(monkeypatch: pytest.MonkeyPatch) -> None:
    _block_stack(monkeypatch)


# ── configs ──────────────────────────────────────────────────────────────────────────────────────


def _experiment(**sections: Any) -> Experiment:
    doc: dict[str, Any] = {
        "id": "t-run",
        "hypothesis": "the runner runs a config end to end",
        "data": {
            "kind": "synthetic",
            "start": SIM_START,
            "days": SIM_HOURS / 24,
            "locations": 10,
            "generator_config": str(GENERATOR),
        },
        "run": {
            "late_slots": L,
            "strategies": list(BOTH),
            "live": "SelfConsumption",
            "mode": "sim",
            "speed": 96.0,
        },
    }
    for name, value in sections.items():
        doc[name] = {**doc[name], **value} if isinstance(doc.get(name), dict) else value
    return Experiment.model_validate(doc)


def _spec(exp: Experiment, name: str = "run-0") -> RunSpec:
    return RunSpec(name, exp.data, exp.run, exp.kills, exp.changes, 0)


def _first_end(exp: Experiment) -> tuple[int, int]:
    first = quarter_index(SIM_START if exp.run.mode == "sim" else LIVE_FIRST)
    hours = SIM_HOURS if exp.run.mode == "sim" else LIVE_HOURS
    return first, first + 4 * hours


def _run_one(exp: Experiment, ops: FakeOps, out: Path) -> run.RunResult:
    now = LIVE_NOW if exp.run.mode == "live" else datetime.fromtimestamp(T0, UTC)
    return run.run_one(exp, _spec(exp), ops, out, config_dir=CONFIG_DIR, now=now)


def _run_dir(out: Path) -> Path:
    (summary,) = out.rglob("summary.json")
    return summary.parent


def _summary(out: Path) -> dict[str, Any]:
    return _strict_json(_run_dir(out) / "summary.json")


def _strict_json(path: Path) -> dict[str, Any]:
    def refuse(name: str) -> Any:
        raise AssertionError(f"{path.name} holds {name}")

    return json.loads(path.read_text(), parse_constant=refuse)


# ── kill times, change deadlines, the live change step ───────────────────────────────────────────


def _timed_spec(
    kills: list[dict[str, Any]], changes: list[dict[str, Any]], speed: float
) -> RunSpec:
    exp = _experiment(
        data={"start": datetime(2026, 6, 3, tzinfo=UTC), "days": 1, "locations": 350},
        run={"strategies": list(ONLY), "speed": speed},
        kills=kills,
        changes=changes,
    )
    return _spec(exp)


@pytest.mark.parametrize(("speed", "offsets"), [(96.0, [270.0, 630.0]), (192.0, [135.0, 315.0])])
def test_ac_012_30_kills_fall_at_their_data_time_fraction_of_the_schedule(
    speed: float, offsets: list[float]
) -> None:
    kills = [{"target": "sdk", "at": 0.3}, {"target": "server", "at": 0.7}]
    spec = _timed_spec(kills, [], speed)
    got = run.kill_times(spec, 1000, 1096, 5000.0)
    assert got == pytest.approx([5000.0 + o for o in offsets])


def test_ac_012_17_a_change_is_sent_while_the_schedule_is_two_hours_of_data_before_t() -> None:
    changes = [
        {"at_hour": 1, "strategies": list(BOTH)},
        {"at_hour": 12, "strategies": list(BOTH)},
        {"at_hour": 36, "strategies": list(ONLY)},
    ]
    spec = _timed_spec([], changes, 96.0)
    # (4·at_hour - CHANGE_SEND_STEPS) steps of data, 900 s each, at speed 96.
    assert run.change_deadlines(spec, 1000, 5000.0) == pytest.approx(
        [5000.0 - 37.5, 5000.0 + 375.0, 5000.0 + 1275.0]
    )


@pytest.mark.parametrize(
    ("now", "hour"),
    [
        (datetime(2026, 11, 25, 10, 17, 3, tzinfo=UTC), datetime(2026, 11, 25, 11, tzinfo=UTC)),
        (datetime(2026, 11, 25, 10, 59, 59, tzinfo=UTC), datetime(2026, 11, 25, 11, tzinfo=UTC)),
        (datetime(2026, 11, 25, 23, 0, 1, tzinfo=UTC), datetime(2026, 11, 26, 0, tzinfo=UTC)),
    ],
)
def test_ac_012_17_a_live_change_takes_the_first_full_utc_hour_after_the_request(
    now: datetime, hour: datetime
) -> None:
    assert run.live_change_step(now.timestamp()) == quarter_index(hour)


# ── one sim run: the order of everything ─────────────────────────────────────────────────────────

ADD_AT, REMOVE_AT, KILL_AT = 1, 3, 0.75  # hours, hours, fraction (step 18 of 24)


def _sim_with_changes() -> Experiment:
    return _experiment(
        run={"strategies": list(ONLY)},
        changes=[
            {"at_hour": ADD_AT, "strategies": list(BOTH)},
            {"at_hour": REMOVE_AT, "strategies": list(ONLY)},
        ],
        kills=[{"target": "sdk", "at": KILL_AT}],
    )


@dataclasses.dataclass(frozen=True)
class SimRun:
    exp: Experiment
    ops: FakeOps
    result: run.RunResult
    out: Path


@pytest.fixture(scope="module")
def sim_run(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SimRun]:
    with pytest.MonkeyPatch.context() as mp:
        _block_stack(mp)  # module scope: set up before the autouse fixtures of a test
        exp = _sim_with_changes()
        ops = FakeOps()
        out = tmp_path_factory.mktemp("sim-run")
        result = _run_one(exp, ops, out)
        yield SimRun(exp, ops, result, out)


def _publish(ops: FakeOps) -> tuple[float, RunSpec, str, int, int]:
    ((t, (spec, run_id, first, end)),) = ops.at("publish")
    return t, spec, run_id, first, end


def test_ac_012_25_a_clean_sim_run_is_ok(sim_run: SimRun) -> None:
    assert (sim_run.result.status, sim_run.result.reason) == ("ok", None), sim_run.result.reason
    summary = _summary(sim_run.out)
    assert summary["status"] == "ok"
    assert summary["schema_version"] == run.SUMMARY_SCHEMA == "1.0.0"


def test_ac_012_25_the_run_starts_on_a_fresh_stack_and_ends_closing_it(sim_run: SimRun) -> None:
    ops = sim_run.ops
    names = ops.names()
    assert names[:2] == ["reset", "open"]
    ((_, (run_dir, live)),) = ops.at("open")
    assert live is False
    assert Path(run_dir).resolve() == _run_dir(sim_run.out).resolve()
    assert names[-1] == "close"
    assert names.count("reset") == names.count("close") == 1


def test_ac_012_25_the_run_is_configured_with_its_window_and_strategies(sim_run: SimRun) -> None:
    ops = sim_run.ops
    first, end = _first_end(sim_run.exp)
    params = ops.at("configure")[0][1][0]
    assert len(params) == 3  # the managed sites of the 10-location catalog
    for key, p in params.items():
        assert key == p.household_key
        assert (p.first_step, p.end_step, p.late_slots) == (first, end, L)
        assert (p.strategies, p.live, p.mode) == (ONLY, "SelfConsumption", "sim")
    assert len({p.run_id for p in params.values()}) == 1
    _, _, run_id, pub_first, pub_end = _publish(ops)
    assert {p.run_id for p in params.values()} == {run_id}
    assert (pub_first, pub_end) == (first, end)
    assert ops.first("configure") < ops.first("publish")


def test_ac_012_17_an_added_strategy_is_configured_and_stored_before_the_change_is_sent(
    sim_run: SimRun,
) -> None:
    ops = sim_run.ops
    first, _ = _first_end(sim_run.exp)
    t = first + 4 * ADD_AT
    household = ops.at("configure")[0][1][0]
    added = {k: strategy_params(p, Change(t, BOTH)) for k, p in household.items()}
    later = ops.at("configure")[1:]  # after the run's own configure: the added objects
    assert later, "the added strategy was never configured"
    sent: dict[str, RunParams] = {}
    for _, (params, _) in later:
        sent |= params
    assert sent == added
    when = max(at for at, _ in later)
    assert ops.conflicts == [], "configure sent other parameters to an object configured before"
    assert {k: ops.configured[("NoBattery", k)] for k in added} == added
    sends = [(at, key, ch) for at, (key, ch, _) in ops.at("set_strategies") if ch.t == t]
    assert {key for _, key, _ in sends} == set(added)
    assert all(ch == StrategyChange(t, BOTH) for _, _, ch in sends)
    for at, _, _ in sends:
        assert at >= when + STORED_AFTER_S  # only once the state table shows equal parameters
    seen = [d for at, d in ops.at("stored_params") if at <= min(a for a, _, _ in sends)]
    assert ("NoBattery", added) in seen
    publish_at = _publish(ops)[0]
    assert max(at for at, _, _ in sends) <= publish_at
    assert ops.first("set_strategies") < ops.first("publish")


def test_ac_012_17_a_later_change_is_sent_before_its_deadline(sim_run: SimRun) -> None:
    ops = sim_run.ops
    first, _ = _first_end(sim_run.exp)
    t = first + 4 * REMOVE_AT
    started = ops.publications[0].started_unix
    deadline = run.change_deadlines(_spec(sim_run.exp), first, started)[1]
    assert deadline == pytest.approx(started + (4 * REMOVE_AT - 8) * 900 / 96)
    sends = [(at, key, ch) for at, (key, ch, _) in ops.at("set_strategies") if ch.t == t]
    assert len(sends) == 3
    assert all(ch == StrategyChange(t, ONLY) for _, _, ch in sends)
    assert all(at <= deadline for at, _, _ in sends)


def test_ac_012_30_the_kill_happens_at_its_time_on_the_schedule(sim_run: SimRun) -> None:
    ops = sim_run.ops
    first, end = _first_end(sim_run.exp)
    started = ops.publications[0].started_unix
    (kill_at,) = run.kill_times(_spec(sim_run.exp), first, end, started)
    assert kill_at == pytest.approx(started + KILL_AT * 900 * (end - first) / 96)
    ((at, (target, downtime)),) = ops.at("kill")
    assert (target, downtime) == ("sdk", 0.0)
    assert kill_at <= at <= kill_at + run.POLL_S
    assert ops.first("publish") < ops.first("kill") < ops.first("records")


def test_ac_012_32_the_end_is_detected_from_the_timing_log_before_the_schedules(
    sim_run: SimRun,
) -> None:
    ops = sim_run.ops
    names = ops.names()
    waited = ops.first("publication.wait")
    assert waited < ops.first("records") < ops.first("schedules")
    assert any(n == "timing_lines" for n in names[ops.first("records") : ops.first("schedules")])
    # The end: every Meter has handled the step before the join's end (here the window's end).
    _, end = _first_end(sim_run.exp)
    assert ops.at("schedules")[0][0] >= ops.publications[0].end_unix
    last_meter = max(x.ns for x in ops.lines() if x.svc == "Meter" and x.in_t == end - 1)
    assert _ns(ops.at("schedules")[0][0]) >= last_meter


def test_ac_012_37_wait_run_gets_the_schedule_aware_ends(sim_run: SimRun) -> None:
    ops = sim_run.ops
    first, end = _first_end(sim_run.exp)
    ((_, (run_id, records, ends)),) = ops.at("wait_run")
    assert run_id == ops.run_id
    assert records == len(ops.record_list())
    keys = set(ops.household_params())
    at_end = dict.fromkeys(keys, end)
    assert ends["Meter"] == ends["Battery"] == ends["Ems"] == ends["SelfConsumption"] == at_end
    # NoBattery was added at hour 1 and removed at hour 3: it ends where it was removed.
    assert ends["NoBattery"] == dict.fromkeys(keys, first + 4 * REMOVE_AT)
    assert ops.first("schedules") < ops.first("wait_run") < ops.first("states")


def test_ac_012_28_the_inbox_check_waits_until_nothing_is_pending(sim_run: SimRun) -> None:
    ops = sim_run.ops
    held = ops.first("held")
    assert ops.first("states") < held
    pendings = [i for i, n in enumerate(ops.names()) if n == "pending" and i < held]
    assert pendings, "held was read without asking for pending invocations"
    assert ops.calls[pendings[-1]][2] == 0
    assert 2 in ops.pending_seen  # the first answer was 2: the runner asked again


def test_ac_012_26_the_run_directory_holds_every_file(sim_run: SimRun) -> None:
    run_dir = _run_dir(sim_run.out)
    for name in (*TABLES, "steps", "lags"):
        path = run_dir / f"{name}.parquet"
        assert path.is_file(), name
        assert (pq.read_schema(path).metadata or {}).get(b"ems.schema_version") == b"1.0.0"
    for name in ("manifest.json", "summary.json", "config.yaml"):
        assert (run_dir / name).is_file(), name
    assert sim_run.ops.first("states") < sim_run.ops.last("close")


def test_ac_012_26_the_manifest_makes_the_run_reproducible(sim_run: SimRun) -> None:
    manifest = _strict_json(_run_dir(sim_run.out) / "manifest.json")
    first, end = _first_end(sim_run.exp)
    assert manifest["schema_version"] == run.MANIFEST_SCHEMA == "1.0.0"
    assert manifest["config"]["id"] == "t-run"
    assert manifest["seed"] == load_generator_config(GENERATOR).seed
    assert (manifest["window"]["first_step"], manifest["window"]["end_step"]) == (first, end)
    assert manifest["rcem"], "the proxy RCEm table built for the run"
    assert manifest["rcem_used"] == []  # every catalog site settles on RCE
    assert manifest["clock_offset_s"] == OFFSET_S
    assert manifest["timing_log_version"] == run.TIMING_LOG_VERSION == 1


def test_ac_012_29_the_summary_reports_the_systems_metrics(sim_run: SimRun) -> None:
    summary = _summary(sim_run.out)
    for key in (
        "records_per_s",
        "invocations_per_s",
        "decision_latency_ms",
        "release_lag_s",
        "publisher_lateness_s",
        "retries",
    ):
        assert key in summary, key
    assert summary["retries"] == 0


def test_ac_012_29_release_lag_counts_from_the_record_that_made_the_step_releasable(
    sim_run: SimRun,
) -> None:
    # The fake releases every step RELEASE_S after the publication of its releasing record (the
    # first steps after the first forecast run): a lag from the reading's due time would be larger.
    lag = _summary(sim_run.out)["release_lag_s"]
    assert lag["missing"] == 0 and lag["count"] > 0
    assert lag["max"] == pytest.approx(RELEASE_S, abs=1e-6)
    assert _summary(sim_run.out)["sustained"]["sustained"] is True


def test_ac_012_26_lags_hold_each_released_step_with_its_releasing_record(sim_run: SimRun) -> None:
    rows = pq.read_table(_run_dir(sim_run.out) / "lags.parquet").to_pylist()
    assert len(rows) == _summary(sim_run.out)["release_lag_s"]["count"]
    assert len({(r["household_key"], r["t"]) for r in rows}) == len(rows)
    assert {r["stream"] for r in rows} <= {"price", "weather_forecast", "production", "consumption"}
    assert "weather_forecast" in {r["stream"] for r in rows}  # the first steps wait for a forecast
    for r in rows:
        assert r["released_unix"] - r["due_unix"] == pytest.approx(r["lag_s"], abs=1e-9)
        assert r["lag_s"] == pytest.approx(RELEASE_S, abs=1e-6)


# ── failures ─────────────────────────────────────────────────────────────────────────────────────


def test_ac_012_32_a_stalled_run_fails_with_its_diagnosis(tmp_path: Path) -> None:
    key = "x"
    stuck = [{"id": "inv_stuck_1", "target_service_name": "Ems", "status": "backing-off"}]
    ops = FakeOps(stall_after=10, stuck_rows=stuck)
    exp = _experiment()
    first, _ = _first_end(exp)
    held = [{"service": "Ems", "key": key, "expected": first + 10, "held": [first + 11]}]
    ops.held_rows = held
    result = _run_one(exp, ops, tmp_path)
    assert (result.status, result.reason) == ("failed", "stalled")
    summary = _summary(tmp_path)
    assert (summary["status"], summary["reason"]) == ("failed", "stalled")
    text = json.dumps(summary)
    assert "inv_stuck_1" in text  # invocations paused, backing off or retried (ops.stuck)
    assert json.dumps(held[0]["held"]) in text and str(first + 10) in text  # inboxes (ops.held)
    assert str(first + 9) in text  # the last t of Meter
    assert "wait_run" not in ops.names()
    # Stalled once no Meter line advanced for stall_s (300 s at speed 96), not long after.
    last_meter = max(x.ns for x in ops.lines() if x.svc == "Meter") / 1e9
    (stuck_at, _) = ops.at("stuck")[0]
    assert config.stall_s(exp.run) == 300.0
    assert 300.0 <= stuck_at - last_meter <= 300.0 + 2 * run.POLL_S
    assert ops.names()[-1] == "close"


def test_ac_012_28_a_message_left_in_an_inbox_fails_the_run(tmp_path: Path) -> None:
    exp = _experiment()
    first, _ = _first_end(exp)
    ops = FakeOps(
        held_rows=[{"service": "Meter", "key": "k", "expected": first, "held": [first + 1]}]
    )
    result = _run_one(exp, ops, tmp_path)
    assert result.status == "failed"
    assert result.reason
    assert _summary(tmp_path)["status"] == "failed"
    pendings = [d for n, _, d in ops.calls[: ops.first("held")] if n == "pending"]
    assert pendings[-1] == 0


def test_ac_012_28_a_mismatch_against_the_offline_twin_fails_the_run(tmp_path: Path) -> None:
    ops = FakeOps(perturb=True)
    result = _run_one(_experiment(), ops, tmp_path)
    assert result.status == "failed"
    assert "mismatch" in (result.reason or "")
    assert _summary(tmp_path)["status"] == "failed"
    assert not (_run_dir(tmp_path) / "steps.parquet").exists()  # steps only after the check passes


def test_ac_012_17_a_transport_error_is_retried_with_the_same_change(tmp_path: Path) -> None:
    exp = _experiment(
        run={"strategies": list(ONLY)}, changes=[{"at_hour": ADD_AT, "strategies": list(BOTH)}]
    )
    ops = FakeOps(fail_set_first=True)
    result = _run_one(exp, ops, tmp_path)
    (key, change, raised), retry = ops.set_attempts[0], ops.set_attempts[1]
    assert raised is True
    assert retry == (key, change, False)  # same key, same t and names: the same Idempotency-Key
    assert sorted(k for k, _ in ops.sent) == sorted(ops.household_params())
    assert result.status == "ok", result.reason


def test_ac_012_26_a_file_that_cannot_be_written_fails_the_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_table = export.write_table

    def failing(rows: Any, path: Path, **kwargs: Any) -> Path:
        if Path(path).name == "months.parquet":
            raise OSError("disk full")
        return write_table(rows, path, **kwargs)

    monkeypatch.setattr(export, "write_table", failing)
    result = _run_one(_experiment(), FakeOps(), tmp_path)
    assert result.status == "failed"
    assert _summary(tmp_path)["status"] == "failed"


def test_ac_012_26_a_nan_in_a_metric_fails_the_run_and_stays_out_of_the_summary(
    tmp_path: Path,
) -> None:
    result = _run_one(_experiment(), FakeOps(nan_samples=True), tmp_path)
    assert result.status == "failed"
    assert _summary(tmp_path)["status"] == "failed"  # parsed refusing NaN and infinity


def test_ac_012_26_summary_and_manifest_are_written_with_allow_nan_false(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[tuple[Any, Any]] = []
    dump, dumps = json.dump, json.dumps

    def spy_dump(obj: Any, fp: Any, *args: Any, **kwargs: Any) -> None:
        seen.append((obj, kwargs.get("allow_nan", True)))
        dump(obj, fp, *args, **kwargs)

    def spy_dumps(obj: Any, *args: Any, **kwargs: Any) -> str:
        seen.append((obj, kwargs.get("allow_nan", True)))
        return dumps(obj, *args, **kwargs)

    monkeypatch.setattr(json, "dump", spy_dump)
    monkeypatch.setattr(json, "dumps", spy_dumps)
    _run_one(_experiment(), FakeOps(), tmp_path)
    monkeypatch.undo()
    written = [
        allow
        for obj, allow in seen
        if isinstance(obj, dict)
        and "schema_version" in obj
        and ("status" in obj or "config" in obj)
    ]
    assert len(written) >= 2, "summary.json and manifest.json were not written through json"
    assert all(allow is False for allow in written)


# ── the live path ────────────────────────────────────────────────────────────────────────────────


def _live() -> Experiment:
    return _experiment(
        data={"start": None, "days": None, "hours": LIVE_HOURS},
        run={"mode": "live", "grace_s": GRACE_S},
    )


def test_ac_012_25_a_live_run_publishes_in_real_time_from_first_step(tmp_path: Path) -> None:
    exp = _live()
    ops = FakeOps(start=LIVE_FIRST, hours=LIVE_HOURS, clock=LIVE_NOW.timestamp())
    result = _run_one(exp, ops, tmp_path)
    first, end = _first_end(exp)
    assert first == quarter_index(LIVE_FIRST)
    ((_, (_, live)),) = ops.at("open")
    assert live is True
    publish_at, spec, _, pub_first, pub_end = _publish(ops)
    assert spec.run.mode == "live"
    assert (pub_first, pub_end) == (first, end)
    assert publish_at >= 900.0 * first  # waited until first_step
    assert publish_at <= 900.0 * first + run.POLL_S
    params = ops.at("configure")[0][1][0]
    assert {(p.mode, p.grace_s) for p in params.values()} == {("live", GRACE_S)}
    # The publisher is stopped before the checks; the checks are invariants, not the twin.
    assert ops.states_running == [False]
    assert ops.at("states")[0][0] >= 900.0 * end + GRACE_S + 1.0
    held = ops.first("held")
    assert [d for n, _, d in ops.calls[:held] if n == "pending"][-1] == 0
    assert (result.status, result.reason) == ("ok", None), result.reason
    run_dir = _run_dir(tmp_path)
    assert not (run_dir / "steps.parquet").exists()
    assert not (run_dir / "lags.parquet").exists()
    for name in TABLES:
        assert (run_dir / f"{name}.parquet").is_file(), name
    summary = _summary(tmp_path)
    assert summary["status"] == "ok"
    assert "live_release_delay_s" in summary


# ── run_experiment: every run, failures do not stop the next, the sweep stops ────────────────────


def test_ac_012_25_a_failed_run_is_written_and_the_next_run_goes_on(tmp_path: Path) -> None:
    exp = _experiment(repetitions=2)
    ops = FakeOps(fail_configure_runs=(1,))
    results = run.run_experiment(
        exp, ops, tmp_path, config_dir=CONFIG_DIR, now=datetime.fromtimestamp(T0, UTC)
    )
    assert [r.status for r in results] == ["failed", "ok"], [r.reason for r in results]
    assert "boom" in (results[0].reason or "")
    assert ops.names().count("reset") == 2
    assert ops.names().count("close") == 2
    summaries = sorted(tmp_path.rglob("summary.json"))
    assert len(summaries) == 2
    for path in summaries:
        rel = path.relative_to(tmp_path).parts
        assert len(rel) == 3 and rel[0].startswith("t-run-"), rel
    statuses = sorted(_strict_json(p)["status"] for p in summaries)
    assert statuses == ["failed", "ok"]


def test_ac_012_25_a_failed_command_names_its_stderr_in_the_reason(tmp_path: Path) -> None:
    ops = FakeOps(fail_reset_runs=(1,))
    result = _run_one(_experiment(), ops, tmp_path)
    assert result.status == "failed"
    assert "container exited (0)" in (result.reason or ""), result.reason


def test_ac_012_31_a_sweep_stops_after_the_first_speed_that_is_not_sustained(
    tmp_path: Path,
) -> None:
    exp = _experiment(sweep={"speed": {"start": 96, "factor": 2, "max": 384}})
    ops = FakeOps(release_delay_s={192.0: 100.0})  # far over 900 / 192 s at 192, fine at 96
    results = run.run_experiment(
        exp, ops, tmp_path, config_dir=CONFIG_DIR, now=datetime.fromtimestamp(T0, UTC)
    )
    speeds = [spec.run.speed for _, (spec, _, _, _) in ops.at("publish")]
    assert speeds == [96.0, 192.0]
    assert len(results) == 2
    assert ops.names().count("reset") == 2
