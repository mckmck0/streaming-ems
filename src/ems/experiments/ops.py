"""``StackOps``: the experiment runner's ``Ops`` on the real stack (spec 012, plan §7).

Every run gets a fresh stack (``stack.reset_stack`` with journal retention ``0s``), its own topic
prefix, SDK service (with the timing log on) and ingress project ``ems-exp-*``. Samplers keep the
containers' memory and CPU peaks, the restate cgroup's anonymous memory, the SDK process group's CPU
and RSS, and the ingress groups' lag. Nothing here serves HTTP (P10).
"""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ems.actors.household import HouseholdState
from ems.actors.messages import StrategyChange
from ems.actors.serde import serde_for
from ems.assembler.params import RunParams
from ems.assembler.schedule import Schedule
from ems.domain.actions import POLICIES
from ems.experiments import export, metrics, stack
from ems.experiments.config import RunSpec
from ems.experiments.export import RunStates
from ems.experiments.metrics import KillEvent, Line
from ems.settings import EmsSettings

QUARTER_S = 900
# Where each actor keeps the inboxes whose held messages a lost message would leave behind (A14).
INBOXES: dict[str, tuple[str, ...]] = {
    "Household": ("prices", "forecasts"),
    "Ems": ("steps", "acks"),
    "Battery": ("commands",),
    "Meter": ("flows",),
    **{name: ("steps",) for name in POLICIES},
}


class StackOps:
    """``run.Ops`` over the compose stack."""

    def __init__(self, settings: EmsSettings | None = None) -> None:
        self.settings = settings or EmsSettings()
        self.env: stack.StackEnv | None = None
        self.run_dir: Path | None = None
        self.memory: stack.MemorySampler | None = None
        self.host: stack.HostSampler | None = None
        self.lag: stack.LagSampler | None = None
        self.offset_s: float | None = None
        self.profile = "sim"

    # -- the stack --

    def reset(self) -> None:
        stack.reset_stack("0s")

    def open(self, run_dir: Path, *, live: bool) -> None:
        self.run_dir = run_dir
        self.profile = "live" if live else "sim"
        prefix = f"x{uuid.uuid4().hex[:8]}."
        env = stack.StackEnv(
            prefix,
            settings=self.settings,
            log_dir=run_dir / "logs",
            data_dir=run_dir / "data",
            sdk_env={"EMS_TIMING_LOG": str(self.timing_path)},
            ingress_project_prefix=stack.EXP_PROJECT_PREFIX,
        )
        self.env = env
        env.open(profile=self.profile)
        self.offset_s = stack.clock_offset_s()
        self.memory = stack.MemorySampler()
        self.memory.start()
        self.host = stack.HostSampler(lambda: env.sdk.pgid)
        self.host.start()
        groups = {
            env.ingress.group(svc): env.ingress.service_topics(svc)
            for svc in stack.INGRESS_SERVICES
        }
        self.lag = stack.LagSampler(self.settings, groups)
        self.lag.start()

    def close(self) -> None:
        for sampler in (self.memory, self.host, self.lag):
            if sampler is not None:
                sampler.stop()
        if self.env is not None:
            self.env.close()

    @property
    def timing_path(self) -> Path:
        assert self.run_dir is not None
        return self.run_dir / "timings.jsonl"

    def _env(self) -> stack.StackEnv:
        assert self.env is not None, "the stack is not open"
        return self.env

    # -- configuration and changes --

    def configure(
        self, params: Mapping[str, RunParams], services: tuple[str, ...] | None = None
    ) -> None:
        self._env().restate.configure(params, services)

    def stored_params(self, run_id: str, service: str) -> dict[str, RunParams]:
        serde = serde_for(RunParams)
        out: dict[str, RunParams] = {}
        for key, values in self._env().restate.states(run_id, service).items():
            if "params" in values:
                p = serde.deserialize(values["params"].encode())
                assert p is not None
                out[key] = p
        return out

    def set_strategies(self, key: str, change: StrategyChange) -> None:
        body = serde_for(StrategyChange).serialize(change)
        idempotency_key = f"{key}:set_strategies:{change.t}:{'+'.join(change.strategies)}"
        self._env().restate.send(
            "Household", key, "set_strategies", body, idempotency_key=idempotency_key
        )

    # -- publishing and faults --

    def publish(self, spec: RunSpec, run_id: str, first_step: int, end_step: int) -> Any:
        if spec.data.kind != "synthetic":
            raise ValueError("the runner publishes synthetic data only (history: a later stage)")
        env = self._env()
        live = spec.run.mode == "live"
        return stack.Publisher(
            self.settings,
            env.prefix,
            run_id,
            mode="realtime" if live else "accelerated",
            start=_iso(first_step),
            end=_iso(end_step),
            locations=spec.data.locations or 1,
            data_dir=env.data_dir,
            log_path=env.log_dir / "publisher.log",
            speed=None if live else spec.run.speed,
            faults=spec.data.faults,
            profile=self.profile,
        ).start()

    def kill(self, target: str, downtime_s: float) -> KillEvent:
        env = self._env()
        kill_unix = time.time()
        env.restate.note_kill()
        if target == "sdk":
            env.sdk.kill9()
            time.sleep(downtime_s)
            env.sdk.start()
        else:
            stack.kill_restate(restate=env.restate)
            time.sleep(downtime_s)
            stack.start_restate(env.restate)
        return KillEvent(target, kill_unix, time.time())

    # -- reading the run --

    def records(self, run_id: str) -> list[Any]:
        return stack.read_run(self.settings, self._env().prefix, run_id)

    def timing_lines(self) -> list[Line]:
        path = self.timing_path
        if not path.exists():
            return []
        return metrics.parse(path.read_text().splitlines())

    def schedules(self, run_id: str) -> dict[str, Schedule]:
        serde = serde_for(HouseholdState)
        out: dict[str, Schedule] = {}
        for key, values in self._env().restate.states(run_id, "Household").items():
            if "state" in values:
                state = serde.deserialize(values["state"].encode())
                assert state is not None
                out[key] = state.schedule
        return out

    def wait_run(self, run_id: str, records: int, ends: Mapping[str, Mapping[str, int]]) -> None:
        self._env().restate.wait_run(run_id, records, ends, strict=False, timeout=3600, poll=5.0)

    def pending(self, run_id: str) -> int:
        rows = self._env().restate.query(
            "SELECT count(*) AS n FROM sys_invocation "
            f"WHERE starts_with(target_service_key, '{run_id}:') AND status <> 'completed'"
        )
        return int(rows[0]["n"]) if rows else 0

    def stuck(self, run_id: str) -> list[dict[str, Any]]:
        return self._env().restate.invocations(run_id, suspect_only=True, min_retries=1)

    def held(self, run_id: str) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        restate = self._env().restate
        for service, inboxes in INBOXES.items():
            for key, values in restate.states(run_id, service).items():
                if "state" not in values:
                    continue
                state = json.loads(values["state"])
                for name in inboxes:
                    inbox = state.get(name) or {}
                    held = [n for n, _ in inbox.get("held", [])]
                    if held:
                        out.append(
                            {
                                "service": service,
                                "key": key,
                                "inbox": name,
                                "expected": inbox.get("expected"),
                                "held": held,
                            }
                        )
        return out

    def states(self, run_id: str) -> RunStates:
        return export.read_states(self._env().restate, run_id)

    def samples(self) -> dict[str, Any]:
        memory, host, lag = self.memory, self.host, self.lag
        names: Sequence[str] = sorted(memory.peaks) if memory is not None else ()
        return {
            "memory_peaks": dict(memory.peaks) if memory else {},
            "memory_limits": dict(memory.limits) if memory else {},
            "cpu_peaks": dict(memory.cpu_peaks) if memory else {},
            "anon_peaks": dict(memory.anon_peaks) if memory else {},
            "oom_killed": stack.oom_killed(names),
            "host": {"cpu_peak": host.cpu_peak, "rss_peak": host.rss_peak} if host else {},
            "lag_peaks": dict(lag.peaks) if lag else {},
            "clock_offset_s": self.offset_s,
            "state_bytes": self._state_bytes(),
        }

    def _state_bytes(self) -> dict[str, dict[str, int]]:
        rows = self._env().restate.query(
            "SELECT service_name, sum(value_length) AS total, max(value_length) AS max_value "
            "FROM state WHERE key = 'state' GROUP BY service_name"
        )
        return {
            r["service_name"]: {"total": int(r["total"]), "max": int(r["max_value"])} for r in rows
        }

    # -- the clock --

    def now(self) -> float:
        return time.time()

    def sleep(self, seconds: float) -> None:
        time.sleep(max(0.0, seconds))


def _iso(step: int) -> str:
    return datetime.fromtimestamp(step * QUARTER_S, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
