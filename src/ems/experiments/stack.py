"""Harness for the Restate test tiers and the measurement jobs (spec 010, plan §6).

It starts the SDK service, registers it, runs a test-scoped set of Kafka ingress containers on
prefixed topics, publishes backfills with `ems-gen`, reads the topics back and queries Restate.
Tests and the jobs under jobs/ share it, so neither writes to the stack's own topics. It drives
docker and Kafka, so it needs the `ingest` group; nothing in it serves HTTP (P10).

A test project is the stack's compose file plus tests/compose.it.yml in a compose project named
`ems-it-*`, with topic names and consumer groups prefixed through `EMS_INGRESS_PREFIX`. Probe keys
carry the run's `run_id`, so a run's progress and logs are read by `run_id`.
"""

from __future__ import annotations

import contextlib
import json
import os
import platform
import re
import signal
import socket
import subprocess
import sys
import threading
import time
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from importlib import metadata
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
from ruamel.yaml import YAML

from ems.actors import ingress_check
from ems.contracts.envelope import Stream
from ems.contracts.topics import STREAM_TOPICS, TopicPlan
from ems.settings import EmsSettings

REPO = Path(__file__).resolve().parents[3]
COMPOSE_FILE = REPO / "infra" / "docker-compose.yml"
TEST_COMPOSE_FILE = REPO / "tests" / "compose.it.yml"
STACK_PROJECT = "ems"
TEST_PROJECT_PREFIX = "ems-it-"
SDK_PORT = 9080
INGRESS_LABEL = "ems.role=ingress"
INGRESS_TOPICS: dict[str, tuple[str, ...]] = {
    "ingress-meters": (STREAM_TOPICS[Stream.PRODUCTION], STREAM_TOPICS[Stream.CONSUMPTION]),
    "ingress-forecasts": (STREAM_TOPICS[Stream.WEATHER_FORECAST],),
    "ingress-prices": (STREAM_TOPICS[Stream.PRICE],),
}
INGRESS_GROUPS = {
    "ingress-meters": "ems-ingress-meters",
    "ingress-forecasts": "ems-ingress-forecasts",
    "ingress-prices": "ems-ingress-prices",
}
INGRESS_SERVICES = tuple(INGRESS_TOPICS)
PROBE_TOPICS = tuple(t for topics in INGRESS_TOPICS.values() for t in topics)
_NAME = re.compile(r"^[A-Za-z0-9._-]+$")


def _check_name(value: str) -> str:
    if not _NAME.match(value):
        raise ValueError(f"{value!r}: only letters, digits, '.', '_' and '-' are allowed")
    return value


# ── ports ─────────────────────────────────────────────────────────────────────────────────────────


class PortInUse(RuntimeError):
    """A port the harness needs is taken by another process."""


def port_in_use(port: int, host: str = "127.0.0.1") -> bool:
    """True when something listens on ``host:port``. Decided by binding, so it needs no network
    access.

    SO_REUSEADDR lets connections in TIME_WAIT pass, while an active listener still makes the bind
    fail.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((host, port))
        except OSError:
            return True
    return False


def wait_for(predicate: Any, timeout: float, what: str, poll: float = 0.1) -> float:
    start = time.monotonic()
    while not predicate():
        if time.monotonic() - start > timeout:
            raise TimeoutError(f"{what} after {timeout:.0f} s")
        time.sleep(poll)
    return time.monotonic() - start


# ── the SDK service ───────────────────────────────────────────────────────────────────────────────


class SdkService:
    """`ems.actors.app` served by hypercorn on 127.0.0.1, in a session of its own (never with
    --reload).

    hypercorn and its worker share one process group, so `kill9` and `stop` reach every process of
    it.
    """

    def __init__(
        self,
        port: int = SDK_PORT,
        app: str = "ems.actors.app:app",
        env: Mapping[str, str] | None = None,
        log_path: Path | None = None,
    ) -> None:
        self.port = port
        self.app = app
        self.env = dict(env or {})
        self.log_path = log_path
        self.proc: subprocess.Popen[bytes] | None = None

    def start(self, timeout: float = 60.0) -> None:
        if port_in_use(self.port):
            raise PortInUse(
                f"port {self.port} is in use: stop the process listening there "
                "(for example `make dev`) and run again"
            )
        args = [sys.executable, "-m", "hypercorn", self.app, "--bind", f"127.0.0.1:{self.port}"]
        if self.log_path is None:
            self.proc = self._spawn(args, subprocess.DEVNULL)
        else:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            with self.log_path.open("ab") as out:  # the child keeps its own descriptor
                self.proc = self._spawn(args, out)
        proc = self.proc

        def listening() -> bool:
            if proc.poll() is not None:
                raise RuntimeError(
                    f"the SDK service exited with {proc.returncode} (log: {self.log_path})"
                )
            return port_in_use(self.port)

        wait_for(listening, timeout, f"the SDK service did not listen on {self.port}")

    def _spawn(self, args: list[str], stdout: Any) -> subprocess.Popen[bytes]:
        return subprocess.Popen(
            args,
            env={**os.environ, **self.env},
            stdout=stdout,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            cwd=REPO,
        )

    @property
    def running(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def kill9(self, timeout: float = 30.0) -> int:
        """SIGKILL to the whole process group; returns the group id once nothing listens on the
        port.
        """
        if self.proc is None:
            raise RuntimeError("the SDK service was not started")
        pgid = self.proc.pid  # a new session: the group id is the leader's pid
        with contextlib.suppress(ProcessLookupError):
            os.killpg(pgid, signal.SIGKILL)
        self.proc.wait(timeout=timeout)
        wait_for(
            lambda: not port_in_use(self.port),
            timeout,
            f"port {self.port} still in use after kill -9",
        )
        return pgid

    def stop(self, timeout: float = 30.0) -> None:
        """SIGTERM to the group and wait, so coverage data of the children is written."""
        if self.proc is None or self.proc.poll() is not None:
            return
        pgid = self.proc.pid
        try:
            os.killpg(pgid, signal.SIGTERM)
            self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(pgid, signal.SIGKILL)
            self.proc.wait(timeout=timeout)
        except ProcessLookupError:
            pass
        wait_for(
            lambda: not port_in_use(self.port), timeout, f"port {self.port} still in use after stop"
        )


# ── Restate ───────────────────────────────────────────────────────────────────────────────────────


class Restate:
    """Admin API, ingress and SQL introspection of the stack's Restate server."""

    def __init__(self, settings: EmsSettings | None = None, timeout: float = 30.0) -> None:
        s = settings or EmsSettings()
        self.admin = s.restate_admin_url.rstrip("/")
        self.ingress = s.restate_ingress_url.rstrip("/")
        self.service_url = s.service_url
        self._http = httpx.Client(timeout=timeout)

    def close(self) -> None:
        self._http.close()

    def healthy(self) -> bool:
        try:
            admin = self._http.get(f"{self.admin}/health", timeout=2.0)
            ingress = self._http.get(f"{self.ingress}/restate/health", timeout=2.0)
        except httpx.HTTPError:
            return False
        return admin.status_code == 200 and ingress.status_code == 200

    def wait_healthy(self, timeout: float = 180.0) -> float:
        return wait_for(self.healthy, timeout, "Restate is not healthy", poll=0.25)

    def register(self, uri: str | None = None, *, force: bool = False) -> dict[str, Any]:
        """POST /deployments. With ``force`` false an unchanged deployment answers 200 and keeps its
        id.
        """
        body = {"uri": uri or self.service_url, "force": force}
        resp = self._http.post(f"{self.admin}/deployments", json=body)
        if resp.status_code not in (200, 201):
            hint = (
                " (changed handlers: `make register FORCE=true` in development)"
                if resp.status_code == 409
                else ""
            )
            raise RuntimeError(f"registration failed with {resp.status_code}{hint}: {resp.text}")
        return {"status": resp.status_code, **resp.json()}

    def deployments(self) -> list[dict[str, Any]]:
        resp = self._http.get(f"{self.admin}/deployments")
        resp.raise_for_status()
        return list(resp.json().get("deployments", []))

    def query(self, sql: str) -> list[dict[str, Any]]:
        resp = self._http.post(
            f"{self.admin}/query", json={"query": sql}, headers={"accept": "application/json"}
        )
        resp.raise_for_status()
        return list(resp.json()["rows"])

    def send(
        self,
        service: str,
        key: str,
        handler: str,
        body: Any = None,
        *,
        idempotency_key: str | None = None,
    ) -> None:
        """A one-way call through the HTTP ingress (how exclusive handlers are reached)."""
        headers: dict[str, str] = {}
        content = None
        if body is not None:
            headers["content-type"] = "application/json"
            content = json.dumps(body).encode()
        if idempotency_key is not None:
            headers["idempotency-key"] = idempotency_key
        url = f"{self.ingress}/{service}/{quote(key, safe='')}/{handler}/send"
        self._http.post(url, content=content, headers=headers).raise_for_status()

    def call(self, service: str, key: str, handler: str) -> Any:
        """Request-response, for shared handlers only (R1)."""
        resp = self._http.post(f"{self.ingress}/{service}/{quote(key, safe='')}/{handler}")
        resp.raise_for_status()
        return resp.json() if resp.content else None

    def handled(self, run_id: str) -> int:
        """Records the probe has handled for a run: the sum of the state key ``n`` over the run's
        keys.

        ``value_utf8`` is a Utf8 column holding JSON, so the values are summed here, not in SQL.
        """
        rows = self.query(
            "SELECT value_utf8 FROM state WHERE service_name = 'Probe' AND key = 'n' "
            f"AND starts_with(service_key, '{_check_name(run_id)}:')"
        )
        return sum(int(row["value_utf8"]) for row in rows)

    def wait_handled(
        self, run_id: str, expected: int, timeout: float = 900.0, poll: float = 0.5
    ) -> float:
        """Wait until the probe has handled ``expected`` records of the run (transient errors
        retried).
        """
        start = time.monotonic()
        seen = -1
        while True:
            with contextlib.suppress(httpx.HTTPError):
                seen = self.handled(run_id)
            if seen >= expected:
                return time.monotonic() - start
            if time.monotonic() - start > timeout:
                raise TimeoutError(f"{seen} of {expected} records handled after {timeout:.0f} s")
            time.sleep(poll)

    def probe_states(self, run_id: str) -> dict[str, dict[str, Any]]:
        """``{key: {"log": …, "leads": …, "n": …}}`` for every probe key of the run."""
        rows = self.query(
            "SELECT service_key, key, value_utf8 FROM state WHERE service_name = 'Probe' "
            f"AND starts_with(service_key, '{_check_name(run_id)}:')"
        )
        out: dict[str, dict[str, Any]] = defaultdict(dict)
        for row in rows:
            out[row["service_key"]][row["key"]] = json.loads(row["value_utf8"])
        return dict(out)

    def probe_logs(self, run_id: str) -> dict[str, dict[str, Any]]:
        return {key: state["log"] for key, state in self.probe_states(run_id).items()}

    def invocations(self, run_id: str) -> list[dict[str, Any]]:
        """Probe invocations of the run: handler, status and how they were invoked."""
        return self.query(
            "SELECT target_service_key, target_handler_name, status, invoked_by "
            "FROM sys_invocation WHERE target_service_name = 'Probe' "
            f"AND starts_with(target_service_key, '{_check_name(run_id)}:')"
        )


# ── docker and compose ────────────────────────────────────────────────────────────────────────────


def docker(
    *args: str, check: bool = True, timeout: float = 120.0
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *args], check=check, capture_output=True, text=True, timeout=timeout
    )


def compose(
    *args: str,
    project: str | None = None,
    files: Sequence[Path] = (COMPOSE_FILE,),
    env: Mapping[str, str] | None = None,
    check: bool = True,
    timeout: float = 600.0,
) -> subprocess.CompletedProcess[str]:
    cmd = ["docker", "compose"]
    if project is not None:
        cmd += ["-p", project]
    for path in files:
        cmd += ["-f", str(path)]
    cmd += list(args)
    return subprocess.run(
        cmd,
        check=check,
        capture_output=True,
        text=True,
        timeout=timeout,
        env={**os.environ, **(env or {})},
        cwd=REPO,
    )


def container_of(service: str, project: str = STACK_PROJECT) -> str | None:
    out = docker(
        "ps",
        "-a",
        "-q",
        "--filter",
        f"label=com.docker.compose.project={project}",
        "--filter",
        f"label=com.docker.compose.service={service}",
    ).stdout.split()
    return out[0] if out else None


def container_state(container: str) -> dict[str, Any]:
    out = docker("inspect", "--format", "{{json .State}}", container).stdout
    return dict(json.loads(out))


def restart_count(container: str) -> int:
    return int(docker("inspect", "--format", "{{.RestartCount}}", container).stdout.strip())


def ingress_containers() -> list[str]:
    """Every container labelled as an ingress container, in any project."""
    return docker("ps", "-a", "-q", "--filter", f"label={INGRESS_LABEL}").stdout.split()


def remove_test_projects() -> list[str]:
    """Remove ingress containers that earlier test runs left behind (never the stack's own)."""
    out = docker(
        "ps",
        "-a",
        "--filter",
        f"label={INGRESS_LABEL}",
        "--format",
        '{{.ID}} {{.Label "com.docker.compose.project"}}',
    ).stdout
    stale = [
        line.split()[0]
        for line in out.splitlines()
        if len(line.split()) == 2 and line.split()[1].startswith(TEST_PROJECT_PREFIX)
    ]
    if stale:
        docker("rm", "-f", *stale)
    return stale


def close_stack_ingestion() -> list[str]:
    """Stop the stack's own ingress containers; `make register` starts them again."""
    ids = [cid for svc in INGRESS_SERVICES if (cid := container_of(svc)) is not None]
    running = [cid for cid in ids if container_state(cid).get("Running")]
    if running:
        docker("stop", *running, timeout=180)
    return running


def kill_restate(timeout: float = 60.0) -> int:
    """SIGKILL to the stack's Restate server; returns the container's exit code."""
    cid = container_of("restate")
    if cid is None:
        raise RuntimeError("the restate service has no container: run `make up`")
    docker("kill", "--signal", "KILL", cid)
    wait_for(lambda: not container_state(cid).get("Running"), timeout, "restate still running")
    return int(container_state(cid)["ExitCode"])


def stop_restate(timeout: float = 60.0) -> None:
    cid = container_of("restate")
    if cid is None:
        raise RuntimeError("the restate service has no container: run `make up`")
    docker("stop", "--time", "30", cid, timeout=timeout + 30)


def start_restate(restate: Restate, timeout: float = 180.0) -> float:
    """Start the stack's Restate container again with its configuration unchanged; seconds to
    healthy.
    """
    cid = container_of("restate")
    if cid is None:
        raise RuntimeError("the restate service has no container: run `make up`")
    started = time.monotonic()
    docker("start", cid)
    restate.wait_healthy(timeout)
    return time.monotonic() - started


class IngressSet:
    """The three ingress containers of a test project, reading the topics with ``prefix``."""

    def __init__(self, prefix: str, *, retry_max_attempts: int | None = None) -> None:
        self.prefix = _check_name(prefix)
        self.project = TEST_PROJECT_PREFIX + re.sub(r"[^a-z0-9]", "", prefix.lower())
        self.env = {"EMS_INGRESS_PREFIX": prefix}
        if retry_max_attempts is not None:
            self.env["EMS_INGRESS_RETRY_MAX_ATTEMPTS"] = str(retry_max_attempts)

    @property
    def topics(self) -> tuple[str, ...]:
        return tuple(self.prefix + t for t in PROBE_TOPICS)

    def group(self, service: str) -> str:
        return self.prefix + INGRESS_GROUPS[service]

    def service_topics(self, service: str) -> tuple[str, ...]:
        return tuple(self.prefix + t for t in INGRESS_TOPICS[service])

    def _compose(self, *args: str, check: bool = True, timeout: float = 600.0) -> None:
        compose(
            "--profile",
            "ingress",
            *args,
            project=self.project,
            files=(COMPOSE_FILE, TEST_COMPOSE_FILE),
            env=self.env,
            check=check,
            timeout=timeout,
        )

    def up(self, *, wait: bool = True) -> None:
        self._compose("up", "-d", "--no-deps", *(["--wait"] if wait else []), *INGRESS_SERVICES)

    def down(self) -> None:
        self._compose("down", "--remove-orphans", "--timeout", "10", check=False)

    def container(self, service: str) -> str:
        cid = container_of(service, self.project)
        if cid is None:
            raise RuntimeError(f"{service} has no container in project {self.project}")
        return cid

    def kill9(self, service: str, timeout: float = 60.0) -> int:
        cid = self.container(service)
        docker("kill", "--signal", "KILL", cid)
        wait_for(
            lambda: not container_state(cid).get("Running"), timeout, f"{service} still running"
        )
        return int(container_state(cid)["ExitCode"])

    def start(self, service: str) -> None:
        docker("start", self.container(service))

    def pause_consumption(self) -> None:
        """Stop the three containers (a stop by hand: the restart policy leaves them stopped)."""
        docker("stop", *(self.container(s) for s in INGRESS_SERVICES), timeout=180)

    def resume_consumption(self) -> None:
        docker("start", *(self.container(s) for s in INGRESS_SERVICES))

    def restart_count(self, service: str) -> int:
        return restart_count(self.container(service))


# ── Kafka: topics, publishing, reading back ───────────────────────────────────────────────────────


@dataclass(frozen=True)
class Record:
    key: str
    meta: ingress_check.Meta
    value: bytes


def ensure_topics(settings: EmsSettings, prefix: str, profile: str = "sim") -> TopicPlan:
    from ems.contracts.topics import load_plan
    from ems.ingest.kafka.admin import KafkaTopicAdmin, ensure

    plan = load_plan(REPO / "config", profile, _check_name(prefix))
    report = ensure(KafkaTopicAdmin(settings.kafka_bootstrap), plan, apply_config=True)
    if report.failed:
        raise RuntimeError("\n".join(report.lines()))
    return plan


def delete_topics(settings: EmsSettings, plan: TopicPlan, groups: Iterable[str] = ()) -> None:
    from confluent_kafka.admin import AdminClient

    from ems.ingest.kafka.admin import KafkaTopicAdmin
    from ems.ingest.kafka.config import base_config

    KafkaTopicAdmin(settings.kafka_bootstrap).delete(t.name for t in plan.topics)
    names = list(groups)
    if names:
        # Keep the client referenced until the futures resolve: they never complete once it is gone.
        admin = AdminClient(base_config(settings.kafka_bootstrap, "ems-harness"))
        futures = admin.delete_consumer_groups(names, request_timeout=30)
        for fut in futures.values():
            with contextlib.suppress(Exception):  # a group that never committed does not exist
                fut.result(timeout=30)


def publish_backfill(
    settings: EmsSettings,
    prefix: str,
    run_id: str,
    *,
    locations: int,
    days: float,
    data_dir: Path,
    start: str = "2026-10-20T00:00:00Z",
    faults: bool = False,
) -> subprocess.CompletedProcess[str]:
    """`ems-gen run --mode backfill` into the prefixed topics (sim profile), keys scoped by
    ``run_id``.
    """
    cmd = [
        str(Path(sys.executable).with_name("ems-gen")),
        "run",
        "--mode",
        "backfill",
        "--start",
        start,
        "--days",
        str(days),
        "--run-id",
        _check_name(run_id),
        "--locations",
        str(locations),
    ]
    if faults:
        cmd.append("--faults")
    env = {
        **os.environ,
        "EMS_KAFKA_BOOTSTRAP": settings.kafka_bootstrap,
        "EMS_TOPICS_PROFILE": "sim",
        "EMS_TOPIC_PREFIX": _check_name(prefix),
        "EMS_DATA_DIR": str(data_dir),
    }
    result = subprocess.run(
        cmd, env=env, cwd=REPO, check=False, capture_output=True, text=True, timeout=3600
    )
    if result.returncode != 0:
        tail = (result.stderr or result.stdout or "")[-2000:]
        raise RuntimeError(f"ems-gen exited with {result.returncode}:\n{tail}")
    return result


def read_run(settings: EmsSettings, prefix: str, run_id: str) -> list[Record]:
    """Every record of the run in the four prefixed probe topics, in partition order."""
    from ems.ingest.tools.reader import read_topic

    scope = f"{_check_name(run_id)}:"
    out: list[Record] = []
    for topic in (prefix + t for t in PROBE_TOPICS):
        for rec in read_topic(settings.kafka_bootstrap, topic):
            if rec.key is not None and rec.key.startswith(scope):
                out.append(
                    Record(rec.key, (rec.topic, rec.partition, rec.offset), rec.value or b"")
                )
    return out


def offline_logs(records: Iterable[Record]) -> dict[str, dict[str, Any]]:
    """The probe log of every key, folded offline with the same pure function the handlers use."""
    by_key: dict[str, list[tuple[ingress_check.Meta | None, bytes]]] = defaultdict(list)
    for rec in records:
        by_key[rec.key].append((rec.meta, rec.value))
    return {key: ingress_check.fold_log(items) for key, items in by_key.items()}


def group_progress(settings: EmsSettings, group: str, topics: Sequence[str]) -> tuple[int, int]:
    """(committed, end): the group's committed offsets and the end offsets, summed over the
    partitions.
    """
    from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
    from confluent_kafka.admin import AdminClient

    from ems.ingest.kafka.config import base_config
    from ems.ingest.tools.reader import end_offsets

    ends = {
        (t, p): high for t in topics for p, high in end_offsets(settings.kafka_bootstrap, t).items()
    }
    request = ConsumerGroupTopicPartitions(group, [TopicPartition(t, p) for t, p in ends])
    admin = AdminClient(base_config(settings.kafka_bootstrap, "ems-harness"))
    result = admin.list_consumer_group_offsets([request])[group].result(timeout=30)
    committed = {
        (tp.topic, tp.partition): max(tp.offset, 0) for tp in (result.topic_partitions or [])
    }
    return sum(committed.get(k, 0) for k in ends), sum(ends.values())


# ── one environment for a test module or a job run ────────────────────────────────────────────────


class StackEnv:
    """Closes the stack's ingestion, creates the prefixed topics, starts and registers the SDK
    service and starts this environment's ingress set. ``close`` undoes all of it except the
    registration.
    """

    def __init__(
        self,
        prefix: str,
        *,
        settings: EmsSettings | None = None,
        log_dir: Path = REPO / "logs",
        data_dir: Path | None = None,
        retry_max_attempts: int | None = None,
        sdk_env: Mapping[str, str] | None = None,
    ) -> None:
        self.settings = settings or EmsSettings()
        self.prefix = _check_name(prefix)
        self.log_dir = log_dir
        self.data_dir = data_dir or (log_dir / "data" / prefix.strip("."))
        self.restate = Restate(self.settings)
        self.sdk = SdkService(env=sdk_env, log_path=log_dir / f"sdk-{prefix.strip('.')}.log")
        self.ingress = IngressSet(prefix, retry_max_attempts=retry_max_attempts)
        self.plan: TopicPlan | None = None
        self.deployment: dict[str, Any] = {}

    def open(self, *, start_ingress: bool = True) -> StackEnv:
        self.restate.wait_healthy()
        close_stack_ingestion()
        self.plan = ensure_topics(self.settings, self.prefix)
        self.sdk.start()
        self.deployment = self.restate.register(force=False)
        if start_ingress:
            self.ingress.up()
        return self

    def close(self) -> None:
        self.ingress.down()
        self.sdk.stop()
        if self.plan is not None:
            groups = [self.ingress.group(s) for s in INGRESS_SERVICES]
            delete_topics(self.settings, self.plan, groups)
        self.restate.close()

    def publish(self, run_id: str, *, locations: int, days: float, **kwargs: Any) -> list[Record]:
        """Publish a backfill for ``run_id`` and return its records as read back from Kafka."""
        publish_backfill(
            self.settings,
            self.prefix,
            run_id,
            locations=locations,
            days=days,
            data_dir=self.data_dir,
            **kwargs,
        )
        return read_run(self.settings, self.prefix, run_id)

    def meters_progress(self) -> tuple[int, int]:
        svc = "ingress-meters"
        return group_progress(
            self.settings, self.ingress.group(svc), self.ingress.service_topics(svc)
        )


# ── measurement support for the jobs ──────────────────────────────────────────────────────────────


_UNITS = {"b": 1, "kib": 2**10, "mib": 2**20, "gib": 2**30, "kb": 10**3, "mb": 10**6, "gb": 10**9}


def parse_size(text: str) -> int:
    """`docker stats` sizes such as ``123.4MiB`` or ``1.2GB`` in bytes."""
    m = re.match(r"^\s*([0-9.]+)\s*([A-Za-z]+)\s*$", text)
    if not m or m.group(2).lower() not in _UNITS:
        raise ValueError(f"unknown size {text!r}")
    return int(float(m.group(1)) * _UNITS[m.group(2).lower()])


class MemorySampler(threading.Thread):
    """Samples `docker stats` every ``interval`` seconds and keeps each container's peak memory."""

    def __init__(self, interval: float = 2.0) -> None:
        super().__init__(daemon=True)
        self.interval = interval
        self.peaks: dict[str, int] = {}
        self.limits: dict[str, int] = {}
        self._stop = threading.Event()

    def run(self) -> None:
        while not self._stop.is_set():
            out = docker(
                "stats", "--no-stream", "--format", "{{.Name}}\t{{.MemUsage}}", check=False
            )
            for line in out.stdout.splitlines():
                name, _, usage = line.partition("\t")
                used, _, limit = usage.partition("/")
                try:
                    self.peaks[name] = max(self.peaks.get(name, 0), parse_size(used))
                    self.limits[name] = parse_size(limit)
                except ValueError:
                    continue
            self._stop.wait(self.interval)

    def stop(self) -> None:
        self._stop.set()
        self.join(timeout=30)


def oom_killed(names: Iterable[str]) -> dict[str, bool]:
    out: dict[str, bool] = {}
    for name in names:
        res = docker("inspect", "--format", "{{.State.OOMKilled}}", name, check=False)
        if res.returncode == 0:
            out[name] = res.stdout.strip() == "true"
    return out


def compose_images() -> dict[str, str]:
    """The image of every service in infra/docker-compose.yml, as pinned there."""
    data = YAML(typ="safe").load(COMPOSE_FILE.read_text())
    return {name: str(svc["image"]) for name, svc in data["services"].items() if "image" in svc}


def hardware() -> dict[str, Any]:
    info: dict[str, Any] = {
        "machine": platform.machine(),
        "system": platform.system(),
        "release": platform.release(),
        "cpus": os.cpu_count(),
        "processor": platform.processor(),
    }
    if platform.system() == "Darwin":
        res = subprocess.run(
            ["sysctl", "-n", "hw.memsize", "machdep.cpu.brand_string"],
            capture_output=True,
            text=True,
            check=False,
        )
        lines = res.stdout.splitlines()
        if len(lines) == 2:
            info["memory_bytes"] = int(lines[0])
            info["cpu"] = lines[1]
    return info


def docker_info() -> dict[str, Any]:
    res = docker("info", "--format", "{{json .}}", check=False)
    if res.returncode != 0:
        return {}
    data = json.loads(res.stdout)
    keys = ("ServerVersion", "OperatingSystem", "Architecture", "NCPU", "MemTotal")
    return {k: data.get(k) for k in keys}


def manifest(config: Mapping[str, Any]) -> dict[str, Any]:
    """What a result directory records beside its numbers (P14)."""
    from ems.ingest.runner import _git_commit, _versions

    versions = _versions()
    for pkg in ("restate-sdk", "hypercorn"):
        try:
            versions[pkg] = metadata.version(pkg)
        except metadata.PackageNotFoundError:
            versions[pkg] = "absent"
    return {
        "config": dict(config),
        "images": compose_images(),
        "versions": versions,
        "git_commit": _git_commit(REPO),
        "hardware": hardware(),
        "docker": docker_info(),
    }
