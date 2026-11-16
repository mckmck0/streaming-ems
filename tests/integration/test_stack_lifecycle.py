"""AC-010-01, -03 … -07: `make up`, `make dev`, `make register` and `make down` on the stack itself.

This module owns port 9080 and the stack's own ingress containers while it runs, and its tests run
in file order. It stops and starts the stack twice (a few minutes). At the end the stack is up, its
ingestion is closed again and `make dev` is stopped.
"""

from __future__ import annotations

import os
import signal
import subprocess
import uuid
from collections.abc import Iterator

import pytest

from ems.experiments import stack
from tests.integration.conftest import REPO

SERVICE_URI = "http://host.docker.internal:9080/"


def make(*args: str, timeout: float = 900) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["make", *args], cwd=REPO, capture_output=True, text=True, timeout=timeout, check=False
    )


def _health() -> dict[str, str]:
    ps = stack.compose("ps", "--format", "{{.Service}} {{.Health}}").stdout
    return dict(line.split(" ", 1) for line in ps.strip().splitlines() if " " in line)


def _running_ingress() -> list[str]:
    return [cid for cid in stack.ingress_containers() if stack.container_state(cid).get("Running")]


def _deployment_ids(restate: stack.Restate) -> list[str]:
    return [d["id"] for d in restate.deployments() if d.get("uri") == SERVICE_URI]


@pytest.fixture(scope="module")
def restate() -> Iterator[stack.Restate]:
    client = stack.Restate()
    yield client
    client.close()


@pytest.fixture(scope="module")
def dev_server() -> Iterator[subprocess.Popen[bytes]]:
    """`make dev`, stopped at the end of the module (its whole process group)."""
    log = (REPO / "logs" / "tests").resolve()
    log.mkdir(parents=True, exist_ok=True)
    with (log / "make-dev.log").open("ab") as out:
        proc = subprocess.Popen(
            ["make", "dev"], cwd=REPO, stdout=out, stderr=subprocess.STDOUT, start_new_session=True
        )
    try:
        stack.wait_for(
            lambda: stack.port_in_use(stack.SDK_PORT), 120, "make dev did not listen on 9080"
        )
        yield proc
    finally:
        try:
            os.killpg(proc.pid, signal.SIGTERM)
            proc.wait(timeout=30)
        except (ProcessLookupError, subprocess.TimeoutExpired):
            os.killpg(proc.pid, signal.SIGKILL)
        stack.wait_for(lambda: not stack.port_in_use(stack.SDK_PORT), 30, "9080 still in use")
        stack.close_stack_ingestion()


def test_ac_010_01_up_starts_kafka_and_restate_without_ingress(restate: stack.Restate) -> None:
    down = make("down")
    assert down.returncode == 0, down.stderr
    assert stack.ingress_containers() == []  # AC-010-06: nothing left after make down
    up = make("up")
    assert up.returncode == 0, up.stderr
    health = _health()
    assert (health.get("kafka"), health.get("restate")) == ("healthy", "healthy")
    for port in ("8080", "9070"):
        assert stack.compose("port", "restate", port).stdout.strip() == f"127.0.0.1:{port}"
    assert _running_ingress() == []
    restate.wait_healthy()


def _listening_on_sdk_port() -> set[str]:
    out = subprocess.run(
        ["lsof", "-nP", f"-iTCP:{stack.SDK_PORT}", "-sTCP:LISTEN", "-Fn"],
        capture_output=True,
        text=True,
        check=False,
    ).stdout
    return {line[1:] for line in out.splitlines() if line.startswith("n")}


def test_ac_010_07_dev_listens_on_loopback_only(dev_server: subprocess.Popen[bytes]) -> None:
    stack.wait_for(lambda: bool(_listening_on_sdk_port()), 60, "lsof shows no listener on 9080")
    assert _listening_on_sdk_port() == {f"127.0.0.1:{stack.SDK_PORT}"}


def test_ac_010_03_register_registers_then_starts_the_ingress(
    dev_server: subprocess.Popen[bytes], restate: stack.Restate
) -> None:
    result = make("register")
    assert result.returncode == 0, result.stdout + result.stderr
    ids = _deployment_ids(restate)
    assert len(ids) == 1
    services = next(d["services"] for d in restate.deployments() if d["id"] == ids[0])
    assert "Probe" in {s["name"] for s in services}
    for service in stack.INGRESS_SERVICES:
        cid = stack.container_of(service)
        assert cid is not None and stack.container_state(cid)["Running"], service


def test_ac_010_04_register_again_keeps_the_deployment(
    dev_server: subprocess.Popen[bytes], restate: stack.Restate
) -> None:
    before = _deployment_ids(restate)
    result = make("register")
    assert result.returncode == 0, result.stdout + result.stderr
    assert _deployment_ids(restate) == before


def test_ac_010_05_register_fails_without_service_or_admin(
    dev_server: subprocess.Popen[bytes],
) -> None:
    stack.close_stack_ingestion()
    no_service = make("register", "SERVICE_URL=http://host.docker.internal:9")
    assert no_service.returncode != 0
    assert _running_ingress() == []
    no_admin = make("register", "RESTATE_ADMIN=http://127.0.0.1:9")
    assert no_admin.returncode != 0
    assert _running_ingress() == []


def test_ac_010_06_down_and_up_keep_registration_and_state(
    dev_server: subprocess.Popen[bytes], restate: stack.Restate
) -> None:
    key = f"life{uuid.uuid4().hex[:8]}"
    restate.send("StateCostEager", key, "touch", idempotency_key=f"{key}:touch")
    stack.wait_for(
        lambda: restate.call("StateCostEager", key, "count") == 1, 60, "touch not handled"
    )
    ids = _deployment_ids(restate)

    down = make("down")
    assert down.returncode == 0, down.stderr
    assert stack.ingress_containers() == []
    up = make("up")
    assert up.returncode == 0, up.stderr
    restate.wait_healthy()

    assert _deployment_ids(restate) == ids
    assert restate.call("StateCostEager", key, "count") == 1
