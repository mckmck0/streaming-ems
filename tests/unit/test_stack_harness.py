"""AC-010-26: the shared harness fails fast when the SDK service's port is busy (spec 010, plan §6).

A port counts as in use when binding it on 127.0.0.1 fails (this tier blocks connects, not binds).
``SdkService.start()`` then raises ``PortInUse``, naming the port, before it starts any process.
"""

from __future__ import annotations

import socket
import subprocess
from collections.abc import Iterator
from typing import NoReturn

import pytest

from ems.experiments import stack


@pytest.fixture
def busy_port() -> Iterator[int]:
    """A port on 127.0.0.1 held by a listening socket while the test runs."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as held:
        held.bind(("127.0.0.1", 0))
        held.listen()
        yield held.getsockname()[1]


def test_ac_010_26_port_in_use_is_a_runtime_error() -> None:
    assert issubclass(stack.PortInUse, RuntimeError)


def test_ac_010_26_port_in_use_is_decided_by_binding() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as held:
        held.bind(("127.0.0.1", 0))
        held.listen()
        port = held.getsockname()[1]
        assert stack.port_in_use(port) is True
    assert stack.port_in_use(port) is False


def test_ac_010_26_sdk_service_refuses_a_busy_port_before_starting_a_process(
    busy_port: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    started: list[tuple[object, ...]] = []

    def no_process(*args: object, **_kwargs: object) -> NoReturn:
        started.append(args)
        pytest.fail(f"a process was started although port {busy_port} is in use: {args!r}")

    # ``stack.subprocess.Popen`` is ``subprocess.Popen``; a ``Popen`` imported by name goes too.
    monkeypatch.setattr(subprocess, "Popen", no_process)
    if hasattr(stack, "Popen"):
        monkeypatch.setattr(stack, "Popen", no_process)
    service = stack.SdkService(port=busy_port)
    with pytest.raises(stack.PortInUse) as raised:
        service.start()
    message = str(raised.value)
    assert str(busy_port) in message
    assert "in use" in message
    assert started == []
