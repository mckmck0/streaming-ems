"""No network in this tier (AC-002-14): any socket connect fails the test."""

from __future__ import annotations

import socket
from collections.abc import Iterator

import pytest


class NetworkBlocked(RuntimeError):
    pass


@pytest.fixture(autouse=True)
def _block_network(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise NetworkBlocked("network access is not allowed in unit and property tests")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket.socket, "connect_ex", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)
    yield
