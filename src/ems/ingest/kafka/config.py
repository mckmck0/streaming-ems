"""Shared librdkafka client settings for every producer, consumer and admin client."""

from __future__ import annotations

ClientConf = dict[str, str | int | float | bool]


def base_config(bootstrap: str, client_id: str = "ems-gen") -> ClientConf:
    return {
        "bootstrap.servers": bootstrap,
        "client.id": client_id,
        # The broker advertises localhost:9092 and the port is bound to 127.0.0.1: without this,
        # librdkafka on macOS tries ::1 first and logs a refused connection on every bootstrap.
        "broker.address.family": "v4",
        # Never create topics implicitly (metadata requests on producer-type handles could).
        "allow.auto.create.topics": False,
    }
