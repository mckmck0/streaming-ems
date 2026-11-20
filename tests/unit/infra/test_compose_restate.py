"""Compose for spec 010: the Restate server and the three Kafka ingress containers.

AC-010-01: every published port is bound to 127.0.0.1 (Restate ingress 8080 and admin 9070);
Restate keeps its data in the named volume `restate-data`.
AC-010-02: the Restate server and the Kafka ingress integration by exact tag plus `@sha256:`
digest, and every other registry image likewise.
AC-010-08: one ingress container per target handler, with its topics, fixed consumer group and
handler, the earliest offset, the `ingress` profile, the `ems.role=ingress` label, the restart
policy and no published port; together they cover the four Kafka-fed topics, each once.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
from ruamel.yaml import YAML

from ems.contracts.envelope import Stream
from ems.contracts.topics import STREAM_TOPICS

RESTATE_IMAGE = (
    "restatedev/restate:1.8.1-hotfix.2-20261102"
    "@sha256:e3a11e7c694cfe0c134eca1652992c068bef0bcba3caeec9d2a3b3bc50daa62e"
)
INGRESS_IMAGE = (
    "ghcr.io/restatedev/ingress-integration-kafka:0.1.1-20261109"
    "@sha256:20704c886ee4dcc4046bab1743dc424bb08b2317ce1bc6da55ce738345ce01b1"
)
LOCAL_IMAGE = "ems-gen:local"
PINNED = re.compile(r"^[^@\s]+:[^@\s]+@sha256:[0-9a-f]{64}$")
# Test projects pass their own prefix; the stack's values are the defaults.
PREFIX = "${EMS_INGRESS_PREFIX:-}"
SHARED_ENV = {
    "KAFKA_BOOTSTRAP_SERVERS": "kafka:29092",
    "KAFKA_AUTO_OFFSET_RESET": "earliest",
    "RESTATE_INGRESS_URL": "http://restate:8080",
    "RESTATE_RECORD_MAPPER_SERVICE": "Probe",
}
INGRESS = {
    "ingress-meters": {
        "KAFKA_TOPICS": "ems.production.v1,ems.consumption.v1",
        "KAFKA_GROUP_ID": "ems-ingress-meters",
        "RESTATE_RECORD_MAPPER_HANDLER": "on_measurement",
    },
    "ingress-forecasts": {
        "KAFKA_TOPICS": "ems.weather.forecast.v1",
        "KAFKA_GROUP_ID": "ems-ingress-forecasts",
        "RESTATE_RECORD_MAPPER_HANDLER": "on_forecast",
    },
    "ingress-prices": {
        "KAFKA_TOPICS": "ems.price.v1",
        "KAFKA_GROUP_ID": "ems-ingress-prices",
        "RESTATE_RECORD_MAPPER_HANDLER": "on_price",
    },
}
KAFKA_FED = (Stream.PRODUCTION, Stream.CONSUMPTION, Stream.WEATHER_FORECAST, Stream.PRICE)


def _compose(repo: Path) -> dict[str, Any]:
    return YAML(typ="safe").load((repo / "infra" / "docker-compose.yml").read_text())


def _service(repo: Path, name: str) -> dict[str, Any]:
    services = _compose(repo)["services"]
    assert name in services, f"no service {name!r} in infra/docker-compose.yml"
    return services[name]


def _labels(service: dict[str, Any]) -> dict[str, str]:
    """Compose labels as a mapping, from the mapping or the `key=value` list form."""
    labels = service.get("labels") or {}
    if isinstance(labels, list):
        return {key: value for key, _, value in (str(item).partition("=") for item in labels)}
    return {str(key): str(value) for key, value in labels.items()}


def test_ac_010_02_restate_image_by_tag_and_digest(repo: Path) -> None:
    assert _service(repo, "restate").get("image") == RESTATE_IMAGE


@pytest.mark.parametrize("name", sorted(INGRESS))
def test_ac_010_02_ingress_image_by_tag_and_digest(repo: Path, name: str) -> None:
    assert _service(repo, name).get("image") == INGRESS_IMAGE


def test_ac_010_02_every_registry_image_by_tag_and_digest(repo: Path) -> None:
    for name, service in _compose(repo)["services"].items():
        image = service.get("image")
        if image != LOCAL_IMAGE:
            assert PINNED.match(str(image)), (name, image)


def test_ac_010_01_restate_ports_on_loopback(repo: Path) -> None:
    ports = _service(repo, "restate").get("ports", [])
    assert set(ports) == {"127.0.0.1:8080:8080", "127.0.0.1:9070:9070"}


def test_ac_010_01_every_published_port_on_loopback(repo: Path) -> None:
    for name, service in _compose(repo)["services"].items():
        for port in service.get("ports", []):
            assert str(port).startswith("127.0.0.1:"), (name, port)


def test_ac_010_01_restate_data_on_named_volume(repo: Path) -> None:
    assert "restate-data" in (_compose(repo).get("volumes") or {})
    assert "restate-data:/restate-data" in _service(repo, "restate").get("volumes", [])


@pytest.mark.parametrize("name", sorted(INGRESS))
def test_ac_010_08_ingress_container_policy(repo: Path, name: str) -> None:
    service = _service(repo, name)
    assert service.get("profiles") == ["ingress"]
    assert service.get("restart") == "unless-stopped"
    assert _labels(service).get("ems.role") == "ingress"
    assert "ports" not in service


@pytest.mark.parametrize("name", sorted(INGRESS))
def test_ac_010_08_ingress_shared_environment(repo: Path, name: str) -> None:
    env = _service(repo, name).get("environment")
    assert isinstance(env, dict), "environment must be a mapping"
    assert {key: env.get(key) for key in SHARED_ENV} == SHARED_ENV


@pytest.mark.parametrize("name", sorted(INGRESS))
def test_ac_010_08_ingress_topics_group_and_handler(repo: Path, name: str) -> None:
    env = _service(repo, name).get("environment") or {}
    unprefixed = {key: env[key].replace(PREFIX, "") for key in INGRESS[name] if key in env}
    assert unprefixed == INGRESS[name]


def test_ac_010_08_each_kafka_fed_topic_in_exactly_one_ingress(repo: Path) -> None:
    topics = [
        topic
        for name in INGRESS
        for topic in _service(repo, name)["environment"]["KAFKA_TOPICS"]
        .replace(PREFIX, "")
        .split(",")
    ]
    assert sorted(topics) == sorted(STREAM_TOPICS[stream] for stream in KAFKA_FED)
