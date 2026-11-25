"""Compose for spec 010 and spec 011: the Restate server and the three Kafka ingress containers.

AC-010-01: every published port is bound to 127.0.0.1 (Restate ingress 8080 and admin 9070);
Restate keeps its data in the named volume `restate-data`.
AC-010-02: the Restate server and the Kafka ingress integration by exact tag plus `@sha256:`
digest, and every other registry image likewise.
AC-010-08: one ingress container per target handler, with its topics, fixed consumer group and
handler, the earliest offset, the `ingress` profile, the `ems.role=ingress` label, the restart
policy and no published port; together they cover the four Kafka-fed topics, each once.
AC-011-25: since spec 011 each container names its own target service beside its handler
(`Household.on_measurement`, `Weather.on_forecast`, `Tariff.on_price`); the shared anchor names
none, and the harness's `INGRESS_TARGETS` agree with the file.
AC-012-18 (spec 012, plan §4): Restate takes its journal retention from
`EMS_RESTATE_JOURNAL_RETENTION`, `1d` when it is unset.
AC-012-39 (plan §9, F2 and F3): Restate sets its four memory pools (RocksDB, invoker, query engine,
Bifrost record cache) and their sum stays below its `mem_limit`; every long-running container of the
stack (what `make up` and `make register` start) has a `mem_limit`.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
from ruamel.yaml import YAML

from ems.contracts.envelope import Stream
from ems.contracts.topics import STREAM_TOPICS
from ems.experiments import stack

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
}
ANCHOR = "x-ingress-env"
INGRESS = {
    "ingress-meters": {
        "KAFKA_TOPICS": "ems.production.v1,ems.consumption.v1",
        "KAFKA_GROUP_ID": "ems-ingress-meters",
        "RESTATE_RECORD_MAPPER_SERVICE": "Household",
        "RESTATE_RECORD_MAPPER_HANDLER": "on_measurement",
    },
    "ingress-forecasts": {
        "KAFKA_TOPICS": "ems.weather.forecast.v1",
        "KAFKA_GROUP_ID": "ems-ingress-forecasts",
        "RESTATE_RECORD_MAPPER_SERVICE": "Weather",
        "RESTATE_RECORD_MAPPER_HANDLER": "on_forecast",
    },
    "ingress-prices": {
        "KAFKA_TOPICS": "ems.price.v1",
        "KAFKA_GROUP_ID": "ems-ingress-prices",
        "RESTATE_RECORD_MAPPER_SERVICE": "Tariff",
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


TARGETS = {
    "ingress-meters": ("Household", "on_measurement"),
    "ingress-forecasts": ("Weather", "on_forecast"),
    "ingress-prices": ("Tariff", "on_price"),
}


def test_ac_011_25_the_shared_anchor_names_no_target_service(repo: Path) -> None:
    anchor = _compose(repo).get(ANCHOR)
    assert isinstance(anchor, dict), f"no {ANCHOR} mapping in infra/docker-compose.yml"
    assert "RESTATE_RECORD_MAPPER_SERVICE" not in anchor
    assert "RESTATE_RECORD_MAPPER_HANDLER" not in anchor


@pytest.mark.parametrize("name", sorted(TARGETS))
def test_ac_011_25_each_ingress_container_sets_its_own_target(repo: Path, name: str) -> None:
    env = _service(repo, name).get("environment") or {}
    target = (env.get("RESTATE_RECORD_MAPPER_SERVICE"), env.get("RESTATE_RECORD_MAPPER_HANDLER"))
    assert target == TARGETS[name]


def test_ac_011_25_no_ingress_container_targets_the_probe(repo: Path) -> None:
    for name, service in _compose(repo)["services"].items():
        env = service.get("environment") or {}
        if isinstance(env, dict):
            assert env.get("RESTATE_RECORD_MAPPER_SERVICE") != "Probe", name


def test_ac_011_25_harness_targets_match_the_compose_file(repo: Path) -> None:
    from_compose = {
        name: (
            _service(repo, name)["environment"]["RESTATE_RECORD_MAPPER_SERVICE"],
            _service(repo, name)["environment"]["RESTATE_RECORD_MAPPER_HANDLER"],
        )
        for name in stack.INGRESS_TARGETS
    }
    assert from_compose == stack.INGRESS_TARGETS


# ── spec 012: journal retention and memory limits (AC-012-18, AC-012-39) ───────────────────────


def test_ac_012_18_restate_journal_retention_from_the_environment_one_day_by_default(
    repo: Path,
) -> None:
    env = _service(repo, "restate").get("environment") or {}
    assert env.get("RESTATE_DEFAULT_JOURNAL_RETENTION") == "${EMS_RESTATE_JOURNAL_RETENTION:-1d}"


# Restate's memory pools; each variable names a key of `restate-server --dump-config`.
RESTATE_POOLS = (
    "RESTATE_ROCKSDB_TOTAL_MEMORY_SIZE",  # rocksdb-total-memory-size
    "RESTATE_WORKER__INVOKER__MEMORY_LIMIT",  # [worker.invoker] memory-limit
    "RESTATE_ADMIN__QUERY_ENGINE__MEMORY_SIZE",  # [admin.query-engine] memory-size
    "RESTATE_BIFROST__RECORD_CACHE_MEMORY_SIZE",  # [bifrost] record-cache-memory-size
)
_RESTATE_SIZE = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*(B|KiB|MiB|GiB|kB|KB|MB|GB)\s*$")
_RESTATE_UNITS = {
    "B": 1,
    "KiB": 2**10,
    "MiB": 2**20,
    "GiB": 2**30,
    "kB": 10**3,
    "KB": 10**3,
    "MB": 10**6,
    "GB": 10**9,
}
_MEM_LIMIT = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([bkmg]?)b?\s*$", re.IGNORECASE)
_MEM_UNITS = {"": 1, "b": 1, "k": 2**10, "m": 2**20, "g": 2**30}


def restate_size(text: str) -> int:
    """A Restate size such as ``512 MiB`` or ``1.5 GiB``, in bytes."""
    m = _RESTATE_SIZE.match(text)
    if m is None:
        raise ValueError(f"not a Restate size: {text!r}")
    return int(float(m.group(1)) * _RESTATE_UNITS[m.group(2)])


def mem_limit(value: str | int) -> int:
    """A compose ``mem_limit`` such as ``2g``, ``2560m`` or a byte count, in bytes (binary
    units)."""
    if isinstance(value, int):
        return value
    m = _MEM_LIMIT.match(value)
    if m is None:
        raise ValueError(f"not a compose memory limit: {value!r}")
    return int(float(m.group(1)) * _MEM_UNITS[m.group(2).lower()])


@pytest.mark.parametrize(
    ("text", "size"),
    [
        ("512 MiB", 512 * 2**20),
        ("512.0 MiB", 512 * 2**20),
        ("384 MiB", 384 * 2**20),
        ("1.5 GiB", 3 * 2**29),
        ("250.0 MiB", 250 * 2**20),
        ("128MiB", 128 * 2**20),
    ],
)
def test_ac_012_39_restate_sizes_parse(text: str, size: int) -> None:
    assert restate_size(text) == size


@pytest.mark.parametrize(
    ("value", "size"),
    [
        ("2g", 2 * 2**30),
        ("2560m", 2560 * 2**20),
        ("768m", 768 * 2**20),
        ("1g", 2**30),
        (1024, 1024),
    ],
)
def test_ac_012_39_compose_memory_limits_parse(value: str | int, size: int) -> None:
    assert mem_limit(value) == size


@pytest.mark.parametrize("text", ["512", "512 mebibytes", "", "two GiB"])
def test_ac_012_39_an_unknown_size_is_refused(text: str) -> None:
    with pytest.raises(ValueError):
        restate_size(text)


def test_ac_012_39_restate_sets_its_four_memory_pools(repo: Path) -> None:
    env = _service(repo, "restate").get("environment") or {}
    missing = [name for name in RESTATE_POOLS if name not in env]
    assert missing == [], f"restate leaves these pools at their defaults: {missing}"
    for name in RESTATE_POOLS:
        assert restate_size(str(env[name])) > 0, (name, env[name])


def test_ac_012_39_restate_memory_pools_stay_below_its_mem_limit(repo: Path) -> None:
    service = _service(repo, "restate")
    env = service.get("environment") or {}
    pools = {name: restate_size(str(env[name])) for name in RESTATE_POOLS if name in env}
    assert set(pools) == set(RESTATE_POOLS), f"pools set: {sorted(pools)}"
    assert "mem_limit" in service, "restate has no mem_limit"
    assert sum(pools.values()) < mem_limit(service["mem_limit"]), (pools, service["mem_limit"])


def _one_shot(services: dict[str, Any]) -> set[str]:
    """Jobs another service waits for to finish (`condition: service_completed_successfully`)."""
    out: set[str] = set()
    for service in services.values():
        depends = service.get("depends_on") or {}
        if isinstance(depends, dict):
            out |= {
                name
                for name, spec in depends.items()
                if (spec or {}).get("condition") == "service_completed_successfully"
            }
    return out


def long_running(compose: dict[str, Any]) -> list[str]:
    """The services `make up` starts (every service without a profile) and those `make register`
    starts (the `ingress` profile), without one-shot jobs such as `kafka-init`. `ems-gen` (profile
    `gen`, started by hand) needs no limit (user decision, 2026-11-24)."""
    services = compose["services"]
    started = {
        name
        for name, service in services.items()
        if not service.get("profiles") or "ingress" in service["profiles"]
    }
    return sorted(started - _one_shot(services))


LONG_RUNNING = long_running(_compose(stack.REPO))


def test_ac_012_39_long_running_services_are_what_make_up_and_make_register_start() -> None:
    assert LONG_RUNNING == [
        "ingress-forecasts",
        "ingress-meters",
        "ingress-prices",
        "kafka",
        "kafka-ui",
        "restate",
    ]


@pytest.mark.parametrize("name", LONG_RUNNING)
def test_ac_012_39_every_long_running_service_has_a_mem_limit(repo: Path, name: str) -> None:
    service = _service(repo, name)
    assert "mem_limit" in service, f"{name} has no mem_limit"
    assert mem_limit(service["mem_limit"]) > 0, (name, service["mem_limit"])
