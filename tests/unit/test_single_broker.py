"""AC-006-01/-02/-03: one Kafka broker, every topic profile fits it, `cluster` is refused."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError
from ruamel.yaml import YAML

from ems.contracts.topics import TopicConfigError, load_plan, load_topics_file
from ems.settings import EmsSettings


def _compose_services(repo: Path) -> dict[str, Any]:
    data = YAML(typ="safe").load((repo / "infra" / "docker-compose.yml").read_text())
    return data["services"]


def test_ac_006_01_one_combined_broker(repo: Path) -> None:
    services = _compose_services(repo)
    brokers = {
        name: svc
        for name, svc in services.items()
        if str(svc.get("image", "")).startswith("apache/kafka")
    }
    assert set(brokers) == {"kafka"}
    assert brokers["kafka"]["environment"]["KAFKA_PROCESS_ROLES"] == "broker,controller"
    assert all("cluster" not in svc.get("profiles", []) for svc in services.values())


def test_ac_006_02_every_profile_fits_one_broker(repo: Path) -> None:
    profiles = load_topics_file(repo / "config" / "topics.yaml")["profiles"]
    assert profiles
    for profile in profiles:
        for topic in load_plan(repo / "config", profile).topics:
            assert topic.replication_factor == 1, (profile, topic.name)
            assert topic.configs["min.insync.replicas"] == "1", (profile, topic.name)


def test_ac_006_03_cluster_profile_is_refused(repo: Path) -> None:
    with pytest.raises(TopicConfigError, match="choose one of"):
        load_plan(repo / "config", "cluster")
    with pytest.raises(ValidationError):
        EmsSettings(topics_profile="cluster")  # ty: ignore[invalid-argument-type]
