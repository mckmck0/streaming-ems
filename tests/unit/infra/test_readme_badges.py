"""README badges of spec 010.

AC-010-30: the top of README.md shows badges for the CI status, the required
Python version, uv, Ruff, ty, the Restate server version and the Kafka version.
AC-010-31: the Restate and Kafka versions on the badges equal the image tags in
infra/docker-compose.yml (the Restate tag without its rebuild date).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from ruamel.yaml import YAML

TOP_LINES = 15
IMAGE_URL = re.compile(r"!\[[^\]]*\]\(\s*<?([^\s)>]+)")
STATIC_BADGE = re.compile(r"^https://img\.shields\.io/badge/([^?#]+)")
REBUILD_DATE = re.compile(r"-\d{8}$")
BADGES = {
    "ci": "actions/workflows/ci.yml/badge.svg",
    "python": "img.shields.io/python/required-version-toml",
    "uv": "astral-sh/uv/main/assets/badge",
    "ruff": "astral-sh/ruff/main/assets/badge",
    "ty": "astral-sh/ty/main/assets/badge",
    "restate": "img.shields.io/badge/Restate-",
    "kafka": "img.shields.io/badge/Kafka-",
}


def _badge_urls(repo: Path) -> list[str]:
    """URLs of the markdown images in the first lines of README.md."""
    top = "\n".join((repo / "README.md").read_text().splitlines()[:TOP_LINES])
    return IMAGE_URL.findall(top)


def _static_badges(urls: list[str]) -> dict[str, str]:
    """Label to message of the static shields.io badges, `/badge/<label>-<message>-<color>`.

    In label and message a literal `-` is written `--` and `_` is written `__`.
    """
    badges: dict[str, str] = {}
    for url in urls:
        if match := STATIC_BADGE.match(url):
            parts = match.group(1).replace("--", "\0").split("-")
            if len(parts) == 3:
                label, message, _color = (p.replace("\0", "-").replace("__", "_") for p in parts)
                badges[label] = message
    return badges


def _image_tag(repo: Path, service: str) -> str:
    compose = YAML(typ="safe").load((repo / "infra" / "docker-compose.yml").read_text())
    services = compose["services"]
    assert service in services, f"no service {service!r} in infra/docker-compose.yml"
    return str(services[service]["image"]).split("@", 1)[0].rsplit(":", 1)[1]


@pytest.mark.parametrize("fragment", list(BADGES.values()), ids=list(BADGES))
def test_ac_010_30_badge_at_top_of_readme(repo: Path, fragment: str) -> None:
    urls = _badge_urls(repo)
    assert any(fragment in url for url in urls), (
        f"no badge {fragment!r} in the top {TOP_LINES} lines"
    )


def test_ac_010_31_restate_badge_shows_compose_version(repo: Path) -> None:
    badges = _static_badges(_badge_urls(repo))
    assert "Restate" in badges, "no static Restate badge at the top of README.md"
    assert badges["Restate"] == REBUILD_DATE.sub("", _image_tag(repo, "restate"))


def test_ac_010_31_kafka_badge_shows_compose_version(repo: Path) -> None:
    badges = _static_badges(_badge_urls(repo))
    assert "Kafka" in badges, "no static Kafka badge at the top of README.md"
    assert badges["Kafka"] == _image_tag(repo, "kafka")
