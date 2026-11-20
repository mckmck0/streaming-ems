"""Python pins and the Restate settings of spec 010.

AC-010-02: `restate-sdk` 1.5.2 and `hypercorn` 0.18.0 exactly, in `uv.lock` and in the default
dependency group `actors`; no `[tool.uv] exclude-newer`.
AC-010-03: `make register` posts with `force: false` unless `FORCE` is given.
P11: the Restate URLs come from `ems.settings` (`EMS_` prefix), and the Makefile falls back to
the same defaults.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path
from typing import Any

import pytest

from ems.settings import EmsSettings

RESTATE_ENV = ("EMS_RESTATE_INGRESS_URL", "EMS_RESTATE_ADMIN_URL", "EMS_SERVICE_URL")
RESTATE_ADMIN_LINE = re.compile(
    r"^RESTATE_ADMIN\s*\?=\s*\$\(or \$\(EMS_RESTATE_ADMIN_URL\),(\S+)\)\s*$"
)
SERVICE_URL_LINE = re.compile(r"^SERVICE_URL\s*\?=\s*\$\(or \$\(EMS_SERVICE_URL\),(\S+)\)\s*$")
FORCE_LINE = re.compile(r"^FORCE\s*\?=\s*false\s*$")


def _toml(path: Path) -> dict[str, Any]:
    return tomllib.loads(path.read_text())


def _locked_versions(repo: Path, name: str) -> list[str]:
    return [p["version"] for p in _toml(repo / "uv.lock")["package"] if p["name"] == name]


def _makefile_default(repo: Path, pattern: re.Pattern[str]) -> str | None:
    for line in (repo / "Makefile").read_text().splitlines():
        if match := pattern.match(line):
            return match.group(1)
    return None


@pytest.fixture
def no_restate_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in RESTATE_ENV:
        monkeypatch.delenv(name, raising=False)


def test_ac_010_02_sdk_and_hypercorn_locked_exactly(repo: Path) -> None:
    assert _locked_versions(repo, "restate-sdk") == ["1.5.2"]
    assert _locked_versions(repo, "hypercorn") == ["0.18.0"]


def test_ac_010_02_actors_group_is_default_and_exact(repo: Path) -> None:
    pyproject = _toml(repo / "pyproject.toml")
    assert set(pyproject["dependency-groups"]["actors"]) == {
        "restate-sdk==1.5.2",
        "hypercorn==0.18.0",
    }
    assert "actors" in pyproject["tool"]["uv"]["default-groups"]


def test_ac_010_02_no_exclude_newer(repo: Path) -> None:
    assert "exclude-newer" not in _toml(repo / "pyproject.toml")["tool"]["uv"]


@pytest.mark.usefixtures("no_restate_env")
def test_p11_restate_url_defaults() -> None:
    settings = EmsSettings()
    assert settings.restate_ingress_url == "http://127.0.0.1:8080"
    assert settings.restate_admin_url == "http://127.0.0.1:9070"
    assert settings.service_url == "http://host.docker.internal:9080"


@pytest.mark.usefixtures("no_restate_env")
def test_p11_restate_admin_url_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EMS_RESTATE_ADMIN_URL", "http://example:1")
    assert EmsSettings().restate_admin_url == "http://example:1"


@pytest.mark.usefixtures("no_restate_env")
def test_p11_makefile_defaults_equal_settings(repo: Path) -> None:
    settings = EmsSettings()
    assert _makefile_default(repo, RESTATE_ADMIN_LINE) == settings.restate_admin_url
    assert _makefile_default(repo, SERVICE_URL_LINE) == settings.service_url


def test_ac_010_03_makefile_force_defaults_to_false(repo: Path) -> None:
    lines = (repo / "Makefile").read_text().splitlines()
    assert any(FORCE_LINE.match(line) for line in lines), "no line `FORCE ?= false` in the Makefile"
