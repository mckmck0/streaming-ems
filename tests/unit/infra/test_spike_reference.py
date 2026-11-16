"""The reference spike, tracked unedited (AC-010-22).

`reference/restate-ems-spike/` holds exactly the 16 files the spec lists, each with the SHA-256
recorded in `tests/fixtures/spike-files.sha256` when the files were moved. The fixture is in
`sha256sum` format (64 lowercase hex digits, two spaces, the path relative to the spike
directory), one line per file, sorted by path in code-point order (`LC_ALL=C sort`).
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

SPIKE = Path("reference") / "restate-ems-spike"
FIXTURE = Path("tests") / "fixtures" / "spike-files.sha256"
SHA256SUM_LINE = re.compile(r"^([0-9a-f]{64})  ([^\s\\]+)$")
SPIKE_FILES = {
    "README.md",
    "ctl.sh",
    "experiments.py",
    "loadgen.py",
    "rerun.py",
    "requirements.txt",
    "package.json",
    "package-lock.json",
    "ems_spike/__init__.py",
    "ems_spike/model.py",
    "ems_spike/restate_app.py",
    "ems_spike/extra_app.py",
    "tools/killport.py",
    "results.json",
    "results_run1.json",
    "guard_test.txt",
}


def _recorded(repo: Path) -> list[tuple[str, str]]:
    """(path, sha256) for each line of the fixture, in file order."""
    entries: list[tuple[str, str]] = []
    for line in (repo / FIXTURE).read_text().splitlines():
        match = SHA256SUM_LINE.match(line)
        assert match, f"not a sha256sum line: {line!r}"
        entries.append((match.group(2), match.group(1)))
    return entries


def _tree(repo: Path) -> dict[str, Path]:
    """Every file under the spike directory, hidden files and caches included, by relative path."""
    root = repo / SPIKE
    assert root.is_dir(), f"{SPIKE} is missing"
    return {
        (directory / name).relative_to(root).as_posix(): directory / name
        for directory, _subdirectories, files in root.walk()
        for name in files
    }


def test_ac_010_22_fixture_is_sha256sum_sorted_by_path(repo: Path) -> None:
    paths = [path for path, _ in _recorded(repo)]
    assert paths == sorted(set(paths))


def test_ac_010_22_fixture_lists_the_spike_files(repo: Path) -> None:
    assert {path for path, _ in _recorded(repo)} == SPIKE_FILES


def test_ac_010_22_reference_holds_exactly_the_spike_files(repo: Path) -> None:
    assert set(_tree(repo)) == SPIKE_FILES


def test_ac_010_22_reference_files_match_recorded_hashes(repo: Path) -> None:
    actual = {
        path: hashlib.sha256(file.read_bytes()).hexdigest() for path, file in _tree(repo).items()
    }
    assert actual == dict(_recorded(repo))
