"""AC-002-15: every committed fixture folder carries a licence / attribution NOTICE."""

from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def test_ac_002_15_fixture_folders_have_notices() -> None:
    folders = [p for p in (REPO / "tests" / "fixtures").iterdir() if p.is_dir()]
    assert folders
    for folder in folders:
        notice = folder / "NOTICE"
        assert notice.exists(), folder
        assert len(notice.read_text().strip()) > 40
    assert "CC BY 4.0" in (REPO / "tests" / "fixtures" / "openmeteo" / "NOTICE").read_text()
