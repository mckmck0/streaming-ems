"""AC-010-18: the skew summary that ``make measure`` writes (spec 010, plan §7).

The largest lead per key and stream pair (quarter-hours) is summarised across keys by the number of
keys that have the pair, the median, the 95th percentile (nearest rank) and the maximum.

AC-011-46 (spec 011, plan §9): ``skew`` also reports, per receiving service, the messages that
arrived early (``reordered``) or stale, summed over every inbox of an object's stored state; the job
has no ``state-cost`` mode any more.
"""

from __future__ import annotations

import functools
import importlib.util
from pathlib import Path
from types import ModuleType

import pytest
from typer.testing import CliRunner

REPO = Path(__file__).resolve().parents[2]


@functools.cache
def _job() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "ingress_measure", REPO / "jobs" / "ingress_measure.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_ac_010_18_nearest_rank_percentile() -> None:
    job = _job()
    leads_quarter_hours = [7.0, 1.0, 5.0, 3.0, 6.0, 2.0, 4.0]
    assert job.nearest_rank(leads_quarter_hours, 0.95) == 7.0  # rank ceil(6.65) = 7
    assert job.nearest_rank(leads_quarter_hours, 0.5) == 4.0  # rank ceil(3.5) = 4
    assert job.nearest_rank(leads_quarter_hours, 0.1) == 1.0  # rank ceil(0.7) = 1
    assert job.nearest_rank(leads_quarter_hours, 1.0) == 7.0
    assert job.nearest_rank([3.0], 0.95) == 3.0


def test_ac_010_18_nearest_rank_of_no_values_raises() -> None:
    with pytest.raises(ValueError):
        _job().nearest_rank([], 0.95)


def test_ac_010_18_summary_of_three_keys() -> None:
    leads_by_key = {
        "r1:PL-LOC-000001": {
            "consumption>production": 0,
            "production>consumption": 1,
            "weather_forecast>production": 7,
        },
        "r1:PL-LOC-000002": {"consumption>production": -2, "production>consumption": 3},
        "r1:PL-LOC-000003": {
            "consumption>production": 4,
            "production>consumption": 2,
            "weather_forecast>production": 9,
        },
    }
    assert _job().summarize_leads(leads_by_key) == {
        "consumption>production": {"keys": 3, "median": 0, "p95": 4, "max": 4},
        "production>consumption": {"keys": 3, "median": 2, "p95": 3, "max": 3},
        "weather_forecast>production": {"keys": 2, "median": 8.0, "p95": 9, "max": 9},
    }


def test_ac_010_18_p95_is_the_nearest_rank_not_the_maximum() -> None:
    """21 keys: the 95th percentile is the 20th smallest lead (rank ceil(19.95)), not the max."""
    leads_by_key = {f"r1:PL-LOC-{i:06d}": {"consumption>production": i} for i in range(21)}
    assert _job().summarize_leads(leads_by_key) == {
        "consumption>production": {"keys": 21, "median": 10, "p95": 19, "max": 20},
    }


def _inbox(expected: int, reordered: int, stale: int) -> dict[str, object]:
    return {"expected": expected, "held": [], "reordered": reordered, "stale": stale}


def test_ac_011_46_reorder_counts_sum_every_inbox_of_a_state() -> None:
    """Ems keeps two inboxes (steps and acknowledgements); other fields are not inboxes."""
    ems_state = {
        "soc_kwh": 5.0,
        "last_pv_kwh": 0.0,
        "last_load_kwh": 0.0,
        "sent": 1992671,
        "steps": _inbox(1992672, reordered=3, stale=1),
        "acks": _inbox(1992672, reordered=2, stale=4),
        "waiting": [],
    }
    assert _job().reorder_counts(ems_state) == (5, 5)


def test_ac_011_46_reorder_counts_of_a_state_without_inbox_is_zero() -> None:
    assert _job().reorder_counts({"soc_kwh": 5.0, "leads": {"newest": {}, "max": {}}}) == (0, 0)


def test_ac_011_46_reorder_counts_ignore_a_mapping_that_is_no_inbox() -> None:
    state = {
        "join": {"next": 7, "expected": 3},  # not an inbox: no held, reordered, stale
        "prices": {"expected": None, "held": [[2, {}]], "reordered": 1, "stale": 0},
        "forecasts": _inbox(4, reordered=0, stale=2),
    }
    assert _job().reorder_counts(state) == (1, 2)


def test_ac_011_46_the_job_has_no_state_cost_mode(tmp_path: Path) -> None:
    job = _job()
    assert not hasattr(job, "run_state_cost")
    result = CliRunner().invoke(job.app, ["state-cost", "--out", str(tmp_path)])
    assert result.exit_code == 2
    assert "unknown mode" in result.output
    assert "(skew | backlog)" in result.output  # the modes it lists
    assert list(tmp_path.iterdir()) == []  # nothing written


def test_ac_011_46_the_mode_help_names_skew_and_backlog() -> None:
    result = CliRunner().invoke(_job().app, ["--help"])
    assert result.exit_code == 0
    assert "skew | backlog" in result.output
    assert "state-cost" not in result.output
