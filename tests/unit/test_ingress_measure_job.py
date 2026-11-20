"""AC-010-18: the skew summary that ``make measure`` writes (spec 010, plan §7).

The largest lead per key and stream pair (quarter-hours) is summarised across keys by the number of
keys that have the pair, the median, the 95th percentile (nearest rank) and the maximum.
"""

from __future__ import annotations

import functools
import importlib.util
from pathlib import Path
from types import ModuleType

import pytest

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
