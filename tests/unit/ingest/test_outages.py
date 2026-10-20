"""Outage masks are deterministic, with the planned block length and forced rate configured."""

from __future__ import annotations

import numpy as np

from ems.ingest import rng
from ems.ingest.synthetic.outages import availability

KEYS = np.array([rng.location_key(f"PL-LOC-{i:06d}") for i in range(1, 201)], dtype=np.uint64)
T = np.arange(0, 96 * 365 * 2, dtype=np.int64) + 96 * 365 * 56  # two whole "years" of quarter-hours


def test_deterministic_and_daily() -> None:
    a = availability(1, KEYS, T, "x", 0.02, 14)
    assert np.array_equal(a, availability(1, KEYS, T, "x", 0.02, 14))
    days = a.reshape(KEYS.size, -1, 96)
    assert (days.min(axis=2) == days.max(axis=2)).all()  # outages cover whole days


def test_rates() -> None:
    planned_only = availability(2, KEYS, T, "p", 0.0, 14)
    off_days = (1 - planned_only.reshape(KEYS.size, -1, 96)[:, :, 0]).sum(axis=1)
    assert (off_days == 28).all()  # 14 consecutive days per year, two years
    forced_only = availability(3, KEYS, T, "f", 0.05, 0)
    share = 1 - forced_only.mean()
    assert 0.04 < share < 0.06
