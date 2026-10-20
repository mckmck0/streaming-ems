"""Availability masks: planned outages (one block of consecutive days per site and year)
and forced outages
(independent days with a given probability). Keyed per (seed, site, kind, day), so deterministic.
"""

from __future__ import annotations

import numpy as np
from numpy.typing import NDArray

from ems.ingest import rng

F64 = NDArray[np.float64]


def availability(
    seed: int,
    keys: NDArray[np.uint64],
    t: NDArray[np.int64],
    kind: str,
    forced_rate_per_day: float,
    planned_days_per_year: int,
) -> F64:
    """1.0 where the asset is available, 0.0 in an outage, shape [n_loc, n_steps]."""
    days = (t // 96).astype(np.int64)
    uniq = np.unique(days)
    forced = rng.uniform(seed, keys, f"outage.forced.{kind}", uniq) < forced_rate_per_day
    mask = np.ones((keys.size, uniq.size))
    mask[forced] = 0.0
    if planned_days_per_year > 0:
        years = uniq // 365
        for y in np.unique(years):
            offset = np.floor(
                rng.uniform(seed, keys, f"outage.planned.{kind}", np.array([y]))[:, 0]
                * (365 - planned_days_per_year)
            ).astype(np.int64)
            day_in_year = uniq - y * 365
            in_year = years == y
            planned = (day_in_year[None, :] >= offset[:, None]) & (
                day_in_year[None, :] < offset[:, None] + planned_days_per_year
            )
            mask[planned & in_year[None, :]] = 0.0
    return mask[:, np.searchsorted(uniq, days)]
