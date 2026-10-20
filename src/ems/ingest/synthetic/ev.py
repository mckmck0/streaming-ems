"""EV charging: one session per site and local day (overnight at households, daytime at
commercial sites);
the car charges at the charger limit from arrival until the energy need is met or it leaves.
"""

from __future__ import annotations

import numpy as np
from numpy.typing import NDArray
from scipy.special import ndtri

from ems.ingest import rng

F64 = NDArray[np.float64]
DT_H = 0.25
PATTERNS = {  # arrival hour mean/sd, departure hour mean/sd (local), next-day departure
    "overnight": (18.5, 1.2, 7.0, 0.7, True),
    "daytime": (8.5, 0.8, 16.5, 1.0, False),
}


def ev_kwh(
    seed: int,
    keys: NDArray[np.uint64],
    local_unix_start: F64,
    p_max_kw: F64,
    need_kwh: F64,
    patterns: list[str | None],
) -> F64:
    """Charging energy per interval [n_loc, n_steps] (local_unix_start = interval starts
    in local seconds).
    """
    n_loc, n = keys.size, local_unix_start.size
    out = np.zeros((n_loc, n))
    days = np.unique(np.floor(local_unix_start / 86400.0).astype(np.int64))
    days = np.concatenate([[days[0] - 1], days])  # sessions that started the evening before
    for pattern, (a_mu, a_sd, d_mu, d_sd, next_day) in PATTERNS.items():
        rows = np.array([i for i, x in enumerate(patterns) if x == pattern], dtype=np.int64)
        if rows.size == 0:
            continue
        k = keys[rows]
        u_a = ndtri(rng.uniform(seed, k, "ev.arrival", days))
        u_d = ndtri(rng.uniform(seed, k, "ev.departure", days))
        u_n = ndtri(rng.uniform(seed, k, "ev.need", days))
        for j, d in enumerate(days):
            arrive = d * 86400.0 + 3600.0 * np.clip(a_mu + a_sd * u_a[:, j], 0, 23.75)
            depart = (d + (1 if next_day else 0)) * 86400.0 + 3600.0 * np.clip(
                d_mu + d_sd * u_d[:, j], 0, 23.75
            )
            need = need_kwh[rows] * np.exp(0.35 * u_n[:, j] - 0.35**2 / 2)
            full_steps = need / (p_max_kw[rows] * DT_H)
            start_idx = np.ceil((arrive[:, None] - local_unix_start[None, :]) / 900.0)
            k_rel = -start_idx  # intervals since the first charging interval
            active = (local_unix_start[None, :] >= arrive[:, None] - 1e-9) & (
                local_unix_start[None, :] + 900.0 <= depart[:, None] + 1e-9
            )
            share = np.clip(full_steps[:, None] - k_rel, 0.0, 1.0)
            out[rows] += np.where(active, share * (p_max_kw[rows] * DT_H)[:, None], 0.0)
    return np.minimum(out, (p_max_kw * DT_H).reshape(-1, 1))
