"""Europe/Warsaw local calendar fields for arrays of UTC timestamps (vectorised over hours)."""

from __future__ import annotations

from datetime import datetime

import numpy as np
from numpy.typing import NDArray

from ems.contracts.time import WARSAW

F64 = NDArray[np.float64]
I64 = NDArray[np.int64]


def _offset_s(unix: int) -> float:
    off = datetime.fromtimestamp(unix, tz=WARSAW).utcoffset()
    assert off is not None
    return off.total_seconds()


def local_unix(unix_utc: F64) -> F64:
    """UTC seconds shifted by the Warsaw offset in force at each instant (DST-aware)."""
    hours = np.floor(unix_utc / 3600.0).astype(np.int64)
    uniq, inv = np.unique(hours, return_inverse=True)
    offsets = np.array([_offset_s(int(h) * 3600) for h in uniq], dtype=np.float64)
    return unix_utc + offsets[inv]


def local_hour(unix_utc: F64) -> F64:
    return (local_unix(unix_utc) % 86400.0) / 3600.0


def local_year(unix_utc: F64) -> I64:
    years = local_unix(unix_utc).astype("datetime64[s]").astype("datetime64[Y]")
    return years.astype(np.int64) + 1970
