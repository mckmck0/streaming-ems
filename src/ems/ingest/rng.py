"""Counter-based randomness: values are pure functions of (seed, location key, variable, step).

A numpy Generator drawing an ``[n_loc, n_steps]`` block gives values that depend on the batch size
and order; hashing the coordinates instead makes every location reproducible on its own (AC-001-26)
and keeps generation vectorised. Autoregressive noise is a per-row recursion over keyed innovations
with a burn-in, so a value at step ``t`` barely depends on where a window starts.
"""

from __future__ import annotations

import numpy as np
from numpy.typing import NDArray
from scipy.signal import lfilter
from scipy.special import ndtri

from ems.contracts.ids import stable_hash64

U64 = np.uint64
F64 = NDArray[np.float64]
I64 = NDArray[np.int64]

_GOLDEN = U64(0x9E3779B97F4A7C15)
_M1 = U64(0xBF58476D1CE4E5B9)
_M2 = U64(0x94D049BB133111EB)

# The national (shared) process uses this location key: weather fronts and price drivers are common.
NATIONAL_KEY = 0


def splitmix64(x: NDArray[np.uint64]) -> NDArray[np.uint64]:
    with np.errstate(over="ignore"):
        z = x + _GOLDEN
        z = (z ^ (z >> U64(30))) * _M1
        z = (z ^ (z >> U64(27))) * _M2
        return z ^ (z >> U64(31))


def location_key(location_id: str) -> int:
    return stable_hash64("loc", location_id)


def variable_id(name: str) -> int:
    return stable_hash64("var", name)


def uniform(seed: int, keys: list[int] | NDArray[np.uint64], var: str, steps: I64) -> F64:
    """Uniforms in (0, 1), shape ``[len(keys), len(steps)]``."""
    k = np.asarray(keys, dtype=np.uint64).reshape(-1, 1)
    s = np.asarray(steps, dtype=np.int64).astype(np.uint64).reshape(1, -1)
    v = U64(variable_id(var))
    with np.errstate(over="ignore"):
        h = splitmix64(splitmix64(splitmix64(U64(seed & 0xFFFFFFFFFFFFFFFF) ^ k) ^ v) ^ s)
    return ((h >> U64(11)).astype(np.float64) + 0.5) * (1.0 / 9007199254740992.0)


def normal(seed: int, keys: list[int] | NDArray[np.uint64], var: str, steps: I64) -> F64:
    """Standard normals via the inverse CDF of keyed uniforms."""
    return np.asarray(ndtri(uniform(seed, keys, var, steps)), dtype=np.float64)


def ar1(
    seed: int,
    keys: list[int] | NDArray[np.uint64],
    var: str,
    t0: int,
    n: int,
    phi: float,
    burn: int | None = None,
) -> F64:
    """Unit-variance AR(1) noise for steps ``t0 .. t0+n-1``, shape ``[len(keys), n]``.

    The recursion starts ``burn`` steps before ``t0`` (default: 6 time constants), so the value at a
    step depends on the window start only through a residual weight below ``exp(-6)``. Each row is
    an independent scalar recursion: results are bit-identical whatever the batch size (AC-001-26).
    """
    if burn is None:
        burn = int(np.ceil(6.0 / max(1e-9, -np.log(phi)))) if phi > 0 else 0
    steps = np.arange(t0 - burn, t0 + n, dtype=np.int64)
    eps = normal(seed, keys, var, steps)
    if phi <= 0:
        return eps[:, burn:]
    b = np.array([np.sqrt(1.0 - phi * phi)])
    a = np.array([1.0, -phi])
    rest = lfilter(b, a, eps[:, 1:], axis=1, zi=phi * eps[:, :1])[0]
    out = np.concatenate([eps[:, :1], np.asarray(rest, dtype=np.float64)], axis=1)
    return np.ascontiguousarray(out[:, burn:])


def phi_for(tau_hours: float, dt_hours: float = 0.25) -> float:
    """AR coefficient for a time constant ``tau_hours`` at step ``dt_hours``."""
    return float(np.exp(-dt_hours / tau_hours))
