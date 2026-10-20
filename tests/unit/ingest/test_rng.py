"""AC-001-26: counter-based randomness is independent of batch size, order and window start."""

from __future__ import annotations

import numpy as np

from ems.ingest import rng

KEYS = [rng.location_key(f"PL-LOC-{i:06d}") for i in range(1, 6)]


def test_uniform_range_and_moments() -> None:
    u = rng.uniform(1, KEYS, "x", np.arange(20000))
    assert u.shape == (5, 20000) and u.min() > 0 and u.max() < 1
    assert abs(u.mean() - 0.5) < 0.01
    z = rng.normal(1, KEYS, "x", np.arange(20000))
    assert abs(z.mean()) < 0.03 and abs(z.std() - 1) < 0.03


def test_ac_001_26_values_do_not_depend_on_batch_or_order() -> None:
    steps = np.arange(100, 200)
    alone = rng.uniform(7, [KEYS[2]], "v", steps)[0]
    batch = rng.uniform(7, KEYS, "v", steps)[2]
    reversed_batch = rng.uniform(7, KEYS[::-1], "v", steps)[2]
    assert np.array_equal(alone, batch) and np.array_equal(alone, reversed_batch)


def test_streams_differ_by_seed_variable_and_key() -> None:
    steps = np.arange(50)
    a = rng.uniform(7, KEYS[:1], "v", steps)
    assert not np.array_equal(a, rng.uniform(8, KEYS[:1], "v", steps))
    assert not np.array_equal(a, rng.uniform(7, KEYS[:1], "w", steps))
    assert not np.array_equal(a, rng.uniform(7, KEYS[1:2], "v", steps))


def test_ar1_is_window_independent_and_unit_variance() -> None:
    phi = rng.phi_for(6.0)
    long = rng.ar1(3, KEYS, "ar", 1000, 500, phi)
    late = rng.ar1(3, KEYS, "ar", 1200, 300, phi)
    assert np.allclose(long[:, 200:], late, atol=0.05)  # burn-in residual weight < exp(-6)
    assert np.array_equal(
        rng.ar1(3, [KEYS[1]], "ar", 0, 300, phi)[0], rng.ar1(3, KEYS, "ar", 0, 300, phi)[1]
    )
    big = rng.ar1(3, KEYS, "ar", 0, 40000, phi)
    assert abs(big.std() - 1) < 0.08
    lag1 = np.corrcoef(big[0, :-1], big[0, 1:])[0, 1]
    assert abs(lag1 - phi) < 0.02
