"""AC-003-01/-02/-03/-04: fleets, site chunking and site-controlled batteries."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import numpy as np

from ems.contracts.time import quarter_index
from ems.ingest.catalog.build import build_catalog
from ems.ingest.generator import SyntheticModel

REPO = Path(__file__).resolve().parents[2]
T0 = quarter_index(datetime(2026, 6, 1, tzinfo=UTC))


def fleet(n: int, share: float = 0.0) -> list:
    return build_catalog(n, catalog_seed=7, global_seed=4242, site_controlled_bess_share=share)


def test_ac_003_01_site_in_fleet_equals_site_alone() -> None:
    sites = fleet(40)
    big = SyntheticModel(sites, 4242, REPO / "config").chunk(T0, 96, [T0])
    for k in (0, 17, 39):
        alone = SyntheticModel([sites[k]], 4242, REPO / "config").chunk(T0, 96, [T0])
        for name in ("pv_kwh", "load_kwh", "hp_kwh"):
            assert np.array_equal(getattr(big, name)[k], getattr(alone, name)[0]), name
        assert np.array_equal(big.forecasts[T0].ghi[k], alone.forecasts[T0].ghi[0])


def test_ac_003_02_chunked_equals_unchunked() -> None:
    sites = fleet(25, share=0.5)
    whole = SyntheticModel(sites, 4242, REPO / "config", site_chunk=1000).chunk(T0, 96, [T0])
    sliced = SyntheticModel(sites, 4242, REPO / "config", site_chunk=7).chunk(T0, 96, [T0])
    for name in ("pv_kwh", "load_kwh", "hp_kwh", "bess_soc_kwh", "bess_charge_kwh"):
        assert np.array_equal(getattr(whole, name), getattr(sliced, name)), name
    assert np.array_equal(whole.forecasts[T0].temp_c, sliced.forecasts[T0].temp_c)


def test_ac_003_03_site_controlled_share_and_stable_catalog() -> None:
    none = fleet(400, 0.0)
    half = fleet(400, 0.5)
    with_bess = [s for s in half if s.bess is not None]
    share = sum(s.bess.control == "site" for s in with_bess) / len(with_bess)  # type: ignore[union-attr]
    assert 0.35 < share < 0.65
    assert all(s.bess is None or s.bess.control == "ems" for s in none)
    for a, b in zip(none, half, strict=True):  # changing the share changes nothing else
        assert a.model_dump(exclude={"assets"}) == b.model_dump(exclude={"assets"})


def test_ac_003_04_site_batteries_stay_in_bounds_and_balance_closes() -> None:
    sites = [s for s in fleet(200, 1.0) if s.bess is not None][:20]
    model = SyntheticModel(sites, 4242, REPO / "config")
    chunk = model.chunk(quarter_index(datetime(2026, 1, 1, tzinfo=UTC)), 96 * 120, [])
    for k, s in enumerate(sites):
        b = s.bess
        assert b is not None and b.control == "site"
        soc = chunk.bess_soc_kwh[k]
        assert soc.min() >= b.soc_min_kwh - 1e-9 and soc.max() <= b.soc_max_kwh + 1e-9
        assert chunk.bess_charge_kwh[k].max() <= b.p_max_kw * 0.25 + 1e-9
        assert chunk.bess_discharge_kwh[k].max() <= b.p_max_kw * 0.25 + 1e-9
        assert not ((chunk.bess_charge_kwh[k] > 0) & (chunk.bess_discharge_kwh[k] > 0)).any()
    used = chunk.bess_charge_kwh.sum() + chunk.bess_discharge_kwh.sum()
    assert used > 0  # the policy actually cycles the batteries


def test_state_carries_over_consecutive_chunks() -> None:
    """The next chunk continues from the previous SoC and register (values across chunk
    borders differ
    only by the AR burn-in residual, risk G1, so continuity is checked rather than equality).
    """
    sites = [s for s in fleet(100, 1.0) if s.bess is not None][:3]
    model = SyntheticModel(sites, 4242, REPO / "config")
    a = model.chunk(T0, 96, [])
    b = model.chunk(T0 + 96, 96, [])
    p_step = np.array([s.bess.p_max_kw * 0.25 for s in sites])  # type: ignore[union-attr]
    assert (np.abs(b.bess_soc_kwh[:, 0] - a.bess_soc_kwh[:, -1]) <= p_step + 1e-9).all()
    assert (b.import_register_kwh[:, 0] >= a.import_register_kwh[:, -1]).all()
    fresh = SyntheticModel(sites, 4242, REPO / "config").chunk(T0 + 96, 96, [])
    assert not np.array_equal(fresh.import_register_kwh, b.import_register_kwh)  # not a reset
