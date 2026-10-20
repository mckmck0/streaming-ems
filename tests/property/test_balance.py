"""AC-001-19: unmanaged sites close the balance; EMS-managed sites emit no battery or grid."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
from hypothesis import given
from hypothesis import strategies as st
from hypothesis.extra.numpy import arrays

from ems.contracts.envelope import AssetType, Direction
from ems.ingest.catalog.build import build_catalog, load_overrides
from ems.ingest.generator import GeneratorConfig, SyntheticModel, emissions
from ems.ingest.synthetic.balance import grid_flows

REPO = Path(__file__).resolve().parents[2]
NOW = datetime(2026, 10, 20, tzinfo=UTC)
energy = arrays(np.float64, 24, elements=st.floats(0, 20, allow_nan=False))


@given(load=energy, gen=energy)
def test_grid_flows_close_the_balance(load: np.ndarray, gen: np.ndarray) -> None:
    imp, exp = grid_flows(load, gen)
    assert (imp >= 0).all() and (exp >= 0).all()
    assert np.allclose(imp - exp, load - gen, atol=1e-9)
    assert ((imp == 0) | (exp == 0)).all()


def _events(sites: list) -> list:
    model = SyntheticModel(sites, 4242, REPO / "config")
    cfg = GeneratorConfig(streams={"weather_forecast": {"mode": "off"}, "price": {"mode": "off"}})
    out = []
    for _at, _stream, build in emissions(model, cfg, NOW, NOW + timedelta(hours=12)):
        out.append(build(NOW))
    return out


def test_ac_001_19_managed_site_has_no_battery_or_grid_measurements() -> None:
    overrides = load_overrides(REPO / "config" / "sites.yaml", 4242)
    site = build_catalog(1, catalog_seed=7, global_seed=4242, overrides=overrides)[0]
    assert site.ems_managed
    for ev in _events([site]):
        types = {m.asset_type for m in ev.payload.measurements}
        assert AssetType.BESS not in types and AssetType.GRID_CONNECTION not in types


def test_ac_001_19_unmanaged_site_balance_closes_per_interval() -> None:
    unmanaged = [
        s for s in build_catalog(30, catalog_seed=7, global_seed=4242) if not s.ems_managed
    ]
    assert unmanaged
    events = _events(sorted(unmanaged, key=lambda s: s.bess is None)[:6])
    by_key: dict[tuple[str, datetime], dict[str, float]] = {}
    for ev in events:
        acc = by_key.setdefault(
            (ev.location_id, ev.interval_start), {"gen": 0.0, "load": 0.0, "imp": 0.0, "exp": 0.0}
        )
        for m in ev.payload.measurements:
            assert m.value >= 0
            if m.quantity.value != "active_energy":
                continue  # SoC readings are states, not flows
            if m.direction in (Direction.GENERATION, Direction.DISCHARGE):
                acc["gen"] += m.value
            elif m.direction in (Direction.CONSUMPTION, Direction.CHARGE):
                acc["load"] += m.value
            elif m.direction is Direction.IMPORT:
                acc["imp"] += m.value
            elif m.direction is Direction.EXPORT:
                acc["exp"] += m.value
    for acc in by_key.values():
        assert abs((acc["imp"] - acc["exp"]) - (acc["load"] - acc["gen"])) < 1e-6
