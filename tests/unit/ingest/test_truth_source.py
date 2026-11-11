"""AC-007-01, -07 … -10: one truth source per site; hybrid live-site truth is the latest live step
minus the synthetic forecast error, with a synthetic fallback (spec 007, plan §1).
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import respx
from numpy.typing import NDArray

from ems.contracts.envelope import AssetType, Stream, WeatherForecastEvent
from ems.contracts.examples import forecast_event
from ems.contracts.time import QUARTER, from_quarter_index, quarter_index
from ems.ingest import rng
from ems.ingest.generator import FORECAST_STEPS, SyntheticModel, load_sites
from ems.ingest.live.openmeteo import build_event, model_for, parse_meta
from ems.ingest.localtime import local_hour
from ems.ingest.sources.live_runner import LiveRunner
from ems.ingest.sources.router import route
from ems.ingest.synthetic.heat_pump import heat_pump_kwh
from ems.ingest.synthetic.pv import pv_energy_kwh
from ems.ingest.synthetic.solar import decompose
from ems.ingest.synthetic.truth import LiveForecastTruth, SyntheticTruth, TruthSource
from ems.ingest.synthetic.weather import (
    KT_MAX,
    KT_MIN,
    SiteArrays,
    TimeAxis,
    Truth,
    forecast_errors,
    from_x,
    generate_truth,
    lead_hours,
    load_params,
    to_x,
)
from tests.unit.ingest.test_recording_hybrid import (
    FIX,
    META,
    NOW,
    Collect,
    live_cfg,
    mock_sources,
    settings,
)

F64 = NDArray[np.float64]
REPO = Path(__file__).resolve().parents[3]
CONFIG = REPO / "config"
PARAMS = load_params(CONFIG / "calibration" / "weather.yaml")
SEED = 4242  # the seed of config/generator.live.yaml
LIVE = "PL-LOC-000001"
NOON = datetime(2026, 10, 21, 10, 0, tzinfo=UTC)  # daylight, inside the fixture run

# Fixed coordinates per location (a site's values must not depend on its row).
COORDS = {
    "PL-LOC-000001": (52.41, 16.93),
    "PL-LOC-000002": (50.06, 19.94),
    "PL-LOC-000003": (54.35, 18.65),
}
IDS = list(COORDS)
LOCATION_BY_KEY = {int(rng.location_key(i)): i for i in IDS}
TRUTH_FIELDS = (
    "kt",
    "ghi",
    "dni",
    "dhi",
    "temp_c",
    "wind10",
    "wind100",
    "wind120",
    "wind_dir",
    "cloud_pct",
)
HEIGHTS = ("10", "100", "120")


def site_arrays(ids: list[str]) -> SiteArrays:
    return SiteArrays(
        keys=np.array([rng.location_key(i) for i in ids], dtype=np.uint64),
        lat=np.array([COORDS[i][0] for i in ids]),
        lon=np.array([COORDS[i][1] for i in ids]),
        elevation_m=np.full(len(ids), 100.0),
        shear_alpha=np.full(len(ids), 0.22),
        wind_scale_ms=np.full(len(ids), 3.4),
    )


def live_truth(
    forecasts: dict[str, WeatherForecastEvent], fallback: TruthSource | None = None
) -> LiveForecastTruth:
    return LiveForecastTruth(
        location_by_key=LOCATION_BY_KEY,
        forecasts=forecasts,
        fallback=fallback or SyntheticTruth(SEED, PARAMS),
        seed=SEED,
        params=PARAMS,
    )


def live_event(
    location_id: str, issue_t: int, weather: Truth, row: int, steps: int = FORECAST_STEPS
) -> WeatherForecastEvent:
    """A stub Open-Meteo run issued at ``issue_t``: GHI = 0.55 x the clear-sky GHI of ``weather``
    (a mid-range sky), temperature and wind from ``weather`` (another seed, so they differ from the
    fallback truth).
    """
    i0 = issue_t - int(weather.axis.t[0])
    sl = slice(i0, i0 + steps)
    ghi = 0.55 * weather.geometry.cs_ghi[row : row + 1, sl]
    doy = weather.axis.calendar(weather.axis.unix_mid[sl])[0]
    dni, dhi = decompose(ghi, weather.geometry.zenith[row : row + 1, sl], doy)
    values = [
        {
            "ghi_wm2": float(ghi[0, k]),
            "dni_wm2": float(dni[0, k]),
            "dhi_wm2": float(dhi[0, k]),
            "temp_air_c": float(weather.temp_c[row, i0 + k]),
            "cloud_cover_pct": 50,
            "wind_speed_ms": {
                "10": float(weather.wind10[row, i0 + k]),
                "100": float(weather.wind100[row, i0 + k]),
                "120": float(weather.wind120[row, i0 + k]),
            },
            "wind_dir_deg": 250,
        }
        for k in range(steps)
    ]
    issued_at = from_quarter_index(issue_t)
    return forecast_event(
        location_id=location_id,
        issued_at=issued_at,
        steps=values,
        ingested_at=issued_at + timedelta(minutes=90),
        forecast_model="icon_d2",
        source_system="open-meteo",
        synthetic=False,
    )


def step_values(event: WeatherForecastEvent, t: NDArray[np.int64]) -> dict[str, F64]:
    """Step values at quarter indices ``t`` (every ``t`` must have a step)."""
    by_t = {quarter_index(s.valid_start): s for s in event.payload.steps}
    steps = [by_t[int(q)] for q in t]
    out = {
        "ghi": np.array([s.ghi_wm2 for s in steps]),
        "temp": np.array([s.temp_air_c for s in steps]),
    }
    for h in HEIGHTS:
        out[h] = np.array([s.wind_speed_ms[h] for s in steps])
    return out


def assert_same(got: Truth, ref: Truth, rows: Any = slice(None), cols: Any = slice(None)) -> None:
    for name in TRUTH_FIELDS:
        a, b = getattr(got, name)[rows, cols], getattr(ref, name)[rows, cols]
        assert np.array_equal(a, b), name


# ── AC-007-01: the seam ───────────────────────────────────────────────────────────────────────────


@dataclass
class ConstantWeather:
    """Stub truth source: the real solar geometry with a constant sky, temperature and wind."""

    kt: float = 0.6
    temp_c: float = -3.0
    wind_ms: float = 2.5
    calls: list[tuple[int, int, int]] = field(default_factory=list)

    def __call__(self, sites: SiteArrays, axis: TimeAxis) -> Truth:
        self.calls.append((int(sites.keys.size), int(axis.t[0]), axis.n))
        base = generate_truth(sites, axis, 0, PARAMS)  # geometry only
        ghi = self.kt * base.geometry.cs_ghi
        dni, dhi = decompose(ghi, base.geometry.zenith, axis.calendar(axis.unix_mid)[0])

        def const(value: float) -> F64:
            return np.full(base.kt.shape, value)

        return dataclasses.replace(
            base,
            kt=const(self.kt),
            ghi=ghi,
            dni=dni,
            dhi=dhi,
            temp_c=const(self.temp_c),
            wind10=const(self.wind_ms),
            wind100=const(self.wind_ms),
            wind120=const(self.wind_ms),
            wind_dir=const(250.0),
            cloud_pct=const(50.0),
        )


def test_ac_007_01_asset_models_read_weather_only_from_the_truth_source() -> None:
    """AC-007-01: with a stub source, PV and heat pump equal the models applied to the stub's
    arrays; base load reads no weather; the per-call override and site slicing give the same values.
    """
    sites = load_sites(CONFIG, live_cfg())  # PL-LOC-000001 and -000003 have PV and a heat pump
    t0, n = quarter_index(datetime(2026, 1, 15, tzinfo=UTC)), 96
    stub = ConstantWeather()
    model = SyntheticModel(sites, SEED, CONFIG, truth_source=stub)
    chunk = model.chunk(t0, n, [])
    assert stub.calls and all(c[1] == t0 for c in stub.calls)

    axis = chunk.truth_axis
    truth = stub(model.arrays, axis)
    pv = pv_energy_kwh(truth, model.pv)[:, :n]
    hp = heat_pump_kwh(
        truth.temp_c[:, :n],
        local_hour(axis.unix_mid[:n]),
        model.hp_loss,
        model.hp_dhw,
        model.hp_base,
        model.hp_eta,
    )
    assert np.array_equal(chunk.pv_kwh, pv) and np.array_equal(chunk.hp_kwh, hp)
    assert chunk.pv_kwh.max() > 0 and chunk.hp_kwh.max() > 0

    default = SyntheticModel(sites, SEED, CONFIG).chunk(t0, n, [])
    assert not np.array_equal(
        default.pv_kwh, chunk.pv_kwh
    )  # the stub was used, not synthetic truth
    assert np.array_equal(default.load_kwh, chunk.load_kwh)  # base load reads no weather

    over = SyntheticModel(sites, SEED, CONFIG).chunk(t0, n, [], truth_source=ConstantWeather())
    assert np.array_equal(over.pv_kwh, pv) and np.array_equal(over.hp_kwh, hp)

    per_site = ConstantWeather()
    sliced = SyntheticModel(sites, SEED, CONFIG, site_chunk=1, truth_source=per_site).chunk(
        t0, n, []
    )
    assert [c[0] for c in per_site.calls] == [1] * len(sites)  # one call per site slice
    assert np.array_equal(sliced.pv_kwh, pv) and np.array_equal(sliced.hp_kwh, hp)


# ── AC-007-07: live forecasts stay as fetched ─────────────────────────────────────────────────────


@respx.mock
def test_ac_007_07_live_forecast_event_is_published_as_fetched(tmp_path: Path) -> None:
    """AC-007-07: the published Open-Meteo event equals ``build_event`` on the fetched payload, and
    computing the live site's meters leaves it unchanged.
    """
    mock_sources()
    cfg = live_cfg()
    r = route(cfg, load_sites(CONFIG, cfg))
    sink = Collect()
    runner = LiveRunner(
        settings(tmp_path), cfg, sink, r.live_sites, price_live=False, clock=lambda: NOW
    )
    asyncio.run(runner.once())
    published = [e for e in sink.events if e.stream is Stream.WEATHER_FORECAST]
    assert len(published) == 1
    site = r.live_sites[0]
    payload = json.loads((FIX / "openmeteo" / "forecast_PL-LOC-000001.json").read_text())
    expected = build_event(
        site,
        model_for(site),
        parse_meta(META).issued_at,
        payload,
        settings(tmp_path).mappings_dir,
        NOW,
    )
    assert expected is not None
    event = published[0]
    assert event.model_dump(mode="json") == expected.model_dump(mode="json")
    assert event.event_id == expected.event_id and event.issued_at == expected.issued_at
    assert event.source_system == "open-meteo" and not event.synthetic

    before = event.model_dump(mode="json")
    runner.live_meters(NOON)
    assert event.model_dump(mode="json") == before
    assert runner.state.forecasts[LIVE].model_dump(mode="json") == before
    assert [e for e in sink.events if e.stream is Stream.WEATHER_FORECAST] == published


# ── AC-007-08: truth = live step - err(v, r*) ─────────────────────────────────────────────────────


def test_ac_007_08_truth_is_the_step_minus_the_forecast_error() -> None:
    """AC-007-08: kt in the bounded logit space, additive temperature and the mirrored log-normal
    wind factor, with err from ``forecast_errors`` (same seed, keys and variables as the synthetic
    forecasts, column k = interval quarter - issue quarter); DNI and DHI re-split from truth GHI.
    """
    ids = ["PL-LOC-000001", "PL-LOC-000003"]
    s = site_arrays(ids)
    issue = quarter_index(datetime(2026, 6, 10, 3, tzinfo=UTC))
    weather = generate_truth(s, TimeAxis.from_range(issue, FORECAST_STEPS), SEED + 1, PARAMS)
    events = {lid: live_event(lid, issue, weather, row) for row, lid in enumerate(ids)}
    axis = TimeAxis.from_range(issue + 30, 60)  # k = 30 … 89, across dusk and night
    truth = live_truth(events)(s, axis)

    fp = PARAMS["forecast"]
    lead = lead_hours(FORECAST_STEPS)
    k = np.arange(30, 90)

    def err(var: str, base: str, per_h: str) -> tuple[F64, F64]:
        sd = float(fp[base]) + float(fp[per_h]) * lead
        e = forecast_errors(
            SEED,
            s.keys,
            issue,
            var,
            sd,
            float(fp["rho"]),
            float(fp["common_tau_h"]),
            float(fp["step_correlation"]),
        )
        return e[:, k], sd[k]

    e_kt, _ = err("fc.kt", "kt_sd_base", "kt_sd_per_h")
    e_temp, _ = err("fc.temp", "temp_sd_base_c", "temp_sd_per_h_c")
    e_wind, sd_wind = err("fc.wind", "wind_ln_sd_base", "wind_ln_sd_per_h")
    step = [step_values(events[lid], axis.t) for lid in ids]
    cs = truth.geometry.cs_ghi
    day = cs > 0
    assert day.any() and (~day).any()

    step_ghi = np.stack([v["ghi"] for v in step])
    kt = from_x(to_x(np.where(day, step_ghi / np.where(day, cs, 1.0), 0.5)) - e_kt)
    np.testing.assert_allclose(truth.kt[day], kt[day], rtol=1e-12)
    np.testing.assert_allclose(truth.ghi, np.where(day, cs * kt, 0.0), rtol=1e-12, atol=1e-12)
    dni, dhi = decompose(truth.ghi, truth.geometry.zenith, axis.calendar(axis.unix_mid)[0])
    np.testing.assert_allclose(truth.dni, dni, rtol=1e-12, atol=1e-12)
    np.testing.assert_allclose(truth.dhi, dhi, rtol=1e-12, atol=1e-12)
    step_temp = np.stack([v["temp"] for v in step])
    np.testing.assert_allclose(truth.temp_c, step_temp - e_temp, rtol=1e-12, atol=1e-12)
    factor = np.exp(-e_wind - sd_wind[None, :] ** 2 / 2)
    for h in HEIGHTS:
        expected = np.stack([v[h] for v in step]) * factor
        np.testing.assert_allclose(getattr(truth, f"wind{h}"), expected, rtol=1e-12)


def test_ac_007_08_hybrid_deviation_statistics_over_30_days() -> None:
    """AC-007-08: 30 days of 3-hourly stub runs (GHI = 0.55 x clear-sky), each the latest run for
    its next 12 intervals. Truth GHI differs from the step GHI in every daylight interval, truth kt
    stays strictly inside (0.03, 1.1), the wind factor is the same at 10, 100 and 120 m, and the sd
    of to_x(step kt) - to_x(truth kt) is within ±20 % of kt_sd_base + kt_sd_per_h * L (logit units).
    """
    s = site_arrays([LIVE])
    start = quarter_index(datetime(2026, 5, 1, tzinfo=UTC))
    runs = 30 * 8
    span = (runs - 1) * 12 + FORECAST_STEPS
    weather = generate_truth(s, TimeAxis.from_range(start, span), SEED + 1, PARAMS)
    fp = PARAMS["forecast"]
    lead = lead_hours(12)  # k = 0 … 11
    devs: list[F64] = []
    sds: list[F64] = []
    daylight = 0
    for r in range(runs):
        issue = start + 12 * r
        event = live_event(LIVE, issue, weather, 0)
        axis = TimeAxis.from_range(issue, 12)
        truth = live_truth({LIVE: event})(s, axis)
        step = step_values(event, axis.t)
        cs = truth.geometry.cs_ghi[0]
        day = cs > 0
        daylight += int(day.sum())
        assert (truth.ghi[0][day] != step["ghi"][day]).all(), issue
        assert ((truth.kt[0][day] > KT_MIN) & (truth.kt[0][day] < KT_MAX)).all(), issue
        assert (truth.ghi[0][~day] == 0).all(), issue
        f10 = truth.wind10[0] / step["10"]
        np.testing.assert_allclose(truth.wind100[0] / step["100"], f10, rtol=1e-12)
        np.testing.assert_allclose(truth.wind120[0] / step["120"], f10, rtol=1e-12)
        devs.append(to_x(step["ghi"][day] / cs[day]) - to_x(truth.kt[0][day]))
        sds.append(float(fp["kt_sd_base"]) + float(fp["kt_sd_per_h"]) * lead[day])
    assert daylight > 30 * 40  # at least ~10 h of daylight a day in May
    dev = np.concatenate(devs)
    sigma = float(np.sqrt(np.mean(np.concatenate(sds) ** 2)))
    assert 0.8 * sigma <= float(dev.std()) <= 1.2 * sigma, (float(dev.std()), sigma)


# ── AC-007-09: no live step -> synthetic truth ────────────────────────────────────────────────────


def test_ac_007_09_without_a_live_step_the_fallback_truth_is_used() -> None:
    """AC-007-09: no event, an event truncated before v, and v past the horizon all give the
    fallback's arrays; a site without an event keeps the fallback in a mixed batch.
    """
    ids = ["PL-LOC-000001", "PL-LOC-000003"]
    s = site_arrays(ids)
    issue = quarter_index(datetime(2026, 6, 10, 3, tzinfo=UTC))
    weather = generate_truth(s, TimeAxis.from_range(issue, FORECAST_STEPS), SEED + 1, PARAMS)
    full = live_event(ids[0], issue, weather, 0)
    short = live_event(ids[0], issue, weather, 0, steps=40)
    fallback = SyntheticTruth(SEED, PARAMS)

    axis = TimeAxis.from_range(issue, 96)
    stub = ConstantWeather()
    assert_same(live_truth({}, fallback=stub)(s, axis), stub(s, axis))  # no event at all
    assert_same(live_truth({})(s, axis), fallback(s, axis))

    got, ref = live_truth({ids[0]: short})(s, axis), fallback(s, axis)
    assert_same(got, ref, cols=slice(40, None))  # truncated: the fallback past the last step
    assert_same(got, ref, rows=slice(1, 2))  # the site without an event
    day = ref.geometry.cs_ghi[0, :40] > 0
    assert day.any() and (got.ghi[0, :40][day] != ref.ghi[0, :40][day]).all()
    assert (got.temp_c[0, :40] != ref.temp_c[0, :40]).all()  # the steps it has are used

    past = TimeAxis.from_range(issue + FORECAST_STEPS - 12, 48)  # straddles the 48 h horizon
    got, ref = live_truth({ids[0]: full})(s, past), fallback(s, past)
    assert_same(got, ref, cols=slice(12, None))
    assert (got.temp_c[0, :12] != ref.temp_c[0, :12]).all()


def _meters(runner: LiveRunner, sink: Collect, start: datetime) -> dict[AssetType, float]:
    before = len(sink.events)
    runner.live_meters(start)
    new = sink.events[before:]
    assert new and all(e.stream in (Stream.PRODUCTION, Stream.CONSUMPTION) for e in new)
    assert {e.location_id for e in new} == {LIVE}
    return {m.asset_type: m.value for e in new for m in e.payload.measurements}


def _synthetic(model: SyntheticModel, start: datetime) -> dict[AssetType, float]:
    chunk = model.chunk(quarter_index(start), 1, [])
    return {
        AssetType.PV: float(chunk.pv_kwh[0, 0]),
        AssetType.LOAD: float(chunk.load_kwh[0, 0]),
        AssetType.HEAT_PUMP: float(chunk.hp_kwh[0, 0]),
    }


@respx.mock
def test_ac_007_09_live_meters_fall_back_to_synthetic_truth(tmp_path: Path) -> None:
    """AC-007-09: before the first run, after the fixture run's last step (it is truncated) and past
    its horizon, the live site's meters are still emitted and equal the synthetic model's.
    """
    mock_sources()
    cfg = live_cfg()
    r = route(cfg, load_sites(CONFIG, cfg))
    synthetic = SyntheticModel(list(r.live_sites), cfg.seed, CONFIG)
    sink = Collect()
    runner = LiveRunner(
        settings(tmp_path), cfg, sink, r.live_sites, price_live=False, clock=lambda: NOW
    )
    assert _meters(runner, sink, NOON) == _synthetic(synthetic, NOON)  # no run yet

    asyncio.run(runner.once())
    event = runner.state.forecasts[LIVE]
    assert len(event.payload.steps) < FORECAST_STEPS  # the fixture run ends early
    truncated = event.payload.steps[-1].valid_start + QUARTER
    past = event.issued_at + FORECAST_STEPS * QUARTER + timedelta(hours=16)  # next day, daylight
    assert truncated < event.issued_at + FORECAST_STEPS * QUARTER
    for start in (truncated, past):
        assert _meters(runner, sink, start) == _synthetic(synthetic, start), start
    assert _meters(runner, sink, NOON) != _synthetic(synthetic, NOON)  # a live step deviates


# ── AC-007-10: reproducible ───────────────────────────────────────────────────────────────────────


@respx.mock
def test_ac_007_10_same_event_and_seed_give_identical_meters(tmp_path: Path) -> None:
    """AC-007-10: ``live_meters`` for the same interval, event and seed gives identical values,
    twice and from a fresh runner holding the same event.
    """
    mock_sources()
    cfg = live_cfg()
    r = route(cfg, load_sites(CONFIG, cfg))
    sink = Collect()
    runner = LiveRunner(
        settings(tmp_path), cfg, sink, r.live_sites, price_live=False, clock=lambda: NOW
    )
    asyncio.run(runner.once())
    first = _meters(runner, sink, NOON)
    assert _meters(runner, sink, NOON) == first

    other_sink = Collect()
    other = LiveRunner(
        settings(tmp_path), cfg, other_sink, r.live_sites, price_live=False, clock=lambda: NOW
    )
    other.state.forecasts.update(runner.state.forecasts)
    assert _meters(other, other_sink, NOON) == first


def test_ac_007_10_sites_on_different_runs_together_equal_each_alone() -> None:
    """AC-007-10: live sites on different ``issued_at`` (two on one run, one on the next) give the
    same truth together as each alone, and the same values when evaluated again.
    """
    s = site_arrays(IDS)
    t_a = quarter_index(datetime(2026, 6, 10, 0, tzinfo=UTC))
    t_b = t_a + 12
    weather = generate_truth(s, TimeAxis.from_range(t_a, FORECAST_STEPS + 12), SEED + 1, PARAMS)
    issues = {IDS[0]: t_a, IDS[1]: t_b, IDS[2]: t_a}
    events = {lid: live_event(lid, issues[lid], weather, row) for row, lid in enumerate(IDS)}
    axis = TimeAxis.from_range(t_b + 20, 48)
    together = live_truth(events)(s, axis)
    assert_same(live_truth(events)(s, axis), together)
    for row, lid in enumerate(IDS):
        one = site_arrays([lid])
        alone = live_truth({lid: events[lid]})(one, axis)
        for name in TRUTH_FIELDS:
            assert np.array_equal(getattr(together, name)[row], getattr(alone, name)[0]), (
                lid,
                name,
            )
