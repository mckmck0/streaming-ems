"""Fit synthetic-model parameters from history into *candidate* calibration files.

Candidates land in ``config/calibration/candidates/`` with their data window and fit statistics;
adopting them (copying values into ``prices.yaml`` / ``weather.yaml``) is a reviewed change, never
automatic. Quarter-hour price dynamics are fitted only on data from 2025-10-01 (earlier RCE is
hourly in effect).
"""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import numpy as np
from ruamel.yaml import YAML

from ems.ingest.mapping.engine import apply, builtin

QUARTER_HOUR_SINCE = date(2025, 10, 1)


def fit_prices(pse_payloads: list[dict[str, Any]], mappings_dir: Path) -> dict[str, Any]:
    """Monthly base, negative share and residual noise from PSE RCE responses."""
    mapping = builtin("pse.rce.v2", mappings_dir)
    rows = [
        r
        for p in pse_payloads
        for r in apply(mapping, p)
        if r["business_date"] >= QUARTER_HOUR_SINCE
    ]
    if not rows:
        raise ValueError("no quarter-hour RCE data on or after 2025-10-01")
    prices = np.array([r["price"] for r in rows], dtype=float)
    by_month: dict[int, list[float]] = defaultdict(list)
    for r in rows:
        by_month[r["business_date"].month].append(r["price"])
    days = sorted({r["business_date"] for r in rows})
    daily_mean = {d: np.mean([r["price"] for r in rows if r["business_date"] == d]) for d in days}
    residual = np.array([r["price"] - daily_mean[r["business_date"]] for r in rows])
    return {
        "kind": "prices",
        "data_window": {
            "start": days[0].isoformat(),
            "end": days[-1].isoformat(),
            "days": len(days),
        },
        "samples": int(prices.size),
        "mean_pln_per_mwh": round(float(prices.mean()), 2),
        "negative_share": round(float((prices < 0).mean()), 4),
        "min": float(prices.min()),
        "max": float(prices.max()),
        "base_by_month": {m: round(float(np.mean(v)), 1) for m, v in sorted(by_month.items())},
        "intraday_sd": round(float(residual.std()), 2),
    }


def fit_temperature(archive_payload: dict[str, Any]) -> dict[str, Any]:
    """Annual mean, amplitude and coldest day of an hourly temperature series
    (least-squares cosine).
    """
    hourly = archive_payload["hourly"]
    t = np.array(hourly["time"], dtype=float)
    temp = np.array([np.nan if v is None else v for v in hourly["temperature_2m"]], dtype=float)
    ok = np.isfinite(temp)
    t, temp = t[ok], temp[ok]
    doy = np.array([datetime.fromtimestamp(x, UTC).timetuple().tm_yday for x in t], dtype=float)
    w = 2 * np.pi * doy / 365.25
    design = np.column_stack([np.ones_like(w), np.cos(w), np.sin(w)])
    (mean, a, b), *_ = np.linalg.lstsq(design, temp, rcond=None)
    amplitude = float(np.hypot(a, b))
    warmest = float(np.degrees(np.arctan2(b, a)) % 360 / 360 * 365.25)
    coldest = (warmest + 365.25 / 2) % 365.25
    return {
        "kind": "temperature",
        "data_window": {
            "start": datetime.fromtimestamp(t[0], UTC).date().isoformat(),
            "end": datetime.fromtimestamp(t[-1], UTC).date().isoformat(),
        },
        "samples": int(temp.size),
        "annual_mean_c": round(float(mean), 2),
        "annual_amplitude_c": round(amplitude, 2),
        "coldest_day_of_year": round(coldest),
        "latitude": archive_payload.get("latitude"),
        "longitude": archive_payload.get("longitude"),
    }


# ── base load (spec 007) ──────────────────────────────────────────────────────────────────────────
# Input: per-household quarter-hour base-load energy (heat pump and EV excluded), as a CSV with
# LOAD_COLUMNS or the WPuQ 15-minute HDF5 files (Schlemminger et al. 2022, CC BY 4.0).

LOAD_COLUMNS = ("timestamp_utc", "household_id", "kwh")
WPUQ_EXCLUDED_2019 = (
    6,
    13,
    17,
    24,
    25,
    31,
    37,
    40,
)  # authors: at least a full month missing in 2019
WPUQ_DATASET = {
    "name": "WPuQ: electrical single-family house and heat pump load profiles in Germany",
    "citation": "Schlemminger, Ohrdes, Schneider, Knoop (2022), Sci. Data 9:56",
    "zenodo": "https://zenodo.org/records/5642902 (v2.0)",
    "licence": "CC BY 4.0",
    "attribution": "WPuQ dataset, Schlemminger et al. 2022, Sci. Data 9:56, CC BY 4.0",
}
MIN_AVAILABILITY = 0.9
PEAK_KWH = 0.25  # ~1 kW above the median: oven, hob, washing-machine heater, dryer
ACF_LAGS_Q = (1, 2, 4, 8)  # 15 min, 30 min, 1 h, 2 h


def read_load_csv(path: Path) -> Any:
    """Per-household quarter-hour energy from a CSV with header
    ``timestamp_utc,household_id,kwh`` (interval start, UTC).
    """
    import pandas as pd

    frame = pd.read_csv(path, dtype={"household_id": str})
    missing = set(LOAD_COLUMNS) - set(frame.columns)
    if missing:
        raise ValueError(f"{path}: missing columns {sorted(missing)}; expected {LOAD_COLUMNS}")
    frame["timestamp_utc"] = pd.to_datetime(frame["timestamp_utc"], utc=True).dt.as_unit("ns")
    frame["kwh"] = frame["kwh"].astype(float)
    return frame[list(LOAD_COLUMNS)]


def read_wpuq(path: Path, exclude: Any = WPUQ_EXCLUDED_2019) -> Any:
    """Household meters of the WPuQ 15-minute HDF5 file (``NO_PV/SFH<n>/HOUSEHOLD/table``).

    ``index`` is Unix seconds (UTC); ``P_TOT`` is the interval-mean active power in W, so the
    energy per quarter-hour is ``P_TOT * 0.25 / 1000`` kWh. Houses with PV are skipped (their meter
    is net of feed-in). NaN marks the authors' gaps longer than one day and is kept.
    """
    import re

    import h5py
    import pandas as pd

    parts = []
    excluded = {int(x) for x in exclude}
    with h5py.File(path, "r") as f:
        group = f["NO_PV"]
        for name in sorted(group):
            m = re.fullmatch(r"SFH(\d+)", name)
            if m is None or int(m.group(1)) in excluded:
                continue
            node = group[name].get("HOUSEHOLD")
            if node is None or "table" not in node:
                continue
            table = node["table"][()]
            if "P_TOT" not in (table.dtype.names or ()):
                raise ValueError(f"{path}: {name}/HOUSEHOLD/table has no P_TOT column")
            parts.append(
                pd.DataFrame(
                    {
                        "timestamp_utc": pd.to_datetime(table["index"], unit="s", utc=True).as_unit(
                            "ns"
                        ),
                        "household_id": name,
                        "kwh": np.asarray(table["P_TOT"], dtype=float) * 0.25 / 1000.0,
                    }
                )
            )
    if not parts:
        raise ValueError(f"{path}: no NO_PV household meters found")
    return pd.concat(parts, ignore_index=True)


def _linear_runs(e: np.ndarray, min_points: int = 4) -> np.ndarray:
    """Mask of points in runs of >= ``min_points`` with a zero second difference (interpolated)."""
    d2 = np.abs(np.diff(e, 2))
    flat = np.concatenate([[False], d2 <= 1e-9 * np.maximum(1.0, np.abs(e[1:-1])), [False]])
    flat &= np.isfinite(e)
    mask = np.zeros(e.size, dtype=bool)
    i = 0
    while i < e.size:  # one pass over time
        if not flat[i]:
            i += 1
            continue
        j = i
        while j < e.size and flat[j]:
            j += 1
        if j - i + 2 >= min_points:  # interior points plus the two run ends
            mask[max(i - 1, 0) : min(j + 1, e.size)] = True
        i = j
    return mask


def _rolling_median(e: np.ndarray, window: int = 9) -> np.ndarray:
    import pandas as pd

    return np.asarray(
        pd.Series(e).rolling(window, center=True, min_periods=1).median(), dtype=float
    )


def household_stats(
    t: np.ndarray, e: np.ndarray, holidays_days: set[int]
) -> dict[str, float] | None:
    """Full-load statistics of one household on a contiguous quarter-hour grid ``t`` (NaN means
    missing).

    Measured on the whole base load (noise and appliance peaks alike), so data and model are
    compared like for like: r = log(E / (A * shape)) minus the household's own profile (local
    quarter of day x weekday/weekend, not modelled) and minus each local day's median. Returns the
    within-day autocorrelation at ACF_LAGS_Q, the within-day sd, the day-to-day sd (day medians
    around a 31-day running mean) and diagnostics. These are raw statistics: ``fit_load`` removes
    their bias by simulation.
    """
    import pandas as pd

    from ems.ingest.localtime import local_unix
    from ems.ingest.synthetic.load import local_day_index, shares

    valid = np.isfinite(e) & (e > 0)
    if valid.mean() < MIN_AVAILABILITY:
        return None
    interp = _linear_runs(e)
    day = local_day_index(t)
    valid &= ~interp & ~np.isin(day, list(holidays_days))
    shape = shares("H25", t)
    annual = float(np.nansum(np.where(valid, e, 0.0)) / shape[valid].sum())
    peaks = valid & (e - _rolling_median(np.where(valid, e, np.nan)) > PEAK_KWH)
    r = np.full(e.size, np.nan)
    r[valid] = np.log(e[valid] / (annual * shape[valid]))
    local = local_unix((t * 900).astype(float))
    qod = ((local % 86400) // 900).astype(np.int64)
    weekend = ((np.floor(local / 86400).astype(np.int64) + 3) % 7) >= 5  # 1970-01-01 was a Thursday
    cell = qod * 2 + weekend.astype(np.int64)
    sums = np.bincount(cell[valid], weights=r[valid], minlength=200)
    counts = np.bincount(cell[valid], minlength=200)
    profile = np.divide(sums, counts, out=np.zeros_like(sums), where=counts > 0)
    r[valid] -= profile[cell[valid]]
    days, inv = np.unique(day[valid], return_inverse=True)
    m_d = np.asarray(pd.Series(r[valid]).groupby(inv).median().to_numpy(), dtype=np.float64)
    within = np.full(e.size, np.nan)
    within[valid] = r[valid] - m_d[inv]
    acf = []
    for lag in ACF_LAGS_Q:  # within-day pairs
        ok = valid[lag:] & valid[:-lag] & (day[lag:] == day[:-lag])
        x, y = within[:-lag][ok], within[lag:][ok]
        acf.append(float(np.sum(x * y) / np.sqrt(np.sum(x * x) * np.sum(y * y))))
    window = np.ones(31) / 31
    trend = np.convolve(m_d, window, mode="same") / np.convolve(np.ones_like(m_d), window, "same")
    return {
        **{f"acf_{lag}": v for lag, v in zip(ACF_LAGS_Q, acf, strict=True)},
        "interval_sd": float(np.nanstd(within)),
        "day_sd": float(np.std(m_d - trend)),
        "peak_rate_per_day": float(peaks.sum()) / max(int(days.size), 1),
        "profile_deviation_sd": float(np.std(profile[counts > 0])),
        "masked_share": float(interp.sum()) / max(int(np.isfinite(e).sum()), 1),
        "annual_kwh": annual,
    }


SIM_ITERATIONS = 6
SIM_REPLICAS = 3
# The slow noise component's time constant is fixed, not fitted: with it free, the WPuQ fit runs to
# any upper bound it is given, and a longer one overstates day-to-day variability (full-load
# day-to-day sd 0.192 at 6 h, 0.165 at 3 h against the data's 0.150). Plan section 5.
TAU_SLOW_H = 3.0
TAU_FAST_BOUNDS = (0.05, 2.9)  # h, below TAU_SLOW_H
STAT_KEYS = (*(f"acf_{lag}" for lag in ACF_LAGS_Q), "interval_sd", "day_sd")


def _stats(fits: dict[str, dict[str, float]]) -> np.ndarray:
    """Medians over households of the raw full-load statistics (STAT_KEYS order)."""
    return np.array([float(np.median([f[k] for f in fits.values()])) for k in STAT_KEYS])


def _params(stats: np.ndarray) -> tuple[float, float, float, float]:
    """(tau_fast, slow_share, interval sd, daily sd) for a statistics vector, tau_slow fixed."""
    from scipy.optimize import least_squares

    from ems.ingest.synthetic.load import noise_acf

    n = len(ACF_LAGS_Q)
    lags = np.asarray(ACF_LAGS_Q, dtype=np.float64)
    fit = least_squares(
        lambda v: noise_acf(lags, v[0], TAU_SLOW_H, v[1]) - stats[:n],
        x0=[0.4, 0.4],
        bounds=([TAU_FAST_BOUNDS[0], 0.0], [TAU_FAST_BOUNDS[1], 1.0]),
    )
    sd, dsd = max(float(stats[n]), 0.01), max(float(stats[n + 1]), 0.0)
    return float(fit.x[0]), float(fit.x[1]), sd, dsd


def model_stats(
    params: Any,
    series: dict[str, tuple[np.ndarray, np.ndarray, float]],
    holiday_days: set[int],
    seed: int = 4242,
    replicas: int = SIM_REPLICAS,
) -> dict[str, dict[str, float]]:
    """Full-load statistics of model households (noise and impulses from ``params``, H25): for each
    observed household (grid, observed mask, annual energy) ``replicas`` model households with the
    same timestamps, gaps and annual energy.
    """
    from ems.ingest import rng
    from ems.ingest.synthetic.load import base_load_kwh

    out: dict[str, dict[str, float]] = {}
    for hid, (grid, observed, annual) in series.items():
        names = [f"calib-{hid}-{k}" for k in range(replicas)]
        keys = np.array([rng.location_key(x) for x in names], dtype=np.uint64)
        sim = base_load_kwh(seed, keys, ["H25"] * replicas, np.full(replicas, annual), grid, params)
        for k in range(replicas):
            e = sim[k]
            e[~observed] = np.nan  # the same gaps as the data
            stats = household_stats(grid, e, holiday_days)
            if stats is not None:
                out[f"{hid}-{k}"] = stats
    return out


def holiday_day_indices(years: list[int]) -> set[int]:
    """Local day indices of Polish and Lower Saxony (WPuQ) public holidays, dropped from the fit."""
    import holidays

    hol = holidays.Poland(years=years) | holidays.Germany(years=years, subdiv="NI")
    return {(d - date(1970, 1, 1)).days for d in hol}


def fit_load(
    frame: Any,
    dataset: dict[str, Any] | None = None,
    model_params: Any = None,
    seed: int = 4242,
) -> dict[str, Any]:
    """Fit the H25 base-load noise (tau_fast, slow share, interval sd, daily sd; tau_slow fixed at
    TAU_SLOW_H) to the full-load statistics of measured households.

    The noise is fitted jointly with the impulse model held at its configured (literature) rates:
    the statistics are measured on the whole load in the data and in the model, so the noise takes
    what the impulses do not explain. The raw statistics are biased (day-median removal, the
    household-profile removal, impulses); the bias is removed by simulation, a fixed point on the
    statistics: target <- target + (observed - model(params(target))), ``SIM_ITERATIONS`` steps.
    Impulse rates are not fitted: the peak rate is a diagnostic only (it counts the data's own
    appliance mix, plan section 4).
    """
    import dataclasses

    import pandas as pd

    from ems.ingest.synthetic.load import NoiseParams, load_load_params
    from ems.settings import load_settings

    params = model_params or load_load_params(
        load_settings().config_dir / "calibration" / "load.yaml"
    )
    ts = frame["timestamp_utc"]
    t_all = ((ts - pd.Timestamp(0, tz="UTC")) // pd.Timedelta(seconds=900)).to_numpy(np.int64)
    holiday_days = holiday_day_indices(sorted({d.year for d in ts.dt.date}))
    fits: dict[str, dict[str, float]] = {}
    series: dict[str, tuple[np.ndarray, np.ndarray, float]] = {}
    for hid, idx in frame.groupby("household_id").indices.items():
        t = t_all[idx]
        grid = np.arange(t.min(), t.max() + 1, dtype=np.int64)
        e = np.full(grid.size, np.nan)
        e[t - grid[0]] = frame["kwh"].to_numpy()[idx]
        stats = household_stats(grid, e, holiday_days)
        if stats is not None:
            fits[str(hid)] = stats
            series[str(hid)] = (grid, np.isfinite(e), stats["annual_kwh"])
    if not fits:
        raise ValueError("no household with enough data")

    def with_noise(fitted: tuple[float, float, float, float]) -> Any:
        tau_f, w, sd, dsd = fitted
        noise = NoiseParams(tau_f, sd, dsd, True, TAU_SLOW_H, w)
        return dataclasses.replace(params, profiles={**params.profiles, "H25": noise})

    observed = _stats(fits)
    target = observed.copy()
    for _ in range(SIM_ITERATIONS):
        sim = model_stats(with_noise(_params(target)), series, holiday_days, seed)
        target = target + (observed - _stats(sim))
    fitted = _params(target)
    sim = model_stats(with_noise(fitted), series, holiday_days, seed)  # for the comparison below
    tau_f, w, sd, dsd = fitted

    def median(key: str, source: dict[str, dict[str, float]] = fits) -> float:
        return round(float(np.median([f[key] for f in source.values()])), 4)

    def iqr(key: str) -> list[float]:
        v = np.array([f[key] for f in fits.values()])
        return [round(float(np.percentile(v, 25)), 4), round(float(np.percentile(v, 75)), 4)]

    result: dict[str, Any] = {
        "kind": "load",
        "data_window": {
            "start": ts.min().date().isoformat(),
            "end": ts.max().date().isoformat(),
            "days": int((ts.max() - ts.min()).days) + 1,
        },
        "households": len(fits),
        "households_skipped": int(frame["household_id"].nunique()) - len(fits),
        "tau_h": round(tau_f, 4),
        "tau_slow_h": TAU_SLOW_H,  # fixed, not fitted
        "slow_share": round(w, 4),
        "interval_sd": round(sd, 4),
        "daily_sd": round(dsd, 4),
        # full load (noise + impulses), the same estimator on the data and on the fitted model
        "full_load": {
            "acf_lags_q": list(ACF_LAGS_Q),
            "acf_data": [median(f"acf_{lag}") for lag in ACF_LAGS_Q],
            "acf_model": [median(f"acf_{lag}", sim) for lag in ACF_LAGS_Q],
            "interval_sd_data": median("interval_sd"),
            "interval_sd_model": median("interval_sd", sim),
            "day_sd_data": median("day_sd"),
            "day_sd_model": median("day_sd", sim),
        },
        "masked_share": round(float(np.mean([f["masked_share"] for f in fits.values()])), 5),
        "peak_rate_per_day": median("peak_rate_per_day"),
        "model_peak_rate_per_day": median("peak_rate_per_day", sim),
        "profile_deviation_sd": median("profile_deviation_sd"),
    }
    for key in ("interval_sd", "day_sd", "peak_rate_per_day", "profile_deviation_sd"):
        result[f"{key}_iqr"] = iqr(key)  # spread of the raw per-household statistics
    if dataset is not None:
        result["dataset"] = dataset
    return result


def write_candidate(result: dict[str, Any], out_dir: Path, source: str) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{result['kind']}.yaml"
    doc = {"generated_at": datetime.now(UTC).isoformat(), "source": source, **result}
    yaml = YAML()
    yaml.default_flow_style = False
    with path.open("w") as f:
        f.write(
            "# Candidate calibration: review, then copy values into config/calibration/*.yaml\n"
        )
        yaml.dump(json.loads(json.dumps(doc, default=str)), f)
    return path


# ── forecast error measured on a history recording (spec 008, AC-008-20) ─────────────────────────

FORECAST_BIN_H = 3.0
FORECAST_HORIZON_H = 48.0
KT_DAYLIGHT_CS = 50.0  # W/m2: clear-sky GHI from which kt is meaningful
WIND_MIN_MS = 0.5
FORECAST_CAVEATS = [
    "Truth GHI is a satellite (DWD MTG) retrieval: its own error is part of the measured kt error.",
    "Truth temperature and wind are ERA5 (25 km reanalysis, linearly interpolated, damped diurnal "
    "range): their error is part of the measured error, the wind log-ratio most of all.",
    "DMI HARMONIE 15-minute radiation is interpolated from hourly output, so its 15-minute kt "
    "error includes smoothing; compare models on hourly_kt_sd.",
    "The measured kt error includes timing (phase) error, while weather.yaml's kt_sd_base and "
    "kt_sd_per_h set only the amplitude part of the spec 007 model; compare totals, not "
    "parameters.",
    "kt_logit_sd uses only steps whose forecast and truth kt lie strictly inside (0.03, 1.1); on "
    "clear days the satellite often exceeds pvlib's Ineichen clear-sky (kt >= 1.1), so kt_sd is "
    "the more robust figure.",
    "Every sd here is an upper bound to review, not a value to adopt as it is.",
]


@dataclass(frozen=True)
class TruthSeries:
    """One site's truth on the quarter-hour grid; NaN where not observed."""

    t0: int  # quarter index of the first value
    ghi: Any  # W/m2, the mean over [t, t + 1)
    cs_ghi: Any  # clear-sky GHI of the same quarters
    temp_c: Any  # instant at the quarter's end
    wind10: Any  # m/s, instant at the quarter's end


def _sd(x: Any) -> float | None:
    return float(np.std(x, ddof=1)) if len(x) > 1 else None


def _corr(a: Any, b: Any) -> float | None:
    if len(a) < 3:
        return None
    sa, sb = np.std(a), np.std(b)
    if sa == 0 or sb == 0:
        return None
    return float(np.corrcoef(a, b)[0, 1])


def _linear_fit(
    lead: Sequence[Any], sd: Sequence[Any], n: Sequence[Any]
) -> dict[str, float | None]:
    pts = [(x, y, w) for x, y, w in zip(lead, sd, n, strict=True) if y is not None and w > 1]
    if len(pts) < 2:
        return {"base": None, "per_h": None}
    x, y, w = (np.array(v, dtype=np.float64) for v in zip(*pts, strict=True))
    design = np.stack([np.ones_like(x), x], axis=1) * np.sqrt(w)[:, None]
    coef, *_ = np.linalg.lstsq(design, y * np.sqrt(w), rcond=None)
    return {"base": round(float(coef[0]), 4), "per_h": round(float(coef[1]), 6)}


def fit_forecast_error(
    forecasts: Sequence[Any],
    truth: Mapping[str, TruthSeries],
    weather_params: Mapping[str, Any],
) -> dict[str, Any]:
    """Forecast-vs-truth error statistics by forecast model and 3-hour lead bin (spec 008, US5).

    A step pairs with the truth at the quarter of its ``valid_start``; lead = ``valid_end`` -
    ``issued_at``. kt uses daylight steps (clear-sky GHI >= 50 W/m2) with kt clipped to [0.03, 1.1];
    the logit-space error (spec 007) only where both kt lie strictly inside the bounds;
    temperature is forecast - truth; wind is log(forecast / truth) at 10 m where truth >= 0.5 m/s.
    ``rho2_*`` is the correlation of a run's error with the error of the run issued 3 h earlier at
    the same site and valid time (by the newer run's lead).
    """
    from ems.contracts.time import quarter_index
    from ems.ingest.synthetic.weather import to_x

    n_bins = int(FORECAST_HORIZON_H / FORECAST_BIN_H)
    run_gap_q = int(FORECAST_BIN_H * 4)
    # per model: list of (bin, lead, site, issue_q, valid_q, e_kt, e_ktx, e_temp, e_wind)
    rows: dict[str, list[tuple[Any, ...]]] = defaultdict(list)
    for event in forecasts:
        series = truth.get(event.location_id)
        if series is None:
            continue
        issue_q = quarter_index(event.issued_at)
        model = event.payload.forecast_model
        n = len(series.ghi)
        for step in event.payload.steps:
            lead = (step.valid_end - event.issued_at).total_seconds() / 3600.0
            b = int(np.ceil(lead / FORECAST_BIN_H)) - 1
            if not 0 <= b < n_bins:
                continue
            i = quarter_index(step.valid_start) - series.t0
            if not 0 <= i < n:
                continue
            cs, g = float(series.cs_ghi[i]), float(series.ghi[i])
            e_kt = e_ktx = e_t = e_w = np.nan
            if cs >= KT_DAYLIGHT_CS and not np.isnan(g):
                raw_f, raw_t = step.ghi_wm2 / cs, g / cs
                kf, kt = min(max(raw_f, 0.03), 1.1), min(max(raw_t, 0.03), 1.1)
                e_kt = kf - kt
                if 0.03 < raw_f < 1.1 and 0.03 < raw_t < 1.1:  # the logit explodes at the bounds
                    e_ktx = float(to_x(np.array([kf]))[0] - to_x(np.array([kt]))[0])
            tt = float(series.temp_c[i])
            if not np.isnan(tt):
                e_t = step.temp_air_c - tt
            tw, fw = float(series.wind10[i]), step.wind_speed_ms.get("10", np.nan)
            if not np.isnan(tw) and tw >= WIND_MIN_MS and fw > 0:
                e_w = float(np.log(fw / tw))
            key_q = quarter_index(step.valid_start)
            rows[model].append((b, lead, event.location_id, issue_q, key_q, e_kt, e_ktx, e_t, e_w))

    models: dict[str, Any] = {}
    for model, items in sorted(rows.items()):
        arr = np.array([(*r[5:], r[0], r[1]) for r in items], dtype=np.float64)
        e_kt, e_ktx, e_t, e_w, bins, leads = (arr[:, k] for k in range(6))
        index = {(r[2], r[3], r[4]): k for k, r in enumerate(items)}
        older = np.array(
            [index.get((r[2], r[3] - run_gap_q, r[4]), -1) for r in items], dtype=np.int64
        )
        # hourly kt error: four complete daylight quarters of one clock hour in one run
        hourly: dict[tuple[str, int, int], list[int]] = defaultdict(list)
        for k, r in enumerate(items):
            hourly[(r[2], r[3], r[4] // 4)].append(k)
        hour_bin, hour_err = [], []
        for ks in hourly.values():
            if len(ks) == 4 and not np.isnan(e_kt[ks]).any():
                hour_bin.append(int(np.ceil(leads[ks].mean() / FORECAST_BIN_H)) - 1)
                hour_err.append(float(e_kt[ks].mean()))
        hour_bin_a, hour_err_a = np.array(hour_bin), np.array(hour_err)
        out_bins = []
        for b in range(n_bins):
            sel = bins == b
            lo, hi = b * FORECAST_BIN_H, (b + 1) * FORECAST_BIN_H

            def stat(
                err: Any, sel: Any = sel, older: Any = older
            ) -> tuple[int, float | None, float | None]:
                ok = sel & ~np.isnan(err)
                pair = ok & (older >= 0)
                pair &= ~np.isnan(err[np.where(older >= 0, older, 0)])
                return int(ok.sum()), _sd(err[ok]), _corr(err[pair], err[older[pair]])

            n_kt, kt_sd, rho_kt = stat(e_kt)
            _n, ktx_sd, _r = stat(e_ktx)
            n_t, t_sd, rho_t = stat(e_t)
            n_w, w_sd, rho_w = stat(e_w)
            hsel = hour_bin_a == b if hour_bin_a.size else np.zeros(0, dtype=bool)
            out_bins.append(
                {
                    "lead_h": [lo, hi],
                    "mean_lead_h": round(float(leads[sel].mean()), 4) if sel.any() else None,
                    "n_kt": n_kt,
                    "kt_sd": kt_sd,
                    "kt_logit_sd": ktx_sd,
                    "n_temp": n_t,
                    "temp_sd": t_sd,
                    "n_wind": n_w,
                    "wind_ln_sd": w_sd,
                    "rho2_kt": rho_kt,
                    "rho2_temp": rho_t,
                    "rho2_wind": rho_w,
                    "hourly_kt_sd": _sd(hour_err_a[hsel]) if hsel.size else None,
                }
            )
        lead_c = [b["mean_lead_h"] or 0.0 for b in out_bins]
        models[model] = {
            "bins": out_bins,
            "fit": {
                "kt_logit": _linear_fit(
                    lead_c, [b["kt_logit_sd"] for b in out_bins], [b["n_kt"] for b in out_bins]
                ),
                "temp": _linear_fit(
                    lead_c, [b["temp_sd"] for b in out_bins], [b["n_temp"] for b in out_bins]
                ),
                "wind_ln": _linear_fit(
                    lead_c, [b["wind_ln_sd"] for b in out_bins], [b["n_wind"] for b in out_bins]
                ),
            },
        }
    fp = weather_params["forecast"]
    keys = (
        "kt_sd_base",
        "kt_sd_per_h",
        "temp_sd_base_c",
        "temp_sd_per_h_c",
        "wind_ln_sd_base",
        "wind_ln_sd_per_h",
        "rho",
    )
    return {
        "kind": "forecast-error",
        "models": models,
        "weather_yaml": {k: fp[k] for k in keys},
        "caveats": list(FORECAST_CAVEATS),
    }
