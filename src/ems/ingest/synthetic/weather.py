"""Synthetic "true weather" and forecasts, vectorised over sites.

Every series is a pure function of (seed, site, variable, quarter-hour index ``t``): shared national
processes (weather regimes, synoptic anomalies) give spatial correlation, local processes add site
detail. Forecasts are the truth plus errors shared by consecutive runs, with a per-run part, a
timing error on the clear-sky index and a spread that grows with lead (spec 007). Field semantics
mirror Open-Meteo: radiation is the mean over the interval (labelled at its
end); temperature, wind and cloud cover are instants at the interval end.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any

import numpy as np
from numpy.typing import NDArray
from ruamel.yaml import YAML
from scipy.special import expit, ndtr

from ems.contracts.time import QUARTER_S
from ems.ingest import rng
from ems.ingest.synthetic.solar import SolarGeometry, decompose, solar_geometry

F64 = NDArray[np.float64]
I64 = NDArray[np.int64]
STATES = ("clear", "broken", "overcast")


@lru_cache(maxsize=4)
def load_params(path: Path) -> dict[str, Any]:
    data: dict[str, Any] = YAML(typ="safe").load(path.read_text())
    return data


@dataclass(frozen=True)
class SiteArrays:
    """Per-site columns used by the weather and asset models (1-D, length n_loc)."""

    keys: NDArray[np.uint64]
    lat: F64
    lon: F64
    elevation_m: F64
    shear_alpha: F64
    wind_scale_ms: F64


@dataclass(frozen=True)
class TimeAxis:
    t: I64  # quarter-hour index of each interval start
    unix_start: F64

    @classmethod
    def from_range(cls, t0: int, n: int) -> TimeAxis:
        t = np.arange(t0, t0 + n, dtype=np.int64)
        return cls(t=t, unix_start=(t * QUARTER_S).astype(np.float64))

    @property
    def n(self) -> int:
        return int(self.t.size)

    @property
    def unix_mid(self) -> F64:
        return self.unix_start + QUARTER_S / 2

    @property
    def unix_end(self) -> F64:
        return self.unix_start + QUARTER_S

    def calendar(self, unix: F64) -> tuple[I64, I64, F64]:
        """Day of year, month and fractional UTC hour for timestamps ``unix``."""
        dt = unix.astype("datetime64[s]")
        days = dt.astype("datetime64[D]")
        years = dt.astype("datetime64[Y]")
        doy = (days - years.astype("datetime64[D]")).astype(np.int64) + 1
        month = (dt.astype("datetime64[M]") - years.astype("datetime64[M]")).astype(np.int64) + 1
        hour = (unix % 86400) / 3600.0
        return doy, month, hour

    def start_datetime(self, i: int) -> datetime:
        from ems.contracts.time import from_quarter_index

        return from_quarter_index(int(self.t[i]))


@dataclass(frozen=True)
class Truth:
    axis: TimeAxis
    geometry: SolarGeometry  # at interval midpoints
    kt: F64  # clear-sky index, [n_loc, n]
    ghi: F64
    dni: F64
    dhi: F64
    temp_c: F64  # instant at interval end
    wind10: F64
    wind100: F64
    wind120: F64
    wind_dir: F64
    cloud_pct: F64
    national_kt: F64  # [n] regime mean, shared
    national_wind100: F64  # [n]


def _regimes(seed: int, hours: I64, months: I64, params: dict[str, Any]) -> I64:
    """National hourly regime chain. A shared draw decides 'leave', a second picks the new state,
    so two runs starting at different times couple at the first leave (window independence).
    """
    p_stay = float(params["p_stay"])
    monthly = np.asarray(params["monthly"], dtype=np.float64)
    burn = 96  # hours before the window: chains started earlier have coupled by then
    span = np.arange(hours[0] - burn, hours[-1] + 1, dtype=np.int64)
    leave_u = rng.uniform(seed, [rng.NATIONAL_KEY], "regime.leave", span)[0]
    pick_u = rng.uniform(seed, [rng.NATIONAL_KEY], "regime.pick", span)[0]
    month_of = np.empty(span.size, dtype=np.int64)
    month_of[:burn] = months[0]
    month_of[burn:] = np.interp(span[burn:], hours, months).astype(np.int64)
    cum = np.cumsum(monthly, axis=1)
    state = int(np.searchsorted(cum[month_of[0] - 1], pick_u[0]))
    out = np.empty(span.size, dtype=np.int64)
    for i in range(span.size):  # loop over time only, never over sites
        if leave_u[i] > p_stay:
            state = int(np.searchsorted(cum[month_of[i] - 1], pick_u[i]))
        out[i] = min(state, 2)
    return out[burn:]


def generate_truth(sites: SiteArrays, axis: TimeAxis, seed: int, params: dict[str, Any]) -> Truth:
    keys = sites.keys
    doy_mid, month_mid, _hour_mid = axis.calendar(axis.unix_mid)
    doy_end, _month_end, hour_end = axis.calendar(axis.unix_end)
    geom = solar_geometry(
        axis.unix_mid, doy_mid, month_mid, sites.lat, sites.lon, sites.elevation_m
    )

    # ── clear-sky index: national regime + regional and local AR noise ───────────────────────────
    ck = params["clear_sky_index"]
    hours = (axis.t // 4).astype(np.int64)
    uniq_hours = np.unique(hours)
    months_h = axis.calendar((uniq_hours * 3600).astype(np.float64))[1]
    regime_h = _regimes(seed, uniq_hours, months_h, ck)
    regime = regime_h[np.searchsorted(uniq_hours, hours)]
    means = np.array([ck["states"][s]["mean"] for s in STATES])
    sds = np.array([ck["states"][s]["sd"] for s in STATES])
    t0, n = int(axis.t[0]), axis.n
    local = rng.ar1(seed, keys, "kt.local", t0, n, rng.phi_for(float(ck["local_tau_h"])))
    regional = rng.ar1(seed, keys, "kt.regional", t0, n, rng.phi_for(float(ck["regional_tau_h"])))
    kt = means[regime][None, :] + sds[regime][None, :] * local + float(ck["regional_sd"]) * regional
    kt = np.clip(kt, 0.03, 1.1)
    ghi = np.clip(geom.cs_ghi * kt, 0.0, 1.1 * geom.cs_ghi)
    dni, dhi = decompose(ghi, geom.zenith, doy_mid)
    cloud = np.clip(np.round(100.0 * (1.02 - kt) / 0.85), 0, 100)

    # ── temperature (instant at interval end) ─────────────────────────────────────────────────────
    tp = params["temperature"]
    lat_c = sites.lat.reshape(-1, 1)
    lon_c = sites.lon.reshape(-1, 1)
    season = np.cos(2 * np.pi * (doy_end - float(tp["coldest_day_of_year"])) / 365.25)
    amp = float(tp["annual_amplitude_c"]) + float(tp["continentality_c_per_deg_lon"]) * (
        lon_c - 19.0
    )
    mean = (
        float(tp["annual_mean_c"])
        - float(tp["lapse_c_per_m"]) * sites.elevation_m.reshape(-1, 1)
        + float(tp["lat_gradient_c_per_deg"]) * (lat_c - 52.0)
    )
    seasonal = mean - amp * season[None, :]
    summer = 0.5 * (1 - np.cos(2 * np.pi * (doy_end - 15) / 365.25))
    d_amp = float(tp["diurnal_amplitude_winter_c"]) + summer * (
        float(tp["diurnal_amplitude_summer_c"]) - float(tp["diurnal_amplitude_winter_c"])
    )
    diurnal = d_amp[None, :] * np.cos(
        2 * np.pi * (hour_end - float(tp["diurnal_peak_utc_h"])) / 24.0
    )
    diurnal = diurnal * (0.35 + 0.65 * kt)  # cloudy days have flatter daily cycles
    nat = rng.ar1(
        seed,
        [rng.NATIONAL_KEY],
        "temp.national",
        t0 + 1,
        n,
        rng.phi_for(float(tp["national_tau_h"])),
    )
    loc = rng.ar1(seed, keys, "temp.local", t0 + 1, n, rng.phi_for(float(tp["local_tau_h"])))
    temp = seasonal + diurnal + float(tp["national_sd_c"]) * nat + float(tp["local_sd_c"]) * loc

    # ── wind (instant at interval end): Gaussian AR copula -> Weibull, power-law shear ────────────
    wp = params["wind"]
    share = float(wp["national_share"])
    z_nat = rng.ar1(
        seed,
        [rng.NATIONAL_KEY],
        "wind.national",
        t0 + 1,
        n,
        rng.phi_for(float(wp["national_tau_h"])),
    )
    z_loc = rng.ar1(seed, keys, "wind.local", t0 + 1, n, rng.phi_for(float(wp["local_tau_h"])))
    z = share * z_nat + np.sqrt(1 - share**2) * z_loc
    u = np.clip(ndtr(z), 1e-9, 1 - 1e-9)
    k = float(wp["weibull_k"])
    day = np.clip(np.cos(2 * np.pi * (hour_end - 13.0) / 24.0), 0, None)
    v10 = sites.wind_scale_ms.reshape(-1, 1) * (-np.log1p(-u)) ** (1.0 / k)
    v10 = v10 * (1 + float(wp["daytime_boost"]) * day[None, :])
    alpha = sites.shear_alpha.reshape(-1, 1)
    v100 = v10 * (100.0 / 10.0) ** alpha
    v120 = v10 * (120.0 / 10.0) ** alpha
    d_nat = rng.ar1(
        seed, [rng.NATIONAL_KEY], "wind.dir", t0 + 1, n, rng.phi_for(float(wp["dir_tau_h"]))
    )
    d_loc = rng.ar1(seed, keys, "wind.dir.local", t0 + 1, n, rng.phi_for(3.0))
    wdir = np.mod(
        float(wp["prevailing_dir_deg"]) + float(wp["dir_sd_deg"]) * d_nat + 10.0 * d_loc, 360.0
    )

    nat_u = np.clip(ndtr(z_nat[0]), 1e-9, 1 - 1e-9)
    national_wind100 = 6.5 * (-np.log1p(-nat_u)) ** (1.0 / k)
    return Truth(
        axis=axis,
        geometry=geom,
        kt=kt,
        ghi=ghi,
        dni=dni,
        dhi=dhi,
        temp_c=temp,
        wind10=v10,
        wind100=v100,
        wind120=v120,
        wind_dir=wdir,
        cloud_pct=cloud,
        national_kt=means[regime],
        national_wind100=national_wind100,
    )


@dataclass(frozen=True)
class ForecastRun:
    issue_t: int  # quarter index of the issue (run) time = first valid_start
    steps: int
    ghi: F64  # [n_loc, steps]
    dni: F64
    dhi: F64
    temp_c: F64
    cloud_pct: F64
    wind10: F64
    wind100: F64
    wind120: F64
    wind_dir: F64
    kt_error: F64  # forecast kt - truth kt at the same valid time
    phase_h: F64  # timing error of the clear-sky index, hours


# ── forecast errors (spec 007) ────────────────────────────────────────────────────────────────────
# err(v, r) = sd(L) * (rho * Z(v) + sqrt(1 - rho^2) * Y(r, v)): Z is shared by every run for a valid
# time, Y belongs to one run. kt errors act in a bounded logit space, wind errors are log-normal.

KT_MIN = 0.03
KT_MAX = 1.1
_KT_EPS = 1e-6


def to_x(kt: F64) -> F64:
    """Bounded logit of the clear-sky index: (KT_MIN, KT_MAX) -> real line."""
    p = (np.clip(kt, KT_MIN + _KT_EPS, KT_MAX - _KT_EPS) - KT_MIN) / (KT_MAX - KT_MIN)
    return np.asarray(np.log(p) - np.log1p(-p), dtype=np.float64)


def from_x(x: F64) -> F64:
    """Inverse of ``to_x``; the result lies strictly inside (KT_MIN, KT_MAX)."""
    return np.asarray(KT_MIN + (KT_MAX - KT_MIN) * expit(x), dtype=np.float64)


def lead_hours(steps: int) -> F64:
    """Lead of each step at its interval end, (k + 1) * 0.25 h."""
    return np.arange(1, steps + 1, dtype=np.float64) * 0.25


def _run_path(
    seed: int, keys: NDArray[np.uint64], var: str, issue_t: int, steps: int, corr: float
) -> F64:
    """Unit-variance AR(1) along the steps of one run, keyed by (run, step)."""
    base = issue_t * 256
    eps = rng.normal(seed, keys, var, np.arange(base, base + steps, dtype=np.int64))
    out = np.empty_like(eps)
    out[:, 0] = eps[:, 0]
    for j in range(1, steps):  # along the steps of one run only
        out[:, j] = corr * out[:, j - 1] + np.sqrt(1 - corr**2) * eps[:, j]
    return out


def forecast_errors(
    seed: int,
    keys: NDArray[np.uint64],
    issue_t: int,
    var: str,
    sd: F64,
    rho: float,
    tau_common_h: float,
    step_corr: float,
) -> F64:
    """Errors [n_loc, len(sd)] of the run issued at ``issue_t``.

    Z runs over the run's own valid-time window, so a run never depends on the chunk; two runs share
    Z at a valid time up to the AR burn-in residual (risk G1).
    """
    steps = int(sd.size)
    z = rng.ar1(seed, keys, f"{var}.common", issue_t, steps, rng.phi_for(tau_common_h))
    y = _run_path(seed, keys, var, issue_t, steps, step_corr)
    return sd[None, :] * (rho * z + np.sqrt(1.0 - rho**2) * y)


def truth_margin_q(params: dict[str, Any]) -> int:
    """Quarter-hours the truth axis needs after a run's last step (the largest phase shift)."""
    return int(np.ceil(4.0 * float(params["forecast"]["phase_max_h"])))


def phase_shift_h(
    seed: int, keys: NDArray[np.uint64], issue_t: int, steps: int, fp: dict[str, Any]
) -> F64:
    """Timing error [n_loc, steps] in hours of the run issued at ``issue_t``.

    Drawn at knots only: the issue time (0) and every ``phase_knot_h`` of absolute valid time, then
    interpolated linearly. Knots at least 2 * phase_max_h apart and the lower bound -L keep the warp
    ``k + 4 * delta`` non-decreasing and >= 0 (the forecast never samples before its issue time).
    """
    knot_q = round(4.0 * float(fp["phase_knot_h"]))
    dmax = float(fp["phase_max_h"])
    if knot_q < 8.0 * dmax:
        raise ValueError("phase_knot_h must be at least 2 * phase_max_h")
    a, rho = float(fp["phase_sd_per_sqrt_h"]), float(fp["rho_phase"])
    first = (issue_t // knot_q + 1) * knot_q
    knots = np.arange(first, issue_t + steps + knot_q, knot_q, dtype=np.int64)
    zc = rng.normal(seed, keys, "fc.phase.common", knots // knot_q)
    zr = rng.normal(seed, keys, "fc.phase.run", issue_t * 64 + (knots - first) // knot_q)
    lead = (knots - issue_t) * 0.25
    raw = a * np.sqrt(lead)[None, :] * (rho * zc + np.sqrt(1.0 - rho**2) * zr)
    dk = np.clip(raw, -np.minimum(lead, dmax)[None, :], dmax)
    xs = np.concatenate([[issue_t], knots]).astype(np.float64)
    ds = np.concatenate([np.zeros((dk.shape[0], 1)), dk], axis=1)
    v = issue_t + np.arange(steps, dtype=np.float64)
    seg = np.clip(np.searchsorted(xs, v, side="right") - 1, 0, xs.size - 2)
    w = (v - xs[seg]) / (xs[seg + 1] - xs[seg])
    delta = ds[:, seg] * (1.0 - w) + ds[:, seg + 1] * w
    # guard against float round-off at the slope limit: the warp must never step backwards
    pos = np.maximum.accumulate(np.arange(steps)[None, :] + 4.0 * delta, axis=1)
    return np.asarray((pos - np.arange(steps)[None, :]) / 4.0, dtype=np.float64)


def _interp_rows(values: F64, pos: F64) -> F64:
    """Row-wise linear interpolation of ``values`` [n_loc, n] at fractional positions [n_loc, m]."""
    i = np.clip(np.floor(pos).astype(np.int64), 0, values.shape[1] - 2)
    f = pos - i
    lo = np.take_along_axis(values, i, axis=1)
    hi = np.take_along_axis(values, i + 1, axis=1)
    return lo + f * (hi - lo)


def forecast_run(
    truth: Truth,
    issue_t: int,
    steps: int,
    seed: int,
    keys: NDArray[np.uint64],
    params: dict[str, Any],
) -> ForecastRun:
    """Forecast issued at quarter index ``issue_t`` for intervals ``issue_t .. issue_t+steps-1``."""
    fp = params["forecast"]
    i0 = int(issue_t - truth.axis.t[0])
    if i0 < 0 or i0 + steps + truth_margin_q(params) > truth.axis.n:
        raise ValueError("truth window does not cover the forecast horizon")
    sl = slice(i0, i0 + steps)
    lead_h = lead_hours(steps)
    rho, tau = float(fp["rho"]), float(fp["common_tau_h"])
    corr = float(fp["step_correlation"])

    def err(var: str, base: str, per_h: str) -> tuple[F64, F64]:
        sd = float(fp[base]) + float(fp[per_h]) * lead_h
        return forecast_errors(seed, keys, issue_t, var, sd, rho, tau, corr), sd

    phase = phase_shift_h(seed, keys, issue_t, steps, fp)
    kt_shifted = _interp_rows(truth.kt, i0 + np.arange(steps)[None, :] + 4.0 * phase)
    kt_f = from_x(to_x(kt_shifted) + err("fc.kt", "kt_sd_base", "kt_sd_per_h")[0])
    geom = truth.geometry
    cs = geom.cs_ghi[:, sl]
    ghi = np.where(cs > 0, cs * kt_f, 0.0)
    doy = truth.axis.calendar(truth.axis.unix_mid[sl])[0]
    dni, dhi = decompose(ghi, geom.zenith[:, sl], doy)
    temp = truth.temp_c[:, sl] + err("fc.temp", "temp_sd_base_c", "temp_sd_per_h_c")[0]
    e_w, w_sd = err("fc.wind", "wind_ln_sd_base", "wind_ln_sd_per_h")
    w_factor = np.exp(e_w - w_sd[None, :] ** 2 / 2)
    cloud = np.clip(np.round(100.0 * (1.02 - kt_f) / 0.85), 0, 100)
    dir_path = _run_path(seed, keys, "fc.dir", issue_t, steps, corr)
    wdir = np.mod(truth.wind_dir[:, sl] + 8.0 * np.sqrt(lead_h)[None, :] * dir_path, 360.0)
    return ForecastRun(
        issue_t=issue_t,
        steps=steps,
        ghi=ghi,
        dni=dni,
        dhi=dhi,
        temp_c=temp,
        cloud_pct=cloud,
        wind10=truth.wind10[:, sl] * w_factor,
        wind100=truth.wind100[:, sl] * w_factor,
        wind120=truth.wind120[:, sl] * w_factor,
        wind_dir=wdir,
        kt_error=kt_f - truth.kt[:, sl],
        phase_h=phase,
    )
