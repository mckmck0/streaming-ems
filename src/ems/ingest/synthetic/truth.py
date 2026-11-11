"""Where a site's "truth weather" comes from: the one seam between weather sources and asset models.

The PV, heat-pump and base-load models read weather only from a ``Truth``. A ``TruthSource`` builds
it for a slice of sites and a time axis:

- ``SyntheticTruth``: the synthetic weather of ``generate_truth``.
- ``LiveForecastTruth``: for hybrid live sites, the latest live forecast step minus the synthetic
  forecast error at that step's lead (the same ``forecast_errors`` as the synthetic forecasts,
  applied in reverse), so the meters deviate from the forecasts strategies see (spec 007, risk L5).
  Intervals without a live step fall back to another source.

A later historical backfill adds a source (satellite or reanalysis arrays) without touching the
models.
"""

from __future__ import annotations

import dataclasses
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

import numpy as np
from numpy.typing import NDArray

from ems.contracts.envelope import WeatherForecastEvent
from ems.contracts.time import quarter_index
from ems.ingest.synthetic.solar import decompose
from ems.ingest.synthetic.weather import (
    SiteArrays,
    TimeAxis,
    Truth,
    forecast_errors,
    from_x,
    generate_truth,
    lead_hours,
    to_x,
)

F64 = NDArray[np.float64]
FORECAST_STEPS = 192
WIND_HEIGHTS = ("10", "100", "120")


class TruthSource(Protocol):
    def __call__(self, sites: SiteArrays, axis: TimeAxis) -> Truth: ...


@dataclass(frozen=True)
class SyntheticTruth:
    seed: int
    params: dict[str, Any]

    def __call__(self, sites: SiteArrays, axis: TimeAxis) -> Truth:
        return generate_truth(sites, axis, self.seed, self.params)


def _step_arrays(event: WeatherForecastEvent, t: NDArray[np.int64]) -> dict[str, F64]:
    """Step values of one event aligned to axis quarters ``t`` (NaN where the event has no step)."""
    steps = event.payload.steps
    q = np.array([quarter_index(s.valid_start) for s in steps], dtype=np.int64)
    pos = np.searchsorted(q, t)
    pos_c = np.clip(pos, 0, max(q.size - 1, 0))
    has = (pos < q.size) & (q[pos_c] == t) if q.size else np.zeros(t.size, dtype=bool)

    def col(values: list[float]) -> F64:
        arr = np.asarray(values, dtype=np.float64)
        return np.where(has, arr[pos_c], np.nan) if q.size else np.full(t.size, np.nan)

    out = {
        "ghi": col([s.ghi_wm2 for s in steps]),
        "temp": col([s.temp_air_c for s in steps]),
    }
    for h in WIND_HEIGHTS:
        out[f"wind{h}"] = col([s.wind_speed_ms.get(h, float("nan")) for s in steps])
    return out


@dataclass(frozen=True)
class LiveForecastTruth:
    location_by_key: Mapping[int, str]
    forecasts: Mapping[str, WeatherForecastEvent]
    fallback: TruthSource
    seed: int
    params: dict[str, Any]

    def __call__(self, sites: SiteArrays, axis: TimeAxis) -> Truth:
        truth = self.fallback(sites, axis)
        groups: dict[int, list[tuple[int, WeatherForecastEvent]]] = defaultdict(list)
        for row, key in enumerate(sites.keys):
            event = self.forecasts.get(self.location_by_key.get(int(key), ""))
            if event is not None:
                groups[quarter_index(event.issued_at)].append((row, event))
        if not groups:
            return truth
        fp = self.params["forecast"]
        lead = lead_hours(FORECAST_STEPS)
        rho, tau, corr = float(fp["rho"]), float(fp["common_tau_h"]), float(fp["step_correlation"])
        sd = {
            "fc.kt": float(fp["kt_sd_base"]) + float(fp["kt_sd_per_h"]) * lead,
            "fc.temp": float(fp["temp_sd_base_c"]) + float(fp["temp_sd_per_h_c"]) * lead,
            "fc.wind": float(fp["wind_ln_sd_base"]) + float(fp["wind_ln_sd_per_h"]) * lead,
        }
        kt, ghi, temp = truth.kt.copy(), truth.ghi.copy(), truth.temp_c.copy()
        wind = {h: getattr(truth, f"wind{h}").copy() for h in WIND_HEIGHTS}
        changed = np.zeros(kt.shape, dtype=bool)
        cs_all = truth.geometry.cs_ghi
        for issue_t, members in groups.items():  # one group per live run, vectorised over its sites
            rows = np.array([r for r, _ in members])
            k = axis.t[None, :] - issue_t
            valid_k = (k >= 0) & (k < FORECAST_STEPS)
            kc = np.broadcast_to(np.clip(k, 0, FORECAST_STEPS - 1), (rows.size, axis.n))
            err = {
                var: np.take_along_axis(
                    forecast_errors(self.seed, sites.keys[rows], issue_t, var, s, rho, tau, corr),
                    kc,
                    axis=1,
                )
                for var, s in sd.items()
            }
            w_sd = np.broadcast_to(sd["fc.wind"][kc], kc.shape)
            step = [_step_arrays(ev, axis.t) for _, ev in members]
            f = {name: np.stack([s[name] for s in step]) for name in step[0]}
            has = valid_k & ~np.isnan(f["ghi"])
            cs = cs_all[rows]
            day = has & (cs > 0)
            kt_step = np.where(day, f["ghi"] / np.where(cs > 0, cs, 1.0), 0.5)
            kt_true = from_x(to_x(kt_step) - err["fc.kt"])
            kt[rows] = np.where(day, kt_true, kt[rows])
            ghi[rows] = np.where(day, cs * kt_true, np.where(has, 0.0, ghi[rows]))
            temp[rows] = np.where(has, f["temp"] - err["fc.temp"], temp[rows])
            factor = np.exp(-err["fc.wind"] - w_sd**2 / 2)
            for h in WIND_HEIGHTS:
                ok = has & ~np.isnan(f[f"wind{h}"])
                wind[h][rows] = np.where(ok, f[f"wind{h}"] * factor, wind[h][rows])
            changed[rows] |= has
        doy = axis.calendar(axis.unix_mid)[0]
        dni_new, dhi_new = decompose(ghi, truth.geometry.zenith, doy)
        cloud_new = np.clip(np.round(100.0 * (1.02 - kt) / 0.85), 0, 100)
        return dataclasses.replace(
            truth,
            kt=kt,
            ghi=ghi,
            dni=np.where(changed, dni_new, truth.dni),
            dhi=np.where(changed, dhi_new, truth.dhi),
            temp_c=temp,
            wind10=wind["10"],
            wind100=wind["100"],
            wind120=wind["120"],
            cloud_pct=np.where(changed, cloud_new, truth.cloud_pct),
        )
