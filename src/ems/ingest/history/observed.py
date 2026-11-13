"""Observed truth weather for real-data sites (spec 008, plan section 6): satellite radiation and
ERA5 temperature and wind behind the spec 007 truth-source seam.

Pure: arrays in, arrays out (no I/O, no clock). The PV, heat-pump and load models read the
``Truth`` this source returns and are not changed.

- Satellite (DWD MTG via Open-Meteo): 10-minute backward means labelled at their end become
  quarter-hour means over ``[s, s + 15 min)``. A null slot whose own clear-sky GHI is 0 is night
  and counts as 0.
- ERA5 (via Open-Meteo): hourly instants interpolated to each quarter's end, the label ``Truth``
  uses for temperature and wind.
- Gaps: satellite GHI present but DNI or DHI missing or implausible -> Erbs split of that GHI;
  satellite GHI missing in daylight -> ERA5's hourly clear-sky index times the quarter's clear-sky
  GHI, Erbs split; ERA5 missing too -> the fallback (synthetic) truth. Missing ERA5 temperature or
  wind -> the fallback's. ``masks`` says which quarters used which fallback.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import numpy as np
from numpy.typing import NDArray

from ems.ingest.synthetic.solar import decompose, solar_geometry
from ems.ingest.synthetic.truth import TruthSource
from ems.ingest.synthetic.weather import SiteArrays, TimeAxis, Truth

F64 = NDArray[np.float64]
I64 = NDArray[np.int64]
B = NDArray[np.bool_]

QUARTER_S = 900
SLOT_S = 600  # satellite step
HOUR_S = 3600
KT_CLIP = 1.1
MIN_CS_MEAN = 1.0  # W/m2: below it an hour's clear-sky index is 0
CLOSURE_TOL = 20.0  # W/m2: DNI cos z may exceed GHI by this much (15-minute averaging)
MASKS = (
    "radiation_erbs",
    "radiation_era5",
    "radiation_synthetic",
    "temperature_synthetic",
    "wind_synthetic",
)


@dataclass(frozen=True)
class ObservedSeries:
    """One site's raw observations, as parsed from the stored API responses."""

    location_id: str
    sat_labels: I64  # unix s, end labels of 10-minute backward means
    ghi10: F64
    dni10: F64
    dhi10: F64  # NaN where null
    era5_hours: I64  # unix s, hour labels
    temp_h: F64
    wind10_h: F64
    ghi_h: F64  # NaN where null; the mean over [H - 1 h, H)


def _column(block: Mapping[str, Any], name: str) -> F64:
    return np.array([np.nan if v is None else float(v) for v in block[name]], dtype=np.float64)


def parse_series(
    location_id: str, satellite: Mapping[str, Any], era5: Mapping[str, Any]
) -> ObservedSeries:
    """``satellite`` and ``era5`` are the API JSON bodies (``timeformat=unixtime``)."""
    sat, hourly = satellite["hourly"], era5["hourly"]
    return ObservedSeries(
        location_id=location_id,
        sat_labels=np.asarray(sat["time"], dtype=np.int64),
        ghi10=_column(sat, "shortwave_radiation"),
        dni10=_column(sat, "direct_normal_irradiance"),
        dhi10=_column(sat, "diffuse_radiation"),
        era5_hours=np.asarray(hourly["time"], dtype=np.int64),
        temp_h=_column(hourly, "temperature_2m"),
        wind10_h=_column(hourly, "wind_speed_10m"),
        ghi_h=_column(hourly, "shortwave_radiation"),
    )


def _lookup(times: I64, values: F64, wanted: I64) -> F64:
    """``values`` at ``wanted`` times; NaN where a time is absent."""
    pos = np.searchsorted(times, wanted)
    pos_c = np.clip(pos, 0, max(times.size - 1, 0))
    found = (
        (pos < times.size) & (times[pos_c] == wanted) if times.size else np.zeros_like(wanted, bool)
    )
    return np.where(found, values[pos_c] if times.size else np.nan, np.nan)


def quarter_means(labels: I64, values: F64, cs10: F64, t: I64) -> F64:
    """Quarter-hour means of 10-minute backward means ``values`` labelled at their end.

    The quarter ``[s, s + 15 min)`` touches two 10-minute intervals ``[L - 10 min, L)``: weights
    2/3 and 1/3 by overlap. A null slot whose clear-sky GHI ``cs10`` is 0 counts as 0 (night). A
    touched slot that is still null, or absent, makes the quarter NaN.
    """
    vals = np.where(np.isnan(values) & (cs10 <= 0.0), 0.0, values)
    start = t.astype(np.int64) * QUARTER_S
    end = start + QUARTER_S
    first = (start // SLOT_S + 1) * SLOT_S  # the first label whose interval overlaps the quarter
    out = np.zeros(t.size, dtype=np.float64)
    label = first
    for _ in range(2):  # 15 min over 10-minute slots: exactly two touched slots
        overlap = np.minimum(label, end) - np.maximum(label - SLOT_S, start)
        out += overlap / QUARTER_S * _lookup(labels, vals, label)
        label = label + SLOT_S
    return out


def instants_at_end(hours: I64, values: F64, t: I64) -> F64:
    """Hourly instants interpolated linearly to each quarter's end; an end on the hour uses that
    hour alone. NaN where a needed hour is null or absent.
    """
    end = t.astype(np.int64) * QUARTER_S + QUARTER_S
    h0 = (end // HOUR_S) * HOUR_S
    w = (end - h0) / HOUR_S
    v0 = _lookup(hours, values, h0)
    v1 = _lookup(hours, values, h0 + HOUR_S)
    return np.where(w == 0.0, v0, (1.0 - w) * v0 + w * v1)


def _hour_index(hours: I64, ghi_h: F64, cs_q: F64, t: I64) -> tuple[F64, F64]:
    """Per quarter: the clear-sky index of its ERA5 hour (unclipped) and its clipped value."""
    t = t.astype(np.int64)
    if t.size % 4 or (t.size and t[0] % 4):
        raise ValueError("ERA5 fill needs an hour-aligned axis (whole hours of quarters)")
    cs_mean = np.repeat(cs_q.reshape(-1, 4).mean(axis=1), 4)
    label = (t * QUARTER_S // HOUR_S) * HOUR_S + HOUR_S  # floor_hour(s) + 1 h
    ghi = _lookup(hours, ghi_h, label)
    with np.errstate(divide="ignore", invalid="ignore"):
        raw = np.where(cs_mean < MIN_CS_MEAN, 0.0, ghi / cs_mean)
    return raw, np.clip(raw, 0.0, KT_CLIP)


def era5_fill_ghi(hours: I64, ghi_h: F64, cs_q: F64, t: I64) -> F64:
    """GHI for quarters ``t`` from ERA5's hourly mean: the hour's clear-sky index (ERA5 GHI over the
    mean clear-sky GHI of its four quarters, clipped to [0, 1.1]) times each quarter's clear-sky
    GHI ``cs_q``. The quarter ``[s, s + 15)`` uses the label ``floor_hour(s) + 1 h``.
    """
    _raw, index = _hour_index(hours, ghi_h, cs_q, t)
    return index * cs_q


def check_split(ghi: F64, dni: F64, dhi: F64, zenith_deg: F64, dni_extra: F64) -> B:
    """True where the satellite's DNI/DHI split is physically plausible."""
    cosz = np.cos(np.radians(zenith_deg))
    return (dhi <= ghi) & (dni <= dni_extra) & (dni * cosz <= ghi + CLOSURE_TOL)


@dataclass(frozen=True)
class _Row:
    ghi: F64
    dni: F64
    dhi: F64
    temp: F64
    wind10: F64
    masks: dict[str, B]
    clip_share: float
    low_sun_zero_share: float


@dataclass(frozen=True)
class ObservedTruth:
    """``TruthSource`` for real-data sites; rows without a series keep the fallback's truth."""

    location_by_key: Mapping[int, str]
    series: Mapping[str, ObservedSeries]
    fallback: TruthSource

    def _rows(self, sites: SiteArrays, axis: TimeAxis) -> tuple[Truth, dict[int, tuple[str, _Row]]]:
        if axis.n % 4 or int(axis.t[0]) % 4:
            raise ValueError("observed truth needs an hour-aligned axis (whole hours of quarters)")
        truth = self.fallback(sites, axis)
        doy = axis.calendar(axis.unix_mid)[0]
        rows: dict[int, tuple[str, _Row]] = {}
        for i, key in enumerate(sites.keys):
            loc = self.location_by_key.get(int(key))
            series = self.series.get(loc) if loc is not None else None
            if series is None:
                continue
            assert loc is not None
            rows[i] = (loc, self._row(series, sites, i, axis, truth, doy))
        return truth, rows

    def _row(
        self, s: ObservedSeries, sites: SiteArrays, i: int, axis: TimeAxis, truth: Truth, doy: I64
    ) -> _Row:
        g = truth.geometry
        cs_q, zenith, dni_extra = g.cs_ghi[i], g.zenith[i], g.dni_extra
        cs10 = _slot_clear_sky(s.sat_labels, sites, i, axis)
        ghi = quarter_means(s.sat_labels, s.ghi10, cs10, axis.t)
        dni = quarter_means(s.sat_labels, s.dni10, cs10, axis.t)
        dhi = quarter_means(s.sat_labels, s.dhi10, cs10, axis.t)
        day = cs_q > 0.0
        ghi_gap = np.isnan(ghi)
        ghi = np.where(ghi_gap & ~day, 0.0, ghi)
        ghi_gap &= day
        era5_raw, era5_index = _hour_index(s.era5_hours, s.ghi_h, cs_q, axis.t)
        era5 = era5_index * cs_q
        use_era5 = ghi_gap & ~np.isnan(era5)
        use_synth = ghi_gap & np.isnan(era5)
        ghi = np.where(use_era5, era5, ghi)
        ghi = np.where(use_synth, truth.ghi[i], ghi)
        with np.errstate(invalid="ignore"):
            split_ok = (
                ~np.isnan(dni)
                & ~np.isnan(dhi)
                & check_split(ghi, np.nan_to_num(dni), np.nan_to_num(dhi), zenith, dni_extra)
            )
        use_erbs = ~ghi_gap & ~split_ok
        erbs_dni, erbs_dhi = decompose(ghi[None, :], zenith[None, :], doy)
        resplit = use_erbs | use_era5
        dni = np.where(resplit, erbs_dni[0], dni)
        dhi = np.where(resplit, erbs_dhi[0], dhi)
        dni = np.where(use_synth, truth.dni[i], dni)
        dhi = np.where(use_synth, truth.dhi[i], dhi)

        temp = instants_at_end(s.era5_hours, s.temp_h, axis.t)
        wind = instants_at_end(s.era5_hours, s.wind10_h, axis.t)
        temp_fb, wind_fb = np.isnan(temp), np.isnan(wind)
        temp = np.where(temp_fb, truth.temp_c[i], temp)
        wind = np.where(wind_fb, truth.wind10[i], wind)

        fills = int(use_era5.sum())
        clip_share = float((era5_raw[use_era5] > KT_CLIP).sum() / fills) if fills else 0.0
        observed = day & ~ghi_gap
        cs_day = float(cs_q[day].sum())
        low_sun = float(cs_q[observed & (ghi == 0.0)].sum() / cs_day) if cs_day > 0 else 0.0
        return _Row(
            ghi=ghi,
            dni=dni,
            dhi=dhi,
            temp=temp,
            wind10=wind,
            masks={
                "radiation_erbs": use_erbs & ~ghi_gap,
                "radiation_era5": use_era5,
                "radiation_synthetic": use_synth,
                "temperature_synthetic": temp_fb,
                "wind_synthetic": wind_fb,
            },
            clip_share=clip_share,
            low_sun_zero_share=low_sun,
        )

    def __call__(self, sites: SiteArrays, axis: TimeAxis) -> Truth:
        truth, rows = self._rows(sites, axis)
        if not rows:
            return truth
        arrays = {
            name: getattr(truth, name).copy() for name in ("ghi", "dni", "dhi", "temp_c", "wind10")
        }
        for i, (_loc, row) in rows.items():
            arrays["ghi"][i], arrays["dni"][i], arrays["dhi"][i] = row.ghi, row.dni, row.dhi
            arrays["temp_c"][i], arrays["wind10"][i] = row.temp, row.wind10
        for name, arr in arrays.items():
            if np.isnan(arr).any():
                raise ValueError(f"observed truth left NaN in {name}")
        return dataclasses.replace(truth, **arrays)

    def masks(self, sites: SiteArrays, axis: TimeAxis) -> dict[str, dict[str, B]]:
        _truth, rows = self._rows(sites, axis)
        return {loc: row.masks for loc, row in rows.values()}

    def diagnostics(self, sites: SiteArrays, axis: TimeAxis) -> dict[str, dict[str, float]]:
        _truth, rows = self._rows(sites, axis)
        return {
            loc: {"era5_clip_share": row.clip_share, "low_sun_zero_share": row.low_sun_zero_share}
            for loc, row in rows.values()
        }


def _slot_clear_sky(labels: I64, sites: SiteArrays, i: int, axis: TimeAxis) -> F64:
    """Clear-sky GHI at the midpoint of each 10-minute slot that touches the axis (0 elsewhere)."""
    lo = int(axis.t[0]) * QUARTER_S
    hi = (int(axis.t[-1]) + 1) * QUARTER_S + SLOT_S
    near = (labels > lo) & (labels <= hi)
    out = np.zeros(labels.size, dtype=np.float64)
    if not near.any():
        return out
    mid = (labels[near] - SLOT_S // 2).astype(np.float64)
    doy, month, _hour = axis.calendar(mid)
    geom = solar_geometry(
        mid,
        doy,
        month,
        sites.lat[i : i + 1],
        sites.lon[i : i + 1],
        sites.elevation_m[i : i + 1],
    )
    out[near] = geom.cs_ghi[0]
    return out
