"""Fit synthetic-model parameters from history into *candidate* calibration files.

Candidates land in ``config/calibration/candidates/`` with their data window and fit statistics;
adopting them (copying values into ``prices.yaml`` / ``weather.yaml``) is a reviewed change, never
automatic. Quarter-hour price dynamics are fitted only on data from 2025-10-01 (earlier RCE is
hourly in effect).
"""

from __future__ import annotations

import json
from collections import defaultdict
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
