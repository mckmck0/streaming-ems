"""Units of the domain core: the quarter-hour step and the one PLN/MWh → PLN/kWh conversion.

Wire prices are PLN/MWh; the domain works in PLN/kWh. ``pln_per_mwh_to_pln_per_kwh`` is the only
place that divides by 1000 (AC-009-23).
"""

from __future__ import annotations

DT_H = 0.25  # length of one interval t, hours
QUARTERS_PER_HOUR = 4


def pln_per_mwh_to_pln_per_kwh(price_pln_per_mwh: float) -> float:
    return price_pln_per_mwh / 1000.0
