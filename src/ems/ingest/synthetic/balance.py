"""Site energy balance for sites whose battery is not controlled by the EMS.

grid_import - grid_export = loads + bess_charge - (generation + bess_discharge), split into two
non-negative flows. EMS-managed sites never get grid flows from the generator: the EMS computes
them.
"""

from __future__ import annotations

import numpy as np
from numpy.typing import NDArray

F64 = NDArray[np.float64]


def grid_flows(
    load_kwh: F64,
    generation_kwh: F64,
    charge_kwh: F64 | None = None,
    discharge_kwh: F64 | None = None,
) -> tuple[F64, F64]:
    zero = np.zeros_like(load_kwh)
    net = (
        load_kwh
        + (charge_kwh if charge_kwh is not None else zero)
        - generation_kwh
        - (discharge_kwh if discharge_kwh is not None else zero)
    )
    return np.clip(net, 0.0, None), np.clip(-net, 0.0, None)
