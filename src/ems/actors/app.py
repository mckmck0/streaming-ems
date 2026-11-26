"""The SDK service: every Virtual Object of the EMS, served by hypercorn (`make dev`).

Stage 3 (spec 011): the actors of `01-architecture.md`, one service per strategy of ``POLICIES``.
The ingress containers feed ``Household``, ``Weather`` and ``Tariff``.
"""

from __future__ import annotations

import restate

from ems.actors import timing
from ems.actors.battery import battery
from ems.actors.ems import ems
from ems.actors.household import household
from ems.actors.meter import meter
from ems.actors.strategy import strategy_object
from ems.actors.tariff import tariff
from ems.actors.weather import weather
from ems.domain.actions import POLICIES

SERVICES = (
    household,
    tariff,
    weather,
    *(strategy_object(name) for name in POLICIES),
    ems,
    battery,
    meter,
)

app = restate.app(services=list(SERVICES))

# Spec 012: the timing log, on only when EMS_TIMING_LOG names a file.
timing.start()
