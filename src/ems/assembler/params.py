"""The parameters of a run for one EMS-managed household (ADR 0019).

The operator sends one ``RunParams`` to every object of the household (``configure``) before data
flows; each object stores it once, so a config edit or a later deployment cannot change a running
household (P14). The offline side takes the same record.

Version 1.1.0 (spec 012) adds ``mode``: in ``live`` a wall-clock deadline closes each quarter-hour
``grace_s`` seconds after it ends (ADR 0020); in ``sim`` no clock is read. A 1.0.0 record decodes
with the defaults.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from ems.domain.actions import POLICIES
from ems.domain.netbilling import RcemTable
from ems.domain.reference import SiteModel
from ems.domain.units import QUARTERS_PER_HOUR

MAX_LATE_SLOTS = 48  # keeps the lag (2L) far below a month, so at most one month closes per step
MAX_GRACE_S = 3600
MODES = ("sim", "live")


@dataclass(frozen=True)
class RunParams:
    run_id: str | None
    location_id: str
    household_key: str  # [run_id:]location_id: the key of every object of the household
    market_key: str  # [run_id:]PL:RCE: the key of its Tariff
    first_step: int  # on a UTC hour
    end_step: int  # readings at or after it are ignored, so no step leaves the validated window
    late_slots: int  # L: how many slots a meter stream waits for a missing reading
    strategies: tuple[str, ...]  # services that receive every step
    live: str  # the strategy Ems runs on the real battery
    has_pv: bool  # without PV there is no production stream
    initial_soc_kwh: float
    model: SiteModel
    rcem: RcemTable
    mode: Literal["sim", "live"] = "sim"
    grace_s: int = 120  # live: a quarter-hour is closed this long after it ends
    schema_version: str = "1.1.0"

    def __post_init__(self) -> None:
        def fail(msg: str) -> ValueError:
            return ValueError(f"{self.location_id}: {msg}")

        if self.first_step % QUARTERS_PER_HOUR:
            raise fail("first_step must start a UTC hour")
        if self.end_step <= self.first_step:
            raise fail("end_step must come after first_step")
        if self.mode not in MODES:
            raise fail(f"mode must be one of {MODES}, not {self.mode!r}")
        if not 0 <= self.grace_s <= MAX_GRACE_S:
            raise fail(f"grace_s must be within 0…{MAX_GRACE_S}")
        if not 1 <= self.late_slots <= MAX_LATE_SLOTS:
            raise fail(f"late_slots must be within 1…{MAX_LATE_SLOTS}")
        unknown = [s for s in self.strategies if s not in POLICIES]
        if unknown:
            raise fail(f"unknown strategies {unknown}")
        if self.live not in self.strategies:
            raise fail(f"live strategy {self.live!r} is not among the strategies")
        b = self.model.battery
        if not b.soc_min_kwh <= self.initial_soc_kwh <= b.soc_max_kwh:
            raise fail(f"initial SoC {self.initial_soc_kwh} kWh outside its bounds")
        if self.location_id != self.model.location_id:
            raise fail(f"site model is for {self.model.location_id}")
        if self.household_key.rsplit(":", 1)[-1] != self.location_id:
            raise fail(f"household key {self.household_key!r} names another location")

    @property
    def lag(self) -> int:
        """Quarter-hours a settled hour stays open to correction: 2L."""
        return 2 * self.late_slots
