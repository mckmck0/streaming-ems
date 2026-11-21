"""Messages between actors (internal, not wire contract; `08-contracts.md`).

Every message carries ``schema_version`` and ``t`` (R7): the step it belongs to, or, for a stream
update, the stream's watermark. ``t`` is the simulated time; there is no separate timestamp. Price
and forecast updates also carry ``seq``, the stream's number of the newest record they hold. Within
version 1 changes are additive: a new field must be optional with a default.

``Step`` (household to strategies and Ems) and ``RunParams`` (the body of ``configure``) live in the
assembler, because the offline side uses them too.
"""

from __future__ import annotations

from dataclasses import dataclass

from ems.assembler.sparse_streams import Curve, Run
from ems.domain.actions import Action
from ems.domain.settlement import Correction

SCHEMA_VERSION = "1.0.0"


@dataclass(frozen=True)
class PriceUpdate:
    """Tariff to Household: a snapshot of every accepted curve on subscribe, then one per curve."""

    t: int  # the price stream's watermark
    seq: int  # the number of the newest curve it holds (the stream's count for a snapshot)
    snapshot: bool
    curves: tuple[Curve, ...]
    schema_version: str = SCHEMA_VERSION


@dataclass(frozen=True)
class ForecastUpdate:
    """Weather to Household: one accepted run (issue time and visibility; no payload in stage 3)."""

    t: int  # the forecast stream's watermark
    seq: int
    run: Run
    schema_version: str = SCHEMA_VERSION


@dataclass(frozen=True)
class Subscribe:
    """Household to Tariff, once, when the household is configured."""

    t: int  # the household's first step
    household_key: str
    schema_version: str = SCHEMA_VERSION


@dataclass(frozen=True)
class Apply:
    """Ems to Battery: the action for step ``t`` and the flows it resolves against."""

    t: int
    action: Action
    pv_kwh: float
    load_kwh: float
    rce_pln_per_kwh: float
    corrections: tuple[Correction, ...]
    schema_version: str = SCHEMA_VERSION


@dataclass(frozen=True)
class Ack:
    """Battery to Ems: the real SoC at the end of step ``t``."""

    t: int
    soc_kwh: float
    schema_version: str = SCHEMA_VERSION


@dataclass(frozen=True)
class Flows:
    """Battery to Meter: what step ``t`` exchanged, and the corrections it carries."""

    t: int
    pv_kwh: float
    load_kwh: float
    p_kw: float  # the battery power executed, > 0 charges
    rce_pln_per_kwh: float
    corrections: tuple[Correction, ...]
    schema_version: str = SCHEMA_VERSION
