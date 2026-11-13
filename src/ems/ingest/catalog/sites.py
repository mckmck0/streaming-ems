"""Site catalog: metadata of every location (coordinates, metering point, tariff, assets)."""

from __future__ import annotations

from datetime import date
from enum import StrEnum
from typing import Annotated, Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ems.contracts.ids import LOCATION_PATTERN, is_valid_gsrn


class SiteType(StrEnum):
    HOUSEHOLD = "household"
    COMMERCIAL = "commercial"
    SOLAR_FARM = "solar_farm"


class TariffGroup(StrEnum):
    G11 = "G11"
    G12 = "G12"
    G12W = "G12w"  # weekend tariff; the generator never assigns it, hand-defined sites may
    C11 = "C11"
    C21 = "C21"


# BDEW 2025 standard profile per tariff group (G-tariffs: households; C-tariffs: commerce).
PROFILE_FOR_TARIFF = {
    TariffGroup.G11: "H25",
    TariffGroup.G12: "H25",
    TariffGroup.G12W: "H25",
    TariffGroup.C11: "G25",
    TariffGroup.C21: "G25",
}


def tariff_for(
    site_type: SiteType, contracted_kw: float, fuse_a: float, two_zone: bool
) -> TariffGroup:
    """C11 needs contracted power <= 40 kW and a fuse <= 63 A (low voltage, one zone); else C21."""
    if site_type is SiteType.HOUSEHOLD:
        return TariffGroup.G12 if two_zone else TariffGroup.G11
    if contracted_kw <= 40 and fuse_a <= 63:
        return TariffGroup.C11
    return TariffGroup.C21


class NetBilling(BaseModel):
    """Net-billing cohort of an EMS-managed site (spec 009): first feed-in and an optional switch to
    RCE from a month (``YYYY-MM``). The domain (``ems.domain.netbilling.Cohort``) checks the rules.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)
    first_feed_in: date
    rce_from: str | None = Field(default=None, pattern=r"^\d{4}-\d{2}$")


class _Asset(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    asset_id: str = Field(pattern=r"^[A-Za-z0-9._-]+$")


class PvAsset(_Asset):
    kind: Literal["pv"] = "pv"
    kwp: float = Field(gt=0)
    tilt_deg: float = Field(ge=0, le=90)
    azimuth_deg: float = Field(ge=0, lt=360)  # 180 = south
    ac_kw: float = Field(gt=0)
    gamma_pdc: float = -0.0037
    losses: float = Field(default=0.10, ge=0, lt=1)


class HeatPumpAsset(_Asset):
    kind: Literal["heat_pump"] = "heat_pump"
    heat_loss_kw_per_k: float = Field(gt=0)  # building heat loss coefficient
    dhw_kwh_per_year: float = Field(ge=0)  # domestic hot water (thermal)
    base_temp_c: float = 15.0
    carnot_eta: float = Field(default=0.45, gt=0, lt=1)


class BessAsset(_Asset):
    kind: Literal["bess"] = "bess"
    capacity_kwh: float = Field(gt=0)
    p_max_kw: float = Field(gt=0)
    eta: float = Field(default=0.95, gt=0, le=1)  # one-way, charge and discharge
    soc_min_kwh: float = Field(ge=0)
    soc_max_kwh: float = Field(gt=0)
    control: Literal["ems", "site"]

    @model_validator(mode="after")
    def _bounds(self) -> Self:
        if not self.soc_min_kwh < self.soc_max_kwh <= self.capacity_kwh:
            raise ValueError("need soc_min < soc_max <= capacity")
        return self


class EvChargerAsset(_Asset):
    kind: Literal["ev_charger"] = "ev_charger"
    p_max_kw: float = Field(gt=0)
    daily_need_kwh: float = Field(gt=0)  # median energy need per session
    pattern: Literal["overnight", "daytime"] = "overnight"


Asset = Annotated[
    PvAsset | HeatPumpAsset | BessAsset | EvChargerAsset,
    Field(discriminator="kind"),
]
AssetModel = PvAsset | HeatPumpAsset | BessAsset | EvChargerAsset


class Site(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    location_id: str = Field(pattern=LOCATION_PATTERN.pattern)
    site_type: SiteType
    lat: float = Field(ge=49.0, le=54.9)
    lon: float = Field(ge=14.1, le=24.2)
    elevation_m: float = Field(ge=-10, le=2500)
    voivodeship: str = Field(pattern=r"^\d{2}$")  # TERYT code
    city: str
    street: str
    postcode: str = Field(pattern=r"^\d{2}-\d{3}$")
    dso: str
    metering_point_id: str
    tariff_group: TariffGroup
    contracted_kw: float = Field(gt=0)
    fuse_a: float = Field(gt=0)
    annual_base_load_kwh: float = Field(gt=0)  # excludes heat pump and EV
    assets: list[Asset] = Field(default_factory=list)
    seed: int = Field(ge=0)
    net_billing: NetBilling | None = None  # config, not wire contract (spec 009)

    @model_validator(mode="after")
    def _check(self) -> Self:
        if not is_valid_gsrn(self.metering_point_id):
            raise ValueError("metering_point_id must be a valid 18-digit GSRN")
        ids = [a.asset_id for a in self.assets]
        if len(ids) != len(set(ids)):
            raise ValueError("asset ids must be unique per site")
        return self

    def first(self, kind: str) -> AssetModel | None:
        return next((a for a in self.assets if a.kind == kind), None)

    @property
    def pv(self) -> PvAsset | None:
        a = self.first("pv")
        return a if isinstance(a, PvAsset) else None

    @property
    def heat_pump(self) -> HeatPumpAsset | None:
        a = self.first("heat_pump")
        return a if isinstance(a, HeatPumpAsset) else None

    @property
    def bess(self) -> BessAsset | None:
        a = self.first("bess")
        return a if isinstance(a, BessAsset) else None

    def typed(self, cls: type[_Asset]) -> Any:
        return next((a for a in self.assets if isinstance(a, cls)), None)

    @property
    def ev(self) -> EvChargerAsset | None:
        return self.typed(EvChargerAsset)

    @property
    def ems_managed(self) -> bool:
        """The EMS controls this site's battery: battery and grid flows are the EMS's facts."""
        b = self.bess
        return b is not None and b.control == "ems"

    @property
    def profile(self) -> str:
        return PROFILE_FOR_TARIFF[self.tariff_group]
