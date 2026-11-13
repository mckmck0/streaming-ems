"""Tariff and RCEm config: sourced YAML, validated at the edge, converted to domain objects.

Layout (spec 009, plan §3): ``<root>/2026/national.yaml`` plus one file per DSO with its operator
and incumbent seller, and ``<root>/rcem.yaml``. Loading fails fast (P11) with the file and field
path when a value has no ``ref``, a version no ``source``, a component, zone or unit is unknown, or
versions overlap, leave a gap or do not start on the 1st of a month.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, ValidationError
from ruamel.yaml import YAML

from ems.domain.calendar import DayType, ZoneRule, ZoneScheme, month_of
from ems.domain.netbilling import NetBillingRules, Publication, RcemEntry, RcemTable
from ems.domain.tariff import National, OperatorVersion, QualityPeriod, SellerVersion, SiteTariff
from ems.domain.units import pln_per_mwh_to_pln_per_kwh

YEAR = 2026
GROUPS = ("G11", "G12", "G12w")
Text = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class TariffConfigError(ValueError):
    """A tariff or RCEm file is invalid; the message names the file and the field."""


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class _Value(_Model):
    value: float
    ref: Text


class PerKwh(_Value):
    unit: Literal["PLN/kWh"]


class PerMwh(_Value):
    unit: Literal["PLN/MWh"]


class PerMonth(_Value):
    unit: Literal["PLN/month"]


class Fraction(_Value):
    unit: Literal["fraction"]


class Factor(_Value):
    unit: Literal["factor"]


class Months(_Model):
    value: int
    unit: Literal["months"]
    ref: Text


class Source(_Model):
    document: Text
    url: Text
    checked: date


MonthDay = Annotated[str, StringConstraints(pattern=r"^(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$")]
ZONE_NAMES = {"G11": ("all",), "G12": ("day", "night"), "G12w": ("peak", "offpeak")}


class Rule(_Model):
    zone: Text
    days: list[DayType] = Field(min_length=1)
    hours: list[tuple[int, int]] = Field(min_length=1)
    season: tuple[MonthDay, MonthDay] | None = None


class Scheme(_Model):
    default: Text
    rules: list[Rule] = Field(default_factory=list)


class Zones(_Model):
    ref: Text
    G11: Scheme
    G12: Scheme
    G12w: Scheme


class OperatorGroup(_Model):
    variable_fee: dict[str, PerKwh]
    fixed_fee: PerMonth
    subscription_fee: PerMonth


class OperatorVersionCfg(_Model):
    id: Text
    valid_from: date
    source: Source
    groups: dict[Literal["G11", "G12", "G12w"], OperatorGroup]


class Operator(_Model):
    name: Text
    zones: Zones
    versions: list[OperatorVersionCfg] = Field(min_length=1)


class SellerGroup(_Model):
    energy: dict[str, PerKwh]


class SellerVersionCfg(_Model):
    id: Text
    valid_from: date
    source: Source
    groups: dict[Literal["G11", "G12", "G12w"], SellerGroup]


class Seller(_Model):
    name: Text
    versions: list[SellerVersionCfg] = Field(min_length=1)


class DsoFile(_Model):
    dso: Text
    operator: Operator
    seller: Seller


class QualityCfg(_Model):
    id: Text
    valid_from: date
    rate: PerKwh


class Bands(_Model):
    value: tuple[float, float, float]
    unit: Literal["kWh"]
    ref: Text


class BandRates(_Model):
    value: tuple[float, float, float, float]
    unit: Literal["PLN/month"]
    ref: Text


class Capacity(_Model):
    bands: Bands
    rates: BandRates


class Holidays(_Model):
    ref: Text
    dates: list[date]


class NetBillingCfg(_Model):
    deposit_factor: Factor
    refund_cap_rcem: Fraction
    refund_cap_rce: Fraction
    validity_months: Months


class NationalFile(_Model):
    year: int
    valid_until: date
    sources: list[Source] = Field(min_length=1)
    vat: Fraction
    excise: PerMwh
    oze_fee: PerMwh
    cogeneration_fee: PerMwh
    quality_fee: list[QualityCfg] = Field(min_length=1)
    capacity_fee: Capacity
    holidays: Holidays
    net_billing: NetBillingCfg


class RcemPublication(_Model):
    value: float
    unit: Literal["PLN/MWh"]
    published: date
    ref: Text


class RcemMonth(_Model):
    month: Annotated[str, StringConstraints(pattern=r"^\d{4}-(0[1-9]|1[0-2])$")]
    publications: list[RcemPublication] = Field(min_length=1)


class RcemFile(_Model):
    source: Source
    months: list[RcemMonth] = Field(min_length=1)


# ── loading ──────────────────────────────────────────────────────────────────────────────────────


def _read(path: Path) -> Any:
    try:
        return YAML(typ="safe").load(path.read_text())
    except Exception as exc:  # pragma: no cover - YAML syntax errors
        raise TariffConfigError(f"{path.name}: cannot parse: {exc}") from exc


def _parse[M: BaseModel](cls: type[M], path: Path) -> M:
    try:
        return cls.model_validate(_read(path))
    except ValidationError as exc:
        err = exc.errors()[0]
        loc = ".".join(str(x) for x in err["loc"])
        raise TariffConfigError(f"{path.name}: {loc}: {err['msg']}") from exc


def _fail(path: Path, field: str, msg: str) -> TariffConfigError:
    return TariffConfigError(f"{path.name}: {field}: {msg}")


def _check_versions(path: Path, field: str, starts: list[date], until: date) -> None:
    """Versions are half-open from ``valid_from`` to the next one; together they cover the year."""
    for i, d in enumerate(starts):
        if d.day != 1:
            raise _fail(path, f"{field}[{i}].valid_from", f"{d} is not the 1st of a month")
        if not date(YEAR, 1, 1) <= d < until:
            raise _fail(path, f"{field}[{i}].valid_from", f"{d} is outside {YEAR}")
    if starts[0] != date(YEAR, 1, 1):
        raise _fail(path, f"{field}[0].valid_from", f"gap: {YEAR}-01-01 … {starts[0]} not covered")
    for i in range(1, len(starts)):
        if starts[i] <= starts[i - 1]:
            raise _fail(path, f"{field}[{i}].valid_from", "overlaps the previous version")


def _scheme(path: Path, group: str, s: Scheme) -> ZoneScheme:
    field = f"operator.zones.{group}"
    rules = []
    for i, r in enumerate(s.rules):
        try:
            rules.append(
                ZoneRule(
                    r.zone,
                    tuple(r.days),
                    tuple((a, b) for a, b in r.hours),
                    (r.season[0], r.season[1]) if r.season else None,
                )
            )
        except ValueError as exc:
            raise _fail(path, f"{field}.rules[{i}]", str(exc)) from exc
    scheme = ZoneScheme(s.default, tuple(rules))
    if sorted(scheme.zones) != sorted(ZONE_NAMES[group]):
        raise _fail(path, field, f"zones {list(scheme.zones)}; {group} needs {ZONE_NAMES[group]}")
    return scheme


def _rates(
    path: Path, field: str, values: dict[str, PerKwh], zones: tuple[str, ...]
) -> tuple[tuple[str, float], ...]:
    for name in values:
        if name not in zones:
            raise _fail(path, f"{field}.{name}", f"unknown zone; the scheme has {list(zones)}")
    for z in zones:
        if z not in values:
            raise _fail(path, field, f"missing zone {z!r}")
    return tuple((z, values[z].value) for z in zones)


@dataclass(frozen=True)
class DsoTariffs:
    dso: str
    operator: str
    seller: str
    schemes: tuple[tuple[str, ZoneScheme], ...]
    operator_versions: tuple[tuple[str, tuple[OperatorVersion, ...]], ...]
    seller_versions: tuple[tuple[str, tuple[SellerVersion, ...]], ...]


def _by_group[V](pairs: tuple[tuple[str, V], ...], group: str) -> V:
    for g, v in pairs:
        if g == group:
            return v
    raise KeyError(group)


def _load_dso(path: Path, until: date) -> DsoTariffs:
    cfg = _parse(DsoFile, path)
    zones = cfg.operator.zones
    schemes = {g: _scheme(path, g, getattr(zones, g)) for g in GROUPS}
    _check_versions(path, "operator.versions", [v.valid_from for v in cfg.operator.versions], until)
    _check_versions(path, "seller.versions", [v.valid_from for v in cfg.seller.versions], until)
    ops: dict[str, list[OperatorVersion]] = {g: [] for g in GROUPS}
    for i, v in enumerate(cfg.operator.versions):
        for g in GROUPS:
            field = f"operator.versions[{i}].groups.{g}"
            if g not in v.groups:
                raise _fail(path, field, "missing group")
            grp = v.groups[g]
            ops[g].append(
                OperatorVersion(
                    id=v.id,
                    valid_from=month_of(v.valid_from),
                    variable_fee_pln_per_kwh=_rates(
                        path, f"{field}.variable_fee", grp.variable_fee, schemes[g].zones
                    ),
                    fixed_fee_pln_per_month=grp.fixed_fee.value,
                    subscription_fee_pln_per_month=grp.subscription_fee.value,
                )
            )
    sels: dict[str, list[SellerVersion]] = {g: [] for g in GROUPS}
    for i, v in enumerate(cfg.seller.versions):
        for g in GROUPS:
            field = f"seller.versions[{i}].groups.{g}"
            if g not in v.groups:
                raise _fail(path, field, "missing group")
            sels[g].append(
                SellerVersion(
                    id=v.id,
                    valid_from=month_of(v.valid_from),
                    energy_pln_per_kwh=_rates(
                        path, f"{field}.energy", v.groups[g].energy, schemes[g].zones
                    ),
                )
            )
    return DsoTariffs(
        dso=cfg.dso,
        operator=cfg.operator.name,
        seller=cfg.seller.name,
        schemes=tuple((g, schemes[g]) for g in GROUPS),
        operator_versions=tuple((g, tuple(ops[g])) for g in GROUPS),
        seller_versions=tuple((g, tuple(sels[g])) for g in GROUPS),
    )


def _load_national(path: Path) -> tuple[National, NetBillingRules]:
    cfg = _parse(NationalFile, path)
    if cfg.year != YEAR or cfg.valid_until != date(YEAR + 1, 1, 1):
        raise _fail(path, "year", f"only {YEAR} is modelled (valid until {YEAR + 1}-01-01)")
    _check_versions(path, "quality_fee", [q.valid_from for q in cfg.quality_fee], cfg.valid_until)
    nb = cfg.net_billing
    national = National(
        vat=cfg.vat.value,
        excise_pln_per_kwh=pln_per_mwh_to_pln_per_kwh(cfg.excise.value),
        oze_fee_pln_per_kwh=pln_per_mwh_to_pln_per_kwh(cfg.oze_fee.value),
        cogeneration_fee_pln_per_kwh=pln_per_mwh_to_pln_per_kwh(cfg.cogeneration_fee.value),
        quality=tuple(
            QualityPeriod(q.id, month_of(q.valid_from), q.rate.value) for q in cfg.quality_fee
        ),
        capacity_band_kwh=cfg.capacity_fee.bands.value,
        capacity_fee_pln_per_month=cfg.capacity_fee.rates.value,
        holidays=frozenset(cfg.holidays.dates),
        valid_until=month_of(cfg.valid_until),
    )
    rules = NetBillingRules(
        deposit_factor=nb.deposit_factor.value,
        refund_cap_rcem=nb.refund_cap_rcem.value,
        refund_cap_rce=nb.refund_cap_rce.value,
        validity_months=nb.validity_months.value,
    )
    return national, rules


@dataclass(frozen=True)
class TariffBook:
    national: National
    rules: NetBillingRules
    dsos: tuple[DsoTariffs, ...]

    @property
    def holidays(self) -> frozenset[date]:
        return self.national.holidays

    def dso(self, name: str) -> DsoTariffs:
        for d in self.dsos:
            if d.dso == name:
                return d
        raise KeyError(f"no tariff for DSO {name!r}")

    def scheme(self, dso: str, group: str) -> ZoneScheme:
        return _by_group(self.dso(dso).schemes, group)

    def site_tariff(self, dso: str, group: str) -> SiteTariff:
        d = self.dso(dso)
        return SiteTariff(
            operator=d.operator,
            seller=d.seller,
            group=group,
            scheme=_by_group(d.schemes, group),
            operator_versions=_by_group(d.operator_versions, group),
            seller_versions=_by_group(d.seller_versions, group),
            national=self.national,
        )


def load_book(root: Path) -> TariffBook:
    """Load ``<root>/2026/``: ``national.yaml`` and every DSO file next to it."""
    folder = root / str(YEAR)
    national_path = folder / "national.yaml"
    if not national_path.exists():
        raise TariffConfigError(f"{national_path}: missing")
    national, rules = _load_national(national_path)
    until = date(YEAR + 1, 1, 1)
    paths = [p for p in sorted(folder.glob("*.yaml")) if p.name != "national.yaml"]
    dsos = tuple(_load_dso(p, until) for p in paths)
    names = [d.dso for d in dsos]
    if len(names) != len(set(names)):
        raise TariffConfigError(f"{folder}: a DSO is defined twice: {names}")
    owners = [(q.id, national_path.name) for q in national.quality]
    for p, d in zip(paths, dsos, strict=True):  # every group carries its version's id: use one
        owners += [(v.id, p.name) for v in d.operator_versions[0][1]]
        owners += [(v.id, p.name) for v in d.seller_versions[0][1]]
    ids = [i for i, _ in owners]
    for vid, name in owners:
        if ids.count(vid) > 1:
            raise TariffConfigError(f"{name}: version ids must be unique, repeated: {vid!r}")
    return TariffBook(national, rules, dsos)


def load_rcem(path: Path) -> tuple[RcemEntry, ...]:
    """PSE's published RCEm, every publication of each month (value converted to PLN/kWh)."""
    cfg = _parse(RcemFile, path)
    months = [m.month for m in cfg.months]
    if months != sorted(set(months)):
        raise _fail(path, "months", "months must be unique and in order")
    out: list[RcemEntry] = []
    for i, m in enumerate(cfg.months):
        try:
            out.append(
                RcemEntry(
                    m.month,
                    tuple(
                        Publication(pln_per_mwh_to_pln_per_kwh(p.value), p.published)
                        for p in m.publications
                    ),
                    "pse",
                )
            )
        except ValueError as exc:
            raise _fail(path, f"months[{i}]", str(exc)) from exc
    try:
        RcemTable(tuple(out))
    except ValueError as exc:
        raise _fail(path, "months", str(exc)) from exc
    return tuple(out)
