"""The parameters of a run for every EMS-managed household of the generator's catalog (ADR 0019).

The same records go to the actors (``configure``) and to the offline fold, so both sides settle
the same sites with the same tariffs. Everything that could make a handler raise later is checked
here, before anything is sent: the site can be settled, the parameters are consistent, and every
step of the window has a tariff version and, for an RCEm site, an RCEm entry.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal

from ems.assembler.params import RunParams
from ems.contracts.time import quarter_index
from ems.domain.calendar import civil_month
from ems.domain.netbilling import PriceBasis, RcemTable, basis_for
from ems.domain.reference import prices
from ems.domain.reference import rcem_used as reference_rcem_used
from ems.domain.tariff import fixed_charges
from ems.ingest.generator import load_generator_config, load_sites
from ems.tariffs import load_book, load_rcem, site_model

REPO = Path(__file__).resolve().parents[3]
CONFIG = REPO / "config"
STRATEGIES = ("NoBattery", "SelfConsumption")
LIVE = "SelfConsumption"
MARKET = "PL:RCE"


def scoped(run_id: str | None, subject: str) -> str:
    """The key of an object of the run: ``[run_id:]subject``, as the record keys are."""
    return subject if run_id is None else f"{run_id}:{subject}"


def managed_params(
    *,
    run_id: str | None,
    start: datetime,
    days: float,
    locations: int,
    rcem: RcemTable,
    late_slots: int = 8,
    strategies: Sequence[str] = STRATEGIES,
    live: str = LIVE,
    mode: Literal["sim", "live"] = "sim",
    grace_s: int = 120,
    generator_config: Path = CONFIG / "generator.yaml",
    config_dir: Path = CONFIG,
) -> dict[str, RunParams]:
    """``{household_key: RunParams}`` for the EMS-managed sites of a run of ``ems-gen run``.

    ``rcem`` is the run's RCEm table: ``pse_table`` for real-data runs and the tests, the proxy of
    ``ems.experiments.rcem`` for synthetic ones (spec 012). A live run refuses a site that settles
    on RCEm in a month of its window: no RCEm of the running month exists before the 11th of the
    next one.
    """
    cfg = dataclasses.replace(
        load_generator_config(generator_config), synthetic_locations=locations
    )
    sites = load_sites(config_dir, cfg)
    book = load_book(config_dir / "tariffs")
    first, end = quarter_index(start), quarter_index(start + timedelta(days=days))
    out: dict[str, RunParams] = {}
    for site in sites:
        if not site.ems_managed or site.bess is None:
            continue
        model = site_model(site, book, (first, end))
        key = scoped(run_id, site.location_id)
        params = RunParams(
            run_id=run_id,
            location_id=site.location_id,
            household_key=key,
            market_key=scoped(run_id, MARKET),
            first_step=first,
            end_step=end,
            late_slots=late_slots,
            strategies=tuple(strategies),
            live=live,
            has_pv=site.pv is not None,
            initial_soc_kwh=site.bess.capacity_kwh / 2,
            model=model,
            rcem=rcem,
            mode=mode,
            grace_s=grace_s,
        )
        if mode == "live" and _rcem_basis_months(params):
            raise ValueError(
                f"{site.location_id}: settles on RCEm in the window; a live run cannot price it "
                "(no RCEm of the running month exists yet)"
            )
        _check_window(params)
        out[key] = params
    return out


def _check_window(p: RunParams) -> None:
    """Every step of the window can be priced and every month settled (a raise here, not later in a
    handler that would block its key).
    """
    months: list[str] = []
    for t in range(p.first_step, p.end_step):
        prices(p.model, p.rcem, t, 0.0)
        month = civil_month(t)
        if month not in months:
            months.append(month)
    for month in months:
        fixed_charges(p.model.tariff, month, p.model.fixed_inputs)


def pse_table(config_dir: Path = CONFIG) -> RcemTable:
    """The RCEm values PSE published (``config/tariffs/rcem.yaml``)."""
    return RcemTable(tuple(load_rcem(config_dir / "tariffs" / "rcem.yaml")))


def _rcem_basis_months(p: RunParams) -> list[str]:
    months: list[str] = []
    for t in range(p.first_step, p.end_step):
        month = civil_month(t)
        if month not in months and basis_for(p.model.cohort, month) is PriceBasis.RCEM:
            months.append(month)
    return months


def rcem_used(params: Mapping[str, RunParams]) -> list[dict[str, Any]]:
    """The RCEm entries the run's RCEm-basis sites read (the signal's month and each month's own
    entry, ``reference.rcem_used``), for the manifest; empty when every site settles on RCE.
    """
    seen: dict[str, dict[str, Any]] = {}
    for p in params.values():
        for t in range(p.first_step, p.end_step):
            if basis_for(p.model.cohort, civil_month(t)) is not PriceBasis.RCEM:
                continue
            signal, _pub, own = reference_rcem_used(p.rcem, t)
            for entry in (signal, own):
                seen.setdefault(
                    entry.month,
                    {"month": entry.month, "value": entry.billing_value, "source": entry.source},
                )
    return [seen[m] for m in sorted(seen)]
