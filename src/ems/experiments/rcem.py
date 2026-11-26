"""The proxy RCEm table of a synthetic run (spec 012, AC-012-24; risk D4).

Synthetic prices are not PSE's, so the monthly price RCEm-basis prosumers are paid comes from the
same synthetic data: the run's daily RCE weighted by its national PV index, one entry per civil
month "published" on the 11th of the next month (``ems.tariffs.sites.proxy_rcem``). The table covers
every month the run's price signal and settlement read: from two months before the window's first
month (the signal of a day reads the newest month published before it) through the month of its last
step.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from ems.contracts.time import local_day_bounds, quarter_index
from ems.domain.calendar import civil_month, month_add
from ems.domain.netbilling import RcemTable
from ems.domain.units import pln_per_mwh_to_pln_per_kwh
from ems.ingest.generator import GeneratorConfig, SyntheticModel, load_sites
from ems.ingest.synthetic.prices import day_prices, national_pv_index
from ems.tariffs.sites import proxy_rcem


def proxy_table(
    cfg: GeneratorConfig, config_dir: Path, first_step: int, end_step: int
) -> RcemTable:
    """Whole civil months from two before ``civil_month(first_step)`` through
    ``civil_month(end_step - 1)``, with the run's seed.
    """
    first_month = month_add(civil_month(first_step), -2)
    last_month = civil_month(end_step - 1)
    first_day = _day(first_month)
    last_day = _day(month_add(last_month, 1)) - timedelta(days=1)
    synth = SyntheticModel(load_sites(config_dir, cfg), cfg.seed, config_dir)
    rce: list[float] = []
    weights: list[float] = []
    day = first_day
    while day <= last_day:
        rce += [
            pln_per_mwh_to_pln_per_kwh(float(p))
            for p in day_prices(day, cfg.seed, synth.price_params, synth.weather_params)
        ]
        start, stop = local_day_bounds(day)
        t0, t1 = quarter_index(start), quarter_index(stop)
        weights += [
            float(w)
            for w in national_pv_index(
                t0, t1 - t0, cfg.seed, synth.price_params, synth.weather_params
            )
        ]
        day += timedelta(days=1)
    return proxy_rcem(quarter_index(local_day_bounds(first_day)[0]), rce, weights)


def _day(month: str) -> date:
    return date(int(month[:4]), int(month[5:]), 1)
