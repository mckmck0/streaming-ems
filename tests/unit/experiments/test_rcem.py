"""AC-012-24: the proxy RCEm table of a synthetic run (``ems.experiments.rcem.proxy_table``), the
PSE table (``runs.pse_table``) and the RCEm rules of ``runs.managed_params`` and
``runs.rcem_used``.

The expected proxy table is the one the spec 009 test helper builds: ``ref_window._daily`` (RCE and
PV-index weights per local day, as the generator emits them, with its ``SEED`` 4242, which is also
``config/generator.yaml``'s seed) passed to ``proxy_rcem``, over the same whole civil months.

Every catalog site settles on RCE (``DEFAULT_NET_BILLING`` and ``config/sites.yaml``), so the
RCEm-basis cases change PL-LOC-000001's cohort in a private copy of ``config/``: a first feed-in
of 2023-05-01 settles on RCEm until ``rce_from`` (AC-009-09). The window is 2026-06-03 x 1 day
over the 30-location catalog. The names that arrive with T27 are imported inside the tests.
"""

from __future__ import annotations

import dataclasses
import json
import shutil
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from ems.assembler.params import RunParams
from ems.contracts.time import quarter_index
from ems.domain.calendar import civil_month
from ems.domain.netbilling import Cohort, PriceBasis, RcemTable, basis_for
from ems.experiments.runs import CONFIG, scoped
from ems.ingest.catalog.build import load_overrides
from ems.ingest.generator import GeneratorConfig, SyntheticModel, load_generator_config, load_sites
from ems.tariffs import load_rcem, proxy_rcem
from tests.unit.domain import ref_window
from tests.unit.domain.ref_window import bits

RUN_ID = "rcem-1"
START = datetime(2026, 6, 3, tzinfo=UTC)
DAYS = 1.0
LOCATIONS = 30
JUNE_MONTHS = ("2026-04", "2026-05", "2026-06")
RCEM_SITE = "PL-LOC-000001"  # the site whose cohort the private config changes
RCEM_COHORT = "2023-05-01"  # first feed-in before 2024-07-01: RCEm until rce_from
OTHER_SEED = 7
SITES_LINE = "net_billing: {first_feed_in: 2024-09-01, rce_from: null}"  # in config/sites.yaml


def _proxy_table(cfg: GeneratorConfig, first: int, end: int) -> RcemTable:
    from ems.experiments.rcem import proxy_table

    return proxy_table(cfg, CONFIG, first, end)


def _managed_params(**kwargs: Any) -> dict[str, RunParams]:
    from ems.experiments.runs import managed_params

    args: dict[str, Any] = {"run_id": RUN_ID, "start": START, "days": DAYS, "locations": LOCATIONS}
    return managed_params(**(args | kwargs))


def _pse_table(config_dir: Path = CONFIG) -> RcemTable:
    from ems.experiments.runs import pse_table

    return pse_table(config_dir)


def _rcem_used(params: Mapping[str, RunParams]) -> list[dict[str, Any]]:
    from ems.experiments.runs import rcem_used

    return rcem_used(params)


def _cfg(seed: int = ref_window.SEED) -> GeneratorConfig:
    cfg = load_generator_config(CONFIG / "generator.yaml")
    return dataclasses.replace(cfg, synthetic_locations=LOCATIONS, seed=seed)


def _steps(start: datetime, days: float) -> tuple[int, int]:
    return quarter_index(start), quarter_index(start + timedelta(days=days))


def _month_days(first_month: str, last_month: str) -> tuple[date, date]:
    """The first local day of ``first_month`` and the last of ``last_month``."""
    y, m = int(last_month[:4]), int(last_month[5:])
    after = date(y + m // 12, m % 12 + 1, 1)
    return date(int(first_month[:4]), int(first_month[5:]), 1), after - timedelta(days=1)


def _helper_table(months: tuple[str, ...], seed: int) -> RcemTable:
    """The spec 009 helper's table over whole civil months ``months`` (``_daily`` reads
    ``ref_window.SEED``; a test that wants another seed patches it).
    """
    first, last = _month_days(months[0], months[-1])
    synth = SyntheticModel(load_sites(CONFIG, _cfg(seed)), seed, CONFIG)
    rce, weights = ref_window._daily(synth, first, last)
    return proxy_rcem(ref_window.local_t(first), rce, weights)


def _config_with_cohort(tmp: Path, rce_from: str | None) -> Path:
    """A private ``config/`` in which PL-LOC-000001 has the RCEm cohort, RCE from ``rce_from``."""
    dest = tmp / "config"
    shutil.copytree(CONFIG, dest)
    sites = dest / "sites.yaml"
    text = sites.read_text()
    assert text.count(SITES_LINE) == 1, "config/sites.yaml no longer has the expected cohort line"
    cohort = f"net_billing: {{first_feed_in: {RCEM_COHORT}, rce_from: {json.dumps(rce_from)}}}"
    sites.write_text(text.replace(SITES_LINE, cohort))
    return dest


def _months(start: datetime, days: float) -> set[str]:
    first, end = _steps(start, days)
    return {civil_month(t) for t in range(first, end)}


def _rcem_site_bases(config: Path, start: datetime) -> set[PriceBasis]:
    """The bases PL-LOC-000001 settles on over the window, from its cohort in ``config``."""
    nb = load_overrides(config / "sites.yaml", ref_window.SEED)[RCEM_SITE].net_billing
    assert nb is not None
    cohort = Cohort(nb.first_feed_in, nb.rce_from)
    return {basis_for(cohort, m) for m in _months(start, DAYS)}


@pytest.fixture(scope="module")
def june_table() -> RcemTable:
    """``proxy_table`` for the June window with the generator's seed (4242)."""
    return _proxy_table(_cfg(), *_steps(START, DAYS))


# ── proxy_table ──────────────────────────────────────────────────────────────────────────────────


def test_ac_012_24_proxy_table_equals_the_spec_009_helper_table(june_table: RcemTable) -> None:
    assert _cfg().seed == ref_window.SEED == 4242
    expected = _helper_table(JUNE_MONTHS, ref_window.SEED)
    assert june_table == expected
    assert bits(june_table) == bits(expected)  # equal bits, not just ==
    assert {e.source for e in june_table.entries} == {"proxy"}


def test_ac_012_24_proxy_table_uses_the_runs_seed(
    june_table: RcemTable, monkeypatch: pytest.MonkeyPatch
) -> None:
    other = _proxy_table(_cfg(OTHER_SEED), *_steps(START, DAYS))
    monkeypatch.setattr(ref_window, "SEED", OTHER_SEED)
    assert other == _helper_table(JUNE_MONTHS, OTHER_SEED)
    assert other != june_table


@pytest.mark.parametrize(
    ("start", "days", "months"),
    [
        (START, 1.0, JUNE_MONTHS),
        # 2026-06-01 00:00 Warsaw: the first step's civil month is June, not May (UTC).
        (datetime(2026, 5, 31, 22, tzinfo=UTC), 1.0, JUNE_MONTHS),
        # Ends at 2026-07-01 00:00 Warsaw: end_step is July's, end_step - 1 is still June's.
        (datetime(2026, 6, 29, 22, tzinfo=UTC), 1.0, JUNE_MONTHS),
        (datetime(2026, 6, 30, tzinfo=UTC), 2.0, (*JUNE_MONTHS, "2026-07")),
        (datetime(2026, 1, 2, tzinfo=UTC), 1.0, ("2025-11", "2025-12", "2026-01")),
    ],
    ids=["inside-june", "starts-on-june-1-local", "ends-on-july-1-local", "into-july", "new-year"],
)
def test_ac_012_24_proxy_table_covers_m_minus_2_through_the_last_steps_month(
    start: datetime, days: float, months: tuple[str, ...]
) -> None:
    table = _proxy_table(_cfg(), *_steps(start, days))
    assert tuple(e.month for e in table.entries) == months
    assert {e.source for e in table.entries} == {"proxy"}


# ── managed_params ───────────────────────────────────────────────────────────────────────────────


def test_ac_012_24_managed_params_requires_the_rcem_table() -> None:
    with pytest.raises(TypeError):
        _managed_params()


def test_ac_012_24_managed_params_with_the_proxy_table_passes_its_window_check(
    june_table: RcemTable,
) -> None:
    params = _managed_params(rcem=june_table)
    assert params
    for p in params.values():
        assert p.rcem == june_table
        assert (p.mode, p.grace_s) == ("sim", 120)


def test_ac_012_24_an_rcem_basis_site_settles_on_the_proxy_table_in_sim(
    june_table: RcemTable, tmp_path: Path
) -> None:
    config = _config_with_cohort(tmp_path, None)
    params = _managed_params(
        rcem=june_table, generator_config=config / "generator.yaml", config_dir=config
    )
    p = params[scoped(RUN_ID, RCEM_SITE)]
    assert basis_for(p.model.cohort, "2026-06") is PriceBasis.RCEM
    assert p.rcem == june_table


def test_ac_012_24_the_window_check_reads_the_given_table(
    june_table: RcemTable, tmp_path: Path
) -> None:
    """Without April, no proxy month is published before 2026-06-03 (May's on 2026-06-11)."""
    config = _config_with_cohort(tmp_path, None)
    without_april = RcemTable(june_table.entries[1:])
    with pytest.raises(ValueError):
        _managed_params(
            rcem=without_april, generator_config=config / "generator.yaml", config_dir=config
        )


def test_ac_012_24_mode_and_grace_go_into_every_run_params() -> None:
    params = _managed_params(rcem=_pse_table(), mode="live", grace_s=300)
    assert params
    assert {(p.mode, p.grace_s) for p in params.values()} == {("live", 300)}


@pytest.mark.parametrize(
    ("rce_from", "start", "refused"),
    [
        (None, START, True),  # RCEm throughout
        ("2026-07", START, True),  # RCEm in June, the window's month
        ("2026-06", START, False),  # RCE from June: no RCEm month in the window
        ("2026-07", datetime(2026, 6, 30, tzinfo=UTC), True),  # June 30 and July 1 (local)
        ("2026-07", datetime(2026, 6, 30, 22, tzinfo=UTC), False),  # July 1 00:00 local on
    ],
    ids=["rcem", "rce-next-month", "rce-this-month", "window-ends-in-rce", "window-all-rce"],
)
def test_ac_012_24_live_run_refuses_a_site_on_rcem_in_any_month_of_its_window(
    tmp_path: Path, rce_from: str | None, start: datetime, refused: bool
) -> None:
    config = _config_with_cohort(tmp_path, rce_from)
    kwargs: dict[str, Any] = {
        "start": start,
        "rcem": _pse_table(config),
        "generator_config": config / "generator.yaml",
        "config_dir": config,
    }
    assert refused == (PriceBasis.RCEM in _rcem_site_bases(config, start))
    sim = _managed_params(**kwargs)  # the same run is fine in sim
    assert scoped(RUN_ID, RCEM_SITE) in sim
    if refused:
        with pytest.raises(ValueError):
            _managed_params(**kwargs, mode="live")
    else:
        live = _managed_params(**kwargs, mode="live")
        assert {p.mode for p in live.values()} == {"live"}
        assert scoped(RUN_ID, RCEM_SITE) in live


# ── rcem_used ────────────────────────────────────────────────────────────────────────────────────


def test_ac_012_24_rcem_used_is_empty_for_an_all_rce_fleet(june_table: RcemTable) -> None:
    params = _managed_params(rcem=june_table)
    assert all(
        basis_for(p.model.cohort, m) is PriceBasis.RCE
        for p in params.values()
        for m in _months(START, DAYS)
    )
    assert _rcem_used(params) == []


def test_ac_012_24_rcem_used_is_empty_when_the_cohort_settles_on_rce_in_the_window(
    june_table: RcemTable, tmp_path: Path
) -> None:
    config = _config_with_cohort(tmp_path, "2026-06")
    params = _managed_params(
        rcem=june_table, generator_config=config / "generator.yaml", config_dir=config
    )
    assert _rcem_used(params) == []


def test_ac_012_24_rcem_used_lists_the_entries_an_rcem_basis_site_reads(
    june_table: RcemTable, tmp_path: Path
) -> None:
    """On 2026-06-03 the signal reads April (published 2026-05-11; May's comes on 06-11) and the
    ledger reads June's own entry (``ems.domain.reference.rcem_used``); May is read by no step.
    """
    config = _config_with_cohort(tmp_path, None)
    params = _managed_params(
        rcem=june_table, generator_config=config / "generator.yaml", config_dir=config
    )
    used = _rcem_used(params)
    assert len(used) == 2
    assert sorted((e["month"], e["value"], e["source"]) for e in used) == [
        (m, june_table.entry(m).billing_value, "proxy") for m in ("2026-04", "2026-06")
    ]


# ── pse_table ────────────────────────────────────────────────────────────────────────────────────


def test_ac_012_24_pse_table_loads_the_published_table() -> None:
    table = _pse_table()
    assert table == RcemTable(tuple(load_rcem(CONFIG / "tariffs" / "rcem.yaml")))
    assert {e.source for e in table.entries} == {"pse"}
