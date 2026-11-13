"""AC-008-05 … -12, -16: ``ems-gen history build`` on raw recordings (spec 008, T11).

Every recording is written by the real ``history.fetch`` against respx with synthetic API bodies
(``history_recording.py``); the only real bodies are the PSE days of AC-008-10 and AC-008-12.
``ems.ingest.history.build`` is imported inside the tests (it arrives with T18), so each test
fails on its own until then.
"""

from __future__ import annotations

import asyncio
import json
import re
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

import httpx
import numpy as np
import pytest
import respx
from typer.testing import CliRunner

from ems.contracts import schema_export
from ems.contracts.envelope import AssetType, Quality, Stream, validate_strict
from ems.contracts.time import WARSAW
from ems.ingest import cli
from ems.ingest.live.http import Fetcher
from ems.ingest.live.pse import REPORTS, PseSource
from tests.unit.ingest.history_recording import (
    CONFIG,
    MAPPINGS,
    POZNAN,
    PSE_FALL_BACK_DAY,
    PSE_URL,
    RADOM,
    REPO,
    UNMANAGED,
    Gap,
    Recording,
    Revision,
    build,
    build_info,
    clear_sky,
    copy_config,
    copy_recording,
    event_lines,
    fetched_at,
    make_recording,
    manifest,
    price_pln_mwh,
    raw_pse_days,
    raw_publications,
    raw_runs,
    read_built,
    sha256,
)

Q = timedelta(minutes=15)
MASKS = (
    "radiation_erbs",
    "radiation_era5",
    "radiation_synthetic",
    "temperature_synthetic",
    "wind_synthetic",
)

# The main recording: two UTC days at Poznań (ICON-D2), one missing run, one malformed run, and a
# one-hour satellite gap at local noon.
START, END = date(2026, 5, 15), date(2026, 5, 17)
T_START = datetime(2026, 5, 15, tzinfo=UTC)
GAP = Gap(POZNAN, datetime(2026, 5, 16, 10, tzinfo=UTC), datetime(2026, 5, 16, 11, tzinfo=UTC))
GAP_QUARTERS = {GAP.start + k * Q for k in range(4)}
MISSING = (POZNAN, datetime(2026, 5, 15, 6, tzinfo=UTC))
MALFORMED = (POZNAN, datetime(2026, 5, 16, 9, tzinfo=UTC))


def _refused() -> type[Exception]:
    from ems.ingest.history.build import BuildRefused

    return BuildRefused


def _count(value: Any) -> int:
    """A counter in build.json: a number, or the list of what it counts."""
    return value if isinstance(value, int) else len(value)


def _flatten(obj: Any, prefix: str = "") -> Iterator[tuple[str, Any]]:
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from _flatten(v, f"{prefix}{k}/")
    else:
        yield prefix.rstrip("/"), obj


@dataclass(frozen=True)
class Built:
    rec: Recording
    result: Any
    lines: list[bytes]
    events: list[Any]
    info: dict[str, Any]

    def of(self, stream: Stream, location_id: str | None = None) -> list[Any]:
        return [
            e
            for e in self.events
            if e.stream is stream and (location_id is None or e.location_id == location_id)
        ]


def _build(rec: Recording, **kwargs: Any) -> Built:
    result = build(rec, **kwargs)
    return Built(rec, result, event_lines(rec.root), read_built(rec.root), build_info(rec.root))


@pytest.fixture(scope="module")
def may(tmp_path_factory: pytest.TempPathFactory) -> Recording:
    return make_recording(
        tmp_path_factory.mktemp("may"),
        START,
        END,
        name="may",
        sat_gaps=[GAP],
        missing_runs=[MISSING],
        malformed_runs=[MALFORMED],
    )


@pytest.fixture(scope="module")
def may_built(may: Recording) -> Built:
    return _build(may)


@pytest.fixture(scope="module")
def day(tmp_path_factory: pytest.TempPathFactory) -> Recording:
    """One plain UTC day at Poznań; tests copy it before changing anything."""
    return make_recording(
        tmp_path_factory.mktemp("day"), date(2026, 5, 15), date(2026, 5, 16), name="day"
    )


# AC-008-10: the fall-back day 2026-10-25 (real PSE body) inside 2026-10-24 … 10-27, both sites.
OCT_START, OCT_END = date(2026, 10, 24), date(2026, 10, 27)
FALL_BACK = date(2026, 10, 25)


@pytest.fixture(scope="module")
def oct_built(tmp_path_factory: pytest.TempPathFactory) -> Built:
    rec = make_recording(
        tmp_path_factory.mktemp("oct"),
        OCT_START,
        OCT_END,
        name="oct",
        locations=(POZNAN, RADOM),
        synthetic_locations=4,
        pse_bodies={FALL_BACK: PSE_FALL_BACK_DAY.read_bytes()},
    )
    return _build(rec)


# ── AC-008-05: what build writes ─────────────────────────────────────────────────────────────────


def test_ac_008_05_event_kinds_and_counts(may_built: Built) -> None:
    root = may_built.rec.root
    quarters = 96 * (END - START).days
    stored_runs = set(raw_runs(root))
    assert MISSING not in stored_runs and MALFORMED in stored_runs  # a marker; a 200 body

    counts = Counter(e.stream.value for e in may_built.events)
    assert counts == {
        "weather_forecast": len(stored_runs) - 1,  # one per stored run, minus the malformed one
        "price": 3,  # business days 05-15 (published before start), 05-16, 05-17
        "production": quarters,
        "consumption": quarters,
    }
    forecasts = {(e.location_id, e.issued_at) for e in may_built.of(Stream.WEATHER_FORECAST)}
    assert forecasts == stored_runs - {MALFORMED}
    assert _count(may_built.info["malformed_runs"]) == 1  # counted, not fatal
    assert _count(may_built.info["empty_runs"]) == 0
    assert may_built.info["counts"] == dict(counts)
    assert (may_built.result.root, may_built.result.events) == (root, len(may_built.lines))

    expected = [T_START + k * Q for k in range(quarters)]
    for stream in (Stream.PRODUCTION, Stream.CONSUMPTION):
        assert sorted(e.interval_start for e in may_built.of(stream, POZNAN)) == expected


def test_ac_008_05_forecasts_start_at_the_first_complete_step(oct_built: Built) -> None:
    """ICON-D2: 191 steps from +15 min (cloud cover missing at +48 h); DMI: first step ends at
    +45 min (no GHI/DHI at +15 and +30 min), so 190 steps.
    """
    icon = oct_built.of(Stream.WEATHER_FORECAST, POZNAN)
    dmi = oct_built.of(Stream.WEATHER_FORECAST, RADOM)
    assert len(icon) == len(dmi) == 8 * (OCT_END - OCT_START).days
    for e in icon:
        steps = e.payload.steps
        assert e.payload.forecast_model == "icon_d2"
        assert (len(steps), steps[0].valid_end, steps[0].lead_time) == (
            191,
            e.issued_at + Q,
            "PT15M",
        )
    for e in dmi:
        steps = e.payload.steps
        assert e.payload.forecast_model == "dmi_harmonie_arome_europe"
        assert (len(steps), steps[0].valid_end, steps[0].lead_time) == (
            190,
            e.issued_at + 3 * Q,
            "PT45M",
        )
        assert e.interval_start == e.issued_at + 2 * Q


# ── AC-008-06: deterministic, hashed, refused when the inputs moved ──────────────────────────────


def test_ac_008_06_two_builds_are_byte_identical(may_built: Built, tmp_path) -> None:
    again = copy_recording(may_built.rec, tmp_path, raw_only=True)
    assert not (again.root / "events.jsonl").exists()
    build(again)
    first = (may_built.rec.root / "events.jsonl").read_bytes()
    assert (again.root / "events.jsonl").read_bytes() == first  # another folder, same bytes
    assert build_info(again.root)["raw_set_digest"] == may_built.info["raw_set_digest"]

    build(again)  # same run_id, nothing changed: allowed, and the same bytes again
    assert (again.root / "events.jsonl").read_bytes() == first


def test_ac_008_06_build_json_records_hashes_and_digests(may_built: Built) -> None:
    root, info = may_built.rec.root, may_built.info
    assert info["events_sha256"] == sha256(root / "events.jsonl") == may_built.result.sha256
    assert re.fullmatch(r"[0-9a-f]{64}", info["raw_set_digest"])
    assert (info["seed"], info["run_id"]) == (4242, "may")  # the config's seed; the name

    digests = dict(_flatten(info["digests"]))

    def digest_of(suffix: str) -> str:
        found = [str(v) for k, v in digests.items() if k.endswith(suffix)]
        assert len(found) == 1, (suffix, sorted(digests))
        return found[0]

    assert sha256(root / "generator.yaml") in digest_of("generator.yaml")
    for name in ("weather.yaml", "load.yaml", "prices.yaml"):
        assert sha256(CONFIG / "calibration" / name) in digest_of(name)
    assert sha256(CONFIG / "sites.yaml") in digest_of("sites.yaml")
    for mapping in ("openmeteo.forecast.v1", "pse.rce.v2"):
        (key,) = [k for k in digests if mapping in k]
        assert sha256(MAPPINGS / f"{mapping}.yaml") in str(digests[key])


def test_ac_008_06_ingested_at_is_the_recorded_fetch_time(may_built: Built) -> None:
    root = may_built.rec.root
    runs, days = raw_runs(root), raw_pse_days(root)
    meters = max(
        fetched_at(root / "raw" / "satellite" / f"{POZNAN}.json"),
        fetched_at(root / "raw" / "era5" / f"{POZNAN}.json"),
    )
    for e in may_built.events:
        if e.stream is Stream.WEATHER_FORECAST:
            assert e.ingested_at == fetched_at(runs[(e.location_id, e.issued_at)])
        elif e.stream is Stream.PRICE:
            assert e.ingested_at == max(fetched_at(p) for p in days[e.payload.delivery_day])
        else:
            assert e.ingested_at == meters


@pytest.mark.parametrize("change", ["incomplete", "no-manifest"])
def test_ac_008_06_incomplete_fetch_is_refused(day: Recording, tmp_path, change: str) -> None:
    rec = copy_recording(day, tmp_path)
    path = rec.root / "manifest.json"
    if change == "incomplete":
        m = manifest(rec.root)
        m["complete"] = False
        path.write_text(json.dumps(m, indent=2, sort_keys=True) + "\n")
    else:
        path.unlink()
    with pytest.raises(_refused()):
        build(rec)
    assert not (rec.root / "events.jsonl").exists()
    assert not (rec.root / "build.json").exists()


@pytest.mark.parametrize(
    "raw",
    [
        f"open-meteo-single-runs/{POZNAN}/20260515T0300.json",
        f"satellite/{POZNAN}.json",
        f"era5/{POZNAN}.json",
        "pse-rce/2026-05-15-p01.json",
    ],
)
def test_ac_008_06_raw_body_changed_after_fetch_is_refused(
    day: Recording, tmp_path, raw: str
) -> None:
    rec = copy_recording(day, tmp_path)
    body = rec.root / "raw" / raw
    body.write_bytes(body.read_bytes() + b" ")  # same JSON, other bytes: only the hash sees it
    with pytest.raises(_refused()):
        build(rec)
    assert not (rec.root / "events.jsonl").exists()


def test_ac_008_06_same_run_id_with_changed_digest_is_refused(day: Recording, tmp_path) -> None:
    config_dir = copy_config(tmp_path)
    rec = copy_recording(day, tmp_path, config_dir=config_dir)
    build(rec)
    before = {n: (rec.root / n).read_bytes() for n in ("events.jsonl", "build.json")}

    load_yaml = config_dir / "calibration" / "load.yaml"
    load_yaml.write_text(load_yaml.read_text() + "# changed by a test\n")
    with pytest.raises(_refused(), match=r"(?i)run[-_ ]?id"):
        build(rec)  # G11: new meter values under the same ids
    assert {n: (rec.root / n).read_bytes() for n in before} == before

    build(rec, run_id="day-v2")  # a new run_id needs no refetch
    assert build_info(rec.root)["run_id"] == "day-v2"
    assert {e.run_id for e in read_built(rec.root)} == {"day-v2"}


def test_ac_008_06_run_id_outside_the_contract_pattern_is_refused(day: Recording, tmp_path) -> None:
    rec = copy_recording(day, tmp_path)
    with pytest.raises(_refused()):
        build(rec, run_id="spring 2026!")
    assert not (rec.root / "events.jsonl").exists()


# ── AC-008-07: issue times from the source ───────────────────────────────────────────────────────


def test_ac_008_07_forecast_issued_at_is_the_run_of_its_raw_file(may_built: Built) -> None:
    runs = raw_runs(may_built.rec.root)  # keyed by the ``run`` parameter of each stored request
    forecasts = may_built.of(Stream.WEATHER_FORECAST)
    assert forecasts
    for e in forecasts:
        body = runs[(e.location_id, e.issued_at)]
        assert (body.parent.name, body.stem) == (e.location_id, f"{e.issued_at:%Y%m%dT%H%M}")
        assert e.interval_start == e.issued_at  # ICON-D2: from the run's first quarter-hour


def test_ac_008_07_price_issued_at_is_the_latest_raw_publication(may_built: Built) -> None:
    days = raw_pse_days(may_built.rec.root)
    prices = may_built.of(Stream.PRICE)
    assert {e.payload.delivery_day for e in prices} == set(days)
    for e in prices:
        assert e.issued_at == max(raw_publications(days[e.payload.delivery_day]))
        assert e.payload.publication_time == e.issued_at


# ── AC-008-09: which price days, and where they sort ─────────────────────────────────────────────


def test_ac_008_09_day_published_before_start_sorts_at_start(may_built: Built) -> None:
    first = may_built.events[0]
    assert first.stream is Stream.PRICE and first.payload.delivery_day == START
    assert first.issued_at < T_START  # published on D-1, kept as published
    later = {e.payload.delivery_day: e.issued_at for e in may_built.of(Stream.PRICE)[1:]}
    end = datetime.combine(END, time(), UTC)
    assert set(later) == {date(2026, 5, 16), date(2026, 5, 17)}
    assert all(T_START <= issued < end for issued in later.values())  # published in the window


FLAG_START, FLAG_END = date(2026, 5, 10), date(2026, 5, 13)
REVISED_DAY, NEXT_DAY, INCOMPLETE_DAY = date(2026, 5, 11), date(2026, 5, 12), date(2026, 5, 13)
REVISION = Revision(slots=range(40, 48), published=datetime(2026, 5, 11, 15, 0, tzinfo=WARSAW))


def _published_at_two(day: date) -> datetime:
    """Every day published on D-1 at 14:00 local."""
    return datetime.combine(day - timedelta(days=1), time(14, 0), WARSAW)


@pytest.fixture(scope="module")
def flagged_built(tmp_path_factory: pytest.TempPathFactory) -> Built:
    rec = make_recording(
        tmp_path_factory.mktemp("flagged"),
        FLAG_START,
        FLAG_END,
        name="flagged",
        publication=_published_at_two,
        pse_revisions={REVISED_DAY: REVISION},  # republished on D, after NEXT_DAY's publication
        pse_drop_slots={INCOMPLETE_DAY: 4},
    )
    return _build(rec)


def test_ac_008_09_flagged_and_incomplete_days(flagged_built: Built) -> None:
    position = {
        e.payload.delivery_day: (i, e)
        for i, e in enumerate(flagged_built.events)
        if e.stream is Stream.PRICE
    }
    assert set(position) == {FLAG_START, REVISED_DAY, NEXT_DAY}  # the incomplete day: no event

    i_rev, revised = position[REVISED_DAY]
    i_next, nxt = position[NEXT_DAY]
    assert revised.issued_at == REVISION.published.astimezone(UTC)  # the latest, never earlier
    assert nxt.issued_at == _published_at_two(NEXT_DAY).astimezone(UTC) < revised.issued_at
    assert i_next < i_rev  # published after the next day's publication: sorts after that day

    slots = revised.payload.slots
    n = len(slots)
    for k, slot in enumerate(slots):
        delta = REVISION.price_delta_pln_mwh if k in REVISION.slots else 0.0
        assert slot.price == pytest.approx(price_pln_mwh(REVISED_DAY, k, n) + delta)

    days = flagged_built.info["price_days"]
    assert _count(days["flagged"]) == 1 and REVISED_DAY.isoformat() in json.dumps(days["flagged"])
    assert _count(days["incomplete"]) == 1
    assert INCOMPLETE_DAY.isoformat() in json.dumps(days["incomplete"])


# ── AC-008-10: the 100-slot day ──────────────────────────────────────────────────────────────────


def test_ac_008_10_fall_back_day_has_100_slots_and_continuous_meters(oct_built: Built) -> None:
    prices = {e.payload.delivery_day: e for e in oct_built.of(Stream.PRICE)}
    assert set(prices) == {date(2026, 10, d) for d in (24, 25, 26, 27)}
    assert len(prices[FALL_BACK].payload.slots) == 100
    assert prices[FALL_BACK].issued_at == datetime(2026, 10, 24, 12, 9, 21, 62000, tzinfo=UTC)

    t0 = datetime.combine(OCT_START, time(), UTC)
    expected = [t0 + k * Q for k in range(96 * (OCT_END - OCT_START).days)]  # 288 UTC quarters
    for site in (POZNAN, RADOM):
        for stream in (Stream.PRODUCTION, Stream.CONSUMPTION):
            starts = [e.interval_start for e in oct_built.of(stream, site)]
            assert sorted(starts) == expected, (site, stream)  # no gap, no duplicate

    # 10-24 starts at 02:00 CEST and 10-27 ends at 01:00 CET: partial, so not listed.
    assert oct_built.info["complete_local_days"] == {"first": "2026-10-25", "last": "2026-10-26"}


# ── AC-008-11: identity and contract ─────────────────────────────────────────────────────────────


def test_ac_008_11_source_systems_run_id_and_contract(may_built: Built) -> None:
    sources = {
        stream: {e.source_system for e in may_built.of(stream)}
        for stream in (Stream.WEATHER_FORECAST, Stream.PRICE, Stream.PRODUCTION, Stream.CONSUMPTION)
    }
    assert sources == {
        Stream.WEATHER_FORECAST: {"open-meteo-single-runs"},
        Stream.PRICE: {"pse-rce"},
        Stream.PRODUCTION: {"synthetic-history"},
        Stream.CONSUMPTION: {"synthetic-history"},
    }
    assert {e.run_id for e in may_built.events} == {"may"}  # non-null; default = the name
    assert not may_built.of(Stream.WEATHER_ACTUAL)  # truth weather is never published
    for e in may_built.of(Stream.WEATHER_FORECAST):
        assert e.source_record_ref == f"open-meteo-single-runs:icon_d2:{e.issued_at.isoformat()}"
    for line in may_built.lines:
        validate_strict(json.loads(line))  # wire contract v1, identity rule included
    assert schema_export.stale(REPO / "schemas") == []  # schema export unchanged


def test_ac_008_11_run_id_option(day: Recording, tmp_path) -> None:
    rec = copy_recording(day, tmp_path)
    result = build(rec, run_id="exp-7")
    events = read_built(rec.root)
    assert events and {e.run_id for e in events} == {"exp-7"}
    assert build_info(rec.root)["run_id"] == "exp-7"
    assert result.events == len(events)


# ── AC-008-12: a PSE day built from history equals the live one ──────────────────────────────────


@respx.mock
def test_ac_008_12_pse_day_through_poll_and_build_is_equal(oct_built: Built) -> None:
    respx.get(url__startswith=PSE_URL).mock(
        return_value=httpx.Response(
            200,
            content=PSE_FALL_BACK_DAY.read_bytes(),
            headers={"content-type": "application/json"},
        )
    )

    async def no_sleep(_s: float) -> None:
        return None

    source = PseSource(
        Fetcher("pse-rce", client=httpx.AsyncClient(), sleep=no_sleep), REPORTS["rce-pln"], MAPPINGS
    )
    live_ingested = datetime(2026, 10, 24, 12, 30, tzinfo=UTC)
    live = asyncio.run(source.poll(FALL_BACK, live_ingested, run_id="oct"))
    assert live is not None
    (built,) = [e for e in oct_built.of(Stream.PRICE) if e.payload.delivery_day == FALL_BACK]
    assert live.event_id == built.event_id
    assert live.model_dump(mode="json", exclude={"ingested_at"}) == built.model_dump(
        mode="json", exclude={"ingested_at"}
    )
    assert built.ingested_at != live_ingested  # the fetch time, not the build's


# ── AC-008-16: gap filling marked on the measurements ────────────────────────────────────────────


def _measurement(event: Any, asset_type: AssetType) -> list[Any]:
    return [m for m in event.payload.measurements if m.asset_type is asset_type]


def test_ac_008_16_pv_in_a_daylight_gap_is_estimated(may_built: Built) -> None:
    production = may_built.of(Stream.PRODUCTION, POZNAN)
    estimated = set()
    for e in production:
        (pv,) = _measurement(e, AssetType.PV)
        if pv.quality is Quality.ESTIMATED:
            estimated.add(e.interval_start)
            assert pv.quality_raw == "truth:era5"  # ERA5 GHI was there
            assert pv.value > 0  # local noon: a fill, not a zero
        else:
            assert pv.quality is Quality.SYNTHETIC
            assert not (pv.quality_raw or "").startswith("truth:")
    assert estimated == GAP_QUARTERS

    for e in may_built.of(Stream.CONSUMPTION, POZNAN):
        # ERA5 temperature everywhere: the heat pump (and base load) keep their quality.
        for m in e.payload.measurements:
            assert m.quality is Quality.SYNTHETIC, (e.interval_start, m.asset_type)


def test_ac_008_16_gap_shares_in_build_json(may_built: Built) -> None:
    shares = may_built.info["gap_shares"][POZNAN]
    assert set(MASKS) <= set(shares)
    t0 = T_START.timestamp()
    mids = np.array([t0 + 900 * k + 450 for k in range(96 * (END - START).days)])
    daylight = int((clear_sky(52.41, 16.93, 85.0, mids)[0] > 0).sum())
    assert shares["radiation_era5"] == pytest.approx(len(GAP_QUARTERS) / daylight, rel=0.1)
    for mask in (
        "radiation_erbs",
        "radiation_synthetic",
        "temperature_synthetic",
        "wind_synthetic",
    ):
        assert shares[mask] == 0, mask


def test_ac_008_16_grid_flows_follow_pv_at_an_unmanaged_site(tmp_path) -> None:
    gap = Gap(
        UNMANAGED, datetime(2026, 5, 15, 11, tzinfo=UTC), datetime(2026, 5, 15, 12, tzinfo=UTC)
    )
    gap_quarters = {gap.start + k * Q for k in range(4)}
    rec = make_recording(
        tmp_path,
        date(2026, 5, 15),
        date(2026, 5, 16),
        name="grid",
        locations=(UNMANAGED,),
        synthetic_locations=3,
        sat_gaps=[gap],
    )
    built = _build(rec)

    consumption = built.of(Stream.CONSUMPTION, UNMANAGED)
    assert len(consumption) == 96
    for e in consumption:
        grid = _measurement(e, AssetType.GRID_CONNECTION)
        assert len(grid) == 2  # import and export
        for m in grid:
            if e.interval_start in gap_quarters:
                assert m.quality is Quality.ESTIMATED
                assert (m.quality_raw or "").startswith("truth:")
            else:
                assert m.quality is Quality.SYNTHETIC
        for m in _measurement(e, AssetType.LOAD) + _measurement(e, AssetType.HEAT_PUMP):
            assert m.quality is Quality.SYNTHETIC  # temperature was real throughout
    pv_estimated = {
        e.interval_start
        for e in built.of(Stream.PRODUCTION, UNMANAGED)
        if _measurement(e, AssetType.PV)[0].quality is Quality.ESTIMATED
    }
    assert pv_estimated == gap_quarters


# ── CLI ──────────────────────────────────────────────────────────────────────────────────────────


def _cli_env(monkeypatch: pytest.MonkeyPatch, rec: Recording) -> None:
    monkeypatch.setenv("EMS_DATA_DIR", str(rec.settings.data_dir))
    monkeypatch.setenv("EMS_CONFIG_DIR", str(CONFIG))
    monkeypatch.setenv("EMS_MAPPINGS_DIR", str(MAPPINGS))
    monkeypatch.delenv("EMS_OPEN_METEO_API_KEY", raising=False)


def test_ac_008_16_cli_build_prints_the_gap_shares(
    may: Recording, tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rec = copy_recording(may, tmp_path, raw_only=True)
    _cli_env(monkeypatch, rec)
    result = CliRunner().invoke(cli.app, ["history", "build", "--name", rec.name])
    assert result.exit_code == 0, result.output
    assert POZNAN in result.output
    assert "era5" in result.output.lower()  # the share filled from ERA5
    assert (rec.root / "events.jsonl").exists() and (rec.root / "build.json").exists()


def test_ac_008_06_cli_build_refusal_exits_2(
    day: Recording, tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rec = copy_recording(day, tmp_path)
    m = manifest(rec.root)
    m["complete"] = False
    (rec.root / "manifest.json").write_text(json.dumps(m, indent=2, sort_keys=True) + "\n")
    _cli_env(monkeypatch, rec)
    result = CliRunner().invoke(cli.app, ["history", "build", "--name", rec.name])
    assert "No such command" not in result.output  # the command exists …
    assert result.exit_code == 2, result.output  # … and refuses
    assert isinstance(result.exception, SystemExit)
    assert re.search(r"(?i)complete", result.output)
    assert not (rec.root / "events.jsonl").exists()
