"""``ems-gen``: topics, schemas, catalog, generation and inspection.

Contract: ``specs/001-data-pipeline-core/contracts/cli.md``.
"""

from __future__ import annotations

import dataclasses
import sys
from pathlib import Path
from typing import Annotated

import typer

from ems.settings import EmsSettings, TopicsProfile, load_settings

app = typer.Typer(no_args_is_help=True, add_completion=False, help=__doc__)
topics_app = typer.Typer(no_args_is_help=True, help="Create, inspect and delete the EMS topics.")
schemas_app = typer.Typer(no_args_is_help=True, help="Export and check the JSON Schemas.")
app.add_typer(topics_app, name="topics")
app.add_typer(schemas_app, name="schemas")

ProfileOpt = Annotated[
    TopicsProfile | None,
    typer.Option("--profile", help="Topic profile (default EMS_TOPICS_PROFILE)."),
]
BootstrapOpt = Annotated[
    str | None, typer.Option("--bootstrap", help="Kafka bootstrap (default EMS_KAFKA_BOOTSTRAP).")
]


def _settings(profile: TopicsProfile | None = None, bootstrap: str | None = None) -> EmsSettings:
    s = load_settings()
    update: dict[str, object] = {}
    if profile:
        update["topics_profile"] = profile
    if bootstrap:
        update["kafka_bootstrap"] = bootstrap
    return s.model_copy(update=update)


def _need_ingest() -> None:
    try:
        import confluent_kafka  # noqa: F401
    except ImportError:  # pragma: no cover - depends on the install
        typer.echo(
            "this command needs the 'ingest' dependency group: uv sync --group ingest", err=True
        )
        raise typer.Exit(2) from None


@topics_app.command("ensure")
def topics_ensure(
    profile: ProfileOpt = None,
    bootstrap: BootstrapOpt = None,
    allow_partition_increase: Annotated[
        bool, typer.Option(help="Grow partitions (breaks per-key order).")
    ] = False,
    apply_config: Annotated[
        bool, typer.Option(help="Apply config drift instead of only reporting it.")
    ] = False,
) -> None:
    """Create missing topics; report or apply drift. Exit 0 ok, 1 refused, 2 broker error."""
    _need_ingest()
    from confluent_kafka import KafkaException

    from ems.contracts.topics import load_plan
    from ems.ingest.kafka.admin import KafkaTopicAdmin, ensure

    s = _settings(profile, bootstrap)
    plan = load_plan(s.config_dir, s.topics_profile, s.topic_prefix)
    try:
        report = ensure(
            KafkaTopicAdmin(s.kafka_bootstrap),
            plan,
            allow_partition_increase=allow_partition_increase,
            apply_config=apply_config,
        )
    except KafkaException as exc:
        typer.echo(f"broker error: {exc}", err=True)
        raise typer.Exit(2) from exc
    typer.echo(f"profile={plan.profile} bootstrap={s.kafka_bootstrap}")
    for line in report.lines():
        typer.echo(line)
    raise typer.Exit(1 if report.failed else 0)


@topics_app.command("describe")
def topics_describe(profile: ProfileOpt = None, bootstrap: BootstrapOpt = None) -> None:
    """Show desired vs actual partitions, replication and configs."""
    _need_ingest()
    from ems.contracts.topics import load_plan
    from ems.ingest.kafka.admin import KafkaTopicAdmin, config_diff

    s = _settings(profile, bootstrap)
    plan = load_plan(s.config_dir, s.topics_profile, s.topic_prefix)
    actual = KafkaTopicAdmin(s.kafka_bootstrap).actual(t.name for t in plan.topics)
    for want in plan.topics:
        have = actual.get(want.name)
        if have is None:
            typer.echo(f"{want.name}: MISSING (want partitions={want.partitions})")
            continue
        typer.echo(
            f"{want.name}: partitions {have.partitions}/{want.partitions} "
            f"rf {have.replication_factor}/{want.replication_factor}"
        )
        for key, (old, new) in sorted(config_diff(want, have).items()):
            typer.echo(f"    drift {key}: {old!r} (want {new!r})")


@topics_app.command("delete")
def topics_delete(
    yes: Annotated[bool, typer.Option("--yes", help="Confirm deletion.")] = False,
    profile: ProfileOpt = None,
    bootstrap: BootstrapOpt = None,
) -> None:
    """Delete the defined topics (dev only: refuses unless the broker is local)."""
    _need_ingest()
    from ems.contracts.topics import load_plan
    from ems.ingest.kafka.admin import KafkaTopicAdmin, is_local_bootstrap

    s = _settings(profile, bootstrap)
    if not yes or not is_local_bootstrap(s.kafka_bootstrap):
        typer.echo("refused: needs --yes and a localhost or compose bootstrap", err=True)
        raise typer.Exit(1)
    plan = load_plan(s.config_dir, s.topics_profile, s.topic_prefix)
    for name in KafkaTopicAdmin(s.kafka_bootstrap).delete(t.name for t in plan.topics):
        typer.echo(f"deleted {name}")


@schemas_app.command("export")
def schemas_export(
    out: Annotated[Path, typer.Option(help="Output directory.")] = Path("schemas"),
    check: Annotated[bool, typer.Option(help="Fail if the committed schemas are stale.")] = False,
) -> None:
    """Write the JSON Schemas and golden examples (or check them with --check)."""
    from ems.contracts import schema_export as se

    if check:
        problems = se.stale(out)
        if problems:
            typer.echo(
                f"stale schemas: {', '.join(problems)}; run: ems-gen schemas export", err=True
            )
            raise typer.Exit(1)
        typer.echo("schemas up to date")
        return
    for path in [*se.export(out), *se.export_examples(out / "examples")]:
        typer.echo(f"wrote {path}")


@app.command("tail")
def tail(
    topic: Annotated[str, typer.Option(help="Topic name (the topic prefix is added).")],
    key: Annotated[str | None, typer.Option(help="Only records with this key.")] = None,
    from_beginning: Annotated[bool, typer.Option(help="Start at the oldest record.")] = False,
    max_records: Annotated[int, typer.Option("--max", help="Stop after N records.")] = 20,
    follow: Annotated[
        bool, typer.Option("--follow", "-f", help="Keep waiting for new records.")
    ] = False,
    bootstrap: BootstrapOpt = None,
) -> None:
    """Pretty-print key, partition, offset, headers and value (``--follow`` to keep tailing)."""
    _need_ingest()
    import json

    from ems.ingest.tools.reader import read_topic

    s = _settings(bootstrap=bootstrap)
    shown = 0
    records = read_topic(
        s.kafka_bootstrap, s.topic_prefix + topic, from_beginning=from_beginning, follow=follow
    )
    for rec in records:
        if key is not None and rec.key != key:
            continue
        value = json.loads(rec.value) if rec.value else None
        typer.echo(
            f"--- {rec.topic}[{rec.partition}]@{rec.offset} key={rec.key} ts={rec.timestamp_ms} "
            f"headers={rec.headers}"
        )
        typer.echo(json.dumps(value, indent=2)[:4000])
        shown += 1
        if shown >= max_records:
            break


@app.command("validate")
def validate(
    topic: Annotated[str | None, typer.Option(help="Topic name (without prefix).")] = None,
    all_topics: Annotated[
        bool, typer.Option("--all", help="Validate every defined topic.")
    ] = False,
    from_beginning: Annotated[bool, typer.Option(help="Read from the oldest record.")] = True,
    profile: ProfileOpt = None,
    bootstrap: BootstrapOpt = None,
) -> None:
    """Check every record against the schema, the routing rule and partition stability."""
    _need_ingest()
    from ems.contracts.topics import load_plan
    from ems.ingest.tools.reader import read_topic
    from ems.ingest.tools.validate import validate_records

    s = _settings(profile, bootstrap)
    plan = load_plan(s.config_dir, s.topics_profile, s.topic_prefix)
    desired = plan.by_name()
    if all_topics:
        names = list(desired)
    elif topic:
        names = [s.topic_prefix + topic]
    else:
        typer.echo("give --topic or --all", err=True)
        raise typer.Exit(2)
    clean = True
    for name in names:
        partitions = desired[name].partitions if name in desired else 1
        records = read_topic(s.kafka_bootstrap, name, from_beginning=from_beginning)
        report = validate_records(name, records, partitions, s.topic_prefix)
        for line in report.lines():
            typer.echo(line)
        clean = clean and report.clean
    raise typer.Exit(0 if clean else 1)


@app.command("catalog")
def catalog(
    n: Annotated[int, typer.Option("--n", help="Number of locations PL-LOC-000001..n.")] = 1000,
    out: Annotated[Path, typer.Option(help="Parquet file.")] = Path("data/catalog.parquet"),
    config: Annotated[Path, typer.Option(help="Generator config.")] = Path("config/generator.yaml"),
) -> None:
    """Export the generated site catalog (hand-defined sites included) as Parquet."""
    _need_ingest()
    import dataclasses

    from ems.ingest.catalog.build import export_catalog
    from ems.ingest.generator import load_generator_config, load_sites

    s = load_settings()
    cfg = dataclasses.replace(load_generator_config(config), synthetic_locations=n)
    sites = load_sites(s.config_dir, cfg)
    typer.echo(f"wrote {export_catalog(sites, out)} ({len(sites)} sites)")


@app.command("run")
def run(
    mode: Annotated[str | None, typer.Option(help="backfill | accelerated | realtime")] = None,
    start: Annotated[str | None, typer.Option(help="UTC start, e.g. 2026-10-20T00:00:00Z")] = None,
    end: Annotated[str | None, typer.Option(help="UTC end (exclusive)")] = None,
    days: Annotated[float | None, typer.Option(help="Window length in days from --start")] = None,
    speed: Annotated[
        float | None, typer.Option(help="Accelerated: sim seconds per wall second")
    ] = None,
    run_id: Annotated[
        str | None, typer.Option(help="Run scope: prefixes keys, enters event ids")
    ] = None,
    config: Annotated[Path, typer.Option(help="Generator config.")] = Path("config/generator.yaml"),
    faults: Annotated[bool, typer.Option(help="Enable the configured fault injection.")] = False,
    locations: Annotated[
        int | None, typer.Option(help="Override scale.synthetic_locations.")
    ] = None,
    bootstrap: BootstrapOpt = None,
) -> None:
    """Generate and publish. Exit 0 all delivered, 1 preflight refused, 3 delivery failures."""
    _need_ingest()
    import asyncio

    from ems.ingest.generator import load_generator_config, load_sites
    from ems.ingest.runner import PreflightError, parse_window
    from ems.ingest.runner import run as do_run

    s = _settings(bootstrap=bootstrap)
    cfg = load_generator_config(config)
    if faults:
        cfg = dataclasses.replace(cfg, faults={**cfg.faults, "enabled": True})
    if locations is not None:
        cfg = dataclasses.replace(cfg, synthetic_locations=locations)
    chosen = mode or cfg.mode
    if chosen not in ("backfill", "accelerated", "realtime"):
        typer.echo(f"unknown mode {chosen}", err=True)
        raise typer.Exit(2)
    import signal

    def _stop(_signum: int, _frame: object) -> None:  # docker stop: shut down like Ctrl+C
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _stop)
    try:
        w_start, w_end = parse_window(start, end, days)
        if w_start is None and chosen != "realtime":
            w_start = cfg.start
        if w_end is None and days is None and end is None:
            w_end = cfg.end
        from ems.ingest.runner import run_hybrid
        from ems.ingest.sources.router import route

        routing = route(cfg, load_sites(s.config_dir, cfg))
        if routing.needs_live:
            if chosen != "realtime":
                typer.echo("live sources need --mode realtime (use replay for past data)", err=True)
                raise typer.Exit(2)
            result = asyncio.run(run_hybrid(s, cfg, run_id=run_id))
        else:
            result = asyncio.run(
                do_run(s, cfg, mode=chosen, start=w_start, end=w_end, run_id=run_id, speed=speed)
            )
    except PreflightError as exc:
        typer.echo(f"preflight refused: {exc}", err=True)
        raise typer.Exit(1) from exc
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(2) from exc
    except KeyboardInterrupt:
        typer.echo("stopped; buffered records flushed and manifest written")
        raise typer.Exit(0) from None
    typer.echo(f"events {dict(result.counts)}; manifest {result.manifest}")
    raise typer.Exit(result.exit_code)


@app.command("record")
def record_cmd(
    name: Annotated[str, typer.Option(help="Recording name under data/recordings/.")],
    once: Annotated[bool, typer.Option(help="Poll every live source once, then stop.")] = True,
    duration: Annotated[
        float | None, typer.Option(help="Seconds to record when not --once.")
    ] = None,
    out: Annotated[
        Path | None, typer.Option(help="Target folder (default data/recordings/NAME).")
    ] = None,
    config: Annotated[Path, typer.Option(help="Generator config.")] = Path("config/generator.yaml"),
) -> None:
    """Record live PSE prices and Open-Meteo forecasts (raw + events + manifest); no
    Kafka needed.
    """
    _need_ingest()
    import asyncio

    from ems.ingest.generator import load_generator_config
    from ems.ingest.runner import record

    s = load_settings()
    root = asyncio.run(
        record(
            s, load_generator_config(config), name=name, out_dir=out, once=once, duration_s=duration
        )
    )
    typer.echo(f"recorded into {root}")


@app.command("replay")
def replay_cmd(
    name: Annotated[str, typer.Option(help="Recording name under data/recordings/.")],
    speed: Annotated[
        float | None, typer.Option(help="Pace by recorded ingest times / speed.")
    ] = None,
    root: Annotated[
        Path | None, typer.Option(help="Recording folder (default data/recordings/NAME).")
    ] = None,
    bootstrap: BootstrapOpt = None,
) -> None:
    """Publish a recording; events keep their ids and are marked replay:<source>."""
    _need_ingest()
    import asyncio

    from ems.ingest.runner import replay

    s = _settings(bootstrap=bootstrap)
    result = asyncio.run(replay(s, name, root=root, speed=speed))
    typer.echo(f"replayed {dict(result.counts)}")
    raise typer.Exit(result.exit_code)


@app.command("stats")
def stats_cmd(
    topic: Annotated[str | None, typer.Option(help="Topic name (without prefix).")] = None,
    profile: ProfileOpt = None,
    bootstrap: BootstrapOpt = None,
) -> None:
    """Counts per key and partition, event-time range, gaps, duplicate ids and late events."""
    _need_ingest()
    from ems.contracts.topics import DLQ_TOPIC, load_plan
    from ems.ingest.tools.reader import read_topic
    from ems.ingest.tools.stats import topic_stats

    s = _settings(profile, bootstrap)
    plan = load_plan(s.config_dir, s.topics_profile, s.topic_prefix)
    names = [s.topic_prefix + topic] if topic else [t.name for t in plan.topics]
    for name in names:
        if name.endswith(DLQ_TOPIC):
            continue
        stats = topic_stats(name, read_topic(s.kafka_bootstrap, name))
        meter = name.endswith(("production.v1", "consumption.v1"))
        for line in stats.lines(meter):
            typer.echo(line)


def main() -> None:  # pragma: no cover
    app()


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
