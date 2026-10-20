"""``ems-gen``: topics, schemas, catalog, generation and inspection.

Contract: ``specs/001-data-pipeline-core/contracts/cli.md``.
"""

from __future__ import annotations

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


def main() -> None:  # pragma: no cover
    app()


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
