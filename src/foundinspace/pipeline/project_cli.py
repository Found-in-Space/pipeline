from __future__ import annotations

from pathlib import Path

import click

from foundinspace.pipeline.cli_support import (
    load_project_or_die,
    project_option,
    record_command_or_die,
)
from foundinspace.pipeline.project import (
    PROJECT_PROFILES,
    render_project_template,
    render_run_project_template,
    validate_controlled_project,
)


@click.group(name="project")
def cli() -> None:
    """Manage pipeline project files."""


@cli.command("init")
@click.option(
    "--profile",
    type=click.Choice(PROJECT_PROFILES),
    default="full",
    show_default=True,
    help="Starter project profile to write.",
)
@click.argument(
    "project_path",
    type=click.Path(path_type=Path),
)
def project_init(profile: str, project_path: Path) -> None:
    """Write a starter ad-hoc project file."""
    project_path = project_path.expanduser()
    if project_path.exists():
        raise click.ClickException(f"Project file already exists: {project_path}")
    project_path.parent.mkdir(parents=True, exist_ok=True)
    project_path.write_text(
        render_project_template(profile=profile),
        encoding="utf-8",
    )
    click.echo(f"Wrote {profile} project file to {project_path.resolve()}")


@cli.command("init-run")
@click.option(
    "--profile",
    type=click.Choice(PROJECT_PROFILES),
    default="full",
    show_default=True,
    help="Starter project profile to write.",
)
@click.argument(
    "run_dir",
    type=click.Path(path_type=Path),
)
def project_init_run(profile: str, run_dir: Path) -> None:
    """Create a named controlled-run directory and pipeline.toml."""
    run_dir = run_dir.expanduser()
    if run_dir.exists():
        raise click.ClickException(f"Run directory already exists: {run_dir}")
    run_id = run_dir.name
    try:
        rendered = render_run_project_template(run_id=run_id, profile=profile)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc

    run_dir.mkdir(parents=True)
    project_path = run_dir / "pipeline.toml"
    project_path.write_text(rendered, encoding="utf-8")
    (run_dir / "RUN_ID").write_text(f"{run_id}\n", encoding="utf-8")
    (run_dir / "provenance").mkdir()
    click.echo(f"Created controlled run {run_id!r} at {run_dir.resolve()}")
    click.echo(f"Edit and validate {project_path.resolve()} before running stages.")


@cli.command("validate")
@project_option
def project_validate(project_path: Path) -> None:
    """Validate a controlled project and record its resolved configuration."""
    project = load_project_or_die(
        project_path,
        validate_controlled=False,
    )
    with record_command_or_die(
        project,
        command="project validate",
        parameters={},
        inputs={"project": project.project_path},
    ):
        try:
            validate_controlled_project(project)
        except (OSError, ValueError) as exc:
            raise click.ClickException(str(exc)) from exc
    click.echo(f"Controlled project is valid: {project.project_path}")
