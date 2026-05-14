from __future__ import annotations

from pathlib import Path

import click

from foundinspace.pipeline.project import PROJECT_PROFILES, render_project_template


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
    """Write a starter project.toml for pipeline commands."""
    project_path = project_path.expanduser()
    if project_path.exists():
        raise click.ClickException(f"Project file already exists: {project_path}")
    project_path.parent.mkdir(parents=True, exist_ok=True)
    project_path.write_text(
        render_project_template(profile=profile),
        encoding="utf-8",
    )
    click.echo(f"Wrote {profile} project file to {project_path.resolve()}")
