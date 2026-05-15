from __future__ import annotations

from pathlib import Path

import click

from foundinspace.pipeline.gaia.download.archive import GaiaCredentialsError
from foundinspace.pipeline.gaia.download.runner import (
    plan_gaia_download,
    run_gaia_download,
    write_browser_queries,
)
from foundinspace.pipeline.project import load_project


@click.group(name="download")
def cli() -> None:
    """Plan and run Gaia Archive source downloads."""


def _load_project_or_die(project_path: Path, *required: str):
    try:
        project = load_project(project_path)
        if required:
            project.require(*required)
        return project
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc


@cli.command(name="plan")
@click.option(
    "--project",
    "project_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to pipeline project TOML.",
)
@click.option(
    "--refresh-counts",
    is_flag=True,
    default=False,
    help="Re-run the Gaia HEALPix count query even if cached state is current.",
)
def plan(project_path: Path, refresh_counts: bool) -> None:
    """Write or refresh the resumable Gaia download plan."""
    project = _load_project_or_die(project_path, "gaia", "gaia_download")
    try:
        summary = plan_gaia_download(
            project,
            refresh_counts=refresh_counts,
            echo=click.echo,
        )
    except (ValueError, GaiaCredentialsError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"State DB: {summary.state_db.resolve()}")


@cli.command(name="queries")
@click.option(
    "--project",
    "project_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to pipeline project TOML.",
)
@click.option(
    "--output-dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=None,
    help="Directory for generated ADQL files. Defaults beside the Gaia download state DB.",
)
def queries(project_path: Path, output_dir: Path | None) -> None:
    """Write small-profile ADQL files for the Gaia browser UI."""
    project = _load_project_or_die(project_path, "gaia", "gaia_download")
    try:
        summary = write_browser_queries(
            project,
            output_dir=output_dir,
            echo=click.echo,
        )
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Query directory: {summary.query_dir.resolve()}")


@cli.command(name="run")
@click.option(
    "--project",
    "project_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to pipeline project TOML.",
)
@click.option(
    "--poll-seconds",
    type=float,
    default=30.0,
    show_default=True,
    help="Seconds to wait between polls when no local work can progress.",
)
def run(project_path: Path, poll_seconds: float) -> None:
    """Resume or execute a Gaia download plan."""
    project = _load_project_or_die(project_path, "gaia", "gaia_download")
    try:
        summary = run_gaia_download(
            project,
            poll_seconds=poll_seconds,
            echo=click.echo,
        )
    except (ValueError, OSError, GaiaCredentialsError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(
        "Gaia downloads complete: "
        f"downloaded={summary.downloaded_batches:,}, "
        f"remote_deleted={summary.deleted_remote_batches:,}, "
        f"failed={summary.failed_batches:,}"
    )
    click.echo(f"State DB: {summary.state_db.resolve()}")
