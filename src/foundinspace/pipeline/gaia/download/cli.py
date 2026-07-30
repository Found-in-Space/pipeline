from __future__ import annotations

from pathlib import Path

import click

from foundinspace.pipeline.cli_support import (
    load_project_or_die,
    project_option,
    record_command_or_die,
)
from foundinspace.pipeline.gaia.download.archive import GaiaCredentialsError
from foundinspace.pipeline.gaia.download.runner import (
    plan_gaia_download,
    replan_pending_gaia_download,
    run_gaia_download,
    write_browser_queries,
)


@click.group(name="download")
def cli() -> None:
    """Plan and run Gaia Archive source downloads."""


@cli.command(name="plan")
@project_option
@click.option(
    "--refresh-counts",
    is_flag=True,
    default=False,
    help="Re-run the Gaia HEALPix count query even if cached state is current.",
)
@click.option(
    "--poll-seconds",
    type=float,
    default=30.0,
    show_default=True,
    help="Seconds to wait between count-job polls.",
)
def plan(project_path: Path, refresh_counts: bool, poll_seconds: float) -> None:
    """Write or refresh the resumable Gaia download plan."""
    project = load_project_or_die(project_path, "gaia", "gaia_download")
    query_dir = (
        project.gaia_download.state_db.parent
        / f"{project.gaia_download.state_db.stem}-queries"
    )
    with record_command_or_die(
        project,
        command="gaia download plan",
        parameters={
            "refresh_counts": refresh_counts,
            "poll_seconds": poll_seconds,
        },
        outputs={
            "download_state": project.gaia_download.state_db,
            "queries": query_dir,
        },
    ):
        try:
            summary = plan_gaia_download(
                project,
                refresh_counts=refresh_counts,
                poll_seconds=poll_seconds,
                echo=click.echo,
            )
        except (ValueError, GaiaCredentialsError) as exc:
            raise click.ClickException(str(exc)) from exc
    click.echo(f"State DB: {summary.state_db.resolve()}")


@cli.command(name="queries")
@project_option
@click.option(
    "--output-dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=None,
    help="Directory for generated ADQL files. Defaults beside the Gaia download state DB.",
)
def queries(project_path: Path, output_dir: Path | None) -> None:
    """Write small-profile ADQL files for the Gaia browser UI."""
    project = load_project_or_die(project_path, "gaia", "gaia_download")
    query_dir = output_dir or (
        project.gaia_download.state_db.parent
        / f"{project.gaia_download.state_db.stem}-browser-queries"
    )
    with record_command_or_die(
        project,
        command="gaia download queries",
        parameters={"output_dir": output_dir},
        outputs={"queries": query_dir},
    ):
        try:
            summary = write_browser_queries(
                project,
                output_dir=output_dir,
                echo=click.echo,
            )
        except ValueError as exc:
            raise click.ClickException(str(exc)) from exc
    click.echo(f"Query directory: {summary.query_dir.resolve()}")


@cli.command(name="replan-pending")
@project_option
@click.option(
    "--bytes-per-row",
    required=True,
    type=click.IntRange(min=1),
    help="Estimated remote result bytes per Gaia row for newly planned batches.",
)
def replan_pending(project_path: Path, bytes_per_row: int) -> None:
    """Rewrite only unexecuted Gaia download batches."""
    project = load_project_or_die(project_path, "gaia", "gaia_download")
    with record_command_or_die(
        project,
        command="gaia download replan-pending",
        parameters={"bytes_per_row": bytes_per_row},
        inputs={"download_state": project.gaia_download.state_db},
        outputs={"download_state": project.gaia_download.state_db},
    ):
        try:
            summary = replan_pending_gaia_download(
                project,
                bytes_per_row=bytes_per_row,
                echo=click.echo,
            )
        except (ValueError, GaiaCredentialsError) as exc:
            raise click.ClickException(str(exc)) from exc
    click.echo(
        "Gaia pending replan complete: "
        f"protected={summary.protected_batches:,}, "
        f"new={summary.new_batches:,}, "
        f"recycled_failed={summary.recycled_failed_batches:,}, "
        f"effective_row_cap={summary.effective_row_cap:,}"
    )
    click.echo(f"State DB: {summary.state_db.resolve()}")


@cli.command(name="run")
@project_option
@click.option(
    "--poll-seconds",
    type=float,
    default=30.0,
    show_default=True,
    help="Seconds to wait between polls when no local work can progress.",
)
def run(project_path: Path, poll_seconds: float) -> None:
    """Resume or execute a Gaia download plan."""
    project = load_project_or_die(project_path, "gaia", "gaia_download")
    with record_command_or_die(
        project,
        command="gaia download run",
        parameters={"poll_seconds": poll_seconds},
        inputs={"download_state": project.gaia_download.state_db},
        outputs={
            "download_state": project.gaia_download.state_db,
            "gaia_votables": project.gaia.input_dir,
        },
    ):
        try:
            summary = run_gaia_download(
                project,
                poll_seconds=poll_seconds,
                echo=click.echo,
            )
        except (ValueError, OSError, GaiaCredentialsError) as exc:
            raise click.ClickException(str(exc)) from exc
        if summary.failed_batches:
            raise click.ClickException(
                "Gaia downloads finished with failed batches: "
                f"failed={summary.failed_batches:,}, "
                f"state_db={summary.state_db.resolve()}"
            )
    click.echo(
        "Gaia downloads complete: "
        f"downloaded={summary.downloaded_batches:,}, "
        f"remote_deleted={summary.deleted_remote_batches:,}, "
        f"failed={summary.failed_batches:,}"
    )
    click.echo(f"State DB: {summary.state_db.resolve()}")
