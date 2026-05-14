from pathlib import Path

import click

import foundinspace.pipeline.gaia_to_hip.download
from foundinspace.pipeline.gaia_to_hip.pipeline import prepare_gaia_hip_mapping
from foundinspace.pipeline.project import load_project


@click.group(name="gaia-to-hip")
def cli():
    """Gaia↔Hipparcos cross-match: download `hipparcos2_best_neighbour`, build sidecar."""


cli.add_command(foundinspace.pipeline.gaia_to_hip.download.main, name="download")


def _load_project_or_die(project_path: Path):
    try:
        return load_project(project_path)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc


@cli.command(name="build")
@click.option(
    "--project",
    "project_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to pipeline project TOML.",
)
@click.option("--force", "-f", is_flag=True, default=False)
def build_cmd(
    project_path: Path,
    force: bool,
) -> None:
    project = _load_project_or_die(project_path)
    download_output = project.gaia_to_hip.download_ecsv
    foundinspace.pipeline.gaia_to_hip.download.ensure_hipparcos2_best_neighbour_ecsv(
        download_output,
        force=force,
    )
    out = prepare_gaia_hip_mapping(
        download_output,
        project.gaia_to_hip.output_parquet,
        overwrite=force,
    )
    click.echo(f"Wrote Gaia↔HIP mapping sidecar to {out.resolve()}")
