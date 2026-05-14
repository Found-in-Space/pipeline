from pathlib import Path

import click

import foundinspace.pipeline.identifiers.download
from foundinspace.pipeline.identifiers.pipeline import prepare_identifiers_sidecar
from foundinspace.pipeline.project import load_project


@click.group(name="identifiers")
def cli():
    pass


cli.add_command(foundinspace.pipeline.identifiers.download.main, name="download")


def _load_project_or_die(project_path: Path):
    try:
        return load_project(project_path)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc


def _crossmatch_parquet(project) -> Path | None:
    """Read crossmatch path from [gaia-to-hip] if the section and file exist."""
    try:
        cm = project.gaia_to_hip.output_parquet
    except ValueError:
        return None
    if not cm.is_file():
        return None
    return cm


def _overrides_data_dir(project) -> Path | None:
    """Read data_dir from [overrides] if the section and key exist."""
    try:
        return project.overrides.data_dir
    except ValueError:
        return None


@cli.command(name="build")
@click.option(
    "--project",
    "project_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to pipeline project TOML.",
)
@click.option("--force", "-f", is_flag=True, default=False)
def build(
    project_path: Path,
    force: bool,
) -> None:
    project = _load_project_or_die(project_path)
    outputs = foundinspace.pipeline.identifiers.download.ensure_identifier_catalogs(
        hip_hd_output=project.identifiers.hip_hd_ecsv,
        iv27a_catalog_output=project.identifiers.iv27a_catalog_ecsv,
        iv27a_proper_names_output=project.identifiers.iv27a_proper_names_ecsv,
        force=force,
    )
    out = prepare_identifiers_sidecar(
        outputs["hip_hd"],
        outputs["iv27a_catalog"],
        outputs["iv27a_proper_names"],
        project.identifiers.output_parquet,
        crossmatch_parquet=_crossmatch_parquet(project),
        overrides_data_dir=_overrides_data_dir(project),
        overwrite=force,
    )
    click.echo(f"Wrote wide identifier sidecar to {out.resolve()}")
