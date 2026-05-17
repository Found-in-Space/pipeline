from pathlib import Path

import click

import foundinspace.pipeline.identifiers.download
from foundinspace.pipeline.identifiers.pipeline import prepare_identifiers_sidecar
from foundinspace.pipeline.project import load_project


@click.group(name="identifiers")
def cli():
    pass


cli.add_command(foundinspace.pipeline.identifiers.download.main, name="download")


def _load_project_or_die(project_path: Path, *required: str):
    try:
        project = load_project(project_path)
        if required:
            project.require(*required)
        return project
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc


def _crossmatch_parquet(project) -> Path | None:
    """Read crossmatch path from [gaia-to-hip] if the section and file exist."""
    if not project.gaia_to_hip.is_configured:
        return None
    cm = project.gaia_to_hip.output_parquet
    if not cm.is_file():
        return None
    return cm


def _overrides_include_files(project):
    """Read explicit override include files from [overrides] when configured."""
    if not project.overrides.is_configured:
        return ()
    return project.overrides.include_files


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
    project = _load_project_or_die(project_path, "identifiers")
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
        overrides_include_files=_overrides_include_files(project),
        overwrite=force,
    )
    click.echo(f"Wrote wide identifier sidecar to {out.resolve()}")
