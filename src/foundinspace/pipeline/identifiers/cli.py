from pathlib import Path

import click

from foundinspace.pipeline.cli_support import (
    load_project_or_die,
    project_option,
    record_command_or_die,
    require_input_file,
)
from foundinspace.pipeline.identifiers.download import main as download_command
from foundinspace.pipeline.identifiers.pipeline import prepare_identifiers_sidecar


@click.group(name="identifiers")
def cli():
    pass


cli.add_command(download_command, name="download")


def _overrides_data_dir(project) -> Path | None:
    """Read data_dir from [overrides] if the section and key exist."""
    if not project.overrides.is_configured:
        return None
    return project.overrides.data_dir


def _overrides_source_paths(project) -> tuple[Path, ...] | None:
    """Read explicit source_paths from [overrides] if configured."""
    if not project.overrides.is_configured:
        return None
    return project.overrides.source_paths


@cli.command(name="build")
@project_option
@click.option("--force", "-f", is_flag=True, default=False)
def build(
    project_path: Path,
    force: bool,
) -> None:
    project = load_project_or_die(project_path, "identifiers", "gaia-to-hip")
    source_catalogs = {
        "hip_hd": project.identifiers.hip_hd_ecsv,
        "iv27a_catalog": project.identifiers.iv27a_catalog_ecsv,
        "iv27a_proper_names": project.identifiers.iv27a_proper_names_ecsv,
    }
    crossmatch = project.gaia_to_hip.output_parquet
    inputs: dict[str, Path | tuple[Path, ...]] = {
        **source_catalogs,
        "accepted_crossmatch": crossmatch,
    }
    overrides_data_dir = _overrides_data_dir(project)
    overrides_source_paths = _overrides_source_paths(project)
    if overrides_data_dir is not None:
        inputs["override_data_dir"] = overrides_data_dir
    if overrides_source_paths is not None:
        inputs["override_sources"] = overrides_source_paths

    with record_command_or_die(
        project,
        command="identifiers build",
        parameters={"force": force},
        inputs=inputs,
        outputs={"identifier_sidecar": project.identifiers.output_parquet},
    ):
        for name, path in source_catalogs.items():
            require_input_file(path, label=f"[identifiers] {name}_ecsv")
        require_input_file(
            crossmatch,
            label="[gaia-to-hip] output_parquet",
        )
        if overrides_source_paths is not None:
            for source_path in overrides_source_paths:
                require_input_file(
                    source_path,
                    label="[overrides] source_paths entry",
                )
        out = prepare_identifiers_sidecar(
            source_catalogs["hip_hd"],
            source_catalogs["iv27a_catalog"],
            source_catalogs["iv27a_proper_names"],
            project.identifiers.output_parquet,
            crossmatch_parquet=crossmatch,
            overrides_data_dir=overrides_data_dir,
            overrides_source_paths=overrides_source_paths,
            overwrite=force,
        )
    click.echo(f"Wrote wide identifier sidecar to {out.resolve()}")
