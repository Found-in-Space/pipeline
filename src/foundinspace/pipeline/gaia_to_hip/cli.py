from pathlib import Path

import click

from foundinspace.pipeline.cli_support import (
    load_project_or_die,
    project_option,
    record_command_or_die,
    require_input_file,
)
from foundinspace.pipeline.gaia_to_hip.download import main as download_command
from foundinspace.pipeline.gaia_to_hip.pipeline import prepare_gaia_hip_mapping


@click.group(name="gaia-to-hip")
def cli():
    """Gaia↔Hipparcos cross-match: download `hipparcos2_best_neighbour`, build sidecar."""


cli.add_command(download_command, name="download")


@cli.command(name="build")
@project_option
@click.option("--force", "-f", is_flag=True, default=False)
def build_cmd(
    project_path: Path,
    force: bool,
) -> None:
    project = load_project_or_die(project_path, "gaia-to-hip")
    download_output = project.gaia_to_hip.download_ecsv
    inputs = {
        "best_neighbour_ecsv": download_output,
        "supplemental_crossmatches": project.gaia_to_hip.supplemental_parquets,
    }
    with record_command_or_die(
        project,
        command="gaia-to-hip build",
        parameters={"force": force},
        inputs=inputs,
        outputs={"accepted_crossmatch": project.gaia_to_hip.output_parquet},
    ):
        require_input_file(
            download_output,
            label="[gaia-to-hip] download_ecsv",
        )
        for supplemental in project.gaia_to_hip.supplemental_parquets:
            require_input_file(
                supplemental,
                label="[gaia-to-hip] supplemental_parquets entry",
            )
        out = prepare_gaia_hip_mapping(
            download_output,
            project.gaia_to_hip.output_parquet,
            supplemental_parquets=project.gaia_to_hip.supplemental_parquets,
            overwrite=force,
        )
    click.echo(f"Wrote Gaia↔HIP mapping sidecar to {out.resolve()}")
