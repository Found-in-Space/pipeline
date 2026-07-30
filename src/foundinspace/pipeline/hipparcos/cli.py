from pathlib import Path

import click

from foundinspace.pipeline.cli_support import (
    load_project_or_die,
    project_option,
    record_command_or_die,
    require_input_file,
)
from foundinspace.pipeline.hipparcos.download import main as download_command
from foundinspace.pipeline.hipparcos.pipeline import main


@click.group(name="hip")
def cli():
    pass


cli.add_command(download_command, name="download")


@cli.command(name="build")
@project_option
@click.option("--force", "-f", is_flag=True, default=False)
@click.option("--limit", "-l", type=int, default=None)
@click.option(
    "--output",
    "output_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Explicit noncanonical output required when --limit is used.",
)
def build_cmd(
    project_path: Path,
    force: bool,
    limit: int | None,
    output_path: Path | None,
) -> None:
    project = load_project_or_die(project_path, "hip")
    input_file = project.hip.download_ecsv
    output_file = output_path or project.hip.output_parquet
    with record_command_or_die(
        project,
        command="hip build",
        parameters={
            "force": force,
            "limit": limit,
            "output": output_file,
        },
        inputs={"hipparcos_ecsv": input_file},
        outputs={"hipparcos_parquet": output_file},
    ):
        require_input_file(input_file, label="[hip] download_ecsv")
        if limit is not None:
            if limit <= 0:
                raise click.ClickException("--limit must be greater than zero")
            if output_path is None:
                raise click.ClickException(
                    "--limit requires --output so a partial catalog cannot "
                    "replace the canonical [hip] output_parquet"
                )
            if output_file.resolve() == project.hip.output_parquet.resolve():
                raise click.ClickException(
                    "Limited Hipparcos output must differ from [hip] output_parquet"
                )
        output_file.parent.mkdir(parents=True, exist_ok=True)
        main(input_file, output_file, skip_if_exists=not force, limit=limit)
