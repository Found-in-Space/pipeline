from pathlib import Path

import click

from foundinspace.pipeline.cli_support import (
    load_project_or_die,
    project_option,
    record_command_or_die,
    require_input_file,
)
from foundinspace.pipeline.overrides.pipeline import prepare_overrides_parquet


@click.group(name="overrides")
def cli():
    """Manual override YAML → processed Parquet for merger."""


@cli.command(name="build")
@project_option
@click.option("--force", "-f", is_flag=True, default=False)
def build(project_path: Path, force: bool) -> None:
    """Write OUTPUT_COLS + override metadata to Parquet (zstd)."""
    project = load_project_or_die(project_path, "overrides")
    data_dir = project.overrides.data_dir
    source_paths = project.overrides.source_paths
    inputs: dict[str, Path | tuple[Path, ...]] = {}
    if data_dir is not None:
        inputs["override_data_dir"] = data_dir
    if source_paths is not None:
        inputs["override_sources"] = source_paths
    with record_command_or_die(
        project,
        command="overrides build",
        parameters={"force": force},
        inputs=inputs,
        outputs={"overrides_parquet": project.overrides.output_parquet},
    ):
        if data_dir is not None and not data_dir.is_dir():
            raise click.ClickException(
                f"[overrides] data_dir does not exist: {data_dir}"
            )
        if source_paths is not None:
            for source_path in source_paths:
                require_input_file(
                    source_path,
                    label="[overrides] source_paths entry",
                )
        out = prepare_overrides_parquet(
            project.overrides.output_parquet,
            data_dir=data_dir,
            source_paths=source_paths,
            overwrite=force,
        )
    click.echo(f"Wrote overrides table to {out.resolve()}")
