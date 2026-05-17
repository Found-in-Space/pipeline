from pathlib import Path

import click

from foundinspace.pipeline.overrides.pipeline import prepare_overrides_parquet
from foundinspace.pipeline.project import load_project


@click.group(name="overrides")
def cli():
    """Manual override YAML → processed Parquet for merger."""


def _load_project_or_die(project_path: Path, *required: str):
    try:
        project = load_project(project_path)
        if required:
            project.require(*required)
        return project
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
def build(project_path: Path, force: bool) -> None:
    """Write OUTPUT_COLS + override metadata to Parquet (zstd)."""
    project = _load_project_or_die(project_path, "overrides")
    out = prepare_overrides_parquet(
        project.overrides.output_parquet,
        include_files=project.overrides.include_files,
        overwrite=force,
    )
    click.echo(f"Wrote overrides table to {out.resolve()}")
