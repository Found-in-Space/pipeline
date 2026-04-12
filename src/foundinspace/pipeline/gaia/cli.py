from pathlib import Path

import click

from foundinspace.pipeline.gaia import download
from foundinspace.pipeline.gaia.pipeline import main
from foundinspace.pipeline.project import PipelineProject, load_project


@click.group(name="gaia")
def cli():
    pass


cli.add_command(download.main, name="download")


def _load_project_or_die(project_path: Path, *required: str) -> PipelineProject:
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
@click.argument(
    "input_files",
    nargs=-1,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
)
@click.option("--force", "-f", is_flag=True, default=False)
def build_gaia(
    project_path: Path,
    input_files: list[Path],
    force: bool = False,
):
    if len(input_files) == 0:
        click.echo("No input files provided")
        return

    project = _load_project_or_die(project_path, "gaia")
    output_root = project.gaia.output_dir
    output_root.mkdir(parents=True, exist_ok=True)

    for input_file in input_files:
        output_name = _output_path_for(input_file)
        output_file = output_root / output_name
        main(
            input_file,
            output_file,
            skip_if_exists=not force,
            mag_limit=project.gaia.mag_limit,
        )


def _output_path_for(input_path: Path) -> str:
    """Parquet output filename for a given VOTable input (stem from name)."""
    name_lower = input_path.name.lower()
    if name_lower.endswith(".vot.gz"):
        output_base = input_path.name[: -len(".vot.gz")]
    elif name_lower.endswith(".vot.xz"):
        output_base = input_path.name[: -len(".vot.xz")]
    else:
        output_base = input_path.stem
    return f"{output_base}.parquet"
