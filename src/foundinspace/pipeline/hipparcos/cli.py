from pathlib import Path

import click

import foundinspace.pipeline.hipparcos.download
from foundinspace.pipeline.hipparcos.pipeline import main
from foundinspace.pipeline.project import load_project


@click.group(name="hip")
def cli():
    pass


cli.add_command(foundinspace.pipeline.hipparcos.download.main, name="download")


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
@click.option("--limit", "-l", type=int, default=None)
def build_cmd(
    project_path: Path,
    force: bool,
    limit: int | None,
) -> None:
    project = _load_project_or_die(project_path, "hip")
    input_file = foundinspace.pipeline.hipparcos.download.ensure_hipparcos_ecsv(
        project.hip.download_ecsv,
        force=force,
    )
    output_file = project.hip.output_parquet
    output_file.parent.mkdir(parents=True, exist_ok=True)
    main(input_file, output_file, skip_if_exists=not force, limit=limit)
