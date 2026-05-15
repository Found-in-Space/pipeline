from pathlib import Path

import click

from foundinspace.pipeline.gaia.download.cli import cli as download_cli
from foundinspace.pipeline.gaia.download.fieldsets import load_gaia_field_sets
from foundinspace.pipeline.gaia.pipeline import main
from foundinspace.pipeline.project import load_project

_VOT_SUFFIXES = {".vot", ".vot.gz", ".vot.xz"}


@click.group(name="gaia")
def cli():
    pass


cli.add_command(download_cli)


def _load_project_or_die(project_path: Path):
    try:
        return load_project(project_path)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc


def _is_votable(path: Path) -> bool:
    name = path.name.lower()
    return any(name.endswith(s) for s in _VOT_SUFFIXES)


@cli.command(name="build")
@click.option(
    "--project",
    "project_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to pipeline project TOML.",
)
@click.option("--force", "-f", is_flag=True, default=False)
def build_gaia(
    project_path: Path,
    force: bool = False,
):
    project = _load_project_or_die(project_path)
    input_dir = project.gaia.input_dir
    if not input_dir.is_dir():
        raise click.ClickException(f"[gaia] input_dir does not exist: {input_dir}")

    input_files = sorted(p for p in input_dir.iterdir() if p.is_file() and _is_votable(p))
    if not input_files:
        raise click.ClickException(f"No VOTable files found in {input_dir}")

    output_root = project.gaia.output_dir
    output_root.mkdir(parents=True, exist_ok=True)
    carry_fields = ()
    if project.gaia_download.configured:
        carry_fields = load_gaia_field_sets(project.gaia_download.carry_field_sets)

    for input_file in input_files:
        output_name = _output_path_for(input_file)
        output_file = output_root / output_name
        main(
            input_file,
            output_file,
            skip_if_exists=not force,
            mag_limit=project.gaia.mag_limit,
            carry_fields=carry_fields,
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
