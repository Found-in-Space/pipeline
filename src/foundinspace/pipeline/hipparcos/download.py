"""Fetch Hipparcos New Reduction (Vizier I/311/hip2) as ECSV."""

from __future__ import annotations

from pathlib import Path

import click

from foundinspace.pipeline.cli_support import (
    load_project_or_die,
    project_option,
    record_command_or_die,
)

VIZIER_CATALOG = "I/311/hip2"


def fetch_hipparcos_to_ecsv(output_path: Path, *, overwrite: bool = False) -> Path:
    """Download the catalog from Vizier and write ``output_path`` (ASCII ECSV).

    Creates parent directories as needed. Pass ``overwrite=True`` to replace an
    existing file.
    """
    output_path = Path(output_path).expanduser()
    if output_path.exists() and not overwrite:
        raise FileExistsError(str(output_path))

    from astroquery.vizier import Vizier

    v = Vizier(columns=["*"], row_limit=-1)
    tables = v.get_catalogs(VIZIER_CATALOG)
    hip_table = tables[0]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    hip_table.write(output_path, format="ascii.ecsv", overwrite=True)
    return output_path


def ensure_hipparcos_ecsv(
    output_path: Path,
    *,
    force: bool = False,
) -> Path:
    """Return path to the Hipparcos ECSV, downloading only if the file is missing.

    Unless ``force`` is true, an existing file is left unchanged.
    """
    path = Path(output_path).expanduser()

    if path.exists() and not force:
        if not path.is_file():
            raise ValueError(f"Output path exists but is not a regular file: {path}")
        return path

    return fetch_hipparcos_to_ecsv(path, overwrite=True)


@click.command()
@project_option
@click.option(
    "--force",
    "-f",
    is_flag=True,
    default=False,
    help="Re-download even if the output file already exists.",
)
def main(project_path: Path, force: bool) -> None:
    project = load_project_or_die(project_path, "hip")
    with record_command_or_die(
        project,
        command="hip download",
        parameters={"force": force},
        outputs={"hipparcos_ecsv": project.hip.download_ecsv},
    ):
        path = ensure_hipparcos_ecsv(project.hip.download_ecsv, force=force)
    click.echo(f"Hipparcos catalog ready at {path.resolve()}")


if __name__ == "__main__":
    main()
