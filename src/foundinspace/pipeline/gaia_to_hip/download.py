"""Fetch Gaia DR3 `hipparcos2_best_neighbour` cross-match from the Gaia archive (TAP)."""

from __future__ import annotations

from pathlib import Path

import click
from astropy.table import Table

from foundinspace.pipeline.common.gaia_credentials import (
    login_gaia_from_environment_if_available,
)
from foundinspace.pipeline.project import load_project

BEST_NEIGHBOUR_QUERY = """
SELECT
  source_id,
  original_ext_source_id,
  angular_distance,
  number_of_neighbours,
  xm_flag
FROM gaiadr3.hipparcos2_best_neighbour
"""


def _launch_gaia_job_async(query: str, gaia_client=None):
    if gaia_client is None:
        from astroquery.gaia import Gaia

        gaia_client = Gaia

    login_gaia_from_environment_if_available(gaia_client)
    return gaia_client.launch_job_async(query)


def fetch_hipparcos2_best_neighbour_to_ecsv(
    output_path: Path,
    *,
    overwrite: bool = False,
) -> Path:
    """Run TAP query and write ``output_path`` (ASCII ECSV).

    Creates parent directories as needed. Pass ``overwrite=True`` to replace an
    existing file.
    """
    output_path = Path(output_path).expanduser()
    if output_path.exists() and not overwrite:
        raise FileExistsError(str(output_path))

    # Use async TAP job to avoid astroquery sync-query TOP truncation.
    job = _launch_gaia_job_async(BEST_NEIGHBOUR_QUERY)
    result = job.get_results()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.write(output_path, format="ascii.ecsv", overwrite=True)
    return output_path


def ensure_hipparcos2_best_neighbour_ecsv(
    output_path: Path,
    *,
    force: bool = False,
) -> Path:
    """Return path to the cross-match ECSV, downloading only if missing.

    Unless ``force`` is true, an existing file is left unchanged.
    """
    path = Path(output_path).expanduser()

    if path.exists() and not force:
        if not path.is_file():
            raise ValueError(f"Output path exists but is not a regular file: {path}")
        return path

    return fetch_hipparcos2_best_neighbour_to_ecsv(path, overwrite=True)


@click.command()
@click.option(
    "--project",
    "project_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to pipeline project TOML.",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    default=False,
    help="Re-download even if the output file already exists.",
)
def main(project_path: Path, force: bool) -> None:
    try:
        project = load_project(project_path)
        project.require("gaia-to-hip")
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    path = ensure_hipparcos2_best_neighbour_ecsv(
        project.gaia_to_hip.download_ecsv,
        force=force,
    )
    row_count = len(Table.read(path, format="ascii.ecsv"))
    click.echo(
        "hipparcos2_best_neighbour catalog ready at "
        f"{path.resolve()} ({row_count:,} rows)"
    )


if __name__ == "__main__":
    main()
