"""Fetch identifier source catalogs from Vizier as ECSV files."""

from __future__ import annotations

from pathlib import Path

import click

from foundinspace.pipeline.cli_support import (
    load_project_or_die,
    project_option,
    record_command_or_die,
)

HIP_HD_CATALOG = "I/239/hip_main"
IV27A_CATALOG = "IV/27A/catalog"
IV27A_PROPER_NAMES = "IV/27A/table3"


def _fetch_catalog_to_ecsv(
    *,
    catalog: str,
    columns: list[str],
    output_path: Path,
    overwrite: bool = False,
) -> Path:
    output_path = Path(output_path).expanduser()
    if output_path.exists() and not overwrite:
        raise FileExistsError(str(output_path))

    from astroquery.vizier import Vizier

    v = Vizier(columns=columns, row_limit=-1)
    tables = v.get_catalogs(catalog)
    table = tables[0]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    table.write(output_path, format="ascii.ecsv", overwrite=True)
    return output_path


def ensure_identifier_catalogs(
    *,
    hip_hd_output: Path,
    iv27a_catalog_output: Path,
    iv27a_proper_names_output: Path,
    force: bool = False,
) -> dict[str, Path]:
    outputs = {
        "hip_hd": Path(hip_hd_output).expanduser(),
        "iv27a_catalog": Path(iv27a_catalog_output).expanduser(),
        "iv27a_proper_names": Path(iv27a_proper_names_output).expanduser(),
    }
    specs = {
        "hip_hd": (HIP_HD_CATALOG, ["HIP", "HD"]),
        "iv27a_catalog": (
            IV27A_CATALOG,
            ["HIP", "HD", "Bayer", "Fl", "Cst"],
        ),
        "iv27a_proper_names": (IV27A_PROPER_NAMES, ["HD", "Name"]),
    }
    for name, path in outputs.items():
        if path.exists() and not path.is_file():
            raise ValueError(f"Output path exists but is not a regular file: {path}")
        if path.is_file() and not force:
            continue
        catalog, columns = specs[name]
        outputs[name] = _fetch_catalog_to_ecsv(
            catalog=catalog,
            columns=columns,
            output_path=path,
            overwrite=True,
        )
    return outputs


@click.command()
@project_option
@click.option(
    "--force",
    "-f",
    is_flag=True,
    default=False,
    help="Re-download files even when all outputs already exist.",
)
def main(
    project_path: Path,
    force: bool,
) -> None:
    project = load_project_or_die(project_path, "identifiers")
    output_paths = {
        "hip_hd": project.identifiers.hip_hd_ecsv,
        "iv27a_catalog": project.identifiers.iv27a_catalog_ecsv,
        "iv27a_proper_names": project.identifiers.iv27a_proper_names_ecsv,
    }
    with record_command_or_die(
        project,
        command="identifiers download",
        parameters={"force": force},
        outputs=output_paths,
    ):
        outputs = ensure_identifier_catalogs(
            hip_hd_output=project.identifiers.hip_hd_ecsv,
            iv27a_catalog_output=project.identifiers.iv27a_catalog_ecsv,
            iv27a_proper_names_output=project.identifiers.iv27a_proper_names_ecsv,
            force=force,
        )
    click.echo(f"HIP→HD catalog ready at {outputs['hip_hd'].resolve()}")
    click.echo(f"IV/27A catalog ready at {outputs['iv27a_catalog'].resolve()}")
    click.echo(
        "IV/27A proper names catalog ready at "
        f"{outputs['iv27a_proper_names'].resolve()}"
    )


if __name__ == "__main__":
    main()
