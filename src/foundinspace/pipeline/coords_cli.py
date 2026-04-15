from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import click
import pandas as pd

from foundinspace.pipeline.coordinate_converter import (
    convert_coordinate_table,
    dataframe_to_json_records,
    to_viewer_records,
)


@click.group(name="coords")
def cli() -> None:
    """Ad hoc coordinate conversion utilities."""


def _single_row_from_options(**values: Any) -> pd.DataFrame:
    row = {key: value for key, value in values.items() if value is not None}
    return pd.DataFrame([row])


@cli.command(name="convert")
@click.option(
    "--input",
    "input_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="CSV file with coordinate rows.",
)
@click.option(
    "--output",
    "output_path",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Write converted rows here instead of stdout.",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["csv", "json", "viewer-json"]),
    default="csv",
    show_default=True,
    help="Output format.",
)
@click.option("--name", help="Optional object name for single-row conversion.")
@click.option("--source", help="Optional source label for single-row conversion.")
@click.option("--source-id", help="Optional source id for single-row conversion.")
@click.option("--ra", help="RA as decimal degrees or sexagesimal hourangle.")
@click.option("--dec", help="Dec as decimal degrees or sexagesimal degrees.")
@click.option("--epoch-yr", type=float, help="Source position epoch, e.g. 2000.0.")
@click.option("--pmra-masyr", type=float, help="pmRA* in mas/yr.")
@click.option("--pmdec-masyr", type=float, help="pmDec in mas/yr.")
@click.option("--parallax-mas", type=float, help="Parallax in mas.")
@click.option("--distance-pc", type=float, help="Distance in parsecs.")
@click.option("--teff-k", type=float, help="Effective temperature in kelvin.")
def convert(
    input_path: Path | None,
    output_path: Path | None,
    output_format: str,
    name: str | None,
    source: str | None,
    source_id: str | None,
    ra: str | None,
    dec: str | None,
    epoch_yr: float | None,
    pmra_masyr: float | None,
    pmdec_masyr: float | None,
    parallax_mas: float | None,
    distance_pc: float | None,
    teff_k: float | None,
) -> None:
    """Convert a CSV or one coordinate row to project ICRS pc coordinates."""
    if input_path is not None:
        single_values = [
            name,
            source,
            source_id,
            ra,
            dec,
            epoch_yr,
            pmra_masyr,
            pmdec_masyr,
            parallax_mas,
            distance_pc,
            teff_k,
        ]
        if any(value is not None for value in single_values):
            raise click.ClickException(
                "Use either --input or single-row flags, not both"
            )
        input_df = pd.read_csv(input_path)
    else:
        input_df = _single_row_from_options(
            name=name,
            source=source,
            source_id=source_id,
            ra=ra,
            dec=dec,
            epoch_yr=epoch_yr,
            pmra_masyr=pmra_masyr,
            pmdec_masyr=pmdec_masyr,
            parallax_mas=parallax_mas,
            distance_pc=distance_pc,
            teff_k=teff_k,
        )

    try:
        converted = convert_coordinate_table(input_df)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc

    if output_format == "json":
        text = dataframe_to_json_records(converted)
    elif output_format == "viewer-json":
        text = json.dumps(to_viewer_records(converted), indent=2)
    else:
        text = converted.to_csv(index=False)

    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            text if text.endswith("\n") else f"{text}\n", encoding="utf-8"
        )
        click.echo(f"Wrote converted coordinates to {output_path.resolve()}")
        return

    click.echo(text, nl=not text.endswith("\n"))
