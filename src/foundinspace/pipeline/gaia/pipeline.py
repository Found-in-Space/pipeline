"""Gaia-only pipeline: stream VOTable -> {input_stem}.parquet (batched).

Reads Gaia data via votpipe in large batches, runs Gaia-specific astrometry,
photometry, coordinates, absolute magnitude, and Teff preparation; appends each
non-empty batch to a single compressed Parquet file. No overrides; composition
happens later.
"""

import gzip
import lzma
import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from votpipe import parse_votable

from foundinspace.pipeline.common.coords import calculate_coordinates_fast
from foundinspace.pipeline.constants import OUTPUT_COLS
from foundinspace.pipeline.gaia.astrometry import select_astrometry_gaia
from foundinspace.pipeline.gaia.download.fieldsets import (
    GaiaCarryField,
    gaia_enrichment_columns,
)
from foundinspace.pipeline.gaia.engines import (
    DEFAULT_GAIA_CALCULATION_ENGINE,
    GaiaCalculationEngine,
    get_gaia_calculation_engine,
)
from foundinspace.pipeline.gaia.photometry import (
    assign_photometry_gaia,
    compute_mag_abs_gaia,
    compute_teff_gaia,
)

BATCH_SIZE = 1_000_000

GAIA_DECISION_AUX_COLS = ["ruwe", "phot_g_mean_mag"]
GAIA_OUTPUT_COLS = OUTPUT_COLS + GAIA_DECISION_AUX_COLS


def _coerce_carry_series(series: pd.Series, dtype: str, index: pd.Index) -> pd.Series:
    aligned = series.reindex(index)
    if dtype == "string":
        return aligned.astype("string")
    if dtype in {"int64", "uint64"}:
        numeric = pd.to_numeric(aligned, errors="coerce")
        out_dtype = "Int64" if dtype == "int64" else "UInt64"
        return numeric.round().astype(out_dtype)
    if dtype == "bool":
        return aligned.astype("boolean")
    return pd.to_numeric(aligned, errors="coerce").astype("float64")


def _apply_carry_fields(
    work: pd.DataFrame,
    input_df: pd.DataFrame,
    carry_fields: tuple[GaiaCarryField, ...],
) -> pd.DataFrame:
    if not carry_fields:
        return work
    for field in carry_fields:
        source_df = work if field.source == "stage" else input_df
        source_col = field.input_column
        if source_col not in source_df.columns:
            raise ValueError(
                "Missing required Gaia carry field source column "
                f"{source_col!r} for field {field.name!r}"
            )
        work[field.output_column] = _coerce_carry_series(
            source_df[source_col],
            field.dtype,
            work.index,
        )
    return work


def _engine_from_name_or_object(
    calculation_engine: str | GaiaCalculationEngine,
) -> GaiaCalculationEngine:
    if isinstance(calculation_engine, GaiaCalculationEngine):
        return calculation_engine
    return get_gaia_calculation_engine(calculation_engine)


def _run_gaia_pipeline_batch(
    df: pd.DataFrame,
    *,
    carry_fields: tuple[GaiaCarryField, ...] = (),
    calculation_engine: str | GaiaCalculationEngine = DEFAULT_GAIA_CALCULATION_ENGINE,
) -> pd.DataFrame:
    """Run the Gaia pipeline on a single batch and return dense plus aux columns."""
    engine = _engine_from_name_or_object(calculation_engine)
    engine.validate_input_columns(set(df.columns))
    df = df.copy()
    bp = df["phot_bp_mean_mag"].astype(float)
    rp = df["phot_rp_mean_mag"].astype(float)
    df["bp_rp"] = bp - rp

    work = select_astrometry_gaia(df)
    work = assign_photometry_gaia(work)
    work = calculate_coordinates_fast(work)
    work = compute_mag_abs_gaia(work)
    work = compute_teff_gaia(work)
    work["source"] = "gaia"
    work["source_id"] = work["source_id"].astype("uint64")
    work = _apply_carry_fields(work, df, carry_fields)
    missing_aux = [col for col in GAIA_DECISION_AUX_COLS if col not in work.columns]
    if missing_aux:
        raise ValueError(
            "Gaia pipeline lost required auxiliary column(s): " + ", ".join(missing_aux)
        )
    output_cols = GAIA_OUTPUT_COLS + gaia_enrichment_columns(carry_fields)
    return work[output_cols]


def main(
    input_path: Path,
    output_path: Path,
    *,
    skip_if_exists: bool = True,
    mag_limit: float | None = None,
    carry_fields: tuple[GaiaCarryField, ...] = (),
    calculation_engine: str | GaiaCalculationEngine = DEFAULT_GAIA_CALCULATION_ENGINE,
) -> None:
    """Stream input_path (VOTable), run pipeline per batch, write to output_path."""
    if skip_if_exists and output_path.exists():
        print(f"Skipping {input_path} (output exists: {output_path})")
        return
    engine = _engine_from_name_or_object(calculation_engine)

    name_lower = input_path.name.lower()
    if name_lower.endswith(".vot.gz"):
        open_fn = gzip.open
    elif name_lower.endswith(".vot.xz"):
        open_fn = lzma.open
    else:
        open_fn = open
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_name(f".{output_path.name}.tmp")
    if tmp_path.exists():
        tmp_path.unlink()
    writer = None
    written_rows = 0
    batch_count = 0

    def on_batch(fields: list, rows: list) -> None:
        nonlocal writer, written_rows, batch_count
        if not rows:
            return
        names = [f["name"] for f in fields]
        names_lower = [n.lower() for n in names]
        df = pd.DataFrame(rows, columns=names_lower)
        engine.validate_input_columns(set(df.columns))
        if mag_limit is not None:
            g_mag = pd.to_numeric(
                df["phot_g_mean_mag"],
                errors="coerce",
            )
            df = df[g_mag <= mag_limit]
            if df.empty:
                return
        result = _run_gaia_pipeline_batch(
            df,
            carry_fields=carry_fields,
            calculation_engine=engine,
        )
        if len(result) == 0:
            return
        table = pa.Table.from_pandas(result, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter(
                str(tmp_path),
                table.schema,
                compression="zstd",
            )
        writer.write_table(table)
        written_rows += len(result)
        batch_count += 1
        print(f"  Wrote batch {batch_count:,}: {len(result):,} rows")

    print(f"Streaming {input_path} (batch_size={BATCH_SIZE:,})...")
    try:
        with open_fn(input_path, "rb") as f:
            parse_votable(f, on_batch, batch_size=BATCH_SIZE)
        if writer is not None:
            writer.close()
            writer = None
            os.replace(tmp_path, output_path)
    finally:
        if writer is not None:
            writer.close()
        if tmp_path.exists():
            tmp_path.unlink()
    print(f"Done. Wrote {written_rows:,} rows to {output_path}")
