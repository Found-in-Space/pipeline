"""Gaia-only pipeline: stream VOTable -> {input_stem}.parquet (batched).

Reads Gaia data via votpipe in large batches, runs Gaia-specific astrometry,
photometry, coordinates, absolute magnitude, and Teff preparation; appends each
non-empty batch to a single compressed Parquet file. No overrides; composition
happens later.
"""

import gzip
import lzma
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from votpipe import parse_votable

from foundinspace.pipeline.common.coords import calculate_coordinates_fast
from foundinspace.pipeline.constants import OUTPUT_COLS
from foundinspace.pipeline.gaia.astrometry import select_astrometry_gaia
from foundinspace.pipeline.gaia.photometry import (
    assign_photometry_gaia,
    compute_mag_abs_gaia,
    compute_teff_gaia,
)

BATCH_SIZE = 1_000_000

GAIA_AUXILIARY_COLS = ["ruwe", "phot_g_mean_mag"]
GAIA_OUTPUT_COLS = OUTPUT_COLS + GAIA_AUXILIARY_COLS


def _run_gaia_pipeline_batch(df: pd.DataFrame) -> pd.DataFrame:
    """Run the Gaia pipeline on a single batch; returns DataFrame with _OUTPUT_COLS only."""
    df = df.copy()
    bp = df.get("phot_bp_mean_mag", pd.Series(np.nan, index=df.index)).astype(float)
    rp = df.get("phot_rp_mean_mag", pd.Series(np.nan, index=df.index)).astype(float)
    df["bp_rp"] = bp - rp

    work = select_astrometry_gaia(df)
    work = assign_photometry_gaia(work)
    # work = filter_positional_gaia(work)
    work = calculate_coordinates_fast(work)
    work = compute_mag_abs_gaia(work)
    work = compute_teff_gaia(work)
    # work = compute_log_g_gaia(work)
    work["source"] = "gaia"
    work["source_id"] = work["source_id"].astype("uint64")
    for col in GAIA_AUXILIARY_COLS:
        if col not in work.columns:
            work[col] = np.nan
    return work[GAIA_OUTPUT_COLS]


def main(
    input_path: Path,
    output_path: Path,
    *,
    skip_if_exists: bool = True,
    mag_limit: float | None = None,
) -> None:
    """Stream input_path (VOTable), run pipeline per batch, write to output_path."""
    if skip_if_exists and output_path.exists():
        print(f"Skipping {input_path} (output exists: {output_path})")
        return

    name_lower = input_path.name.lower()
    if name_lower.endswith(".vot.gz"):
        open_fn = gzip.open
    elif name_lower.endswith(".vot.xz"):
        open_fn = lzma.open
    else:
        open_fn = open
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
        if mag_limit is not None:
            g_mag = pd.to_numeric(
                df.get("phot_g_mean_mag", pd.Series(np.nan, index=df.index)),
                errors="coerce",
            )
            df = df[g_mag <= mag_limit]
            if df.empty:
                return
        result = _run_gaia_pipeline_batch(df)
        if len(result) == 0:
            return
        table = pa.Table.from_pandas(result, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter(
                str(output_path),
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
    finally:
        if writer is not None:
            writer.close()
    print(f"Done. Wrote {written_rows:,} rows to {output_path}")
