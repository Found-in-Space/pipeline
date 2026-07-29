"""Hipparcos-only pipeline: ECSV -> parquet (single pass)."""

import logging
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from astropy.table import Table

from foundinspace.pipeline.common.coords import calculate_coordinates_fast
from foundinspace.pipeline.constants import OUTPUT_COLS
from foundinspace.pipeline.hipparcos.astrometry import select_astrometry_hip
from foundinspace.pipeline.hipparcos.photometry import (
    assign_photometry_hip,
    compute_mag_abs_hip,
    compute_teff_hip,
)

_LOG = logging.getLogger(__name__)

HIP_AUXILIARY_COLS = ["Sn", "Hpmag"]
HIP_OUTPUT_COLS = OUTPUT_COLS + HIP_AUXILIARY_COLS

_COLUMN_RENAMES = {
    "HIP": "source_id",
    "RArad": "ra_deg",
    "DErad": "dec_deg",
    "B-V": "bv",
}

_REQUIRED_INPUT_COLS = {
    "HIP",
    "RArad",
    "DErad",
    "Plx",
    "e_Plx",
    "pmRA",
    "pmDE",
    "Hpmag",
    "B-V",
}


def _read_hipparcos_ecsv(input_path: Path) -> pd.DataFrame:
    table = Table.read(input_path, format="ascii.ecsv")
    return table.to_pandas()


def _normalize_hipparcos_columns(df: pd.DataFrame) -> pd.DataFrame:
    missing = sorted(_REQUIRED_INPUT_COLS - set(df.columns))
    if missing:
        joined = ", ".join(missing)
        raise ValueError(f"Missing required Hipparcos columns: {joined}")
    return df.rename(columns=_COLUMN_RENAMES)


def _run_hipparcos_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    work = _normalize_hipparcos_columns(df).copy()
    n_in = len(work)
    work = select_astrometry_hip(work)
    work = assign_photometry_hip(work)
    work = calculate_coordinates_fast(work)
    work = compute_mag_abs_hip(work)
    work = compute_teff_hip(work)
    work["source"] = "hip"
    work["source_id"] = work["source_id"].astype("uint64")
    for col in HIP_AUXILIARY_COLS:
        if col not in work.columns:
            work[col] = np.nan
    ok = np.isfinite(work["distance_use_pc"].to_numpy(dtype=float, copy=False))
    n_drop = int((~ok).sum())
    if n_drop:
        _LOG.info(
            "Hipparcos: dropped %s / %s rows after distance estimation failed",
            n_drop,
            n_in,
        )
        print(
            f"Hipparcos: dropped {n_drop:,} / {n_in:,} rows without finite distance_use_pc",
            flush=True,
        )
        work = work.loc[ok].reset_index(drop=True)
    return work[HIP_OUTPUT_COLS]


def main(
    input_path: Path,
    output_path: Path,
    *,
    skip_if_exists: bool = True,
    limit: int | None = None,
) -> None:
    """Run Hipparcos pipeline for one ECSV input and write parquet output.

    ``limit``, if set, is applied after distance estimation: at most ``limit``
    rows with finite ``distance_use_pc`` are written.
    """
    if skip_if_exists and output_path.exists():
        print(f"Skipping {input_path} (output exists: {output_path})")
        return

    print(f"Reading {input_path}...")
    raw = _read_hipparcos_ecsv(input_path)
    result = _run_hipparcos_pipeline(raw)
    if limit is not None:
        result = result.head(limit)

    table = pa.Table.from_pandas(result, preserve_index=False)
    pq.write_table(table, str(output_path), compression="zstd")
    print(f"Done. Wrote {len(result):,} rows to {output_path}")
