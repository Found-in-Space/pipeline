"""HEALPix shard helpers for merged pipeline output."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from foundinspace.pipeline.constants import OUTPUT_COLS

# Merged HEALPix shards use the same schema as per-catalog OUTPUT_COLS.
MERGED_OUTPUT_COLS = list(OUTPUT_COLS)
MERGED_OUTPUT_SCHEMA = pa.schema(
    [
        pa.field("source", pa.large_string()),
        pa.field("source_id", pa.large_string()),
        pa.field("x_icrs_pc", pa.float64()),
        pa.field("y_icrs_pc", pa.float64()),
        pa.field("z_icrs_pc", pa.float64()),
        pa.field("ra_deg", pa.float64()),
        pa.field("dec_deg", pa.float64()),
        pa.field("r_pc", pa.float64()),
        pa.field("mag_abs", pa.float64()),
        pa.field("teff", pa.float64()),
        pa.field("quality_flags", pa.uint16()),
        pa.field("astrometry_quality", pa.float64()),
        pa.field("photometry_quality", pa.float64()),
    ]
)
if MERGED_OUTPUT_SCHEMA.names != MERGED_OUTPUT_COLS:
    raise RuntimeError("Merged output schema must match OUTPUT_COLS")


def _build_healpix(order: int):
    try:
        from astropy_healpix import HEALPix
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "astropy-healpix is required for merge HEALPix sharding. "
            "Install dependencies (uv sync) and retry."
        ) from exc
    return HEALPix(nside=2**order, order="nested")


def _healpix_pixels(
    hp: Any,
    ra_deg: pd.Series | np.ndarray,
    dec_deg: pd.Series | np.ndarray,
) -> np.ndarray:
    from astropy import units as u

    ra_arr = np.asarray(ra_deg, dtype=float)
    dec_arr = np.asarray(dec_deg, dtype=float)
    return np.asarray(
        hp.lonlat_to_healpix(ra_arr * u.deg, dec_arr * u.deg),
        dtype=np.int64,
    )


def _write_shards(
    df: pd.DataFrame,
    *,
    hp: Any,
    shards_root: Path,
    phase_tag: str,
    seq_by_pixel: dict[int, int],
) -> int:
    if df.empty:
        return 0
    pixels = _healpix_pixels(hp, df["ra_deg"], df["dec_deg"])
    rows_written = 0
    for pixel in sorted(np.unique(pixels)):
        pixel_i = int(pixel)
        pixel_dir = shards_root / str(pixel_i)
        pixel_dir.mkdir(parents=True, exist_ok=True)
        next_seq = seq_by_pixel.get(pixel_i, 0) + 1
        seq_by_pixel[pixel_i] = next_seq
        out_path = pixel_dir / f"{next_seq:06d}_{phase_tag}.parquet"
        part = df.loc[pixels == pixel_i, MERGED_OUTPUT_COLS]
        table = pa.Table.from_pandas(
            part,
            schema=MERGED_OUTPUT_SCHEMA,
            preserve_index=False,
            safe=True,
        )
        pq.write_table(table, str(out_path), compression="zstd")
        rows_written += len(part)
    return rows_written


def _write_sidecar_shards(
    df: pd.DataFrame,
    *,
    hp: Any,
    sidecar_root: Path,
    sidecar_name: str,
    phase_tag: str,
    output_cols: list[str],
    seq_by_key: dict[tuple[str, int], int],
) -> int:
    if df.empty:
        return 0
    pixels = _healpix_pixels(hp, df["_shard_ra_deg"], df["_shard_dec_deg"])
    rows_written = 0
    shards_root = sidecar_root / sidecar_name
    for pixel in sorted(np.unique(pixels)):
        pixel_i = int(pixel)
        pixel_dir = shards_root / str(pixel_i)
        pixel_dir.mkdir(parents=True, exist_ok=True)
        key = (sidecar_name, pixel_i)
        next_seq = seq_by_key.get(key, 0) + 1
        seq_by_key[key] = next_seq
        out_path = pixel_dir / f"{next_seq:06d}_{phase_tag}.parquet"
        part = df.loc[pixels == pixel_i, output_cols]
        table = pa.Table.from_pandas(part, preserve_index=False)
        pq.write_table(table, str(out_path), compression="zstd")
        rows_written += len(part)
    return rows_written
