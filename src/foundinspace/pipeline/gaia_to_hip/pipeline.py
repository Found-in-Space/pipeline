"""Build Gaia↔HIP mapping Parquet from `hipparcos2_best_neighbour` ECSV."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from astropy.table import Table

from foundinspace.pipeline.common.ids import coerce_positive_integer_values

GAIA_HIP_MAP_COLS = [
    "gaia_source_id",
    "hip_source_id",
    "mapping_source",
    "number_of_neighbours",
    "angular_distance",
    "xm_flag",
]
MAPPING_SOURCE_HIPPARCOS2_BEST_NEIGHBOUR = "hipparcos2_best_neighbour"


def empty_gaia_hip_mapping() -> pd.DataFrame:
    """Return an empty Gaia↔HIP mapping frame with stable dtypes."""
    return pd.DataFrame(
        {
            "gaia_source_id": pd.Series(dtype=np.uint64),
            "hip_source_id": pd.Series(dtype=np.uint64),
            "mapping_source": pd.Series(dtype="object"),
            "number_of_neighbours": pd.Series(dtype=np.int16),
            "angular_distance": pd.Series(dtype=np.float32),
            "xm_flag": pd.Series(dtype=np.int16),
        }
    )


def _normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase column names for robust ECSV reads."""
    out = df.copy()
    out.columns = [str(c).lower() for c in out.columns]
    return out


def build_gaia_hip_mapping_from_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Map `source_id` / `original_ext_source_id` rows to the sidecar schema."""
    df = _normalize_column_names(df)
    if "source_id" not in df.columns or "original_ext_source_id" not in df.columns:
        return empty_gaia_hip_mapping()

    gaia_numeric, gaia_valid = coerce_positive_integer_values(df["source_id"])
    hip_numeric, hip_valid = coerce_positive_integer_values(
        df["original_ext_source_id"]
    )
    valid = gaia_valid & hip_valid
    if not valid.any():
        return empty_gaia_hip_mapping()

    out = pd.DataFrame(
        {
            "gaia_source_id": gaia_numeric[valid].astype("uint64"),
            "hip_source_id": hip_numeric[valid].astype("uint64"),
            "mapping_source": MAPPING_SOURCE_HIPPARCOS2_BEST_NEIGHBOUR,
        }
    )

    n_neigh_col = "number_of_neighbours"
    ang_dist_col = "angular_distance"
    xm_flag_col = "xm_flag"
    if n_neigh_col in df.columns:
        out[n_neigh_col] = (
            pd.to_numeric(df.loc[valid, n_neigh_col].values, errors="coerce")
            .astype(np.float32)
            .astype(np.int16)
        )
    else:
        out[n_neigh_col] = np.int16(0)
    if ang_dist_col in df.columns:
        out[ang_dist_col] = pd.to_numeric(
            df.loc[valid, ang_dist_col].values, errors="coerce"
        ).astype(np.float32)
    else:
        out[ang_dist_col] = np.float32(np.nan)
    if xm_flag_col in df.columns:
        out[xm_flag_col] = (
            pd.to_numeric(df.loc[valid, xm_flag_col].values, errors="coerce")
            .astype(np.float32)
            .astype(np.int16)
        )
    else:
        out[xm_flag_col] = np.int16(0)

    out = out.drop_duplicates(subset=["gaia_source_id", "hip_source_id"])
    return out.sort_values(
        by=["gaia_source_id", "hip_source_id"],
        kind="mergesort",
        ignore_index=True,
    )


def write_gaia_hip_mapping(mapping_df: pd.DataFrame, output_path: Path) -> None:
    """Write Gaia↔HIP mapping sidecar as Parquet with deterministic schema."""
    table = pa.Table.from_pandas(mapping_df[GAIA_HIP_MAP_COLS], preserve_index=False)
    pq.write_table(table, str(output_path), compression="zstd")


def prepare_gaia_hip_mapping(
    input_ecsv: Path,
    output_parquet: Path,
    *,
    overwrite: bool = False,
) -> Path:
    """Read cross-match ECSV and write `gaia_hip_map.parquet`."""
    input_ecsv = Path(input_ecsv).expanduser()
    output_parquet = Path(output_parquet).expanduser()

    if not input_ecsv.is_file():
        raise FileNotFoundError(str(input_ecsv))
    if output_parquet.exists() and not overwrite:
        raise FileExistsError(str(output_parquet))

    table = Table.read(input_ecsv, format="ascii.ecsv")
    df = table.to_pandas()
    mapping = build_gaia_hip_mapping_from_dataframe(df)

    output_parquet.parent.mkdir(parents=True, exist_ok=True)
    write_gaia_hip_mapping(mapping, output_parquet)
    return output_parquet
