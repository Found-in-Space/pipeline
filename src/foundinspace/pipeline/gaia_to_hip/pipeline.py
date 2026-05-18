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

    out = out.drop_duplicates(subset=["gaia_source_id", "hip_source_id"])
    return out.sort_values(
        by=["gaia_source_id", "hip_source_id"],
        kind="mergesort",
        ignore_index=True,
    )


def normalize_gaia_hip_mapping_dataframe(
    df: pd.DataFrame,
    *,
    source_label: str,
) -> pd.DataFrame:
    """Validate and normalize an existing Gaia-HIP mapping frame."""
    missing = [c for c in GAIA_HIP_MAP_COLS if c not in df.columns]
    if missing:
        raise ValueError(
            f"{source_label} is missing Gaia-HIP mapping column(s): {missing}"
        )

    gaia_numeric, gaia_valid = coerce_positive_integer_values(df["gaia_source_id"])
    hip_numeric, hip_valid = coerce_positive_integer_values(df["hip_source_id"])
    valid_ids = gaia_valid & hip_valid
    if not bool(valid_ids.all()):
        bad = int((~valid_ids).sum())
        raise ValueError(f"{source_label} contains {bad} invalid Gaia/HIP id row(s)")

    mapping_source = df["mapping_source"].astype("string").str.strip()
    if bool(mapping_source.isna().any()) or bool((mapping_source == "").any()):
        raise ValueError(f"{source_label} contains blank mapping_source value(s)")

    number_of_neighbours = pd.to_numeric(df["number_of_neighbours"], errors="coerce")
    if bool(number_of_neighbours.isna().any()):
        raise ValueError(
            f"{source_label} contains invalid number_of_neighbours value(s)"
        )

    out = pd.DataFrame(
        {
            "gaia_source_id": gaia_numeric.astype("uint64"),
            "hip_source_id": hip_numeric.astype("uint64"),
            "mapping_source": mapping_source.astype("object"),
            "number_of_neighbours": number_of_neighbours.astype(np.int16),
            "angular_distance": pd.to_numeric(
                df["angular_distance"], errors="coerce"
            ).astype(np.float32),
        }
    )
    return out[GAIA_HIP_MAP_COLS]


def read_gaia_hip_mapping_parquet(input_parquet: Path) -> pd.DataFrame:
    """Read and validate a Gaia-HIP mapping Parquet sidecar."""
    input_parquet = Path(input_parquet).expanduser()
    if not input_parquet.is_file():
        raise FileNotFoundError(str(input_parquet))
    return normalize_gaia_hip_mapping_dataframe(
        pd.read_parquet(input_parquet),
        source_label=str(input_parquet),
    )


def _raise_for_non_one_to_one(mapping_df: pd.DataFrame) -> None:
    for key_col, other_col in (
        ("gaia_source_id", "hip_source_id"),
        ("hip_source_id", "gaia_source_id"),
    ):
        duplicated = mapping_df.duplicated(subset=[key_col], keep=False)
        if not bool(duplicated.any()):
            continue
        example = mapping_df.loc[duplicated, [key_col, other_col]].head(5)
        raise ValueError(
            "Combined Gaia-HIP mapping is not one-to-one; duplicate "
            f"{key_col} value(s), examples: {example.to_dict(orient='records')}"
        )


def combine_gaia_hip_mappings(mappings: list[pd.DataFrame]) -> pd.DataFrame:
    """Combine official and supplemental Gaia-HIP maps with conflict checks."""
    frames = [m[GAIA_HIP_MAP_COLS] for m in mappings if not m.empty]
    if not frames:
        return empty_gaia_hip_mapping()

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=GAIA_HIP_MAP_COLS, keep="first")

    duplicate_pairs = combined.duplicated(
        subset=["gaia_source_id", "hip_source_id"],
        keep=False,
    )
    if bool(duplicate_pairs.any()):
        example = combined.loc[
            duplicate_pairs,
            ["gaia_source_id", "hip_source_id", "mapping_source"],
        ].head(5)
        raise ValueError(
            "Combined Gaia-HIP mapping has duplicate pair(s) with conflicting "
            f"metadata: {example.to_dict(orient='records')}"
        )

    _raise_for_non_one_to_one(combined)
    return combined.sort_values(
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
    supplemental_parquets: tuple[Path, ...] = (),
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
    official_mapping = build_gaia_hip_mapping_from_dataframe(df)
    supplemental_mappings = [
        read_gaia_hip_mapping_parquet(path) for path in supplemental_parquets
    ]
    mapping = combine_gaia_hip_mappings([official_mapping, *supplemental_mappings])

    output_parquet.parent.mkdir(parents=True, exist_ok=True)
    write_gaia_hip_mapping(mapping, output_parquet)
    return output_parquet
