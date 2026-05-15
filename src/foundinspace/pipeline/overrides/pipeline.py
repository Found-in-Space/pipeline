"""Build processed overrides Parquet (OUTPUT_COLS + override metadata) from YAML."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from foundinspace.pipeline.common.ids import serialize_source_id
from foundinspace.pipeline.common.quality_flags import pack_status_flags
from foundinspace.pipeline.constants import DIST_SRC_OVERRIDE, OUTPUT_COLS
from foundinspace.pipeline.overrides.loader import load_normalized_override_stars

OVERRIDE_METADATA_COLS = [
    "override_id",
    "action",
    "override_reason",
    "override_policy_version",
]

OUTPUT_OVERRIDES_COLS = [*OUTPUT_COLS, *OVERRIDE_METADATA_COLS]


def _row_for_star(star: dict[str, Any]) -> dict[str, Any]:
    action = star.get("action")
    meta = {
        "override_id": star["override_id"],
        "action": action,
        "override_reason": star["override_reason"],
        "override_policy_version": star["override_policy_version"],
    }
    if action == "drop":
        out = {
            "source": str(star["source"]),
            "source_id": serialize_source_id(star["source_id"]),
            "x_icrs_pc": pd.NA,
            "y_icrs_pc": pd.NA,
            "z_icrs_pc": pd.NA,
            "ra_deg": pd.NA,
            "dec_deg": pd.NA,
            "r_pc": pd.NA,
            "mag_abs": pd.NA,
            "teff": pd.NA,
            "quality_flags": pd.NA,
            "astrometry_quality": pd.NA,
            "photometry_quality": pd.NA,
        }
        out.update(meta)
        return out

    missing = [k for k in ("mag_abs", "teff") if k not in star or star[k] is None]
    if missing:
        oid = star.get("override_id", "?")
        raise ValueError(f"Non-drop override {oid!r} missing required keys: {missing}")

    qf = int(pack_status_flags(DIST_SRC_OVERRIDE, distance_valid=True))
    out = {
        "source": str(star["source"]),
        "source_id": serialize_source_id(star["source_id"]),
        "x_icrs_pc": float(star["x_icrs_pc"]),
        "y_icrs_pc": float(star["y_icrs_pc"]),
        "z_icrs_pc": float(star["z_icrs_pc"]),
        "ra_deg": float(star["ra_deg"]),
        "dec_deg": float(star["dec_deg"]),
        "r_pc": float(star["r_pc"]),
        "mag_abs": float(star["mag_abs"]),
        "teff": float(star["teff"]),
        "quality_flags": qf,
        "astrometry_quality": 0.0,
        "photometry_quality": 0.0,
    }
    out.update(meta)
    return out


def build_overrides_dataframe(
    *,
    data_dir: Path | None = None,
) -> pd.DataFrame:
    """Load YAML overrides and return a frame with OUTPUT_COLS + override metadata."""
    stars = load_normalized_override_stars(data_dir=data_dir)
    if not stars:
        return pd.DataFrame(columns=OUTPUT_OVERRIDES_COLS)

    rows = [_row_for_star(s) for s in stars]
    df = pd.DataFrame(rows)

    # Enforce column order; drop any stray keys if present
    for c in OUTPUT_OVERRIDES_COLS:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[OUTPUT_OVERRIDES_COLS]

    df["source"] = df["source"].astype("string")
    df["source_id"] = df["source_id"].astype("string")
    df["override_id"] = df["override_id"].astype("string")
    df["action"] = df["action"].astype("string")
    df["override_reason"] = df["override_reason"].astype("string")
    df["override_policy_version"] = df["override_policy_version"].astype("string")

    return df


def prepare_overrides_parquet(
    output_path: Path,
    *,
    data_dir: Path | None = None,
    overwrite: bool = False,
) -> Path:
    """Write ``output_path`` (zstd Parquet) from packaged or custom override YAML dir."""
    output_path = Path(output_path).expanduser()
    if output_path.exists() and not overwrite:
        raise FileExistsError(str(output_path))

    df = build_overrides_dataframe(data_dir=data_dir)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, str(output_path), compression="zstd")
    return output_path
