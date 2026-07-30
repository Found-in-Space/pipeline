from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from foundinspace.pipeline.gaia.download.fieldsets import (
    GaiaCarryField,
    fields_by_sidecar,
)
from foundinspace.pipeline.merge import shards

SIDECAR_ID_COLS = ["source", "source_id", "gaia_source_id"]


def gaia_enrichment_columns(columns: list[str] | set[str]) -> list[str]:
    return [
        column
        for column in columns
        if column.startswith("gaia_") and column != "gaia_source_id"
    ]


def from_frames(
    gaia_df: pd.DataFrame,
    dense_df: pd.DataFrame,
    *,
    enrichment_cols: list[str],
) -> pd.DataFrame:
    if not enrichment_cols or gaia_df.empty or dense_df.empty:
        return pd.DataFrame()
    gaia = gaia_df.reset_index(drop=True)
    dense = dense_df.reset_index(drop=True)
    out = pd.DataFrame(
        {
            "source": dense["source"].astype("string"),
            "source_id": dense["source_id"].astype("string"),
            "gaia_source_id": pd.to_numeric(gaia["source_id"], errors="raise").astype(
                "uint64"
            ),
            "_shard_ra_deg": pd.to_numeric(dense["ra_deg"], errors="coerce"),
            "_shard_dec_deg": pd.to_numeric(dense["dec_deg"], errors="coerce"),
        }
    )
    for col in enrichment_cols:
        out[col] = gaia[col].reset_index(drop=True) if col in gaia.columns else pd.NA
    return out


def from_records(
    gaia_records: list[dict[str, Any]],
    dense_records: list[dict[str, Any]],
    *,
    enrichment_cols: list[str],
) -> pd.DataFrame:
    if not enrichment_cols or not gaia_records or not dense_records:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    for gaia_rec, dense_rec in zip(gaia_records, dense_records, strict=True):
        row = {
            "source": str(dense_rec["source"]),
            "source_id": str(dense_rec["source_id"]),
            "gaia_source_id": int(gaia_rec["source_id"]),
            "_shard_ra_deg": dense_rec["ra_deg"],
            "_shard_dec_deg": dense_rec["dec_deg"],
        }
        for col in enrichment_cols:
            row[col] = gaia_rec.get(col, pd.NA)
        rows.append(row)
    return pd.DataFrame(rows)


def write_gaia_sidecars(
    sidecar_df: pd.DataFrame,
    *,
    carry_fields: tuple[GaiaCarryField, ...],
    hp: Any,
    sidecar_root: Path,
    phase_tag: str,
    seq_by_key: dict[tuple[str, int], int],
) -> int:
    if sidecar_df.empty:
        return 0
    enrichment_cols = gaia_enrichment_columns(list(sidecar_df.columns))
    if not enrichment_cols:
        return 0

    rows = shards._write_sidecar_shards(
        sidecar_df,
        hp=hp,
        sidecar_root=sidecar_root,
        sidecar_name="gaia_enrichment",
        phase_tag=phase_tag,
        output_cols=SIDECAR_ID_COLS + enrichment_cols,
        seq_by_key=seq_by_key,
    )
    for sidecar_name, declared_fields in fields_by_sidecar(carry_fields).items():
        rows += _write_declared_sidecar(
            sidecar_df,
            hp=hp,
            sidecar_root=sidecar_root,
            sidecar_name=sidecar_name,
            fields=declared_fields,
            phase_tag=phase_tag,
            seq_by_key=seq_by_key,
        )
    return rows


def _write_declared_sidecar(
    sidecar_df: pd.DataFrame,
    *,
    hp: Any,
    sidecar_root: Path,
    sidecar_name: str,
    fields: tuple[GaiaCarryField, ...],
    phase_tag: str,
    seq_by_key: dict[tuple[str, int], int],
) -> int:
    available = [
        (field.output_column, field.sidecar_output_column)
        for field in fields
        if field.output_column in sidecar_df.columns
    ]
    if not available:
        return 0
    out = sidecar_df[[*SIDECAR_ID_COLS, "_shard_ra_deg", "_shard_dec_deg"]].copy()
    output_cols = list(SIDECAR_ID_COLS)
    for src, dst in available:
        out[dst] = sidecar_df[src]
        output_cols.append(dst)
    return shards._write_sidecar_shards(
        out,
        hp=hp,
        sidecar_root=sidecar_root,
        sidecar_name=sidecar_name,
        phase_tag=phase_tag,
        output_cols=output_cols,
        seq_by_key=seq_by_key,
    )
