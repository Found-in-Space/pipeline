"""Supported HEALPix sharding helpers for downstream catalog tooling."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from foundinspace.pipeline.merge.shards import (
    MERGED_OUTPUT_COLS,
    _build_healpix,
    _write_sidecar_shards,
)


def build_healpix(order: int):
    """Build the nested HEALPix index used by pipeline shard layouts."""
    return _build_healpix(order)


def write_sidecar_shards(
    df: pd.DataFrame,
    *,
    hp: Any,
    sidecar_root: Path,
    sidecar_name: str,
    phase_tag: str,
    output_cols: list[str],
    seq_by_key: dict[tuple[str, int], int],
) -> int:
    """Write a sparse sidecar using the pipeline HEALPix layout."""
    return _write_sidecar_shards(
        df,
        hp=hp,
        sidecar_root=sidecar_root,
        sidecar_name=sidecar_name,
        phase_tag=phase_tag,
        output_cols=output_cols,
        seq_by_key=seq_by_key,
    )


__all__ = ["MERGED_OUTPUT_COLS", "build_healpix", "write_sidecar_shards"]
