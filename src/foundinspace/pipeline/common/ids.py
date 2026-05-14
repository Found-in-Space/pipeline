"""Shared source and identifier normalization helpers."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def normalize_source(source: Any) -> str:
    """Normalize a catalog/source namespace for compound keys."""
    return str(source).strip().lower()


def serialize_source_id(value: Any) -> Any:
    """Stable string identity for numeric catalog IDs and string manual IDs."""
    if value is None:
        return pd.NA
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, (int, np.integer)):
        return str(int(value))
    return str(value)


def normalize_compound_key(source: Any, source_id: Any) -> tuple[str, int | str]:
    """Normalize a `(source, source_id)` pair for in-memory lookup maps."""
    src = normalize_source(source)
    sid = str(source_id).strip()
    if src in {"gaia", "hip"}:
        return src, int(sid)
    return src, sid


def coerce_positive_integer_values(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Return numeric values and a mask for finite positive integer rows."""
    numeric = pd.to_numeric(series, errors="coerce")
    values = numeric.to_numpy(dtype=float, na_value=np.nan, copy=False)
    finite = np.isfinite(values)
    integral = np.equal(np.floor(values), values)
    positive = values > 0
    valid = finite & integral & positive
    return numeric, pd.Series(valid, index=series.index)


def coerce_positive_int_series(series: pd.Series) -> pd.Series:
    """Coerce positive integer rows to nullable Int64, otherwise NA."""
    numeric, valid = coerce_positive_integer_values(series)
    values = numeric.to_numpy(dtype=float, na_value=np.nan, copy=False)
    out = pd.Series(pd.NA, index=series.index, dtype="Int64")
    if np.any(valid):
        out.loc[valid] = values[valid].astype("int64")
    return out
