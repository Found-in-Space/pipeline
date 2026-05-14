"""Identifier sidecar schema helpers."""

from __future__ import annotations

import pandas as pd

IDENTIFIER_OUTPUT_COLS = [
    "source",
    "source_id",
    "gaia_source_id",
    "hip_id",
    "hd",
    "bayer",
    "flamsteed",
    "constellation",
    "proper_name",
]


def empty_identifier_frame() -> pd.DataFrame:
    """Return an empty identifier sidecar frame with stable dtypes."""
    return pd.DataFrame(
        {
            "source": pd.Series(dtype="string"),
            "source_id": pd.Series(dtype="string"),
            "gaia_source_id": pd.Series(dtype="Int64"),
            "hip_id": pd.Series(dtype="Int64"),
            "hd": pd.Series(dtype="Int64"),
            "bayer": pd.Series(dtype="string"),
            "flamsteed": pd.Series(dtype="Int64"),
            "constellation": pd.Series(dtype="string"),
            "proper_name": pd.Series(dtype="string"),
        }
    )
