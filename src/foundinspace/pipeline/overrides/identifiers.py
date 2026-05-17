"""Build identifier sidecar rows from override YAML documents."""

from __future__ import annotations

from collections.abc import Sequence
from contextlib import suppress
from typing import Any

import pandas as pd

from foundinspace.pipeline.common.ids import normalize_source
from foundinspace.pipeline.identifiers.schema import (
    IDENTIFIER_OUTPUT_COLS,
    empty_identifier_frame,
)
from foundinspace.pipeline.overrides.loader import (
    OverrideInclude,
    load_parsed_override_documents,
)

_INT_IDENTIFIER_KEYS = ("gaia_source_id", "hip_id", "hd", "flamsteed")
_STR_IDENTIFIER_KEYS = ("bayer", "constellation", "proper_name")


def _row_for_override_identifiers(star: dict[str, Any]) -> dict[str, Any] | None:
    ident = star.get("identifiers")
    if not ident or not isinstance(ident, dict):
        return None
    sid_raw = star.get("source_id")
    if sid_raw is None:
        return None

    row: dict[str, Any] = {
        "source": normalize_source(star.get("source", "")),
        "source_id": str(sid_raw).strip(),
        "gaia_source_id": pd.NA,
        "hip_id": pd.NA,
        "hd": pd.NA,
        "bayer": pd.NA,
        "flamsteed": pd.NA,
        "constellation": pd.NA,
        "proper_name": pd.NA,
    }
    for key in _INT_IDENTIFIER_KEYS:
        if key in ident and ident[key] is not None and ident[key] != "":
            with suppress(TypeError, ValueError):
                row[key] = int(ident[key])
    for key in _STR_IDENTIFIER_KEYS:
        if key in ident and ident[key] is not None and str(ident[key]).strip() != "":
            row[key] = str(ident[key]).strip()
    return row


def build_override_identifier_rows(
    include_files: Sequence[OverrideInclude] = (),
) -> pd.DataFrame:
    """Return identifier rows declared in override YAML documents."""
    rows: list[dict[str, Any]] = []
    for doc in load_parsed_override_documents(include_files):
        stars = doc.get("stars")
        if not isinstance(stars, list):
            continue
        for star in stars:
            if not isinstance(star, dict):
                continue
            row = _row_for_override_identifiers(star)
            if row is not None:
                rows.append(row)

    if not rows:
        return empty_identifier_frame()

    df = pd.DataFrame(rows)
    df["source"] = df["source"].astype("string")
    df["source_id"] = df["source_id"].astype("string")
    for col in _INT_IDENTIFIER_KEYS:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in _STR_IDENTIFIER_KEYS:
        df[col] = df[col].astype("string")
    return df[IDENTIFIER_OUTPUT_COLS]
