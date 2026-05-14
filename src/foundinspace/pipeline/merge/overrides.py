"""Override indexing and pair lookup helpers for merge orchestration."""

from __future__ import annotations

from typing import Any

import pandas as pd

from foundinspace.pipeline.common.ids import normalize_compound_key, normalize_source
from foundinspace.pipeline.constants import OUTPUT_COLS

DROP_OVERRIDE_PAYLOAD_COLS = [
    col for col in OUTPUT_COLS if col not in {"source", "source_id"}
]
OVERRIDE_REQUIRED_COLS = [
    *OUTPUT_COLS,
    "override_id",
    "action",
    "override_reason",
    "override_policy_version",
]


def validate_drop_override_payload(override: dict[str, Any]) -> None:
    """Reject drop overrides that carry payload columns."""
    bad_cols = [
        col for col in DROP_OVERRIDE_PAYLOAD_COLS if not pd.isna(override.get(col))
    ]
    if bad_cols:
        raise ValueError(
            "Drop override "
            f"{override.get('override_id')} must not include payload columns; "
            f"found values in {bad_cols}"
        )


def split_override_rows(
    overrides_df: pd.DataFrame,
) -> tuple[dict[tuple[str, int | str], dict[str, Any]], list[dict[str, Any]]]:
    """Index replace/drop overrides by target key and return add overrides separately."""
    overrides_by_key: dict[tuple[str, int | str], dict[str, Any]] = {}
    add_overrides: list[dict[str, Any]] = []
    for ov in overrides_df.to_dict(orient="records"):
        action = str(ov["action"]).strip().lower()
        source = normalize_source(ov["source"])
        key = normalize_compound_key(source, ov["source_id"])
        ov["action"] = action
        ov["source"] = source
        ov["source_id"] = str(ov["source_id"]).strip()
        if action == "add":
            add_overrides.append(ov)
            continue
        if action not in {"replace", "drop"}:
            raise ValueError(f"Unsupported override action: {action}")
        if action == "drop":
            validate_drop_override_payload(ov)
        if key in overrides_by_key:
            raise ValueError(f"Duplicate override target key: {key}")
        overrides_by_key[key] = ov
    return overrides_by_key, add_overrides


def find_pair_override(
    overrides_by_key: dict[tuple[str, int | str], dict[str, Any]],
    *,
    gaia_id: int | None,
    hip_id: int | None,
) -> dict[str, Any] | None:
    """Find the override that targets either side of a Gaia/HIP pair."""
    hits: list[dict[str, Any]] = []
    if gaia_id is not None:
        ov = overrides_by_key.get(("gaia", gaia_id))
        if ov is not None:
            hits.append(ov)
    if hip_id is not None:
        ov = overrides_by_key.get(("hip", hip_id))
        if ov is not None:
            hits.append(ov)
    if not hits:
        return None
    unique_ids = {str(h["override_id"]) for h in hits}
    if len(unique_ids) > 1:
        raise ValueError(
            f"Conflicting overrides for pair gaia={gaia_id} hip={hip_id}: "
            f"{sorted(unique_ids)}"
        )
    return hits[0]


def gaia_special_ids_for_overrides(
    overrides_by_key: dict[tuple[str, int | str], dict[str, Any]],
    *,
    hip_to_gaia: dict[int, int],
) -> set[int]:
    """Return Gaia IDs that need pair-aware handling because of overrides."""
    out: set[int] = set()
    for src, sid in overrides_by_key:
        if src == "gaia":
            out.add(int(sid))
        elif src == "hip":
            partner_gaia = hip_to_gaia.get(int(sid))
            if partner_gaia is not None:
                out.add(partner_gaia)
    return out
