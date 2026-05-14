"""Merge decision sidecar schema helpers."""

from __future__ import annotations

from typing import Any

import pandas as pd

DECISION_COLS = [
    "decision_type",
    "gaia_source_id",
    "hip_source_id",
    "winner_catalog",
    "winner_source_id",
    "gaia_score",
    "hip_score",
    "tie_break_reason",
    "override_id",
    "override_action",
    "override_reason",
    "override_policy_version",
    "note",
    "number_of_neighbours",
    "angular_distance_arcsec",
    "gaia_ruwe",
    "gaia_phot_g_mean_mag",
    "hip_solution_type",
    "hip_apparent_mag",
]


def decision_record(**kwargs: Any) -> dict[str, Any]:
    """Build a merge decision record with all decision sidecar columns."""
    rec: dict[str, Any] = dict.fromkeys(DECISION_COLS, pd.NA)
    rec.update(kwargs)
    return rec
