"""Merge decision policy helpers.

This module holds the catalog winner rules used by ``merge.pipeline``. Keeping
it separate makes the policy easier to test and explain without reading the
streaming orchestration code.
"""

from __future__ import annotations

import math
from typing import Any

from foundinspace.pipeline.common.photometry import apparent_magnitude_from_absolute

# ---------------------------------------------------------------------------
# Merge policy thresholds
# ---------------------------------------------------------------------------
# Hip wins only if hip_score < gaia_score * HIP_MARGIN_*. Lower margin = harder for Hip.
BRIGHT_AUTO_MAG = 3.5
BRIGHT_REVIEW_MAG = 6.0
HIP_MARGIN_VERY_BRIGHT = 1.0  # G < 3.5: Hip wins if strictly better
HIP_MARGIN_BRIGHT = 0.6  # 3.5 <= G < 6: Hip must be >=40% better
HIP_MARGIN_NORMAL = 0.5  # G >= 6: Hip must be >=50% better

RUWE_WARN_THRESHOLD = 1.4
HIP_SOLUTION_STANDARD = 5  # Hipparcos Sn=5 = standard 5-param single-star


def _safe_score(value: Any) -> float:
    try:
        score = float(value)
    except (TypeError, ValueError):
        return math.inf
    return score if math.isfinite(score) else math.inf


def _safe_float(value: Any) -> float:
    """Coerce to float; return NaN for missing or non-numeric values."""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return math.nan
    return v


def _safe_int(value: Any, default: int | None = None) -> int | None:
    """Coerce to int; return ``default`` for missing or non-numeric values."""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(v):
        return default
    return int(v)


def _choose_matched_winner(
    gaia_row: dict[str, Any],
    hip_row: dict[str, Any],
    *,
    number_of_neighbours: int | None = None,
) -> tuple[str, str]:
    """V2 winner selection: Gaia-default with vetoes, bright-star gate, and margin.

    Returns (winner_catalog, tie_break_reason).
    """
    gaia_score = _safe_score(gaia_row.get("astrometry_quality"))
    hip_score = _safe_score(hip_row.get("astrometry_quality"))

    # Veto 1: ambiguous crossmatch (multiple Gaia neighbours for this HIP entry)
    if number_of_neighbours is not None and number_of_neighbours > 1:
        return "gaia", "neighbour_veto"

    # Veto 2: Hipparcos non-standard solution (likely multiplicity / acceleration)
    hip_sn = _safe_int(hip_row.get("Sn"))
    if hip_sn is not None and hip_sn != HIP_SOLUTION_STANDARD:
        return "gaia", "hip_multiplicity"

    # Bright-star gate: determine margin from Gaia apparent magnitude.
    g_mag = _safe_float(gaia_row.get("phot_g_mean_mag"))
    if not math.isfinite(g_mag):
        # Fall back to distance-modulus estimate from the Gaia row's own data.
        mag_abs = _safe_float(gaia_row.get("mag_abs"))
        r_pc = _safe_float(gaia_row.get("r_pc"))
        if math.isfinite(mag_abs) and r_pc > 0:
            g_mag = apparent_magnitude_from_absolute(mag_abs, r_pc)

    if math.isfinite(g_mag) and g_mag < BRIGHT_AUTO_MAG:
        margin = HIP_MARGIN_VERY_BRIGHT
    elif math.isfinite(g_mag) and g_mag < BRIGHT_REVIEW_MAG:
        margin = HIP_MARGIN_BRIGHT
    else:
        margin = HIP_MARGIN_NORMAL

    if hip_score < gaia_score * margin:
        return "hip", ""
    if gaia_score <= hip_score:
        return "gaia", ""
    return "gaia", "gaia_margin"
