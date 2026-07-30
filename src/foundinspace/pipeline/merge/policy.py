"""Merge decision policy helpers.

This module holds the catalog winner rules used by ``merge.pipeline``. Keeping
it separate makes the policy easier to test and explain without reading the
streaming orchestration code.
"""

from __future__ import annotations

import math
from typing import Any

import numpy as np

from foundinspace.pipeline.common.photometry import (
    absolute_magnitude_from_apparent,
    apparent_magnitude_from_absolute,
)
from foundinspace.pipeline.common.quality_flags import combine_quality_flags
from foundinspace.pipeline.constants import qf_teff_src

# ---------------------------------------------------------------------------
# Merge policy thresholds
# ---------------------------------------------------------------------------
# Hip wins only if hip_score < gaia_score * HIP_MARGIN_*. Lower margin = harder for Hip.
BRIGHT_AUTO_MAG = 3.5
BRIGHT_REVIEW_MAG = 6.0
HIP_MARGIN_VERY_BRIGHT = 1.0  # G < 3.5: Hip wins if strictly better
HIP_MARGIN_BRIGHT = 0.6  # 3.5 <= G < 6: Hip must be >=40% better
HIP_MARGIN_NORMAL = 0.5  # G >= 6: Hip must be >=50% better

HIP_SOLUTION_STANDARD = 5  # Hipparcos Sn=5 = standard 5-param single-star


def _safe_score(value: Any) -> float:
    try:
        score = float(value)
    except (TypeError, ValueError):
        return math.inf
    return score if math.isfinite(score) and score >= 0.0 else math.inf


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


def _safe_flags(value: Any) -> int:
    flags = _safe_int(value, default=0)
    return flags if flags is not None else 0


def _valid_astrometry_payload(row: dict[str, Any]) -> bool:
    """Return whether a complete staged row has usable spatial astrometry.

    Policy-only unit tests may pass score-only dictionaries; those are treated
    as valid so this structural guard remains an orchestration safeguard.
    """
    required = ("x_icrs_pc", "y_icrs_pc", "z_icrs_pc", "ra_deg", "dec_deg", "r_pc")
    if not all(key in row for key in required):
        return True
    values = [_safe_float(row.get(key)) for key in required]
    return all(math.isfinite(value) for value in values) and values[-1] > 0.0


def _valid_render_photometry(row: dict[str, Any]) -> bool:
    mag_abs = _safe_float(row.get("mag_abs"))
    r_pc = _safe_float(row.get("r_pc"))
    return math.isfinite(mag_abs) and math.isfinite(r_pc) and r_pc > 0.0


def _choose_photometry_donor(
    gaia_row: dict[str, Any],
    hip_row: dict[str, Any],
) -> str:
    """Choose the valid staged magnitude with the lower uncertainty estimate."""
    gaia_valid = _valid_render_photometry(gaia_row)
    hip_valid = _valid_render_photometry(hip_row)
    if gaia_valid and not hip_valid:
        return "gaia"
    if hip_valid and not gaia_valid:
        return "hip"
    if not gaia_valid and not hip_valid:
        return "gaia"

    gaia_quality = _safe_score(gaia_row.get("photometry_quality"))
    hip_quality = _safe_score(hip_row.get("photometry_quality"))
    return "gaia" if gaia_quality <= hip_quality else "hip"


def _choose_teff_donor(
    gaia_row: dict[str, Any],
    hip_row: dict[str, Any],
) -> str:
    """Prefer measured/derived Gaia Teff, then Hipparcos B-V, then a default."""
    gaia_teff = _safe_float(gaia_row.get("teff"))
    hip_teff = _safe_float(hip_row.get("teff"))
    gaia_valid = math.isfinite(gaia_teff) and gaia_teff > 0.0
    hip_valid = math.isfinite(hip_teff) and hip_teff > 0.0
    gaia_source = qf_teff_src(_safe_flags(gaia_row.get("quality_flags")))
    hip_source = qf_teff_src(_safe_flags(hip_row.get("quality_flags")))

    if gaia_valid and gaia_source != 0:
        return "gaia"
    if hip_valid and hip_source != 0:
        return "hip"
    if gaia_valid:
        return "gaia"
    return "hip" if hip_valid else "gaia"


def fuse_matched_payload(
    gaia_row: dict[str, Any],
    hip_row: dict[str, Any],
    *,
    astrometry_donor: str,
) -> tuple[dict[str, Any], str, str, bool]:
    """Fuse independently selected astrometry, render photometry, and Teff.

    When the photometry donor differs from the astrometry donor, preserve the
    donor's implied apparent render magnitude and rebase its absolute magnitude
    to the selected distance.
    """
    rows = {"gaia": gaia_row, "hip": hip_row}
    astrometry_row = rows[astrometry_donor]
    photometry_donor = _choose_photometry_donor(gaia_row, hip_row)
    photometry_row = rows[photometry_donor]
    teff_donor = _choose_teff_donor(gaia_row, hip_row)
    teff_row = rows[teff_donor]

    fused = dict(astrometry_row)
    photometry_rebased = False
    if _valid_render_photometry(photometry_row):
        selected_mag_abs = _safe_float(photometry_row.get("mag_abs"))
        if photometry_donor != astrometry_donor:
            apparent_mag = apparent_magnitude_from_absolute(
                selected_mag_abs,
                _safe_float(photometry_row.get("r_pc")),
            )
            selected_mag_abs = absolute_magnitude_from_apparent(
                apparent_mag,
                _safe_float(astrometry_row.get("r_pc")),
            )
            photometry_rebased = True
        fused["mag_abs"] = float(selected_mag_abs)
        fused["photometry_quality"] = photometry_row.get("photometry_quality")

    if math.isfinite(_safe_float(teff_row.get("teff"))):
        fused["teff"] = teff_row.get("teff")

    combined_flags = combine_quality_flags(
        _safe_flags(astrometry_row.get("quality_flags")),
        _safe_flags(photometry_row.get("quality_flags")),
        _safe_flags(teff_row.get("quality_flags")),
    )
    fused["quality_flags"] = int(np.asarray(combined_flags).item())
    return fused, photometry_donor, teff_donor, photometry_rebased


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
    gaia_valid = _valid_astrometry_payload(gaia_row) and math.isfinite(gaia_score)
    hip_valid = _valid_astrometry_payload(hip_row) and math.isfinite(hip_score)

    if gaia_valid and not hip_valid:
        return "gaia", "hip_astrometry_unavailable"
    if hip_valid and not gaia_valid:
        return "hip", "gaia_astrometry_unavailable"

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
