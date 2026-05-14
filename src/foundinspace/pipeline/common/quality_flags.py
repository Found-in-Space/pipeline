"""Helpers for packing pipeline quality flag bit fields."""

from __future__ import annotations

import numpy as np

from foundinspace.pipeline.common.astrometry import (
    finite_positive_mask,
    plausible_distance_mask,
)
from foundinspace.pipeline.constants import (
    FLAG_DIST_PLAUSIBLE,
    FLAG_DIST_VALID,
    FLAG_NEEDS_REVIEW,
    PHOT_SRC_SHIFT,
    TEFF_SRC_SHIFT,
)


def pack_distance_flags(
    dist_src: object,
    *,
    distance_pc: object,
    needs_review: object,
) -> np.ndarray:
    """Pack distance source and distance status bits."""
    dist_src_bits = np.asarray(dist_src, dtype=np.uint16)
    valid_bit = np.where(finite_positive_mask(distance_pc), FLAG_DIST_VALID, 0).astype(
        np.uint16
    )
    review_bit = np.where(needs_review, FLAG_NEEDS_REVIEW, 0).astype(np.uint16)
    plausible_bit = np.where(
        plausible_distance_mask(distance_pc),
        FLAG_DIST_PLAUSIBLE,
        0,
    ).astype(np.uint16)
    return (dist_src_bits | valid_bit | review_bit | plausible_bit).astype(np.uint16)


def pack_status_flags(
    dist_src: object,
    *,
    distance_valid: object,
    needs_review: object = False,
    distance_plausible: object = False,
) -> np.ndarray:
    """Pack distance source with explicit status bit masks."""
    dist_src_bits = np.asarray(dist_src, dtype=np.uint16)
    valid_bit = np.where(distance_valid, FLAG_DIST_VALID, 0).astype(np.uint16)
    review_bit = np.where(needs_review, FLAG_NEEDS_REVIEW, 0).astype(np.uint16)
    plausible_bit = np.where(distance_plausible, FLAG_DIST_PLAUSIBLE, 0).astype(
        np.uint16
    )
    return (dist_src_bits | valid_bit | review_bit | plausible_bit).astype(np.uint16)


def add_photometry_source(flags: object, phot_src: int) -> np.ndarray:
    """OR a photometry source value into existing quality flags."""
    base = np.asarray(flags, dtype=np.uint16)
    return (base | (np.uint16(phot_src) << PHOT_SRC_SHIFT)).astype(np.uint16)


def add_teff_source(flags: object, teff_src: object) -> np.ndarray:
    """OR Teff source values into existing quality flags."""
    base = np.asarray(flags, dtype=np.uint16)
    teff_src_bits = np.asarray(teff_src, dtype=np.uint16)
    return (base | (teff_src_bits << TEFF_SRC_SHIFT)).astype(np.uint16)
