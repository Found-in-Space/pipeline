"""Shared astrometry primitives used by catalog-specific stages."""

from __future__ import annotations

import numpy as np

DISTANCE_PLAUSIBLE_MIN_PC = 0.1
DISTANCE_PLAUSIBLE_MAX_PC = 200_000.0


def finite_positive_mask(values: object) -> np.ndarray:
    """Return a boolean mask for finite values greater than zero."""
    arr = np.asarray(values, dtype=float)
    return np.isfinite(arr) & (arr > 0)


def valid_parallax_mask(parallax_mas: object, parallax_error_mas: object) -> np.ndarray:
    """Return rows with finite positive parallax and parallax uncertainty."""
    return finite_positive_mask(parallax_mas) & finite_positive_mask(parallax_error_mas)


def parallax_distance_pc(parallax_mas: object) -> np.ndarray:
    """Convert parallax in milliarcseconds to distance in parsecs.

    Invalid or non-positive parallaxes return NaN.
    """
    plx = np.asarray(parallax_mas, dtype=float)
    out = np.full(plx.shape, np.nan, dtype=float)
    valid = finite_positive_mask(plx)
    np.divide(1000.0, plx, out=out, where=valid)
    return out


def fractional_parallax_error(
    parallax_mas: object,
    parallax_error_mas: object,
) -> np.ndarray:
    """Return fractional parallax error for valid positive parallax rows."""
    plx = np.asarray(parallax_mas, dtype=float)
    e_plx = np.asarray(parallax_error_mas, dtype=float)
    out = np.full(plx.shape, np.nan, dtype=float)
    valid = valid_parallax_mask(plx, e_plx)
    np.divide(e_plx, plx, out=out, where=valid)
    return out


def fractional_distance_interval_width(
    distance_pc: object,
    lower_pc: object,
    upper_pc: object,
) -> np.ndarray:
    """Return the fractional half-width of a valid distance interval.

    The lower/median/upper values are expected to be ordered positive
    quantiles. Malformed intervals return NaN instead of producing a negative
    or otherwise artificially favourable quality score.
    """
    distance, lower, upper = np.broadcast_arrays(
        np.asarray(distance_pc, dtype=float),
        np.asarray(lower_pc, dtype=float),
        np.asarray(upper_pc, dtype=float),
    )
    out = np.full(distance.shape, np.nan, dtype=float)
    valid = (
        finite_positive_mask(distance)
        & finite_positive_mask(lower)
        & finite_positive_mask(upper)
        & (lower <= distance)
        & (distance <= upper)
        & (lower <= upper)
    )
    np.divide(upper - lower, 2.0 * distance, out=out, where=valid)
    return out


def plausible_distance_mask(distance_pc: object) -> np.ndarray:
    """Return rows within the broad distance sanity bounds used by the pipeline."""
    distance = np.asarray(distance_pc, dtype=float)
    return (
        np.isfinite(distance)
        & (distance > DISTANCE_PLAUSIBLE_MIN_PC)
        & (distance < DISTANCE_PLAUSIBLE_MAX_PC)
    )
