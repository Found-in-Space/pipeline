"""Tests for coordinate propagation (coords module).

In particular, we check that calculate_coordinates (Astropy SkyCoord) and
calculate_coordinates_fast (pure NumPy) agree to within numerical tolerance.
"""

import numpy as np
import pandas as pd

from foundinspace.pipeline.common.coords import (
    calculate_coordinates,
    calculate_coordinates_fast,
)
from foundinspace.pipeline.constants import CANONICAL_EPOCH_JYEAR


def _make_coords_df(
    n: int = 5,
    *,
    seed: int | None = 42,
) -> pd.DataFrame:
    """Build a DataFrame with columns required by calculate_coordinates*."""
    rng = np.random.default_rng(seed)
    return pd.DataFrame(
        {
            "ra_use_deg": rng.uniform(0, 360, size=n),
            "dec_use_deg": rng.uniform(-90, 90, size=n),
            "distance_use_pc": rng.uniform(10, 5000, size=n),
            "pmra_use_masyr": rng.uniform(-50, 50, size=n),
            "pmdec_use_masyr": rng.uniform(-50, 50, size=n),
            "epoch_yr": rng.uniform(1990, 2020, size=n),
        }
    )


# Columns both functions add; we compare these
COORD_OUTPUT_COLS = [
    "x_icrs_pc",
    "y_icrs_pc",
    "z_icrs_pc",
    "ra_deg",
    "dec_deg",
    "r_pc",
    "epoch_norm_yr",
]


def test_astropy_and_fast_agree_on_random_input():
    """Both approaches produce the same Cartesian and spherical coordinates."""
    df = _make_coords_df(n=20, seed=123)
    df_astropy = calculate_coordinates(df.copy())
    df_fast = calculate_coordinates_fast(df.copy())

    # Slightly relaxed tolerance: two different code paths can differ at ~1e-8
    for col in COORD_OUTPUT_COLS:
        a = df_astropy[col].to_numpy()
        f = df_fast[col].to_numpy()
        np.testing.assert_allclose(a, f, rtol=1e-7, atol=1e-6, err_msg=col)


def test_astropy_and_fast_agree_single_row():
    """Single-row case: both methods match."""
    df = _make_coords_df(n=1, seed=0)
    df_astropy = calculate_coordinates(df.copy())
    df_fast = calculate_coordinates_fast(df.copy())

    for col in COORD_OUTPUT_COLS:
        a = df_astropy[col].to_numpy()
        f = df_fast[col].to_numpy()
        np.testing.assert_allclose(a, f, rtol=1e-9, atol=1e-9, err_msg=col)


def test_astropy_and_fast_agree_zero_pm():
    """No proper motion: position unchanged apart from epoch label."""
    df = _make_coords_df(n=5, seed=7)
    df["pmra_use_masyr"] = 0.0
    df["pmdec_use_masyr"] = 0.0
    # Same epoch as canonical so dt=0
    df["epoch_yr"] = CANONICAL_EPOCH_JYEAR

    df_astropy = calculate_coordinates(df.copy())
    df_fast = calculate_coordinates_fast(df.copy())

    for col in COORD_OUTPUT_COLS:
        a = df_astropy[col].to_numpy()
        f = df_fast[col].to_numpy()
        np.testing.assert_allclose(a, f, rtol=1e-12, atol=1e-12, err_msg=col)

    # ra/dec/distance should match input (epoch is canonical)
    np.testing.assert_allclose(df_astropy["ra_deg"], df["ra_use_deg"], rtol=1e-12)
    np.testing.assert_allclose(df_astropy["dec_deg"], df["dec_use_deg"], rtol=1e-12)
    np.testing.assert_allclose(df_astropy["r_pc"], df["distance_use_pc"], rtol=1e-12)


def test_epoch_norm_yr_is_canonical():
    """Both functions set epoch_norm_yr to the canonical epoch."""
    df = _make_coords_df(n=3)
    out1 = calculate_coordinates(df.copy())
    out2 = calculate_coordinates_fast(df.copy())
    assert (out1["epoch_norm_yr"] == CANONICAL_EPOCH_JYEAR).all()
    assert (out2["epoch_norm_yr"] == CANONICAL_EPOCH_JYEAR).all()


def test_output_columns_present():
    """Both functions add the expected columns."""
    df = _make_coords_df(n=2)
    out1 = calculate_coordinates(df.copy())
    out2 = calculate_coordinates_fast(df.copy())
    for col in COORD_OUTPUT_COLS:
        assert col in out1, f"calculate_coordinates missing {col}"
        assert col in out2, f"calculate_coordinates_fast missing {col}"


def test_missing_proper_motion_preserves_position_at_canonical_epoch():
    df = _make_coords_df(n=2, seed=5)
    df["epoch_yr"] = CANONICAL_EPOCH_JYEAR
    df["pmra_use_masyr"] = np.nan
    df["pmdec_use_masyr"] = np.nan

    out_astropy = calculate_coordinates(df.copy())
    out_fast = calculate_coordinates_fast(df.copy())

    for out in (out_astropy, out_fast):
        np.testing.assert_allclose(out["ra_deg"], df["ra_use_deg"])
        np.testing.assert_allclose(out["dec_deg"], df["dec_use_deg"])
        np.testing.assert_allclose(out["r_pc"], df["distance_use_pc"])
        assert np.isfinite(out[COORD_OUTPUT_COLS].to_numpy(dtype=float)).all()
