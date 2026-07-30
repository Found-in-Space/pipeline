"""Compare production coordinate propagation with an Astropy test oracle."""

import numpy as np
import pandas as pd
from astropy import units as u
from astropy.coordinates import SkyCoord
from astropy.time import Time

from foundinspace.pipeline.common.coords import calculate_coordinates_fast
from foundinspace.pipeline.constants import CANONICAL_EPOCH_JYEAR


def calculate_coordinates_reference(df: pd.DataFrame) -> pd.DataFrame:
    """Astropy oracle for the production NumPy coordinate implementation."""
    canonical_epoch = Time(CANONICAL_EPOCH_JYEAR, format="jyear")
    pmra = df["pmra_use_masyr"].astype(float).to_numpy()
    pmdec = df["pmdec_use_masyr"].astype(float).to_numpy()
    pmra = np.where(np.isfinite(pmra), pmra, 0.0)
    pmdec = np.where(np.isfinite(pmdec), pmdec, 0.0)
    coordinates = SkyCoord(
        ra=df["ra_use_deg"].astype(float).to_numpy() * u.deg,
        dec=df["dec_use_deg"].astype(float).to_numpy() * u.deg,
        distance=df["distance_use_pc"].astype(float).to_numpy() * u.pc,
        pm_ra_cosdec=pmra * u.mas / u.yr,
        pm_dec=pmdec * u.mas / u.yr,
        obstime=Time(df["epoch_yr"].astype(float).to_numpy(), format="jyear"),
        frame="icrs",
    )
    propagated = coordinates.apply_space_motion(new_obstime=canonical_epoch)
    cartesian = propagated.cartesian
    df["x_icrs_pc"] = cartesian.x.to_value(u.pc)
    df["y_icrs_pc"] = cartesian.y.to_value(u.pc)
    df["z_icrs_pc"] = cartesian.z.to_value(u.pc)
    df["epoch_norm_yr"] = CANONICAL_EPOCH_JYEAR
    df["ra_deg"] = propagated.ra.to_value(u.deg)
    df["dec_deg"] = propagated.dec.to_value(u.deg)
    df["r_pc"] = propagated.distance.to_value(u.pc)
    return df


def _make_coords_df(
    n: int = 5,
    *,
    seed: int | None = 42,
) -> pd.DataFrame:
    """Build a DataFrame with columns required by coordinate propagation."""
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
    df_astropy = calculate_coordinates_reference(df.copy())
    df_fast = calculate_coordinates_fast(df.copy())

    # Slightly relaxed tolerance: two different code paths can differ at ~1e-8
    for col in COORD_OUTPUT_COLS:
        a = df_astropy[col].to_numpy()
        f = df_fast[col].to_numpy()
        np.testing.assert_allclose(a, f, rtol=1e-7, atol=1e-6, err_msg=col)


def test_astropy_and_fast_agree_single_row():
    """Single-row case: both methods match."""
    df = _make_coords_df(n=1, seed=0)
    df_astropy = calculate_coordinates_reference(df.copy())
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

    df_astropy = calculate_coordinates_reference(df.copy())
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
    out1 = calculate_coordinates_reference(df.copy())
    out2 = calculate_coordinates_fast(df.copy())
    assert (out1["epoch_norm_yr"] == CANONICAL_EPOCH_JYEAR).all()
    assert (out2["epoch_norm_yr"] == CANONICAL_EPOCH_JYEAR).all()


def test_output_columns_present():
    """Both functions add the expected columns."""
    df = _make_coords_df(n=2)
    out1 = calculate_coordinates_reference(df.copy())
    out2 = calculate_coordinates_fast(df.copy())
    for col in COORD_OUTPUT_COLS:
        assert col in out1, f"reference implementation missing {col}"
        assert col in out2, f"calculate_coordinates_fast missing {col}"


def test_missing_proper_motion_preserves_position_at_canonical_epoch():
    df = _make_coords_df(n=2, seed=5)
    df["epoch_yr"] = CANONICAL_EPOCH_JYEAR
    df["pmra_use_masyr"] = np.nan
    df["pmdec_use_masyr"] = np.nan

    out_astropy = calculate_coordinates_reference(df.copy())
    out_fast = calculate_coordinates_fast(df.copy())

    for out in (out_astropy, out_fast):
        np.testing.assert_allclose(out["ra_deg"], df["ra_use_deg"])
        np.testing.assert_allclose(out["dec_deg"], df["dec_use_deg"])
        np.testing.assert_allclose(out["r_pc"], df["distance_use_pc"])
        assert np.isfinite(out[COORD_OUTPUT_COLS].to_numpy(dtype=float)).all()
