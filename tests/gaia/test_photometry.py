"""Tests for Gaia photometry: assign_photometry_gaia, compute_mag_abs_gaia, compute_teff_gaia, quality_flags."""

import numpy as np
import pandas as pd

from foundinspace.pipeline.constants import (
    PHOT_SRC_GAIA_G,
    PHOT_SRC_MASK,
    PHOT_SRC_SHIFT,
    TEFF_DEFAULT_K,
    TEFF_SRC_BPRP,
    TEFF_SRC_DEFAULT,
    TEFF_SRC_ESPHS,
    TEFF_SRC_ESPUCD,
    TEFF_SRC_GSPPHOT,
    TEFF_SRC_GSPSPEC,
    qf_phot_src,
    qf_teff_src,
)
from foundinspace.pipeline.gaia.astrometry import select_astrometry_gaia
from foundinspace.pipeline.gaia.photometry import (
    assign_photometry_gaia,
    compute_mag_abs_gaia,
    compute_teff_gaia,
)


def _gaia_after_astrometry(parallax=1.0, parallax_error=0.01, **extra):
    """DataFrame after select_astrometry_gaia with optional extra columns."""
    base = {
        "ra": 0.0,
        "dec": 0.0,
        "pmra": 0.0,
        "pmdec": 0.0,
        "parallax": parallax,
        "parallax_error": parallax_error,
        "r_lo_geo": np.nan,
        "r_med_geo": np.nan,
        "r_hi_geo": np.nan,
        "r_lo_photogeo": np.nan,
        "r_med_photogeo": np.nan,
        "r_hi_photogeo": np.nan,
        "phot_g_mean_mag": 14.0,
        "bp_rp": 0.5,
        "mg_gspphot": np.nan,
        "mg_gspphot_lower": np.nan,
        "mg_gspphot_upper": np.nan,
        "ag_gspphot": np.nan,
        "teff_esphs": np.nan,
        "teff_gspspec": np.nan,
        "teff_espucd": np.nan,
        "teff_gspphot": np.nan,
    }
    base.update(extra)
    df = pd.DataFrame([base])
    return select_astrometry_gaia(df)


def test_assign_photometry_gaia_sets_mag_color_and_phot_src():
    """assign_photometry_gaia sets mag, color, and ORs PHOT_SRC_GAIA_G into quality_flags."""
    df = _gaia_after_astrometry()
    out = assign_photometry_gaia(df)
    assert "mag" in out.columns
    assert "color" in out.columns
    assert out["mag"].iloc[0] == out["phot_g_mean_mag"].iloc[0]
    assert qf_phot_src(out["quality_flags"].iloc[0]) == PHOT_SRC_GAIA_G


def test_assign_photometry_gaia_preserves_dist_src():
    """assign_photometry_gaia does not overwrite dist_src bits."""
    df = _gaia_after_astrometry()
    flags_before = df["quality_flags"].iloc[0]
    out = assign_photometry_gaia(df)
    flags_after = out["quality_flags"].iloc[0]
    assert (flags_after & 0x000F) == (flags_before & 0x000F)
    assert (flags_after & PHOT_SRC_MASK) == (PHOT_SRC_GAIA_G << PHOT_SRC_SHIFT)


def test_compute_mag_abs_gaia_uses_quality_flags():
    """compute_mag_abs_gaia uses quality_flags (Gaia G) for extinction path; produces mag_abs."""
    df = _gaia_after_astrometry(
        phot_g_mean_mag=14.0,
        bp_rp=0.5,
        mg_gspphot=5.0,
        ag_gspphot=0.3,
    )
    df = assign_photometry_gaia(df)
    out = compute_mag_abs_gaia(df)
    assert "mag_abs" in out.columns
    assert np.isfinite(out["mag_abs"].iloc[0])
    # With distance 1000 pc, dm = 5*log10(100) = 10; mag_abs from GSP-Phot = 5, or dm path
    assert out["mag_abs"].iloc[0] == 5.0  # mg_gspphot used first


def test_compute_mag_abs_gaia_photometry_quality_finite():
    """photometry_quality is finite (no inf from bad astrometry_quality)."""
    # Tier B row: astrometry_quality = 10.0 (finite)
    df = _gaia_after_astrometry(
        parallax=0.0,
        parallax_error=0.0,
        r_med_photogeo=200.0,
        r_lo_photogeo=np.nan,
        r_hi_photogeo=np.nan,
        phot_g_mean_mag=12.0,
        mg_gspphot=np.nan,
        ag_gspphot=np.nan,
    )
    df = assign_photometry_gaia(df)
    out = compute_mag_abs_gaia(df)
    assert np.isfinite(out["photometry_quality"].iloc[0])
    assert not np.isinf(out["photometry_quality"].iloc[0])


def test_compute_mag_abs_gaia_photometry_quality_reversed_bounds_non_negative():
    """When GSP-Phot bounds are reversed (upper < lower), photometry_quality is still non-negative."""
    df = _gaia_after_astrometry(
        phot_g_mean_mag=14.0,
        bp_rp=0.5,
        mg_gspphot=5.0,
        ag_gspphot=0.3,
        mg_gspphot_upper=4.8,  # reversed: upper < lower
        mg_gspphot_lower=5.2,
    )
    df = assign_photometry_gaia(df)
    out = compute_mag_abs_gaia(df)
    # quality_gspphot = abs(upper - lower) / 2 = abs(4.8 - 5.2) / 2 = 0.2
    assert out["photometry_quality"].iloc[0] >= 0.0
    assert np.isclose(out["photometry_quality"].iloc[0], 0.2)


def test_compute_teff_gaia_adds_teff_src_to_quality_flags():
    """compute_teff_gaia sets teff and ORs teff_src into quality_flags."""
    df = _gaia_after_astrometry(bp_rp=0.6, bv=np.nan)
    df = assign_photometry_gaia(df)
    # No valid Gaia spectroscopic Teff values -> cascade to bp_rp or default
    out = compute_teff_gaia(df)
    assert "teff" in out.columns
    assert out["teff"].iloc[0] >= 1000 and out["teff"].iloc[0] <= 50000
    teff_src = qf_teff_src(out["quality_flags"].iloc[0])
    # Should be TEFF_SRC_BPRP or TEFF_SRC_DEFAULT
    assert teff_src in (TEFF_SRC_BPRP, TEFF_SRC_DEFAULT)


def test_compute_teff_gaia_gspphot_wins_when_valid():
    """When teff_gspphot is valid, it wins and teff_src = TEFF_SRC_GSPPHOT."""
    df = _gaia_after_astrometry(bp_rp=0.5)
    df["teff_gspphot"] = 5500.0
    df = assign_photometry_gaia(df)
    out = compute_teff_gaia(df)
    assert out["teff"].iloc[0] == 5500.0
    assert qf_teff_src(out["quality_flags"].iloc[0]) == TEFF_SRC_GSPPHOT


def test_compute_teff_gaia_esphs_highest_priority():
    """ESP-HS wins over GSP-Spec when both present."""
    df = _gaia_after_astrometry()
    df["teff_esphs"] = 6000.0
    df["teff_gspspec"] = 5500.0
    df = assign_photometry_gaia(df)
    out = compute_teff_gaia(df)
    assert out["teff"].iloc[0] == 6000.0
    assert qf_teff_src(out["quality_flags"].iloc[0]) == TEFF_SRC_ESPHS


def test_compute_teff_gaia_default_fallback():
    """No valid Teff source -> default 5800 K and teff_src = TEFF_SRC_DEFAULT."""
    df = _gaia_after_astrometry()
    df["bp_rp"] = np.nan
    df = assign_photometry_gaia(df)
    out = compute_teff_gaia(df)
    assert out["teff"].iloc[0] == TEFF_DEFAULT_K
    assert qf_teff_src(out["quality_flags"].iloc[0]) == TEFF_SRC_DEFAULT


def test_compute_teff_gaia_ignores_bv_without_bp_rp():
    """Gaia temperature fallback does not use B-V in the strict Gaia engine."""
    df = _gaia_after_astrometry()
    df["bp_rp"] = np.nan
    df["bv"] = 0.65
    df = assign_photometry_gaia(df)
    out = compute_teff_gaia(df)
    assert out["teff"].iloc[0] == TEFF_DEFAULT_K
    assert qf_teff_src(out["quality_flags"].iloc[0]) == TEFF_SRC_DEFAULT


def test_full_gaia_photometry_chain_quality_flags():
    """Astrometry -> assign_photometry -> mag_abs -> teff: quality_flags has dist_src, phot_src, teff_src."""
    df = _gaia_after_astrometry(
        phot_g_mean_mag=13.0,
        bp_rp=0.5,
        mg_gspphot=4.5,
        ag_gspphot=0.2,
    )
    df = assign_photometry_gaia(df)
    df = compute_mag_abs_gaia(df)
    df = compute_teff_gaia(df)
    flags = df["quality_flags"].iloc[0]
    assert qf_phot_src(flags) == PHOT_SRC_GAIA_G
    assert qf_teff_src(flags) in (
        TEFF_SRC_DEFAULT,
        TEFF_SRC_BPRP,
        TEFF_SRC_GSPSPEC,
        TEFF_SRC_ESPHS,
        TEFF_SRC_ESPUCD,
        TEFF_SRC_GSPPHOT,
    )
    assert "mag_abs" in df.columns and "teff" in df.columns
