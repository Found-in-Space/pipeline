"""Tests for Gaia astrometry: select_astrometry_gaia and quality_flags."""

import numpy as np
import pandas as pd
import pytest

from foundinspace.pipeline.constants import (
    DIST_SRC_BJ_GEO,
    DIST_SRC_BJ_PHOTOGEO,
    DIST_SRC_DR3_WEAK,
    DIST_SRC_GEO_WEAK,
    DIST_SRC_PHOTO_MG_AG,
    DIST_SRC_PHOTOGEO_WEAK,
    DIST_SRC_PRIOR,
    qf_dist_plausible,
    qf_dist_src,
    qf_dist_valid,
    qf_needs_review,
)
from foundinspace.pipeline.gaia.astrometry import select_astrometry_gaia


def _base_gaia_df(**overrides):
    """Minimal Gaia-like DataFrame for astrometry (required columns)."""
    d = {
        "ra": 180.0,
        "dec": 0.0,
        "pmra": 0.0,
        "pmdec": 0.0,
        "parallax": np.nan,
        "parallax_error": np.nan,
        "ruwe": 1.0,
        "r_lo_geo": np.nan,
        "r_med_geo": np.nan,
        "r_hi_geo": np.nan,
        "r_lo_photogeo": np.nan,
        "r_med_photogeo": np.nan,
        "r_hi_photogeo": np.nan,
        "phot_g_mean_mag": 14.0,
        "mg_gspphot": np.nan,
        "mg_gspphot_lower": np.nan,
        "mg_gspphot_upper": np.nan,
        "ag_gspphot": np.nan,
    }
    d.update(overrides)
    return pd.DataFrame([d])


def test_astrometry_adds_required_columns():
    """select_astrometry_gaia adds distance_use_pc, quality_flags, *_use_*, etc."""
    df = _base_gaia_df(parallax=1.0, parallax_error=0.01)
    out = select_astrometry_gaia(df)
    for col in (
        "distance_use_pc",
        "quality_flags",
        "astrometry_quality",
        "ra_use_deg",
        "dec_use_deg",
        "pmra_use_masyr",
        "pmdec_use_masyr",
        "epoch_yr",
    ):
        assert col in out.columns, f"missing {col}"


def test_inverse_parallax_is_review_fallback_without_bj_posterior():
    """Raw inverse parallax is retained only as a review fallback."""
    df = _base_gaia_df(parallax=2.0, parallax_error=0.02)
    out = select_astrometry_gaia(df)
    assert out["distance_use_pc"].iloc[0] == 500.0
    flags = out["quality_flags"].iloc[0]
    assert qf_dist_src(flags) == DIST_SRC_DR3_WEAK
    assert qf_dist_valid(flags)
    assert qf_needs_review(flags)
    assert np.isfinite(out["astrometry_quality"].iloc[0])
    assert not np.isinf(out["astrometry_quality"].iloc[0])


def test_tier_a_bj_photogeo_best_score():
    """Tier A: a trustworthy BJ photogeometric posterior can win."""
    df = _base_gaia_df(
        parallax=1.0,
        parallax_error=0.1,
        r_med_photogeo=400.0,
        r_lo_photogeo=350.0,
        r_hi_photogeo=450.0,
    )
    out = select_astrometry_gaia(df)
    assert out["distance_use_pc"].iloc[0] == 400.0
    assert qf_dist_src(out["quality_flags"].iloc[0]) == DIST_SRC_BJ_PHOTOGEO
    assert qf_dist_valid(out["quality_flags"].iloc[0])
    assert not qf_needs_review(out["quality_flags"].iloc[0])
    assert out["r_lo_pc"].iloc[0] == 350.0
    assert out["r_hi_pc"].iloc[0] == 450.0


def test_tier_a_bj_geo():
    """Tier A: BJ geometric can win; dist_src=BJ_GEO, r_lo_pc/r_hi_pc from geo."""
    df = _base_gaia_df(
        parallax=1.0,
        parallax_error=0.1,
        r_med_geo=200.0,
        r_lo_geo=180.0,
        r_hi_geo=220.0,
    )
    out = select_astrometry_gaia(df)
    assert out["distance_use_pc"].iloc[0] == 200.0
    assert qf_dist_src(out["quality_flags"].iloc[0]) == DIST_SRC_BJ_GEO
    assert out["r_lo_pc"].iloc[0] == 180.0
    assert out["r_hi_pc"].iloc[0] == 220.0


def test_broad_bj_posterior_is_retained_as_weak_and_marked_for_review():
    df = _base_gaia_df(
        parallax=0.5,
        parallax_error=0.2,
        r_med_geo=2000.0,
        r_lo_geo=1000.0,
        r_hi_geo=3000.0,
    )
    out = select_astrometry_gaia(df)

    assert out["distance_use_pc"].iloc[0] == 2000.0
    assert qf_dist_src(out["quality_flags"].iloc[0]) == DIST_SRC_GEO_WEAK
    assert qf_needs_review(out["quality_flags"].iloc[0])
    assert out["astrometry_quality"].iloc[0] == pytest.approx(0.5)


@pytest.mark.parametrize(
    ("parallax", "ruwe"),
    [
        (-0.5, 1.0),
        (0.1, 1.0),
        (0.5, 2.0),
        (0.5, np.nan),
    ],
)
def test_narrow_bj_posterior_with_weak_solution_is_marked_for_review(parallax, ruwe):
    df = _base_gaia_df(
        parallax=parallax,
        parallax_error=0.1,
        ruwe=ruwe,
        r_med_photogeo=2000.0,
        r_lo_photogeo=1800.0,
        r_hi_photogeo=2200.0,
    )
    out = select_astrometry_gaia(df)

    assert out["distance_use_pc"].iloc[0] == 2000.0
    assert qf_dist_src(out["quality_flags"].iloc[0]) == DIST_SRC_PHOTOGEO_WEAK
    assert qf_needs_review(out["quality_flags"].iloc[0])


def test_tier_a_bj_posterior_outranks_raw_inverse_parallax():
    df = _base_gaia_df(
        parallax=2.0,
        parallax_error=0.01,
        r_med_geo=510.0,
        r_lo_geo=500.0,
        r_hi_geo=520.0,
    )
    out = select_astrometry_gaia(df)

    assert out["distance_use_pc"].iloc[0] == 510.0
    assert qf_dist_src(out["quality_flags"].iloc[0]) == DIST_SRC_BJ_GEO
    assert out["astrometry_quality"].iloc[0] == pytest.approx(20.0 / 1020.0)


def test_malformed_bj_interval_cannot_win_with_negative_score():
    df = _base_gaia_df(
        parallax=2.0,
        parallax_error=0.02,
        r_med_geo=500.0,
        r_lo_geo=550.0,
        r_hi_geo=450.0,
    )
    out = select_astrometry_gaia(df)

    assert qf_dist_src(out["quality_flags"].iloc[0]) == DIST_SRC_GEO_WEAK
    assert out["astrometry_quality"].iloc[0] == 10.0


def test_tier_b_weak_catalog_when_all_primary_invalid():
    """Tier B: when all three primary candidates invalid, weak catalog used; needs_review set."""
    # No valid quality-tested candidate: plx=0, geo/photogeo have no finite f (e.g. hi=lo=nan)
    df = _base_gaia_df(
        parallax=0.0,
        parallax_error=0.0,
        r_med_geo=np.nan,
        r_lo_geo=np.nan,
        r_hi_geo=np.nan,
        r_med_photogeo=300.0,
        r_lo_photogeo=np.nan,
        r_hi_photogeo=np.nan,
        phot_g_mean_mag=12.0,
        mg_gspphot=4.0,
        ag_gspphot=0.2,
    )
    out = select_astrometry_gaia(df)
    assert out["distance_use_pc"].iloc[0] == 300.0
    flags = out["quality_flags"].iloc[0]
    assert qf_dist_src(flags) == DIST_SRC_PHOTOGEO_WEAK
    assert qf_dist_valid(flags)  # usable distance (any tier)
    assert qf_needs_review(flags)  # fallback, not primary
    assert out["astrometry_quality"].iloc[0] == 10.0  # QUALITY_FALLBACK_CATALOG
    assert np.isfinite(out["astrometry_quality"].iloc[0])


def test_tier_b_dr3_weak_positive_parallax_only():
    """Tier B: only positive plx available -> DR3_WEAK."""
    df = _base_gaia_df(
        parallax=0.5,
        parallax_error=0.0,  # invalid for Tier A (eplx not > 0 or f_dr3 not finite)
        r_med_geo=np.nan,
        r_lo_geo=np.nan,
        r_hi_geo=np.nan,
        r_med_photogeo=np.nan,
        r_lo_photogeo=np.nan,
        r_hi_photogeo=np.nan,
        phot_g_mean_mag=10.0,
        mg_gspphot=5.0,
        ag_gspphot=0.0,
    )
    out = select_astrometry_gaia(df)
    # 1000/0.5 = 2000 pc
    assert out["distance_use_pc"].iloc[0] == 2000.0
    assert qf_dist_src(out["quality_flags"].iloc[0]) == DIST_SRC_DR3_WEAK
    assert qf_dist_valid(out["quality_flags"].iloc[0])  # usable distance


def test_tier_c_photometric_distance():
    """Tier C: no catalog distance, has mag/mg/ag -> PHOTO_MG_AG."""
    # No valid Tier A or B: no plx, no r_med_geo, no r_med_photogeo
    df = _base_gaia_df(
        parallax=0.0,
        r_med_geo=np.nan,
        r_med_photogeo=np.nan,
        phot_g_mean_mag=10.0,
        mg_gspphot=5.0,
        ag_gspphot=0.5,
    )
    out = select_astrometry_gaia(df)
    # d = 10^((10 - 0.5 - 5 + 5)/5) = 10^1.9
    expected = 10.0 ** ((10.0 - 0.5 - 5.0 + 5.0) / 5.0)
    assert np.isclose(out["distance_use_pc"].iloc[0], expected)
    assert qf_dist_src(out["quality_flags"].iloc[0]) == DIST_SRC_PHOTO_MG_AG
    assert qf_dist_valid(out["quality_flags"].iloc[0])  # usable distance
    assert out["astrometry_quality"].iloc[0] == 20.0  # QUALITY_FALLBACK_PHOTOMETRIC


def test_tier_c_rejects_gspphot_magnitude_outside_its_quantiles():
    df = _base_gaia_df(
        parallax=0.0,
        r_med_geo=np.nan,
        r_med_photogeo=np.nan,
        phot_g_mean_mag=10.0,
        mg_gspphot=5.0,
        mg_gspphot_lower=6.0,
        mg_gspphot_upper=4.0,
        ag_gspphot=0.5,
    )
    out = select_astrometry_gaia(df)

    assert qf_dist_src(out["quality_flags"].iloc[0]) == DIST_SRC_PRIOR


def test_tier_d_synthetic_prior():
    """Tier D: only apparent mag -> PRIOR with fixed M_G."""
    df = _base_gaia_df(
        parallax=0.0,
        r_med_geo=np.nan,
        r_med_photogeo=np.nan,
        phot_g_mean_mag=15.0,
        mg_gspphot=np.nan,
        ag_gspphot=np.nan,
    )
    out = select_astrometry_gaia(df)
    assert qf_dist_src(out["quality_flags"].iloc[0]) == DIST_SRC_PRIOR
    assert qf_dist_valid(out["quality_flags"].iloc[0])  # usable distance
    assert out["distance_use_pc"].iloc[0] > 0
    assert np.isfinite(out["distance_use_pc"].iloc[0])
    assert out["astrometry_quality"].iloc[0] == 50.0  # QUALITY_SYNTHETIC


def test_quality_flags_dist_plausible():
    """quality_flags has FLAG_DIST_PLAUSIBLE when distance in 0.1--200k pc."""
    df = _base_gaia_df(parallax=1.0, parallax_error=0.01)  # 1000 pc
    out = select_astrometry_gaia(df)
    assert qf_dist_plausible(out["quality_flags"].iloc[0])


def test_astrometry_quality_never_inf():
    """astrometry_quality is never inf (finite or nan only)."""
    rows = [
        {"parallax": 1.0, "parallax_error": 0.01},
        {
            "parallax": 0.0,
            "r_med_photogeo": 100.0,
            "r_lo_photogeo": np.nan,
            "r_hi_photogeo": np.nan,
        },
        {
            "parallax": 0.0,
            "r_med_geo": np.nan,
            "r_med_photogeo": np.nan,
            "phot_g_mean_mag": 12.0,
            "mg_gspphot": 4.0,
            "ag_gspphot": 0.0,
        },
    ]
    for kw in rows:
        df = _base_gaia_df(**kw)
        out = select_astrometry_gaia(df)
        aq = out["astrometry_quality"]
        assert not np.any(np.isinf(aq)), f"astrometry_quality must not be inf for {kw}"


def test_multi_row_mixed_tiers():
    """Multiple rows get correct dist_src and distance per tier."""
    df = pd.DataFrame(
        [
            {
                "ra": 0,
                "dec": 0,
                "pmra": 0,
                "pmdec": 0,
                "parallax": 1.0,
                "parallax_error": 0.01,
                "r_med_geo": np.nan,
                "r_lo_geo": np.nan,
                "r_hi_geo": np.nan,
                "r_med_photogeo": np.nan,
                "r_lo_photogeo": np.nan,
                "r_hi_photogeo": np.nan,
                "phot_g_mean_mag": 14.0,
                "mg_gspphot": np.nan,
                "ag_gspphot": np.nan,
            },
            {
                "ra": 0,
                "dec": 0,
                "pmra": 0,
                "pmdec": 0,
                "parallax": 0.0,
                "parallax_error": 0.0,
                "r_med_geo": np.nan,
                "r_lo_geo": np.nan,
                "r_hi_geo": np.nan,
                "r_med_photogeo": 500.0,
                "r_lo_photogeo": np.nan,
                "r_hi_photogeo": np.nan,
                "phot_g_mean_mag": 14.0,
                "mg_gspphot": 5.0,
                "ag_gspphot": 0.0,
            },
        ]
    )
    out = select_astrometry_gaia(df)
    assert out["distance_use_pc"].iloc[0] == 1000.0
    assert qf_dist_src(out["quality_flags"].iloc[0]) == DIST_SRC_DR3_WEAK
    assert out["distance_use_pc"].iloc[1] == 500.0
    assert qf_dist_src(out["quality_flags"].iloc[1]) == DIST_SRC_PHOTOGEO_WEAK
