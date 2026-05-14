import numpy as np
import pandas as pd

from foundinspace.pipeline.common.astrometry import (
    finite_positive_mask,
    fractional_distance_interval_width,
    fractional_parallax_error,
    parallax_distance_pc,
)
from foundinspace.pipeline.common.quality_flags import pack_distance_flags
from foundinspace.pipeline.constants import (
    CANONICAL_EPOCH_JYEAR,
    DIST_SRC_BJ_GEO,
    DIST_SRC_BJ_PHOTOGEO,
    DIST_SRC_DR3,
    DIST_SRC_DR3_WEAK,
    DIST_SRC_GEO_WEAK,
    DIST_SRC_PHOTO_MG_AG,
    DIST_SRC_PHOTOGEO_WEAK,
    DIST_SRC_PRIOR,
    DIST_SRC_UNKNOWN,
)

LAMBDA_PHOTO = 0.02
LAMBDA_GEO = 0.01

# Finite quality sentinels for non-primary tiers (no inf leakage into photometry_quality).
QUALITY_FALLBACK_CATALOG = 10.0
QUALITY_FALLBACK_PHOTOMETRIC = 20.0
QUALITY_SYNTHETIC = 50.0

# Tier D absolute-magnitude prior (rough median for the Gaia bright sample).
_MG_PRIOR = 4.0

_DIST_SRC_MAP = {
    "UNKNOWN": DIST_SRC_UNKNOWN,
    "DR3": DIST_SRC_DR3,
    "BJ_GEO": DIST_SRC_BJ_GEO,
    "BJ_PHOTOGEO": DIST_SRC_BJ_PHOTOGEO,
    "DR3_WEAK": DIST_SRC_DR3_WEAK,
    "GEO_WEAK": DIST_SRC_GEO_WEAK,
    "PHOTOGEO_WEAK": DIST_SRC_PHOTOGEO_WEAK,
    "PHOTO_MG_AG": DIST_SRC_PHOTO_MG_AG,
    "PRIOR": DIST_SRC_PRIOR,
}


def _ensure_float(df: pd.DataFrame, cols: list[str]) -> None:
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan
        df[c] = df[c].astype(float)


def _first_valid_catalog(
    a: np.ndarray,
    a_label: str,
    b: np.ndarray,
    b_label: str,
    c: np.ndarray,
    c_label: str,
) -> tuple[np.ndarray, np.ndarray]:
    """Return (distance_array, source_label_array). Priority: a → b → c (applied in reverse)."""
    n = len(a)
    dist = np.full(n, np.nan, dtype=float)
    src = np.full(n, "NONE", dtype=object)
    for arr, label in [(c, c_label), (b, b_label), (a, a_label)]:
        ok = finite_positive_mask(arr)
        dist[ok] = arr[ok]
        src[ok] = label
    return dist, src


def select_astrometry_gaia(df: pd.DataFrame) -> pd.DataFrame:
    """Gaia-only astrometry: multi-tier distance selection with explicit provenance.

    Tier A  primary              – best of DR3 / BJ_GEO / BJ_PHOTOGEO (quality-tested).
    Tier B  fallback_catalog     – any positive finite catalog value:
                                   r_med_photogeo → r_med_geo → 1000/parallax (plx>0).
    Tier C  fallback_photometric – photometric distance from GSP-Phot M_G and A_G.
    Tier D  synthetic            – prior from apparent magnitude + fixed M_G.

    Rows are never dropped here; callers may filter on quality_flags (qf_dist_valid, qf_needs_review).

    Sets distance_use_pc, quality_flags (uint16), astrometry_quality (finite), and *_use_* columns.
    """
    _ensure_float(
        df,
        [
            "parallax",
            "parallax_error",
            "r_lo_geo",
            "r_med_geo",
            "r_hi_geo",
            "r_lo_photogeo",
            "r_med_photogeo",
            "r_hi_photogeo",
        ],
    )
    df["ra_deg"] = df["ra"].astype(float)
    df["dec_deg"] = df["dec"].astype(float)

    plx = df["parallax"].to_numpy(dtype=float)
    eplx = df["parallax_error"].to_numpy(dtype=float)
    r_geo = df["r_med_geo"].to_numpy(dtype=float)
    r_photo = df["r_med_photogeo"].to_numpy(dtype=float)
    r_lo_g = df["r_lo_geo"].to_numpy(dtype=float)
    r_hi_g = df["r_hi_geo"].to_numpy(dtype=float)
    r_lo_p = df["r_lo_photogeo"].to_numpy(dtype=float)
    r_hi_p = df["r_hi_photogeo"].to_numpy(dtype=float)

    f_dr3 = fractional_parallax_error(plx, eplx)
    f_geo = fractional_distance_interval_width(r_geo, r_lo_g, r_hi_g)
    f_photogeo = fractional_distance_interval_width(r_photo, r_lo_p, r_hi_p)

    valid_dr3 = np.isfinite(f_dr3)
    valid_geo = np.isfinite(f_geo) & finite_positive_mask(r_geo)
    valid_photogeo = np.isfinite(f_photogeo) & finite_positive_mask(r_photo)
    any_valid = valid_dr3 | valid_geo | valid_photogeo

    # Tier A: best quality-tested candidate
    _src_a = np.array(["DR3", "BJ_GEO", "BJ_PHOTOGEO"])
    cand = np.column_stack(
        [
            np.where(valid_dr3, f_dr3, np.inf),
            np.where(valid_geo, f_geo + LAMBDA_GEO, np.inf),
            np.where(valid_photogeo, f_photogeo + LAMBDA_PHOTO, np.inf),
        ]
    )
    best_idx_a = np.argmin(cand, axis=1)
    score_a = cand[np.arange(len(df)), best_idx_a]
    src_a_names = _src_a[best_idx_a]

    plx_dist = parallax_distance_pc(plx)
    dist_a = np.where(
        src_a_names == "DR3",
        plx_dist,
        np.where(src_a_names == "BJ_GEO", r_geo, r_photo),
    )

    # Tier B: soft catalog fallback
    tier_b_dist, tier_b_source = _first_valid_catalog(
        r_photo,
        "PHOTOGEO_WEAK",
        r_geo,
        "GEO_WEAK",
        plx_dist,
        "DR3_WEAK",
    )

    # Tier C: photometric distance (phot_g_mean_mag; assign_photometry_gaia not run yet)
    g_mag = df.get("phot_g_mean_mag", pd.Series(np.nan, index=df.index)).to_numpy(float)
    mg = df.get("mg_gspphot", pd.Series(np.nan, index=df.index)).to_numpy(float)
    ag = df.get("ag_gspphot", pd.Series(np.nan, index=df.index)).to_numpy(float)

    mag_ok = np.isfinite(g_mag)
    mg_ok = np.isfinite(mg) & (mg > -10) & (mg < 15)
    ag_ok = np.isfinite(ag) & (ag >= 0) & (ag < 5)
    ag_use = np.where(ag_ok, ag, 0.0)

    tier_c_dist = np.where(
        mg_ok & mag_ok,
        10.0 ** ((g_mag - ag_use - mg + 5.0) / 5.0),
        np.nan,
    )
    tier_c_ok = np.isfinite(tier_c_dist) & (tier_c_dist > 0)

    # Tier D: synthetic prior
    tier_d_dist = np.where(
        mag_ok,
        10.0 ** ((g_mag - ag_use - _MG_PRIOR + 5.0) / 5.0),
        np.nan,
    )
    tier_d_ok = np.isfinite(tier_d_dist) & (tier_d_dist > 0)

    # Assemble: apply in ascending priority so Tier A overwrites
    n = len(df)
    distance = np.full(n, np.nan, dtype=float)
    source = np.full(n, "UNKNOWN", dtype=object)
    quality = np.full(n, np.nan, dtype=float)
    review = np.ones(n, dtype=bool)

    distance[tier_d_ok] = tier_d_dist[tier_d_ok]
    source[tier_d_ok] = "PRIOR"
    quality[tier_d_ok] = QUALITY_SYNTHETIC

    distance[tier_c_ok] = tier_c_dist[tier_c_ok]
    source[tier_c_ok] = "PHOTO_MG_AG"
    quality[tier_c_ok] = QUALITY_FALLBACK_PHOTOMETRIC

    tier_b_ok = np.isfinite(tier_b_dist) & (tier_b_dist > 0)
    distance[tier_b_ok] = tier_b_dist[tier_b_ok]
    source[tier_b_ok] = tier_b_source[tier_b_ok]
    quality[tier_b_ok] = QUALITY_FALLBACK_CATALOG

    distance[any_valid] = dist_a[any_valid]
    source[any_valid] = src_a_names[any_valid]
    quality[any_valid] = score_a[any_valid]
    review[any_valid] = False  # Tier A = trusted primary; B/C/D keep needs_review

    # quality_flags: dist_src + status bits (phot_src OR'd in by assign_photometry_gaia)
    dist_src_bits = np.full(n, DIST_SRC_UNKNOWN, dtype=np.uint16)
    for label, val in _DIST_SRC_MAP.items():
        if label == "UNKNOWN":
            continue
        dist_src_bits[source == label] = val
    df["quality_flags"] = pack_distance_flags(
        dist_src_bits,
        distance_pc=distance,
        needs_review=review,
    )

    df["distance_use_pc"] = distance
    df["r_med_best"] = distance
    df["astrometry_quality"] = np.where(np.isfinite(quality), quality, np.nan)

    # BJ confidence intervals only where BJ is the chosen source
    df["r_lo_pc"] = np.nan
    df["r_hi_pc"] = np.nan
    bj_geo = source == "BJ_GEO"
    bj_photo = source == "BJ_PHOTOGEO"
    if bj_geo.any():
        df.loc[bj_geo, "r_lo_pc"] = df.loc[bj_geo, "r_lo_geo"].astype(float).values
        df.loc[bj_geo, "r_hi_pc"] = df.loc[bj_geo, "r_hi_geo"].astype(float).values
    if bj_photo.any():
        df.loc[bj_photo, "r_lo_pc"] = (
            df.loc[bj_photo, "r_lo_photogeo"].astype(float).values
        )
        df.loc[bj_photo, "r_hi_pc"] = (
            df.loc[bj_photo, "r_hi_photogeo"].astype(float).values
        )

    df["ra_use_deg"] = df["ra"].astype(float)
    df["dec_use_deg"] = df["dec"].astype(float)
    df["pmra_use_masyr"] = df["pmra"].astype(float)
    df["pmdec_use_masyr"] = df["pmdec"].astype(float)
    df["epoch_yr"] = CANONICAL_EPOCH_JYEAR

    return df
