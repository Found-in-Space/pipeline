import numpy as np
import pandas as pd

from foundinspace.pipeline.common.photometry import (
    absolute_magnitude_from_apparent,
    bp_rp_to_teff,
    bv_to_teff,
    distance_modulus,
)
from foundinspace.pipeline.common.quality_flags import (
    add_photometry_source,
    add_teff_source,
)
from foundinspace.pipeline.constants import (
    PHOT_SRC_GAIA_G,
    PHOTOMETRY_QUALITY_DM_FACTOR,
    TEFF_DEFAULT_K,
    TEFF_SRC_BPRP,
    TEFF_SRC_BV,
    TEFF_SRC_DEFAULT,
    TEFF_SRC_ESPHS,
    TEFF_SRC_ESPUCD,
    TEFF_SRC_GSPPHOT,
    TEFF_SRC_GSPSPEC,
    qf_phot_src,
)

# Effective temperature bounds used to validate Gaia Teff estimates.
_TEFF_VALID_LO = 2000.0
_TEFF_VALID_HI = 50000.0

# Surface gravity bounds for Gaia logg (dex). Covers supergiants to white dwarfs.
_LOGG_VALID_LO = 0.0
_LOGG_VALID_HI = 9.0


def compute_teff_gaia(work: pd.DataFrame) -> pd.DataFrame:
    """Derive effective temperature (Teff) via Gaia best-of cascade.

    Cascade (first valid wins):
    1. Gaia spectroscopic Teff, in order of reliability:
       ESP-HS → GSP-Spec → ESP-UCD → GSP-Phot.
    2. Teff estimated from Gaia BP-RP color index.
    3. Teff estimated from Johnson B-V color index.
    4. Default: 5800 K (solar-type).

    Columns added:
        teff : effective temperature (K). Which cascade level was used is encoded in
               quality_flags (bits 4-6; see qf_teff_src).

    Args:
        work: Working DataFrame (must have columns for Gaia Teff, bp_rp, bv as
              available; missing columns are treated as all-NaN).

    Returns:
        New DataFrame with teff added and quality_flags updated with teff_src (bits 4-6).
    """

    def _valid_teff(series: pd.Series) -> pd.Series:
        s = series.astype(float)
        return (
            pd.notnull(s)
            & np.isfinite(s)
            & (s >= _TEFF_VALID_LO)
            & (s <= _TEFF_VALID_HI)
        )

    teff_esphs = (
        work["teff_esphs"]
        if "teff_esphs" in work.columns
        else pd.Series(np.nan, index=work.index)
    )
    teff_gspspec = (
        work["teff_gspspec"]
        if "teff_gspspec" in work.columns
        else pd.Series(np.nan, index=work.index)
    )
    teff_espucd = (
        work["teff_espucd"]
        if "teff_espucd" in work.columns
        else pd.Series(np.nan, index=work.index)
    )
    teff_gspphot = (
        work["teff_gspphot"]
        if "teff_gspphot" in work.columns
        else pd.Series(np.nan, index=work.index)
    )

    has_teff_esphs = _valid_teff(teff_esphs)
    has_teff_gspspec = _valid_teff(teff_gspspec) & ~has_teff_esphs
    has_teff_espucd = _valid_teff(teff_espucd) & ~has_teff_esphs & ~has_teff_gspspec
    has_teff_gspphot = (
        _valid_teff(teff_gspphot)
        & ~has_teff_esphs
        & ~has_teff_gspspec
        & ~has_teff_espucd
    )

    teff_gaia = np.where(
        has_teff_esphs,
        teff_esphs.astype(float),
        np.where(
            has_teff_gspspec,
            teff_gspspec.astype(float),
            np.where(
                has_teff_espucd,
                teff_espucd.astype(float),
                np.where(has_teff_gspphot, teff_gspphot.astype(float), np.nan),
            ),
        ),
    )
    no_gaia_teff = pd.isnull(teff_gaia) | ~np.isfinite(teff_gaia)

    bp_rp = work.get("bp_rp", pd.Series(np.nan, index=work.index)).astype(float)
    bv = work.get("bv", pd.Series(np.nan, index=work.index)).astype(float)

    has_bp_rp = pd.notnull(bp_rp) & np.isfinite(bp_rp)
    has_bv = pd.notnull(bv) & np.isfinite(bv)

    teff_from_bp_rp = bp_rp_to_teff(bp_rp.to_numpy())
    teff_from_bv = bv_to_teff(bv.to_numpy())

    use_bp_rp = no_gaia_teff & has_bp_rp
    use_bv = no_gaia_teff & ~use_bp_rp & has_bv

    teff_final = np.where(
        ~no_gaia_teff,
        teff_gaia,
        np.where(
            use_bp_rp, teff_from_bp_rp, np.where(use_bv, teff_from_bv, TEFF_DEFAULT_K)
        ),
    )
    teff_final = np.where(
        pd.isnull(teff_final) | ~np.isfinite(teff_final),
        TEFF_DEFAULT_K,
        teff_final,
    )

    work["teff"] = teff_final

    # OR teff_src into quality_flags (bits 4-6)
    teff_src_vals = np.where(
        has_teff_esphs,
        TEFF_SRC_ESPHS,
        np.where(
            has_teff_gspspec,
            TEFF_SRC_GSPSPEC,
            np.where(
                has_teff_espucd,
                TEFF_SRC_ESPUCD,
                np.where(
                    has_teff_gspphot,
                    TEFF_SRC_GSPPHOT,
                    np.where(
                        use_bp_rp,
                        TEFF_SRC_BPRP,
                        np.where(use_bv, TEFF_SRC_BV, TEFF_SRC_DEFAULT),
                    ),
                ),
            ),
        ),
    )
    teff_src_vals = np.asarray(teff_src_vals, dtype=np.uint16)
    flags = work["quality_flags"].astype(np.uint16).to_numpy()
    work["quality_flags"] = add_teff_source(flags, teff_src_vals)

    return work


def compute_log_g_gaia(work: pd.DataFrame) -> pd.DataFrame:
    """Derive surface gravity (log g, dex) from Gaia astrophysical parameters.

    Cascade (first valid wins):
    1. logg_esphs (ESP-HS)
    2. logg_gspspec (GSP-Spec)
    3. logg_gspphot (GSP-Phot)

    Valid range: 0–9 dex (supergiants to white dwarfs).
    Missing or invalid values → NaN.

    Columns added:
        log_g: surface gravity in dex

    Args:
        work: Working DataFrame (must have Gaia logg columns as available).

    Returns:
        New DataFrame with log_g added.
    """

    def _valid_logg(series: pd.Series) -> pd.Series:
        s = series.astype(float)
        return (
            pd.notnull(s)
            & np.isfinite(s)
            & (s >= _LOGG_VALID_LO)
            & (s <= _LOGG_VALID_HI)
        )

    logg_esphs = (
        work["logg_esphs"]
        if "logg_esphs" in work.columns
        else pd.Series(np.nan, index=work.index)
    )
    logg_gspspec = (
        work["logg_gspspec"]
        if "logg_gspspec" in work.columns
        else pd.Series(np.nan, index=work.index)
    )
    logg_gspphot = (
        work["logg_gspphot"]
        if "logg_gspphot" in work.columns
        else pd.Series(np.nan, index=work.index)
    )

    has_logg_esphs = _valid_logg(logg_esphs)
    has_logg_gspspec = _valid_logg(logg_gspspec) & ~has_logg_esphs
    has_logg_gspphot = _valid_logg(logg_gspphot) & ~has_logg_esphs & ~has_logg_gspspec

    log_g = np.where(
        has_logg_esphs,
        logg_esphs.astype(float),
        np.where(
            has_logg_gspspec,
            logg_gspspec.astype(float),
            np.where(has_logg_gspphot, logg_gspphot.astype(float), np.nan),
        ),
    )

    work["log_g"] = log_g

    return work


def assign_photometry_gaia(df: pd.DataFrame) -> pd.DataFrame:
    """Gaia-only: mag = phot_g_mean_mag, color = bp_rp; OR phot_src into quality_flags."""
    df["mag"] = df["phot_g_mean_mag"].astype(float)
    df["color"] = df["bp_rp"].astype(float)
    flags = df.get("quality_flags", pd.Series(0, index=df.index)).astype(np.uint16)
    df["quality_flags"] = add_photometry_source(flags, PHOT_SRC_GAIA_G)
    return df


def compute_mag_abs_gaia(df: pd.DataFrame) -> pd.DataFrame:
    """Gaia-only: full cascade (GSP-Phot, ext-corrected, dm). photometry_quality in mag."""
    r_pc = df["distance_use_pc"].astype(float)
    mag = df["mag"].astype(float)
    ag = df.get("ag_gspphot", pd.Series(np.nan, index=df.index)).astype(float)
    mg = df.get("mg_gspphot", pd.Series(np.nan, index=df.index)).astype(float)
    dm = distance_modulus(r_pc)

    qf = df["quality_flags"].astype(np.uint16).to_numpy()
    is_gaia_g = qf_phot_src(qf) == PHOT_SRC_GAIA_G
    ag_ok = pd.notnull(ag) & (ag >= 0) & (ag < 5)
    mg_ok = pd.notnull(mg) & np.isfinite(mg) & (mg > -10) & (mg < 15)

    mag_abs_gspphot = np.where(mg_ok, mg, np.nan)
    mag_abs_ext = np.where(
        is_gaia_g & ag_ok,
        absolute_magnitude_from_apparent(mag, r_pc, ag),
        np.nan,
    )
    mag_abs_dm = mag - dm

    df["mag_abs"] = (
        pd.Series(mag_abs_gspphot, index=df.index)
        .fillna(pd.Series(mag_abs_ext, index=df.index))
        .fillna(pd.Series(mag_abs_dm, index=df.index))
    )

    f_dist = df.get("astrometry_quality", pd.Series(np.nan, index=df.index)).astype(
        float
    )
    quality_dm = PHOTOMETRY_QUALITY_DM_FACTOR * f_dist

    mg_upper = df.get("mg_gspphot_upper", pd.Series(np.nan, index=df.index)).astype(
        float
    )
    mg_lower = df.get("mg_gspphot_lower", pd.Series(np.nan, index=df.index)).astype(
        float
    )
    has_bounds = (
        pd.notnull(mg_upper)
        & pd.notnull(mg_lower)
        & np.isfinite(mg_upper)
        & np.isfinite(mg_lower)
    )
    quality_gspphot = np.where(has_bounds, np.abs(mg_upper - mg_lower) / 2.0, np.nan)

    used_gspphot = pd.notnull(mag_abs_gspphot) & np.isfinite(mag_abs_gspphot)
    used_ext = ~used_gspphot & pd.notnull(mag_abs_ext) & np.isfinite(mag_abs_ext)

    df["photometry_quality"] = np.where(
        used_gspphot,
        np.where(has_bounds, quality_gspphot, np.nan),
        np.where(used_ext, quality_dm, quality_dm),
    )
    df["photometry_quality"] = df["photometry_quality"].astype(float)
    return df
