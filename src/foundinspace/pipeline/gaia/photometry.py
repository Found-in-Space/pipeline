import numpy as np
import pandas as pd

from foundinspace.pipeline.common.photometry import (
    absolute_magnitude_from_apparent,
    bp_rp_to_teff,
    distance_modulus,
)
from foundinspace.pipeline.common.quality_flags import (
    add_photometry_source,
    add_teff_source,
)
from foundinspace.pipeline.constants import (
    PHOT_SRC_GAIA_G,
    PHOT_SRC_GAIA_GSPPHOT,
    PHOT_SRC_UNKNOWN,
    PHOTOMETRY_QUALITY_DM_FACTOR,
    TEFF_DEFAULT_K,
    TEFF_SRC_BPRP,
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

_BP_RP_VALID_LO = -0.5
_BP_RP_VALID_HI = 3.5


def _require_columns(df: pd.DataFrame, columns: tuple[str, ...], *, label: str) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(
            f"Missing required Gaia {label} column(s): " + ", ".join(missing)
        )


def compute_teff_gaia(work: pd.DataFrame) -> pd.DataFrame:
    """Derive effective temperature (Teff) via Gaia best-of cascade.

    Cascade (first valid wins):
    1. Gaia spectroscopic Teff, in order of reliability:
       ESP-HS → GSP-Spec → ESP-UCD → GSP-Phot.
    2. Teff estimated from Gaia BP-RP color index.
    3. Default: 5800 K (solar-type).

    Columns added:
        teff : effective temperature (K). Which cascade level was used is encoded in
               quality_flags (bits 4-6; see qf_teff_src).

    Returns:
        New DataFrame with teff added and quality_flags updated with teff_src
        (bits 4-6).
    """
    _require_columns(
        work,
        (
            "teff_esphs",
            "teff_gspspec",
            "teff_espucd",
            "teff_gspphot",
            "bp_rp",
            "quality_flags",
        ),
        label="temperature",
    )

    def _valid_teff(series: pd.Series) -> pd.Series:
        s = series.astype(float)
        return (
            pd.notnull(s)
            & np.isfinite(s)
            & (s >= _TEFF_VALID_LO)
            & (s <= _TEFF_VALID_HI)
        )

    teff_esphs = work["teff_esphs"]
    teff_gspspec = work["teff_gspspec"]
    teff_espucd = work["teff_espucd"]
    teff_gspphot = work["teff_gspphot"]

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

    bp_rp = work["bp_rp"].astype(float)

    has_bp_rp = (
        pd.notnull(bp_rp)
        & np.isfinite(bp_rp)
        & (bp_rp >= _BP_RP_VALID_LO)
        & (bp_rp <= _BP_RP_VALID_HI)
    )

    teff_from_bp_rp = bp_rp_to_teff(bp_rp.to_numpy())

    use_bp_rp = no_gaia_teff & has_bp_rp

    teff_final = np.where(
        ~no_gaia_teff,
        teff_gaia,
        np.where(use_bp_rp, teff_from_bp_rp, TEFF_DEFAULT_K),
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
                    np.where(use_bp_rp, TEFF_SRC_BPRP, TEFF_SRC_DEFAULT),
                ),
            ),
        ),
    )
    teff_src_vals = np.asarray(teff_src_vals, dtype=np.uint16)
    flags = work["quality_flags"].astype(np.uint16).to_numpy()
    work["quality_flags"] = add_teff_source(flags, teff_src_vals)

    return work


def assign_photometry_gaia(df: pd.DataFrame) -> pd.DataFrame:
    """Gaia-only: mag = phot_g_mean_mag, color = bp_rp; OR phot_src into quality_flags."""
    _require_columns(
        df,
        ("phot_g_mean_mag", "bp_rp", "quality_flags"),
        label="photometry",
    )
    df["mag"] = df["phot_g_mean_mag"].astype(float)
    df["color"] = df["bp_rp"].astype(float)
    flags = df["quality_flags"].astype(np.uint16)
    df["quality_flags"] = add_photometry_source(flags, PHOT_SRC_GAIA_G)
    return df


def compute_mag_abs_gaia(df: pd.DataFrame) -> pd.DataFrame:
    """Gaia-only: full cascade (GSP-Phot, ext-corrected, dm). photometry_quality in mag."""
    _require_columns(
        df,
        (
            "distance_use_pc",
            "mag",
            "ag_gspphot",
            "mg_gspphot",
            "quality_flags",
            "astrometry_quality",
            "mg_gspphot_upper",
            "mg_gspphot_lower",
        ),
        label="absolute-magnitude",
    )
    r_pc = df["distance_use_pc"].astype(float)
    mag = df["mag"].astype(float)
    ag = df["ag_gspphot"].astype(float)
    mg = df["mg_gspphot"].astype(float)
    dm = distance_modulus(r_pc)

    qf = df["quality_flags"].astype(np.uint16).to_numpy()
    is_gaia_g = qf_phot_src(qf) == PHOT_SRC_GAIA_G
    ag_ok = pd.notnull(ag) & (ag >= 0) & (ag < 5)
    mg_upper = df["mg_gspphot_upper"].astype(float)
    mg_lower = df["mg_gspphot_lower"].astype(float)
    bounds_present = (
        pd.notnull(mg_upper)
        & pd.notnull(mg_lower)
        & np.isfinite(mg_upper)
        & np.isfinite(mg_lower)
    )
    has_bounds = bounds_present & (mg_lower <= mg) & (mg <= mg_upper)
    mg_ok = (
        pd.notnull(mg)
        & np.isfinite(mg)
        & (mg > -10)
        & (mg < 15)
        & (~bounds_present | has_bounds)
    )

    mag_ok = pd.notnull(mag) & np.isfinite(mag)
    mag_abs_gspphot = np.where(mg_ok, mg, np.nan)
    mag_abs_ext = np.where(
        is_gaia_g & mag_ok & ag_ok,
        absolute_magnitude_from_apparent(mag, r_pc, ag),
        np.nan,
    )
    mag_abs_dm = np.where(mag_ok & np.isfinite(dm), mag - dm, np.nan)

    df["mag_abs"] = (
        pd.Series(mag_abs_gspphot, index=df.index)
        .fillna(pd.Series(mag_abs_ext, index=df.index))
        .fillna(pd.Series(mag_abs_dm, index=df.index))
    )

    f_dist = df["astrometry_quality"].astype(float)
    quality_dm = PHOTOMETRY_QUALITY_DM_FACTOR * f_dist

    quality_gspphot = np.where(has_bounds, (mg_upper - mg_lower) / 2.0, np.nan)

    used_gspphot = pd.notnull(mag_abs_gspphot) & np.isfinite(mag_abs_gspphot)
    used_ext = ~used_gspphot & pd.notnull(mag_abs_ext) & np.isfinite(mag_abs_ext)
    used_dm = (
        ~used_gspphot & ~used_ext & pd.notnull(mag_abs_dm) & np.isfinite(mag_abs_dm)
    )

    df["photometry_quality"] = np.where(
        used_gspphot,
        np.where(has_bounds, quality_gspphot, np.nan),
        np.where(used_ext | used_dm, quality_dm, np.nan),
    )
    df["photometry_quality"] = df["photometry_quality"].astype(float)

    phot_src = np.where(
        used_gspphot,
        PHOT_SRC_GAIA_GSPPHOT,
        np.where(used_ext | used_dm, PHOT_SRC_GAIA_G, PHOT_SRC_UNKNOWN),
    ).astype(np.uint16)
    df["quality_flags"] = add_photometry_source(qf, phot_src)
    return df
