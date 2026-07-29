import numpy as np
import pandas as pd

from foundinspace.pipeline.common.astrometry import (
    edsd_mode_distance_pc,
    fractional_parallax_error,
    parallax_distance_pc,
    valid_parallax_mask,
)
from foundinspace.pipeline.common.quality_flags import pack_distance_flags
from foundinspace.pipeline.constants import DIST_SRC_HIP, DIST_SRC_HIP_EDSD

HIPPARCOS_EPOCH_JYEAR = 1991.25
HIP_PRIMARY_MAX_FRACTIONAL_ERROR = 0.2
# Minimal isotropic EDSD fallback from Bailer-Jones (2015), using the 1.35 kpc
# scale adopted for the Gaia DR1 distance catalogue. It is intentionally
# reviewable because a direction-dependent or curated distance is preferable.
HIP_EDSD_LENGTH_SCALE_PC = 1350.0
HIP_EDSD_UNKNOWN_QUALITY = 10.0


def select_astrometry_hip(df: pd.DataFrame) -> pd.DataFrame:
    """HIP-only astrometry: use Hipparcos Plx, ra_deg, dec_deg, pmRA, pmDE.

    Sets best_source, astrometry_quality (f_hip), quality_flags, and all *_use_*
    columns for downstream coords. No Gaia/BJ columns needed.

    High-significance positive parallaxes use direct inversion. Noisy, zero,
    negative, or otherwise invalid measurements use the mode of an
    exponentially decreasing space-density posterior. The fallback remains
    marked for review and is expected to be superseded by Gaia or a curated
    distance where available.
    """
    plx_arr = df["Plx"].astype(float).to_numpy()
    e_plx = df["e_Plx"].astype(float).to_numpy()
    valid = valid_parallax_mask(plx_arr, e_plx)
    f_hip = fractional_parallax_error(plx_arr, e_plx)
    primary_quality = valid & (f_hip <= HIP_PRIMARY_MAX_FRACTIONAL_ERROR)

    direct_distance_pc = parallax_distance_pc(plx_arr)
    fallback_distance_pc = edsd_mode_distance_pc(
        plx_arr,
        e_plx,
        length_scale_pc=HIP_EDSD_LENGTH_SCALE_PC,
    )
    dist_pc = np.where(primary_quality, direct_distance_pc, fallback_distance_pc)
    fallback_quality = np.where(
        np.isfinite(f_hip),
        f_hip,
        HIP_EDSD_UNKNOWN_QUALITY,
    )
    quality = np.where(primary_quality, f_hip, fallback_quality)
    df["best_source"] = np.where(primary_quality, "HIP", "HIP_EDSD")
    df["best_score"] = quality
    df["astrometry_quality"] = quality
    df["r_med_best"] = dist_pc
    df["distance_use_pc"] = dist_pc
    dist_src = np.where(primary_quality, DIST_SRC_HIP, DIST_SRC_HIP_EDSD).astype(
        np.uint16
    )
    df["quality_flags"] = pack_distance_flags(
        dist_src,
        distance_pc=dist_pc,
        needs_review=~primary_quality,
    )
    df["ra_use_deg"] = df["ra_deg"].astype(float)
    df["dec_use_deg"] = df["dec_deg"].astype(float)
    df["pmra_use_masyr"] = df["pmRA"].astype(float)
    df["pmdec_use_masyr"] = df["pmDE"].astype(float)
    df["epoch_yr"] = HIPPARCOS_EPOCH_JYEAR
    return df
