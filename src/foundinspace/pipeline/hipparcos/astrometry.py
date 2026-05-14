import numpy as np
import pandas as pd

from foundinspace.pipeline.common.astrometry import (
    fractional_parallax_error,
    parallax_distance_pc,
    valid_parallax_mask,
)
from foundinspace.pipeline.common.quality_flags import pack_distance_flags
from foundinspace.pipeline.constants import DIST_SRC_HIP

HIPPARCOS_EPOCH_JYEAR = 1991.25


def select_astrometry_hip(df: pd.DataFrame) -> pd.DataFrame:
    """HIP-only astrometry: use Hipparcos Plx, ra_deg, dec_deg, pmRA, pmDE.

    Sets best_source, astrometry_quality (f_hip), quality_flags, and all *_use_*
    columns for downstream coords. No Gaia/BJ columns needed.

    Invalid or non-positive parallax yields NaN distance (no fabricated 1/plx floor).
    """
    plx_arr = df["Plx"].astype(float).to_numpy()
    e_plx = df["e_Plx"].astype(float).to_numpy()
    valid = valid_parallax_mask(plx_arr, e_plx)
    f_hip = fractional_parallax_error(plx_arr, e_plx)

    df["best_source"] = "HIP"
    df["best_score"] = f_hip
    df["astrometry_quality"] = f_hip
    dist_pc = parallax_distance_pc(plx_arr)
    df["r_med_best"] = dist_pc
    df["distance_use_pc"] = dist_pc
    df["quality_flags"] = pack_distance_flags(
        np.full(plx_arr.shape, DIST_SRC_HIP, dtype=np.uint16),
        distance_pc=dist_pc,
        needs_review=~valid,
    )
    df["ra_use_deg"] = df["ra_deg"].astype(float)
    df["dec_use_deg"] = df["dec_deg"].astype(float)
    df["pmra_use_masyr"] = df["pmRA"].astype(float)
    df["pmdec_use_masyr"] = df["pmDE"].astype(float)
    df["epoch_yr"] = HIPPARCOS_EPOCH_JYEAR
    return df
