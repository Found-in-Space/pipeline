"""Coordinate propagation and Cartesian conversion for prepared catalog rows."""

import numpy as np
import pandas as pd
from astropy import units as u
from astropy.coordinates import SkyCoord
from astropy.time import Time

from foundinspace.pipeline.constants import CANONICAL_EPOCH_JYEAR

# 1 mas = pi / (180 * 3600 * 1000) rad
MAS_TO_RAD = np.pi / (180.0 * 3_600_000.0)


def calculate_coordinates(df: pd.DataFrame) -> pd.DataFrame:
    """Propagate all positions to J2016.0 and compute Cartesian coordinates.

    Uses distance_use_pc directly (no parallax round-trip for Bailer-Jones rows).
    Applies SkyCoord.apply_space_motion() to advance positions from each row's
    source epoch to the canonical epoch J2016.0.

    Columns added:
        x_icrs_pc, y_icrs_pc, z_icrs_pc   : Cartesian ICRS (pc)
        ra_deg                            : RA at J2016.0 (deg)
        dec_deg                           : Dec at J2016.0 (deg)
        r_pc                              : distance (pc) at J2016.0
        epoch_norm_yr                     : canonical epoch (always 2016.0)

    Args:
        df: Prepared catalog rows with ra_use_deg, dec_use_deg, distance_use_pc,
            pmra_use_masyr, pmdec_use_masyr, and epoch_yr.

    Returns:
        New DataFrame with coordinate columns added.
    """
    canonical_epoch = Time(CANONICAL_EPOCH_JYEAR, format="jyear")

    c = SkyCoord(
        ra=df["ra_use_deg"].astype(float).to_numpy() * u.deg,
        dec=df["dec_use_deg"].astype(float).to_numpy() * u.deg,
        distance=df["distance_use_pc"].astype(float).to_numpy() * u.pc,
        pm_ra_cosdec=df["pmra_use_masyr"].astype(float).to_numpy() * u.mas / u.yr,
        pm_dec=df["pmdec_use_masyr"].astype(float).to_numpy() * u.mas / u.yr,
        obstime=Time(df["epoch_yr"].astype(float).to_numpy(), format="jyear"),
        frame="icrs",
    )
    c2016 = c.apply_space_motion(new_obstime=canonical_epoch)

    cart = c2016.cartesian
    df["x_icrs_pc"] = cart.x.to_value(u.pc)
    df["y_icrs_pc"] = cart.y.to_value(u.pc)
    df["z_icrs_pc"] = cart.z.to_value(u.pc)
    df["epoch_norm_yr"] = CANONICAL_EPOCH_JYEAR

    df["ra_deg"] = c2016.ra.to_value(u.deg)
    df["dec_deg"] = c2016.dec.to_value(u.deg)
    df["r_pc"] = c2016.distance.to_value(u.pc)

    return df


def calculate_coordinates_fast(df: pd.DataFrame) -> pd.DataFrame:
    """Propagate ICRS positions to J2016.0 using pure NumPy.

    Assumptions:
      - Input coordinates are ICRS.
      - pmra_use_masyr is mu_alpha* = d(alpha)/dt * cos(delta), in mas/yr.
      - pmdec_use_masyr is d(delta)/dt, in mas/yr.
      - No radial velocity is applied (equivalent to Astropy's default if RV is absent).

    Added columns:
        x_icrs_pc, y_icrs_pc, z_icrs_pc
        ra_deg, dec_deg, r_pc
        epoch_norm_yr
    """
    # Pull arrays once, without unnecessary copies
    ra = np.deg2rad(df["ra_use_deg"].to_numpy(dtype=np.float64, copy=False))
    dec = np.deg2rad(df["dec_use_deg"].to_numpy(dtype=np.float64, copy=False))
    r0 = df["distance_use_pc"].to_numpy(dtype=np.float64, copy=False)

    mu_a = df["pmra_use_masyr"].to_numpy(dtype=np.float64, copy=False) * MAS_TO_RAD
    mu_d = df["pmdec_use_masyr"].to_numpy(dtype=np.float64, copy=False) * MAS_TO_RAD

    dt = CANONICAL_EPOCH_JYEAR - df["epoch_yr"].to_numpy(dtype=np.float64, copy=False)

    # Trig once
    ca = np.cos(ra)
    sa = np.sin(ra)
    cd = np.cos(dec)
    sd = np.sin(dec)

    # Initial unit vector u
    ux = cd * ca
    uy = cd * sa
    uz = sd

    # Initial Cartesian position
    x0 = r0 * ux
    y0 = r0 * uy
    z0 = r0 * uz

    # Tangent basis on the sphere
    # e_alpha points in increasing RA direction
    eax = -sa
    eay = ca

    # e_delta points in increasing Dec direction
    edx = -ca * sd
    edy = -sa * sd
    edz = cd

    # Transverse velocity in pc/yr
    # du/dt = mu_a * e_alpha + mu_d * e_delta
    vx = r0 * (mu_a * eax + mu_d * edx)
    vy = r0 * (mu_a * eay + mu_d * edy)
    vz = r0 * (mu_d * edz)

    # Propagate in Cartesian space
    x = x0 + dt * vx
    y = y0 + dt * vy
    z = z0 + dt * vz

    # Back to spherical
    xy = np.hypot(x, y)
    r = np.hypot(xy, z)

    ra_out = np.degrees(np.mod(np.arctan2(y, x), 2.0 * np.pi))
    dec_out = np.degrees(np.arctan2(z, xy))

    # Assign back
    df["x_icrs_pc"] = x
    df["y_icrs_pc"] = y
    df["z_icrs_pc"] = z
    df["epoch_norm_yr"] = CANONICAL_EPOCH_JYEAR

    df["ra_deg"] = ra_out
    df["dec_deg"] = dec_out
    df["r_pc"] = r

    return df
