CANONICAL_EPOCH_JYEAR = 2016.0  # Gaia DR3 reference epoch (J2016.0)

EPS = 1e-12  # Epsilon for numerical stability

# σ_M ≈ 2.17 * (σ_r / r) for distance-modulus uncertainty in mag
PHOTOMETRY_QUALITY_DM_FACTOR = 2.17

# Teff bounds for blackbody RGB (Tanner Helland algorithm range)
TEFF_MIN_K = 1000
TEFF_MAX_K = 40000

# Default when no color/Teff available (solar-type)
TEFF_DEFAULT_K = 5800


# ---------------------------------------------------------------------------
# quality_flags: packed uint16 per row.
# Replaces astrometry_method, photometry_method, and provenance string columns.
# ---------------------------------------------------------------------------

# bits 0-3: dist_src (distance / astrometry source)
DIST_SRC_UNKNOWN = 0x0
DIST_SRC_DR3 = 0x1  # Tier A: Gaia DR3 parallax
DIST_SRC_BJ_GEO = 0x2  # Tier A: Bailer-Jones geometric
DIST_SRC_BJ_PHOTOGEO = 0x3  # Tier A: Bailer-Jones photogeometric
DIST_SRC_HIP = 0x4  # Tier A: Hipparcos parallax
DIST_SRC_DR3_WEAK = 0x5  # Tier B: DR3 plx > 0 but fails quality
DIST_SRC_GEO_WEAK = 0x6  # Tier B: r_med_geo > 0 but fails quality
DIST_SRC_PHOTOGEO_WEAK = 0x7  # Tier B: r_med_photogeo > 0 but fails quality
DIST_SRC_PHOTO_MG_AG = 0x8  # Tier C: photometric (M_G + A_G)
DIST_SRC_PRIOR = 0x9  # Tier D: fixed M_G prior
DIST_SRC_OVERRIDE = 0xA  # Manual override / curated row (merger precedence; not used for Gaia-vs-Hip scoring)
DIST_SRC_HIP_EDSD = 0xB  # Hipparcos parallax with an EDSD Bayesian distance prior
DIST_SRC_MASK = 0x000F

# bits 4-6: teff_src
TEFF_SRC_DEFAULT = 0x0  # fallback 5800 K
TEFF_SRC_ESPHS = 0x1
TEFF_SRC_GSPSPEC = 0x2
TEFF_SRC_ESPUCD = 0x3
TEFF_SRC_GSPPHOT = 0x4
TEFF_SRC_BPRP = 0x5
TEFF_SRC_BV = 0x6
TEFF_SRC_SHIFT = 4
TEFF_SRC_MASK = 0x0070

# status bits (bits 7-9)
FLAG_DIST_VALID = 0x0080  # bit 7: has usable (finite positive) distance
FLAG_NEEDS_REVIEW = 0x0100  # bit 8: fallback tier (B/C/D), not trusted primary
FLAG_DIST_PLAUSIBLE = 0x0200  # bit 9

# bits 10-11: phot_src
PHOT_SRC_UNKNOWN = 0x0
PHOT_SRC_GAIA_G = 0x1
PHOT_SRC_HIP_HP = 0x2
PHOT_SRC_GAIA_GSPPHOT = 0x3
PHOT_SRC_SHIFT = 10
PHOT_SRC_MASK = 0x0C00


def qf_dist_src(flags):
    """Extract dist_src (bits 0-3) from quality_flags."""
    return flags & DIST_SRC_MASK


def qf_teff_src(flags):
    """Extract teff_src (bits 4-6) from quality_flags."""
    return (flags & TEFF_SRC_MASK) >> TEFF_SRC_SHIFT


def qf_phot_src(flags):
    """Extract phot_src (bits 10-11) from quality_flags."""
    return (flags & PHOT_SRC_MASK) >> PHOT_SRC_SHIFT


def qf_dist_valid(flags):
    """True if row has a usable distance (finite positive distance_use_pc; any tier)."""
    return (flags & FLAG_DIST_VALID) != 0


def qf_needs_review(flags):
    """True if any non-primary fallback was used."""
    return (flags & FLAG_NEEDS_REVIEW) != 0


def qf_dist_plausible(flags):
    """True if distance is in sanity bounds (e.g. 0.1–200k pc)."""
    return (flags & FLAG_DIST_PLAUSIBLE) != 0


OUTPUT_COLS = [
    "source",
    "source_id",
    "x_icrs_pc",
    "y_icrs_pc",
    "z_icrs_pc",
    "ra_deg",
    "dec_deg",
    "r_pc",
    "mag_abs",
    "teff",
    "quality_flags",
    "astrometry_quality",
    "photometry_quality",
]
