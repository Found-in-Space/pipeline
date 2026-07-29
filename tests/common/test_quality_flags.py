import numpy as np

from foundinspace.pipeline.common.quality_flags import (
    add_photometry_source,
    add_teff_source,
    combine_quality_flags,
    pack_distance_flags,
    pack_status_flags,
)
from foundinspace.pipeline.constants import (
    DIST_SRC_HIP,
    DIST_SRC_OVERRIDE,
    PHOT_SRC_GAIA_G,
    PHOT_SRC_HIP_HP,
    TEFF_SRC_BV,
    TEFF_SRC_GSPPHOT,
    qf_dist_plausible,
    qf_dist_src,
    qf_dist_valid,
    qf_needs_review,
    qf_phot_src,
    qf_teff_src,
)


def test_pack_distance_flags_preserves_extraction_helpers():
    flags = pack_distance_flags(
        np.array([DIST_SRC_HIP, DIST_SRC_HIP], dtype=np.uint16),
        distance_pc=np.array([100.0, np.nan]),
        needs_review=np.array([False, True]),
    )

    assert qf_dist_src(flags[0]) == DIST_SRC_HIP
    assert qf_dist_valid(flags[0])
    assert qf_dist_plausible(flags[0])
    assert not qf_needs_review(flags[0])
    assert not qf_dist_valid(flags[1])
    assert qf_needs_review(flags[1])


def test_pack_status_and_add_source_bits():
    flags = pack_status_flags(DIST_SRC_OVERRIDE, distance_valid=True)
    flags = add_photometry_source(flags, PHOT_SRC_HIP_HP)
    flags = add_teff_source(flags, TEFF_SRC_BV)

    assert qf_dist_src(flags) == DIST_SRC_OVERRIDE
    assert qf_dist_valid(flags)
    assert qf_phot_src(flags) == PHOT_SRC_HIP_HP
    assert qf_teff_src(flags) == TEFF_SRC_BV


def test_source_setters_replace_existing_fields():
    flags = pack_status_flags(DIST_SRC_OVERRIDE, distance_valid=True)
    flags = add_photometry_source(flags, PHOT_SRC_HIP_HP)
    flags = add_photometry_source(flags, PHOT_SRC_GAIA_G)
    flags = add_teff_source(flags, TEFF_SRC_BV)
    flags = add_teff_source(flags, TEFF_SRC_GSPPHOT)

    assert qf_phot_src(flags) == PHOT_SRC_GAIA_G
    assert qf_teff_src(flags) == TEFF_SRC_GSPPHOT


def test_combine_quality_flags_uses_independent_field_donors():
    astrometry = pack_status_flags(DIST_SRC_HIP, distance_valid=True)
    photometry = add_photometry_source(0, PHOT_SRC_GAIA_G)
    teff = add_teff_source(0, TEFF_SRC_GSPPHOT)

    flags = combine_quality_flags(astrometry, photometry, teff)

    assert qf_dist_src(flags) == DIST_SRC_HIP
    assert qf_dist_valid(flags)
    assert qf_phot_src(flags) == PHOT_SRC_GAIA_G
    assert qf_teff_src(flags) == TEFF_SRC_GSPPHOT
