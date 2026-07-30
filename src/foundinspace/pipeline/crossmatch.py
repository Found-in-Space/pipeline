"""Supported Gaia–Hipparcos crossmatch contracts."""

from foundinspace.pipeline.gaia_to_hip.pipeline import (
    GAIA_HIP_MAP_COLS,
    MAPPING_SOURCE_HIPPARCOS2_BEST_NEIGHBOUR,
    build_gaia_hip_mapping_from_dataframe,
    empty_gaia_hip_mapping,
    normalize_gaia_hip_mapping_dataframe,
    prepare_gaia_hip_mapping,
)

__all__ = [
    "GAIA_HIP_MAP_COLS",
    "MAPPING_SOURCE_HIPPARCOS2_BEST_NEIGHBOUR",
    "build_gaia_hip_mapping_from_dataframe",
    "empty_gaia_hip_mapping",
    "normalize_gaia_hip_mapping_dataframe",
    "prepare_gaia_hip_mapping",
]
