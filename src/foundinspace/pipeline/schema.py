"""Supported schema constants for pipeline outputs and consumers."""

from foundinspace.pipeline.constants import OUTPUT_COLS
from foundinspace.pipeline.gaia_to_hip.pipeline import GAIA_HIP_MAP_COLS
from foundinspace.pipeline.identifiers.schema import IDENTIFIER_OUTPUT_COLS
from foundinspace.pipeline.merge.decisions import DECISION_COLS
from foundinspace.pipeline.merge.shards import MERGED_OUTPUT_COLS

__all__ = [
    "DECISION_COLS",
    "GAIA_HIP_MAP_COLS",
    "IDENTIFIER_OUTPUT_COLS",
    "MERGED_OUTPUT_COLS",
    "OUTPUT_COLS",
]
