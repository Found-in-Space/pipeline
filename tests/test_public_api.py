from foundinspace.pipeline.crossmatch import (
    GAIA_HIP_MAP_COLS,
    empty_gaia_hip_mapping,
)
from foundinspace.pipeline.schema import MERGED_OUTPUT_COLS, OUTPUT_COLS
from foundinspace.pipeline.sharding import build_healpix, write_sidecar_shards
from foundinspace.pipeline.validation import (
    ISSUES_FILENAME,
    QualityReport,
    run_quality_report,
)


def test_supported_consumer_api_exports_current_contracts() -> None:
    assert MERGED_OUTPUT_COLS == OUTPUT_COLS
    assert list(empty_gaia_hip_mapping().columns) == GAIA_HIP_MAP_COLS
    assert ISSUES_FILENAME.endswith(".parquet")
    assert callable(build_healpix)
    assert callable(write_sidecar_shards)
    assert callable(run_quality_report)
    assert QualityReport.__name__ == "QualityReport"
