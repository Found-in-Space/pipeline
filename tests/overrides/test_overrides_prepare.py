"""Tests for overrides YAML → Parquet pipeline."""

from pathlib import Path

import pyarrow.parquet as pq

from foundinspace.pipeline.constants import (
    DIST_SRC_OVERRIDE,
    FLAG_DIST_VALID,
    OUTPUT_COLS,
)
from foundinspace.pipeline.overrides.pipeline import (
    OUTPUT_OVERRIDES_COLS,
    build_overrides_dataframe,
    prepare_overrides_parquet,
)


def test_build_overrides_dataframe_has_expected_columns_and_sun_row():
    df = build_overrides_dataframe(data_dir=None)
    assert list(df.columns) == OUTPUT_OVERRIDES_COLS
    assert len(df) >= 1
    sun = df.loc[df["override_id"] == "manual.sun.add.v1"]
    assert len(sun) == 1
    row = sun.iloc[0]
    assert row["action"] == "add"
    assert row["source"] == "manual"
    assert row["source_id"] == "sun"
    assert row["x_icrs_pc"] == 0.0
    assert row["ra_deg"] == 0.0
    assert row["dec_deg"] == 0.0
    assert row["r_pc"] == 0.0
    expected_qf = DIST_SRC_OVERRIDE | FLAG_DIST_VALID
    assert int(row["quality_flags"]) == expected_qf
    assert row["astrometry_quality"] == 0.0
    assert row["photometry_quality"] == 0.0


def test_prepare_overrides_parquet_roundtrip(tmp_path: Path):
    out = tmp_path / "overrides.parquet"
    path = prepare_overrides_parquet(out, data_dir=None, overwrite=True)
    assert path == out
    table = pq.read_table(path)
    names = table.column_names
    assert names == OUTPUT_OVERRIDES_COLS
    pdf = table.to_pandas()
    assert len(pdf) >= 1
    for col in OUTPUT_COLS:
        assert col in pdf.columns
