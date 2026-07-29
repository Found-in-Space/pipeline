"""Tests for overrides YAML → Parquet pipeline."""

from pathlib import Path

import pyarrow.parquet as pq

from foundinspace.pipeline.constants import OUTPUT_COLS
from foundinspace.pipeline.overrides.pipeline import (
    OUTPUT_OVERRIDES_COLS,
    build_overrides_dataframe,
    prepare_overrides_parquet,
)


def test_build_overrides_dataframe_has_expected_columns_and_no_default_rows():
    df = build_overrides_dataframe(data_dir=None)
    assert list(df.columns) == OUTPUT_OVERRIDES_COLS
    assert df.empty


def test_prepare_overrides_parquet_roundtrip(tmp_path: Path):
    out = tmp_path / "overrides.parquet"
    path = prepare_overrides_parquet(out, data_dir=None, overwrite=True)
    assert path == out
    table = pq.read_table(path)
    names = table.column_names
    assert names == OUTPUT_OVERRIDES_COLS
    pdf = table.to_pandas()
    assert pdf.empty
    for col in OUTPUT_COLS:
        assert col in pdf.columns


def test_build_overrides_dataframe_uses_explicit_source_paths_only(tmp_path: Path):
    source = tmp_path / "release-overrides.yaml"
    source.write_text(
        (
            "stars:\n"
            "  - override_id: release.test.drop.v1\n"
            "    action: drop\n"
            "    source: hip\n"
            "    source_id: 123\n"
            "    override_reason: test\n"
            "    override_policy_version: test\n"
        ),
        encoding="utf-8",
    )

    df = build_overrides_dataframe(source_paths=(source,))

    assert df["override_id"].tolist() == ["release.test.drop.v1"]
    assert df["source"].tolist() == ["hip"]
    assert df["source_id"].tolist() == ["123"]
