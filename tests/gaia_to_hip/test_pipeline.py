"""Tests for Gaia↔HIP cross-match pipeline."""

from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from astropy.table import Table

from foundinspace.pipeline.gaia_to_hip.pipeline import (
    GAIA_HIP_MAP_COLS,
    MAPPING_SOURCE_HIPPARCOS2_BEST_NEIGHBOUR,
    build_gaia_hip_mapping_from_dataframe,
    prepare_gaia_hip_mapping,
    write_gaia_hip_mapping,
)


def test_build_gaia_hip_mapping_from_dataframe_filters_and_normalizes():
    df = pd.DataFrame(
        {
            "source_id": [1, "2", 3, 4, 5],
            "original_ext_source_id": [10, np.nan, -1, 14.5, "7"],
        }
    )

    out = build_gaia_hip_mapping_from_dataframe(df)

    assert len(out) == 2
    assert out["gaia_source_id"].tolist() == [1, 5]
    assert out["hip_source_id"].tolist() == [10, 7]
    assert (out["mapping_source"] == MAPPING_SOURCE_HIPPARCOS2_BEST_NEIGHBOUR).all()
    assert list(out.columns) == GAIA_HIP_MAP_COLS


def test_build_gaia_hip_mapping_missing_columns_empty():
    df = pd.DataFrame({"source_id": [1, 2, 3]})
    out = build_gaia_hip_mapping_from_dataframe(df)
    assert out.empty
    assert list(out.columns) == GAIA_HIP_MAP_COLS


def test_build_gaia_hip_mapping_deduplicates_and_sorts():
    df = pd.DataFrame(
        {
            "source_id": [5, 2, 2, 1],
            "original_ext_source_id": [50, 20, 20, 10],
        }
    )
    out = build_gaia_hip_mapping_from_dataframe(df)
    assert out[["gaia_source_id", "hip_source_id"]].values.tolist() == [
        [1, 10],
        [2, 20],
        [5, 50],
    ]


def test_prepare_gaia_hip_mapping_writes_parquet(tmp_path: Path):
    ecsv = tmp_path / "cross.ecsv"
    out_parquet = tmp_path / "map.parquet"
    t = Table(
        rows=[[100, 42, 0.005, 1], [200, 99, 0.010, 2]],
        names=(
            "source_id",
            "original_ext_source_id",
            "angular_distance",
            "number_of_neighbours",
        ),
    )
    t.write(ecsv, format="ascii.ecsv", overwrite=True)

    prepare_gaia_hip_mapping(ecsv, out_parquet, overwrite=True)

    assert out_parquet.exists()
    read_back = pd.read_parquet(out_parquet)
    assert list(read_back.columns) == GAIA_HIP_MAP_COLS
    assert read_back["gaia_source_id"].tolist() == [100, 200]
    assert read_back["hip_source_id"].tolist() == [42, 99]
    assert read_back["number_of_neighbours"].tolist() == [1, 2]
    assert read_back["angular_distance"].tolist() == pytest.approx(
        [0.005, 0.010], abs=1e-6
    )


def test_prepare_gaia_hip_mapping_appends_explicit_supplemental_parquets(
    tmp_path: Path,
):
    ecsv = tmp_path / "cross.ecsv"
    supplemental = tmp_path / "supplemental.parquet"
    out_parquet = tmp_path / "map.parquet"
    t = Table(
        rows=[[100, 42, 0.005, 1]],
        names=(
            "source_id",
            "original_ext_source_id",
            "angular_distance",
            "number_of_neighbours",
        ),
    )
    t.write(ecsv, format="ascii.ecsv", overwrite=True)
    write_gaia_hip_mapping(
        pd.DataFrame(
            {
                "gaia_source_id": [200],
                "hip_source_id": [99],
                "mapping_source": ["fis_raw_sky_render_v1"],
                "number_of_neighbours": [1],
                "angular_distance": [0.010],
            }
        ),
        supplemental,
    )

    prepare_gaia_hip_mapping(
        ecsv,
        out_parquet,
        supplemental_parquets=(supplemental,),
        overwrite=True,
    )

    read_back = pd.read_parquet(out_parquet)
    assert read_back[["gaia_source_id", "hip_source_id"]].values.tolist() == [
        [100, 42],
        [200, 99],
    ]
    assert read_back["mapping_source"].tolist() == [
        MAPPING_SOURCE_HIPPARCOS2_BEST_NEIGHBOUR,
        "fis_raw_sky_render_v1",
    ]


def test_prepare_gaia_hip_mapping_rejects_supplemental_one_to_many_conflict(
    tmp_path: Path,
):
    ecsv = tmp_path / "cross.ecsv"
    supplemental = tmp_path / "supplemental.parquet"
    out_parquet = tmp_path / "map.parquet"
    t = Table(rows=[[100, 42]], names=("source_id", "original_ext_source_id"))
    t.write(ecsv, format="ascii.ecsv", overwrite=True)
    write_gaia_hip_mapping(
        pd.DataFrame(
            {
                "gaia_source_id": [100],
                "hip_source_id": [99],
                "mapping_source": ["fis_raw_sky_render_v1"],
                "number_of_neighbours": [1],
                "angular_distance": [0.010],
            }
        ),
        supplemental,
    )

    with pytest.raises(ValueError, match="not one-to-one"):
        prepare_gaia_hip_mapping(
            ecsv,
            out_parquet,
            supplemental_parquets=(supplemental,),
            overwrite=True,
        )


def test_prepare_gaia_hip_mapping_raises_when_output_exists(tmp_path: Path):
    ecsv = tmp_path / "cross.ecsv"
    out_parquet = tmp_path / "map.parquet"
    out_parquet.write_text("x", encoding="utf-8")
    t = Table(rows=[[1, 1]], names=("source_id", "original_ext_source_id"))
    t.write(ecsv, format="ascii.ecsv", overwrite=True)

    with pytest.raises(FileExistsError):
        prepare_gaia_hip_mapping(ecsv, out_parquet, overwrite=False)
