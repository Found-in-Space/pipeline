"""Tests for Hipparcos import pipeline."""

from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from astropy.table import Table

from foundinspace.pipeline.constants import OUTPUT_COLS
from foundinspace.pipeline.hipparcos.pipeline import (
    HIP_AUXILIARY_COLS,
    HIP_OUTPUT_COLS,
    _run_hipparcos_pipeline,
    main,
)


def _sample_hip_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "HIP": 1,
                "Sn": 5,
                "RArad": 10.0,
                "DErad": 20.0,
                "Plx": 10.0,
                "e_Plx": 1.0,
                "pmRA": 0.0,
                "pmDE": 0.0,
                "Hpmag": 7.0,
                "B-V": 0.6,
            },
            {
                "HIP": 2,
                "Sn": 5,
                "RArad": 30.0,
                "DErad": -10.0,
                "Plx": 5.0,
                "e_Plx": 0.5,
                "pmRA": 1.0,
                "pmDE": -1.0,
                "Hpmag": 9.0,
                "B-V": 1.0,
            },
        ]
    )


def _sample_hip_df_with_invalid_parallax() -> pd.DataFrame:
    """Two valid stars plus one with Plx <= 0 (must be dropped from pipeline output)."""
    base = _sample_hip_df()
    bad = pd.DataFrame(
        [
            {
                "HIP": 99,
                "RArad": 0.0,
                "DErad": 0.0,
                "Plx": 0.0,
                "e_Plx": 0.5,
                "pmRA": 0.0,
                "pmDE": 0.0,
                "Hpmag": 8.0,
                "B-V": 0.5,
            },
        ]
    )
    return pd.concat([base, bad], ignore_index=True)


def _write_ecsv(df: pd.DataFrame, path: Path) -> None:
    table = Table.from_pandas(df, index=False)
    table.write(path, format="ascii.ecsv", overwrite=True)


def test_run_pipeline_outputs_canonical_schema_and_source_fields():
    out = _run_hipparcos_pipeline(_sample_hip_df())
    assert list(out.columns) == HIP_OUTPUT_COLS
    for col in OUTPUT_COLS:
        assert col in out.columns
    for col in HIP_AUXILIARY_COLS:
        assert col in out.columns
    assert (out["source"] == "hip").all()
    assert out["source_id"].dtype == "uint64"
    assert out["source_id"].tolist() == [1, 2]
    assert out["Sn"].tolist() == [5, 5]
    assert out["Hpmag"].tolist() == [7.0, 9.0]


def test_run_pipeline_drops_invalid_parallax_rows():
    out = _run_hipparcos_pipeline(_sample_hip_df_with_invalid_parallax())
    assert len(out) == 2
    assert out["source_id"].tolist() == [1, 2]
    assert np.isfinite(out["mag_abs"].to_numpy()).all()
    r_pc = np.sqrt(
        out["x_icrs_pc"].astype(float) ** 2
        + out["y_icrs_pc"].astype(float) ** 2
        + out["z_icrs_pc"].astype(float) ** 2
    )
    assert np.isfinite(r_pc).all()


def test_run_pipeline_mag_abs_matches_distance_modulus():
    out = _run_hipparcos_pipeline(_sample_hip_df())
    # HIP 1: Plx=10 mas -> 100 pc; Hpmag=7; DM = 5*log10(10)=5 -> M = 2
    row0 = out.loc[out["source_id"] == 1].iloc[0]
    assert row0["mag_abs"] == pytest.approx(2.0, rel=0, abs=1e-9)
    # HIP 2: Plx=5 -> 200 pc; DM = 5*log10(20)
    row1 = out.loc[out["source_id"] == 2].iloc[0]
    dm = 5.0 * np.log10(200.0 / 10.0)
    assert row1["mag_abs"] == pytest.approx(9.0 - dm, rel=0, abs=1e-9)


def test_main_writes_parquet_with_limit(tmp_path: Path):
    input_file = tmp_path / "hip.ecsv"
    output_file = tmp_path / "hip.parquet"
    _write_ecsv(_sample_hip_df(), input_file)

    main(input_file, output_file, skip_if_exists=False, limit=1)

    assert output_file.exists()
    out = pd.read_parquet(output_file)
    assert len(out) == 1
    for col in OUTPUT_COLS:
        assert col in out.columns
    assert out["source"].iloc[0] == "hip"
    assert out["source_id"].iloc[0] == 1
