"""Tests for Gaia pipeline batching."""

import numpy as np
import pandas as pd

from foundinspace.pipeline.constants import OUTPUT_COLS
from foundinspace.pipeline.gaia import pipeline as gaia_pipeline
from foundinspace.pipeline.gaia.download.fieldsets import GaiaCarryField
from foundinspace.pipeline.gaia.pipeline import GAIA_AUXILIARY_COLS, GAIA_OUTPUT_COLS


def test_run_pipeline_batch_emits_output_cols_plus_auxiliary(monkeypatch):
    def _select(df: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({"source_id": [1, 2]})

    def _coords(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out["x_icrs_pc"] = [0.0, 1.0]
        out["y_icrs_pc"] = [0.0, 1.0]
        out["z_icrs_pc"] = [0.0, 1.0]
        out["ra_deg"] = [0.0, 45.0]
        out["dec_deg"] = [0.0, 0.0]
        out["r_pc"] = [0.0, 1.41421356]
        return out

    def _mag_abs(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out["mag_abs"] = [1.0, 2.0]
        out["quality_flags"] = np.array([0, 0], dtype=np.uint16)
        out["astrometry_quality"] = [0.1, 0.2]
        out["photometry_quality"] = [0.3, 0.4]
        return out

    def _teff(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out["teff"] = [5800.0, 6000.0]
        return out

    monkeypatch.setattr(gaia_pipeline, "select_astrometry_gaia", _select)
    monkeypatch.setattr(gaia_pipeline, "assign_photometry_gaia", lambda d: d)
    monkeypatch.setattr(gaia_pipeline, "calculate_coordinates_fast", _coords)
    monkeypatch.setattr(gaia_pipeline, "compute_mag_abs_gaia", _mag_abs)
    monkeypatch.setattr(gaia_pipeline, "compute_teff_gaia", _teff)

    out = gaia_pipeline._run_gaia_pipeline_batch(pd.DataFrame({"source_id": [1, 2]}))

    assert list(out.columns) == GAIA_OUTPUT_COLS
    for col in OUTPUT_COLS:
        assert col in out.columns
    for col in GAIA_AUXILIARY_COLS:
        assert col in out.columns
    assert (out["source"] == "gaia").all()
    assert out["source_id"].dtype == "uint64"


def test_run_pipeline_batch_preserves_configured_gaia_enrichment(monkeypatch):
    def _select(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out["distance_use_pc"] = [10.0, 20.0]
        return out

    def _coords(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out["x_icrs_pc"] = [0.0, 1.0]
        out["y_icrs_pc"] = [0.0, 1.0]
        out["z_icrs_pc"] = [0.0, 1.0]
        out["ra_deg"] = [0.0, 45.0]
        out["dec_deg"] = [0.0, 0.0]
        out["r_pc"] = [0.0, 1.41421356]
        return out

    def _mag_abs(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out["mag_abs"] = [1.0, 2.0]
        out["quality_flags"] = np.array([0, 0], dtype=np.uint16)
        out["astrometry_quality"] = [0.1, 0.2]
        out["photometry_quality"] = [0.3, 0.4]
        return out

    def _teff(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out["teff"] = [5800.0, 6000.0]
        return out

    monkeypatch.setattr(gaia_pipeline, "select_astrometry_gaia", _select)
    monkeypatch.setattr(gaia_pipeline, "assign_photometry_gaia", lambda d: d)
    monkeypatch.setattr(gaia_pipeline, "calculate_coordinates_fast", _coords)
    monkeypatch.setattr(gaia_pipeline, "compute_mag_abs_gaia", _mag_abs)
    monkeypatch.setattr(gaia_pipeline, "compute_teff_gaia", _teff)

    carry_fields = (
        GaiaCarryField(
            name="pmra_masyr",
            dtype="float64",
            sidecar="motion",
            source="input",
            column="pmra",
        ),
        GaiaCarryField(
            name="distance_use_pc",
            dtype="float64",
            sidecar="motion",
            source="stage",
            column="distance_use_pc",
        ),
        GaiaCarryField(
            name="missing_mass",
            dtype="float64",
            sidecar="mass",
            source="input",
            column="mass_flame",
        ),
    )

    out = gaia_pipeline._run_gaia_pipeline_batch(
        pd.DataFrame({"source_id": [1, 2], "pmra": [3.5, 4.5]}),
        carry_fields=carry_fields,
    )

    assert "gaia_pmra_masyr" in out.columns
    assert "gaia_distance_use_pc" in out.columns
    assert "gaia_missing_mass" in out.columns
    assert out["gaia_pmra_masyr"].tolist() == [3.5, 4.5]
    assert out["gaia_distance_use_pc"].tolist() == [10.0, 20.0]
    assert out["gaia_missing_mass"].isna().all()
    assert list(out.columns[: len(GAIA_OUTPUT_COLS)]) == GAIA_OUTPUT_COLS
