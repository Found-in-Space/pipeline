from pathlib import Path

import pandas as pd
import pytest
from tools.curation.distance_resolution_v1.promotion import (
    DEFAULT_RESOLVED_OVERRIDE_PATH,
    build_resolved_distance_override_document,
    validate_resolved_override_against_tracker,
)
from tools.curation.distance_resolution_v1.research import (
    DEFAULT_TRACKER_PATH,
    load_distance_tracker,
)

from foundinspace.pipeline.overrides.pipeline import build_overrides_dataframe


def _write_tracker(path: Path) -> None:
    pd.DataFrame.from_records(
        [
            {
                "status": "resolved",
                "official_source": "hip",
                "official_source_id": "42",
                "gaia_source_id": "123456789",
                "hip_source_id": "42",
                "proper_name": "Example",
                "simbad_main_id": "HD 42",
                "current_distance_source": "HIP_EDSD",
                "reason_for_external_search": "bad_fallback",
                "next_action": "review_and_promote_to_versioned_override",
                "resolved_distance_pc": "200",
                "resolved_distance_error_pc": "10",
                "resolved_reference": "Example et al. 2026",
                "resolution_notes": "Reviewed example.",
            },
            {
                "status": "provisional",
                "official_source": "gaia",
                "official_source_id": "987654321",
                "gaia_source_id": "987654321",
                "hip_source_id": "",
                "proper_name": "",
                "simbad_main_id": "HD 99",
                "current_distance_source": "GEO_WEAK",
                "reason_for_external_search": "component_ambiguity",
                "next_action": "seek_better_object_specific_measurement_before_promotion",
                "resolved_distance_pc": "300",
                "resolved_distance_error_pc": "50",
                "resolved_reference": "Example et al. 2025",
                "resolution_notes": "Provisional example.",
            },
        ]
    ).to_csv(path, index=False)


def _write_preflight(preflight_dir: Path) -> None:
    preflight_dir.mkdir()
    keys = pd.DataFrame.from_records(
        [
            {
                "official_source": "hip",
                "official_source_id": "42",
                "gaia_source_id": 123456789,
                "hip_source_id": 42,
                "astrometry_donor": "gaia",
                "photometry_donor": "hip",
                "teff_donor": "gaia",
                "candidate_apparent_mag_reconstructed": 5.0,
            },
            {
                "official_source": "gaia",
                "official_source_id": "987654321",
                "gaia_source_id": 987654321,
                "hip_source_id": pd.NA,
                "astrometry_donor": "gaia",
                "photometry_donor": "gaia",
                "teff_donor": "gaia",
                "candidate_apparent_mag_reconstructed": 6.0,
            },
        ]
    )
    keys.to_parquet(preflight_dir / "current-pipeline-candidates.parquet")
    keys[["official_source", "official_source_id"]].to_parquet(
        preflight_dir / "external-distance-required.parquet"
    )
    pd.DataFrame.from_records(
        [
            {
                "source_id": "123456789",
                "ra_deg": 10.0,
                "dec_deg": -20.0,
                "r_pc": 100.0,
                "mag_abs": 0.0,
                "teff": 7000.0,
            },
            {
                "source_id": "987654321",
                "ra_deg": 20.0,
                "dec_deg": 30.0,
                "r_pc": 100.0,
                "mag_abs": 1.0,
                "teff": 6000.0,
            },
        ]
    ).to_parquet(preflight_dir / "gaia-staged.parquet")
    pd.DataFrame.from_records(
        [
            {
                "source_id": "42",
                "ra_deg": 11.0,
                "dec_deg": -21.0,
                "r_pc": 100.0,
                "mag_abs": 0.0,
                "teff": 6500.0,
            }
        ]
    ).to_parquet(preflight_dir / "hip-staged.parquet")


def test_builder_selects_only_resolved_and_preserves_donor_payload(tmp_path: Path):
    tracker_path = tmp_path / "tracker.csv"
    preflight_dir = tmp_path / "preflight"
    _write_tracker(tracker_path)
    _write_preflight(preflight_dir)

    document, report = build_resolved_distance_override_document(
        tracker_path=tracker_path,
        preflight_dir=preflight_dir,
    )

    assert report["tracker_rows"] == 2
    assert report["resolved_tracker_rows"] == 1
    assert report["provisional_tracker_rows"] == 1
    assert report["yaml_rows"] == 1
    star = document["stars"][0]
    assert star["source"] == "hip"
    assert star["source_id"] == "42"
    assert star["r_pc"] == 200.0
    assert star["ra_deg"] == 10.0
    assert star["dec_deg"] == -20.0
    assert star["mag_abs"] == pytest.approx(-1.5051499783199063)
    assert star["teff"] == 7000.0
    assert star["provenance"]["preserved_payload"]["photometry_donor"] == "hip:42"


def test_checked_in_resolved_yaml_matches_all_resolved_tracker_rows():
    tracker = load_distance_tracker(DEFAULT_TRACKER_PATH)
    report = validate_resolved_override_against_tracker()
    overrides = build_overrides_dataframe(
        source_paths=(DEFAULT_RESOLVED_OVERRIDE_PATH,)
    )

    assert report["tracker_rows"] == 81
    assert report["resolved_tracker_rows"] == 48
    assert report["provisional_tracker_rows"] == 33
    assert report["yaml_rows"] == 48
    assert len(overrides) == 48
    assert set(overrides["action"]) == {"replace"}
    assert set(overrides["source"] + ":" + overrides["source_id"]) == set(
        tracker.loc[tracker["status"].eq("resolved"), "tracker_key"]
    )
