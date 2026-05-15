from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from foundinspace.pipeline.audit.pipeline import (
    COMBINED_MAP_FILENAME,
    DISTANCE_HISTOGRAM_BINS_FILENAME,
    DISTANCE_QUALITY_SUMMARY_FILENAME,
    DISTANCE_THRESHOLD_SUMMARY_FILENAME,
    LOCAL_CLOSE_PAIR_MAPPING_SOURCE,
    MANUAL_CANDIDATES_CSV_FILENAME,
    MANUAL_CANDIDATES_FILENAME,
    MATCH_EVIDENCE_COLS,
    MATCH_EVIDENCE_FILENAME,
    OCTREE_REVIEW_FILENAME,
    SUPPLEMENTAL_MAP_FILENAME,
    combine_crossmatches,
    run_audit_match,
    run_audit_report,
    validate_one_to_one_crossmatch,
)
from foundinspace.pipeline.constants import OUTPUT_COLS
from foundinspace.pipeline.gaia_to_hip.pipeline import GAIA_HIP_MAP_COLS


def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pandas(df, preserve_index=False), str(path), compression="zstd"
    )


def _row(
    *,
    source: str,
    source_id: int | str,
    ra_deg: float,
    dec_deg: float = 0.0,
    r_pc: float = 100.0,
    mag_abs: float = 4.0,
    astrometry_quality: float = 0.1,
    photometry_quality: float = 0.1,
    teff: float = 5000.0,
) -> dict:
    return {
        "source": source,
        "source_id": source_id,
        "x_icrs_pc": r_pc,
        "y_icrs_pc": 0.0,
        "z_icrs_pc": 0.0,
        "ra_deg": ra_deg,
        "dec_deg": dec_deg,
        "r_pc": r_pc,
        "mag_abs": mag_abs,
        "teff": teff,
        "quality_flags": 1,
        "astrometry_quality": astrometry_quality,
        "photometry_quality": photometry_quality,
    }


def _empty_overrides(path: Path) -> None:
    _write_parquet(
        pd.DataFrame(
            columns=[
                *OUTPUT_COLS,
                "override_id",
                "action",
                "override_reason",
                "override_policy_version",
            ]
        ),
        path,
    )


def test_audit_match_classifies_and_writes_supplemental_maps(tmp_path: Path):
    pytest.importorskip("scipy")

    gaia_dir = tmp_path / "gaia"
    hip_path = tmp_path / "hip.parquet"
    crossmatch_path = tmp_path / "official.parquet"
    overrides_path = tmp_path / "overrides.parquet"
    audit_dir = tmp_path / "audit"

    arcsec = 1.0 / 3600.0
    gaia_rows = [
        {**_row(source="gaia", source_id=100, ra_deg=10.0), "ruwe": 1.1},
        {**_row(source="gaia", source_id=101, ra_deg=11.0), "ruwe": 2.0},
        {**_row(source="gaia", source_id=102, ra_deg=12.0), "ruwe": 1.2},
        {
            **_row(source="gaia", source_id=103, ra_deg=12.0 + 0.02 * arcsec),
            "ruwe": 1.3,
        },
        {**_row(source="gaia", source_id=104, ra_deg=13.0), "ruwe": 1.4},
        {**_row(source="gaia", source_id=105, ra_deg=14.0), "ruwe": 1.5},
        {**_row(source="gaia", source_id=106, ra_deg=16.0), "ruwe": 1.6},
        {**_row(source="gaia", source_id=300, ra_deg=15.0), "ruwe": 1.0},
    ]
    for row in gaia_rows:
        row["phot_g_mean_mag"] = row["mag_abs"] + 5.0
    _write_parquet(pd.DataFrame(gaia_rows), gaia_dir / "g.parquet")

    hip_rows = [
        {
            **_row(source="hip", source_id=200, ra_deg=10.0 + 0.03 * arcsec),
            "Sn": 5,
            "Hpmag": 9.02,
        },
        {
            **_row(source="hip", source_id=201, ra_deg=11.0 + 0.03 * arcsec),
            "Sn": 5,
            "Hpmag": 9.02,
        },
        {
            **_row(source="hip", source_id=202, ra_deg=12.0 + 0.01 * arcsec),
            "Sn": 5,
            "Hpmag": 9.02,
        },
        {
            **_row(source="hip", source_id=203, ra_deg=13.0 + 0.03 * arcsec),
            "Sn": 5,
            "Hpmag": 9.02,
        },
        {
            **_row(
                source="hip", source_id=204, ra_deg=14.0 + 2.0 * arcsec, mag_abs=4.3
            ),
            "Sn": 5,
            "Hpmag": 9.30,
        },
        {
            **_row(source="hip", source_id=205, ra_deg=16.0 + 2.0 * arcsec, r_pc=125.0),
            "Sn": 5,
            "Hpmag": 9.48,
        },
        {
            **_row(source="hip", source_id=400, ra_deg=15.0 + 0.03 * arcsec),
            "Sn": 5,
            "Hpmag": 9.02,
        },
    ]
    _write_parquet(pd.DataFrame(hip_rows), hip_path)
    _write_parquet(
        pd.DataFrame(
            [
                {
                    "gaia_source_id": 999,
                    "hip_source_id": 201,
                    "mapping_source": "official",
                    "number_of_neighbours": 1,
                    "angular_distance": 0.1,
                },
                {
                    "gaia_source_id": 300,
                    "hip_source_id": 400,
                    "mapping_source": "official",
                    "number_of_neighbours": 1,
                    "angular_distance": 0.1,
                },
            ]
        ),
        crossmatch_path,
    )
    override_row = {
        **_row(source="hip", source_id="203", ra_deg=13.0),
        "override_id": "ov.hip203",
        "action": "drop",
        "override_reason": "test",
        "override_policy_version": "v1",
    }
    _write_parquet(pd.DataFrame([override_row]), overrides_path)

    report = run_audit_match(
        gaia_dir=gaia_dir,
        hip_path=hip_path,
        official_crossmatch_path=crossmatch_path,
        overrides_path=overrides_path,
        audit_dir=audit_dir,
        force=True,
    )

    evidence = pd.read_parquet(audit_dir / MATCH_EVIDENCE_FILENAME)
    assert list(evidence.columns) == MATCH_EVIDENCE_COLS
    by_pair = {
        (row.gaia_source_id, row.hip_source_id): row.decision
        for row in evidence.itertuples(index=False)
    }
    assert by_pair[("100", "200")] == "auto_match"
    assert by_pair[("101", "201")] == "manual_review"
    assert by_pair[("102", "202")] == "manual_review"
    assert by_pair[("103", "202")] == "manual_review"
    assert by_pair[("104", "203")] == "manual_review"
    assert by_pair[("105", "204")] == "auto_match"
    assert by_pair[("106", "205")] == "reject"
    assert ("300", "400") not in by_pair

    supplemental = pd.read_parquet(audit_dir / SUPPLEMENTAL_MAP_FILENAME)
    assert supplemental[["gaia_source_id", "hip_source_id"]].values.tolist() == [
        [100, 200],
        [105, 204],
    ]
    assert set(supplemental["mapping_source"]) == {LOCAL_CLOSE_PAIR_MAPPING_SOURCE}

    combined = pd.read_parquet(audit_dir / COMBINED_MAP_FILENAME)
    assert list(combined.columns) == GAIA_HIP_MAP_COLS
    assert len(combined) == 4
    assert report.supplemental_rows == 2
    assert report.decision_counts["manual_review"] == 4
    assert report.decision_counts["reject"] == 1

    threshold_summary = pd.read_csv(audit_dir / DISTANCE_THRESHOLD_SUMMARY_FILENAME)
    by_policy = threshold_summary.set_index("policy")
    assert by_policy.loc["tight or distance <= 10%", "matched_clean_count"] == 2
    assert by_policy.loc["tight or distance <= 25%", "matched_clean_count"] == 3
    assert (audit_dir / DISTANCE_HISTOGRAM_BINS_FILENAME).is_file()
    quality_summary = pd.read_csv(audit_dir / DISTANCE_QUALITY_SUMMARY_FILENAME)
    assert quality_summary["rows"].sum() == 3


def test_validate_combined_crossmatch_rejects_duplicate_ids():
    official = pd.DataFrame(
        [
            {
                "gaia_source_id": 1,
                "hip_source_id": 10,
                "mapping_source": "official",
                "number_of_neighbours": 1,
                "angular_distance": 0.1,
            }
        ]
    )
    supplemental = pd.DataFrame(
        [
            {
                "gaia_source_id": 2,
                "hip_source_id": 10,
                "mapping_source": LOCAL_CLOSE_PAIR_MAPPING_SOURCE,
                "number_of_neighbours": 1,
                "angular_distance": 0.1,
            }
        ]
    )
    combined = combine_crossmatches(official, supplemental)
    with pytest.raises(ValueError, match="duplicate HIP"):
        validate_one_to_one_crossmatch(combined, label="combined")


def test_audit_report_writes_octree_sidecar_and_manual_queue(tmp_path: Path):
    pytest.importorskip("scipy")

    gaia_dir = tmp_path / "gaia"
    hip_path = tmp_path / "hip.parquet"
    crossmatch_path = tmp_path / "official.parquet"
    overrides_path = tmp_path / "overrides.parquet"
    identifiers_path = tmp_path / "identifiers.parquet"
    merge_dir = tmp_path / "merged"
    sidecar_dir = tmp_path / "sidecars"
    audit_dir = merge_dir / "audit"

    _write_parquet(pd.DataFrame(columns=OUTPUT_COLS), gaia_dir / "g.parquet")
    _write_parquet(pd.DataFrame(columns=[*OUTPUT_COLS, "Sn", "Hpmag"]), hip_path)
    _write_parquet(pd.DataFrame(columns=GAIA_HIP_MAP_COLS), crossmatch_path)
    _empty_overrides(overrides_path)
    _write_parquet(
        pd.DataFrame(
            columns=[
                "source",
                "source_id",
                "proper_name",
                "bayer",
                "constellation",
                "hd",
                "hip_id",
            ]
        ),
        identifiers_path,
    )
    _write_parquet(pd.DataFrame(), merge_dir / "merge_decisions.parquet")
    _write_parquet(
        pd.DataFrame(
            [
                _row(
                    source="hip",
                    source_id="90910",
                    ra_deg=100.0,
                    dec_deg=10.0,
                    r_pc=50_000.0,
                    mag_abs=-12.0,
                    astrometry_quality=10.0,
                )
            ],
            columns=OUTPUT_COLS,
        ),
        merge_dir / "healpix" / "0" / "part.parquet",
    )
    evidence = pd.DataFrame(
        [
            {
                **dict.fromkeys(MATCH_EVIDENCE_COLS, pd.NA),
                "gaia_source_id": "100",
                "hip_source_id": "200",
                "decision": "octree_review",
                "recommended_action": "suppress_candidate_duplicate_in_display",
                "severity": "medium",
                "reasons": "close_cross_catalog_position",
                "separation_arcsec": 0.5,
                "apparent_mag_delta": 0.3,
                "hip_ra_deg": 20.0,
                "hip_dec_deg": 5.0,
                "hip_r_pc": 100.0,
                "hip_mag_abs": 4.0,
            }
        ],
        columns=MATCH_EVIDENCE_COLS,
    )
    _write_parquet(evidence, audit_dir / MATCH_EVIDENCE_FILENAME)

    report = run_audit_report(
        gaia_dir=gaia_dir,
        hip_path=hip_path,
        official_crossmatch_path=crossmatch_path,
        overrides_path=overrides_path,
        identifiers_path=identifiers_path,
        merge_dir=merge_dir,
        sidecar_output_dir=sidecar_dir,
        healpix_order=1,
        audit_dir=audit_dir,
        force=True,
    )

    octree = pd.read_parquet(audit_dir / OCTREE_REVIEW_FILENAME)
    manual = pd.read_parquet(audit_dir / MANUAL_CANDIDATES_FILENAME)
    manual_csv = pd.read_csv(audit_dir / MANUAL_CANDIDATES_CSV_FILENAME)
    sharded = sorted((sidecar_dir / "octree_review").glob("*/*.parquet"))

    assert report.octree_review_rows == 2
    assert {row.display_action for row in octree.itertuples(index=False)} == {
        "suppress_candidate_duplicate",
        "quarantine_suspicious_star",
    }
    assert sharded
    assert report.octree_review_sharded_rows == 2
    assert len(manual) == 1
    assert len(manual_csv) == len(manual)
    assert manual["issue_type"].iloc[0] == "merged_row_extreme"
    report_json = json.loads((audit_dir / "audit_report.json").read_text())
    assert report_json["manual_candidate_rows"] == 1
