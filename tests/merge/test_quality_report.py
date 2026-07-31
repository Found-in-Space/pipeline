from __future__ import annotations

import json
import math
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from foundinspace.pipeline.constants import OUTPUT_COLS
from foundinspace.pipeline.merge.pipeline import run_merge
from foundinspace.pipeline.merge.quality_report import run_quality_report


def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pandas(df, preserve_index=False),
        str(path),
        compression="zstd",
    )


def _row(
    *,
    source: str,
    source_id,
    r_pc: float,
    mag_abs: float,
    astrometry_quality: float,
    photometry_quality: float = 0.1,
    teff: float = 5000.0,
) -> dict:
    return {
        "source": source,
        "source_id": source_id,
        "x_icrs_pc": r_pc,
        "y_icrs_pc": 0.0,
        "z_icrs_pc": 0.0,
        "ra_deg": 0.0,
        "dec_deg": 0.0,
        "r_pc": r_pc,
        "mag_abs": mag_abs,
        "teff": teff,
        "quality_flags": 1,
        "astrometry_quality": astrometry_quality,
        "photometry_quality": photometry_quality,
    }


def test_quality_report_flags_suspicious_non_overridden_rows(tmp_path: Path):
    gaia_dir = tmp_path / "gaia"
    hip_path = tmp_path / "hip.parquet"
    crossmatch_path = tmp_path / "gaia_hip.parquet"
    overrides_path = tmp_path / "overrides.parquet"
    identifiers_path = tmp_path / "identifiers.parquet"
    merge_dir = tmp_path / "merged"

    gaia_df = pd.DataFrame(
        [
            {
                **_row(
                    source="gaia",
                    source_id=101,
                    r_pc=300.0,
                    mag_abs=-1.0,
                    astrometry_quality=0.4,
                ),
                "ruwe": 10.0,
                "phot_g_mean_mag": 5.0,
            },
            {
                **_row(
                    source="gaia",
                    source_id=102,
                    r_pc=100.0,
                    mag_abs=1.0,
                    astrometry_quality=0.05,
                ),
                "ruwe": 1.0,
                "phot_g_mean_mag": 8.0,
            },
        ]
    )
    _write_parquet(gaia_df, gaia_dir / "b1.parquet")

    hip_df = pd.DataFrame(
        [
            {
                **_row(
                    source="hip",
                    source_id=201,
                    r_pc=100.0,
                    mag_abs=1.0,
                    astrometry_quality=0.2,
                ),
                "Sn": 55,
                "Hpmag": 5.2,
            },
            {
                **_row(
                    source="hip",
                    source_id=202,
                    r_pc=95.0,
                    mag_abs=1.2,
                    astrometry_quality=0.2,
                ),
                "Sn": 5,
                "Hpmag": 8.2,
            },
        ]
    )
    _write_parquet(hip_df, hip_path)
    _write_parquet(
        pd.DataFrame(
            [
                {"gaia_source_id": 101, "hip_source_id": 201},
                {"gaia_source_id": 102, "hip_source_id": 202},
            ]
        ),
        crossmatch_path,
    )

    decisions_df = pd.DataFrame(
        [
            {
                "decision_type": "score",
                "gaia_source_id": "101",
                "hip_source_id": "201",
                "winner_catalog": "gaia",
                "winner_source_id": "101",
                "gaia_score": 0.4,
                "hip_score": 0.2,
                "tie_break_reason": "hip_multiplicity",
                "number_of_neighbours": 1,
                "angular_distance_arcsec": 0.01,
                "gaia_ruwe": 10.0,
                "gaia_phot_g_mean_mag": 5.0,
                "hip_solution_type": 55,
                "hip_apparent_mag": 5.2,
            },
            {
                "decision_type": "score",
                "gaia_source_id": "102",
                "hip_source_id": "202",
                "winner_catalog": "gaia",
                "winner_source_id": "102",
                "gaia_score": 0.05,
                "hip_score": 0.2,
                "tie_break_reason": pd.NA,
                "number_of_neighbours": 1,
                "angular_distance_arcsec": 0.01,
                "gaia_ruwe": 1.0,
                "gaia_phot_g_mean_mag": 8.0,
                "hip_solution_type": 5,
                "hip_apparent_mag": 8.2,
            },
            {
                "decision_type": "override",
                "gaia_source_id": pd.NA,
                "hip_source_id": "302",
                "winner_catalog": "manual",
                "winner_source_id": "302",
            },
        ]
    )
    _write_parquet(decisions_df, merge_dir / "merge_decisions.parquet")
    _write_parquet(
        pd.DataFrame(
            [
                {
                    **_row(
                        source="hip",
                        source_id="302",
                        r_pc=100_000.0,
                        mag_abs=-12.0,
                        astrometry_quality=50.0,
                    ),
                    "override_id": "manual.hip302.replace",
                    "action": "replace",
                    "override_reason": "test",
                    "override_policy_version": "v1",
                }
            ]
        ),
        overrides_path,
    )
    _write_parquet(
        pd.DataFrame(
            [
                {"source": "hip", "source_id": "201", "proper_name": "Test Pair"},
                {"source": "hip", "source_id": "301", "proper_name": "Extreme Star"},
            ]
        ),
        identifiers_path,
    )
    _write_parquet(
        pd.DataFrame(
            [
                _row(
                    source="hip",
                    source_id="201",
                    r_pc=300.0,
                    mag_abs=-1.0,
                    astrometry_quality=0.4,
                ),
                _row(
                    source="hip",
                    source_id="301",
                    r_pc=100_000.0,
                    mag_abs=-12.0,
                    astrometry_quality=50.0,
                ),
                _row(
                    source="hip",
                    source_id="302",
                    r_pc=100_000.0,
                    mag_abs=-12.0,
                    astrometry_quality=50.0,
                ),
            ],
            columns=OUTPUT_COLS,
        ),
        merge_dir / "healpix" / "0" / "part.parquet",
    )

    report = run_quality_report(
        gaia_dir=gaia_dir,
        hip_path=hip_path,
        crossmatch_path=crossmatch_path,
        overrides_path=overrides_path,
        merge_dir=merge_dir,
        identifiers_path=identifiers_path,
        force=True,
    )

    assert report.matched_pair_issues == 1
    assert report.merged_row_issues == 1
    assert report.total_issues == 2

    issues = pd.read_parquet(merge_dir / "merge_quality_issues.parquet")
    assert set(issues["issue_type"]) == {
        "matched_pair_conflict",
        "merged_row_extreme",
    }
    assert "302" not in set(issues["source_id"].astype(str))
    assert set(issues["label"]) == {"Test Pair", "Extreme Star"}
    extreme = issues.loc[issues["issue_type"] == "merged_row_extreme"].iloc[0]
    assert extreme["merged_ra_deg"] == 0.0
    assert extreme["merged_dec_deg"] == 0.0
    matched = issues.loc[issues["issue_type"] == "matched_pair_conflict"].iloc[0]
    assert matched["spatial_separation_pc"] == 200.0
    assert matched["gaia_apparent_mag"] == pytest.approx(
        -1.0 + 5.0 * math.log10(300.0 / 10.0)
    )
    assert matched["hip_apparent_mag"] == pytest.approx(6.0)
    assert matched["apparent_mag_delta"] == pytest.approx(
        matched["gaia_apparent_mag"] - matched["hip_apparent_mag"]
    )

    report_json = json.loads(
        (merge_dir / "merge_quality_report.json").read_text(encoding="utf-8")
    )
    assert report_json["total_issues"] == 2


def test_merge_can_capture_equivalent_quality_report_inline_with_bounded_buffer(
    tmp_path: Path,
):
    gaia_dir = tmp_path / "gaia"
    hip_path = tmp_path / "hip.parquet"
    crossmatch_path = tmp_path / "gaia_hip.parquet"
    overrides_path = tmp_path / "overrides.parquet"
    merge_dir = tmp_path / "merged"

    gaia_df = pd.DataFrame(
        [
            {
                **_row(
                    source="gaia",
                    source_id=101,
                    r_pc=300.0,
                    mag_abs=-1.0,
                    astrometry_quality=0.4,
                ),
                "ruwe": 10.0,
                "phot_g_mean_mag": 5.0,
            },
            {
                **_row(
                    source="gaia",
                    source_id=301,
                    r_pc=100_000.0,
                    mag_abs=-12.0,
                    astrometry_quality=50.0,
                ),
                "ruwe": 1.0,
                "phot_g_mean_mag": 8.0,
            },
            {
                **_row(
                    source="gaia",
                    source_id=302,
                    r_pc=10.0,
                    mag_abs=2.0,
                    astrometry_quality=0.1,
                ),
                "ruwe": 1.0,
                "phot_g_mean_mag": 8.0,
            },
        ]
    )
    _write_parquet(gaia_df, gaia_dir / "b1.parquet")
    hip_df = pd.DataFrame(
        [
            {
                **_row(
                    source="hip",
                    source_id=201,
                    r_pc=100.0,
                    mag_abs=1.0,
                    astrometry_quality=0.2,
                ),
                "Sn": 55,
                "Hpmag": 5.2,
            }
        ]
    )
    _write_parquet(hip_df, hip_path)
    _write_parquet(
        pd.DataFrame([{"gaia_source_id": 101, "hip_source_id": 201}]),
        crossmatch_path,
    )
    override = {
        **_row(
            source="gaia",
            source_id="302",
            r_pc=100_000.0,
            mag_abs=-12.0,
            astrometry_quality=50.0,
        ),
        "override_id": "manual.gaia302.replace",
        "action": "replace",
        "override_reason": "test",
        "override_policy_version": "v1",
    }
    _write_parquet(pd.DataFrame([override]), overrides_path)

    run_merge(
        gaia_dir=gaia_dir,
        hip_path=hip_path,
        crossmatch_path=crossmatch_path,
        overrides_path=overrides_path,
        output_dir=merge_dir,
        healpix_order=1,
        quality_report=True,
        quality_issue_buffer_rows=1,
        force=True,
    )

    inline_report = json.loads(
        (merge_dir / "merge_quality_report.json").read_text(encoding="utf-8")
    )
    assert inline_report["execution_mode"] == "inline"
    assert inline_report["matched_pair_issues"] == 1
    assert inline_report["merged_row_issues"] == 1
    assert inline_report["total_issues"] == 2
    assert inline_report["max_buffered_issues"] == 1

    inline_issues = pd.read_parquet(
        merge_dir / "merge_quality_issues.parquet"
    ).sort_values(["issue_type", "source_id"])
    assert set(inline_issues["source_id"].astype(str)) == {"201", "301"}
    assert "302" not in set(inline_issues["source_id"].astype(str))

    run_quality_report(
        gaia_dir=gaia_dir,
        hip_path=hip_path,
        crossmatch_path=crossmatch_path,
        overrides_path=overrides_path,
        merge_dir=merge_dir,
        force=True,
    )
    rescanned_issues = pd.read_parquet(
        merge_dir / "merge_quality_issues.parquet"
    ).sort_values(["issue_type", "source_id"])
    comparison_columns = [
        "issue_type",
        "severity",
        "reasons",
        "source",
        "source_id",
        "gaia_source_id",
        "hip_source_id",
        "winner_catalog",
        "winner_source_id",
        "distance_ratio",
        "distance_frac_diff",
        "spatial_separation_pc",
        "merged_r_pc",
        "merged_mag_abs",
    ]
    pd.testing.assert_frame_equal(
        inline_issues[comparison_columns].reset_index(drop=True),
        rescanned_issues[comparison_columns].reset_index(drop=True),
        check_dtype=False,
    )
