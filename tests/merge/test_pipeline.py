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
from foundinspace.pipeline.merge.policy import (
    _choose_matched_winner,
    _safe_float,
    _safe_int,
    _safe_score,
)


def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), str(path), compression="zstd")


def _with_radec_r_pc(row: dict) -> dict:
    """Fill ra_deg, dec_deg, r_pc from Cartesian (matches coordinate pipeline)."""
    x = float(row["x_icrs_pc"])
    y = float(row["y_icrs_pc"])
    z = float(row["z_icrs_pc"])
    r = math.sqrt(x * x + y * y + z * z)
    if r > 0.0:
        ra = (math.degrees(math.atan2(y, x)) + 360.0) % 360.0
        dec = math.degrees(math.asin(max(-1.0, min(1.0, z / r))))
    else:
        ra = 0.0
        dec = 0.0
    out = {**row, "ra_deg": ra, "dec_deg": dec, "r_pc": r}
    return out


def test_run_merge_streaming_with_overrides_and_missing_partners(tmp_path: Path):
    gaia_dir = tmp_path / "gaia"
    hip_path = tmp_path / "hip_stars.parquet"
    crossmatch_path = tmp_path / "gaia_hip_map.parquet"
    overrides_path = tmp_path / "overrides.parquet"
    output_dir = tmp_path / "merged"

    gaia_df = pd.DataFrame(
        [
            _with_radec_r_pc(
                {
                    "source": "gaia",
                    "source_id": 1001,
                    "x_icrs_pc": 1.0,
                    "y_icrs_pc": 0.0,
                    "z_icrs_pc": 0.0,
                    "mag_abs": 1.0,
                    "teff": 5000.0,
                    "quality_flags": 1,
                    "astrometry_quality": 0.1,
                    "photometry_quality": 0.1,
                }
            ),
            _with_radec_r_pc(
                {
                    "source": "gaia",
                    "source_id": 1002,
                    "x_icrs_pc": 0.0,
                    "y_icrs_pc": 1.0,
                    "z_icrs_pc": 0.0,
                    "mag_abs": 2.0,
                    "teff": 5100.0,
                    "quality_flags": 1,
                    "astrometry_quality": 0.2,
                    "photometry_quality": 0.1,
                }
            ),
            _with_radec_r_pc(
                {
                    "source": "gaia",
                    "source_id": 1004,
                    "x_icrs_pc": 0.0,
                    "y_icrs_pc": 0.0,
                    "z_icrs_pc": 1.0,
                    "mag_abs": 3.0,
                    "teff": 5200.0,
                    "quality_flags": 1,
                    "astrometry_quality": 0.4,
                    "photometry_quality": 0.1,
                }
            ),
            _with_radec_r_pc(
                {
                    "source": "gaia",
                    "source_id": 1010,
                    "x_icrs_pc": -1.0,
                    "y_icrs_pc": 0.0,
                    "z_icrs_pc": 0.0,
                    "mag_abs": 4.0,
                    "teff": 5300.0,
                    "quality_flags": 1,
                    "astrometry_quality": 0.2,
                    "photometry_quality": 0.1,
                }
            ),
        ]
    )
    _write_parquet(gaia_df[OUTPUT_COLS], gaia_dir / "b1.parquet")

    hip_df = pd.DataFrame(
        [
            _with_radec_r_pc(
                {
                    "source": "hip",
                    "source_id": 2001,
                    "x_icrs_pc": 1.0,
                    "y_icrs_pc": 0.0,
                    "z_icrs_pc": 0.0,
                    "mag_abs": 1.5,
                    "teff": 6000.0,
                    "quality_flags": 2,
                    "astrometry_quality": 0.3,
                    "photometry_quality": 0.2,
                }
            ),
            _with_radec_r_pc(
                {
                    "source": "hip",
                    "source_id": 2002,
                    "x_icrs_pc": 0.0,
                    "y_icrs_pc": 1.0,
                    "z_icrs_pc": 0.0,
                    "mag_abs": 2.5,
                    "teff": 6100.0,
                    "quality_flags": 2,
                    "astrometry_quality": 0.2,
                    "photometry_quality": 0.2,
                }
            ),
            _with_radec_r_pc(
                {
                    "source": "hip",
                    "source_id": 2003,
                    "x_icrs_pc": 0.0,
                    "y_icrs_pc": 0.0,
                    "z_icrs_pc": -1.0,
                    "mag_abs": 3.5,
                    "teff": 6200.0,
                    "quality_flags": 2,
                    "astrometry_quality": 0.5,
                    "photometry_quality": 0.2,
                }
            ),
            _with_radec_r_pc(
                {
                    "source": "hip",
                    "source_id": 2004,
                    "x_icrs_pc": 1.0,
                    "y_icrs_pc": 1.0,
                    "z_icrs_pc": 0.0,
                    "mag_abs": 4.5,
                    "teff": 6300.0,
                    "quality_flags": 2,
                    "astrometry_quality": 0.1,
                    "photometry_quality": 0.2,
                }
            ),
            _with_radec_r_pc(
                {
                    "source": "hip",
                    "source_id": 2010,
                    "x_icrs_pc": 0.5,
                    "y_icrs_pc": -0.5,
                    "z_icrs_pc": 0.0,
                    "mag_abs": 5.0,
                    "teff": 6400.0,
                    "quality_flags": 2,
                    "astrometry_quality": 0.2,
                    "photometry_quality": 0.2,
                }
            ),
        ]
    )
    _write_parquet(hip_df[OUTPUT_COLS], hip_path)

    crossmatch_df = pd.DataFrame(
        [
            {"gaia_source_id": 1001, "hip_source_id": 2001, "mapping_source": "test"},
            {"gaia_source_id": 1002, "hip_source_id": 2002, "mapping_source": "test"},
            {"gaia_source_id": 1003, "hip_source_id": 2003, "mapping_source": "test"},
            {"gaia_source_id": 1004, "hip_source_id": 2004, "mapping_source": "test"},
            {"gaia_source_id": 1005, "hip_source_id": 2005, "mapping_source": "test"},
        ]
    )
    _write_parquet(crossmatch_df, crossmatch_path)

    overrides_df = pd.DataFrame(
        [
            {
                **_with_radec_r_pc(
                    {
                        "source": "hip",
                        "source_id": "2004",
                        "x_icrs_pc": 2.0,
                        "y_icrs_pc": 0.0,
                        "z_icrs_pc": 0.0,
                        "mag_abs": 10.0,
                        "teff": 7000.0,
                        "quality_flags": 10,
                        "astrometry_quality": 0.0,
                        "photometry_quality": 0.0,
                    }
                ),
                "override_id": "ov.hip.replace",
                "action": "replace",
                "override_reason": "test_replace_hip",
                "override_policy_version": "v1",
            },
            {
                **_with_radec_r_pc(
                    {
                        "source": "gaia",
                        "source_id": "1003",
                        "x_icrs_pc": 0.0,
                        "y_icrs_pc": 2.0,
                        "z_icrs_pc": 0.0,
                        "mag_abs": 11.0,
                        "teff": 7100.0,
                        "quality_flags": 10,
                        "astrometry_quality": 0.0,
                        "photometry_quality": 0.0,
                    }
                ),
                "override_id": "ov.gaia.replace_missing_target",
                "action": "replace",
                "override_reason": "test_replace_gaia_missing_target",
                "override_policy_version": "v1",
            },
            {
                "source": "gaia",
                "source_id": "1005",
                "x_icrs_pc": pd.NA,
                "y_icrs_pc": pd.NA,
                "z_icrs_pc": pd.NA,
                "ra_deg": pd.NA,
                "dec_deg": pd.NA,
                "r_pc": pd.NA,
                "mag_abs": pd.NA,
                "teff": pd.NA,
                "quality_flags": pd.NA,
                "astrometry_quality": pd.NA,
                "photometry_quality": pd.NA,
                "override_id": "ov.gaia.drop_no_effect",
                "action": "drop",
                "override_reason": "test_drop_no_effect",
                "override_policy_version": "v1",
            },
            {
                **_with_radec_r_pc(
                    {
                        "source": "manual",
                        "source_id": "sun",
                        "x_icrs_pc": 0.0,
                        "y_icrs_pc": 0.0,
                        "z_icrs_pc": 0.0,
                        "mag_abs": 4.83,
                        "teff": 5772.0,
                        "quality_flags": 10,
                        "astrometry_quality": 0.0,
                        "photometry_quality": 0.0,
                    }
                ),
                "override_id": "ov.manual.add_sun",
                "action": "add",
                "override_reason": "test_add",
                "override_policy_version": "v1",
            },
        ]
    )
    _write_parquet(overrides_df, overrides_path)

    report = run_merge(
        gaia_dir=gaia_dir,
        hip_path=hip_path,
        crossmatch_path=crossmatch_path,
        overrides_path=overrides_path,
        output_dir=output_dir,
        healpix_order=1,
        force=True,
    )

    merged_files = sorted((output_dir / "healpix").glob("*/*.parquet"))
    assert merged_files
    merged_df = pd.concat([pd.read_parquet(p) for p in merged_files], ignore_index=True)

    assert len(merged_df) == 7
    assert merged_df["source_id"].is_unique
    assert set(merged_df["source_id"].astype(str)) == {
        "2001",
        "2002",
        "1003",
        "1010",
        "2004",
        "2010",
        "sun",
    }

    manual_add = merged_df[merged_df["source"] == "manual"]
    assert set(manual_add["source_id"].astype(str)) == {"sun"}

    matched_2001 = merged_df.loc[merged_df["source_id"].astype(str) == "2001"].iloc[0]
    assert matched_2001["source"] == "hip"
    assert matched_2001["mag_abs"] == pytest.approx(1.0)

    assert report.matched_pairs_scored == 2
    assert report.matched_winner_gaia == 2
    assert report.matched_winner_hip == 0
    assert report.unmatched_gaia == 1
    assert report.unmatched_hip == 1
    assert report.override_replace_applied == 2
    assert report.override_add_applied == 1
    assert report.override_drop_applied == 0
    assert report.override_no_effect == 1
    assert report.rows_emitted_total == 7

    decisions_df = pd.read_parquet(output_dir / "merge_decisions.parquet")
    assert len(decisions_df) >= 5
    assert "override_no_effect" in set(decisions_df["decision_type"].astype(str))

    report_json = json.loads((output_dir / "merge_report.json").read_text(encoding="utf-8"))
    assert report_json["rows_emitted_total"] == 7
    assert report_json["healpix_order"] == 1
    assert report_json["gaia_dir"] == str(gaia_dir)
    assert report_json["hip_path"] == str(hip_path)
    assert report_json["crossmatch_path"] == str(crossmatch_path)
    assert report_json["overrides_path"] == str(overrides_path)


def test_run_merge_rejects_drop_override_with_payload(tmp_path: Path):
    gaia_dir = tmp_path / "gaia"
    hip_path = tmp_path / "hip_stars.parquet"
    crossmatch_path = tmp_path / "gaia_hip_map.parquet"
    overrides_path = tmp_path / "overrides.parquet"
    output_dir = tmp_path / "merged"

    _write_parquet(pd.DataFrame(columns=OUTPUT_COLS), gaia_dir / "b1.parquet")
    _write_parquet(
        pd.DataFrame(
            [
                _with_radec_r_pc(
                    {
                        "source": "hip",
                        "source_id": 2001,
                        "x_icrs_pc": 1.0,
                        "y_icrs_pc": 0.0,
                        "z_icrs_pc": 0.0,
                        "mag_abs": 1.5,
                        "teff": 6000.0,
                        "quality_flags": 2,
                        "astrometry_quality": 0.3,
                        "photometry_quality": 0.2,
                    }
                )
            ]
        )[OUTPUT_COLS],
        hip_path,
    )
    _write_parquet(
        pd.DataFrame([{"gaia_source_id": 1001, "hip_source_id": 2001}]),
        crossmatch_path,
    )
    _write_parquet(
        pd.DataFrame(
            [
                {
                    "source": "hip",
                    "source_id": "2001",
                    "x_icrs_pc": 99.0,
                    "y_icrs_pc": pd.NA,
                    "z_icrs_pc": pd.NA,
                    "ra_deg": pd.NA,
                    "dec_deg": pd.NA,
                    "r_pc": pd.NA,
                    "mag_abs": pd.NA,
                    "teff": pd.NA,
                    "quality_flags": pd.NA,
                    "astrometry_quality": pd.NA,
                    "photometry_quality": pd.NA,
                    "override_id": "ov.invalid.drop.payload",
                    "action": "drop",
                    "override_reason": "invalid_payload",
                    "override_policy_version": "v1",
                }
            ]
        ),
        overrides_path,
    )

    with pytest.raises(ValueError, match="must not include payload columns"):
        run_merge(
            gaia_dir=gaia_dir,
            hip_path=hip_path,
            crossmatch_path=crossmatch_path,
            overrides_path=overrides_path,
            output_dir=output_dir,
            healpix_order=1,
            force=True,
        )


# ---------------------------------------------------------------------------
# V2 policy unit tests for _choose_matched_winner
# ---------------------------------------------------------------------------

def _gaia_row(score: float, *, phot_g_mean_mag: float | None = None, **kw) -> dict:
    row: dict = {"astrometry_quality": score}
    if phot_g_mean_mag is not None:
        row["phot_g_mean_mag"] = phot_g_mean_mag
    row.update(kw)
    return row


def _hip_row(score: float, *, sn: int | None = None, **kw) -> dict:
    row: dict = {"astrometry_quality": score}
    if sn is not None:
        row["Sn"] = sn
    row.update(kw)
    return row


class TestChooseMatchedWinnerV2:
    """Tests for merge winner selection logic."""

    def test_gaia_wins_when_better(self):
        winner, reason = _choose_matched_winner(
            _gaia_row(0.05, phot_g_mean_mag=8.0),
            _hip_row(0.10, sn=5),
        )
        assert winner == "gaia"

    def test_gaia_wins_on_tie(self):
        winner, reason = _choose_matched_winner(
            _gaia_row(0.10, phot_g_mean_mag=8.0),
            _hip_row(0.10, sn=5),
        )
        assert winner == "gaia"

    def test_neighbour_veto_forces_gaia(self):
        winner, reason = _choose_matched_winner(
            _gaia_row(0.50, phot_g_mean_mag=8.0),
            _hip_row(0.01, sn=5),
            number_of_neighbours=3,
        )
        assert winner == "gaia"
        assert reason == "neighbour_veto"

    def test_hip_multiplicity_veto(self):
        winner, reason = _choose_matched_winner(
            _gaia_row(0.50, phot_g_mean_mag=2.0),
            _hip_row(0.01, sn=95),
        )
        assert winner == "gaia"
        assert reason == "hip_multiplicity"

    def test_hip_sn5_no_veto(self):
        """sn=5 (standard single-star) should not trigger veto."""
        winner, reason = _choose_matched_winner(
            _gaia_row(0.50, phot_g_mean_mag=2.0),
            _hip_row(0.01, sn=5),
        )
        assert winner == "hip"

    def test_very_bright_hip_wins_if_better(self):
        """G < 3.5: margin=1.0, so Hip wins if strictly better."""
        winner, reason = _choose_matched_winner(
            _gaia_row(0.10, phot_g_mean_mag=2.0),
            _hip_row(0.05, sn=5),
        )
        assert winner == "hip"

    def test_bright_hip_needs_large_margin(self):
        """3.5 <= G < 6: margin=0.6, so Hip score must be < gaia*0.6."""
        winner, reason = _choose_matched_winner(
            _gaia_row(0.10, phot_g_mean_mag=5.0),
            _hip_row(0.08, sn=5),
        )
        assert winner == "gaia"
        assert reason == "gaia_margin"

    def test_bright_hip_wins_with_large_margin(self):
        """3.5 <= G < 6: Hip score 0.04 < gaia 0.10 * 0.6 = 0.06 → Hip wins."""
        winner, reason = _choose_matched_winner(
            _gaia_row(0.10, phot_g_mean_mag=5.0),
            _hip_row(0.04, sn=5),
        )
        assert winner == "hip"

    def test_normal_hip_needs_very_large_margin(self):
        """G >= 6: margin=0.5, Hip needs to be ≥50% better."""
        winner, reason = _choose_matched_winner(
            _gaia_row(0.10, phot_g_mean_mag=8.0),
            _hip_row(0.06, sn=5),
        )
        assert winner == "gaia"
        assert reason == "gaia_margin"

    def test_normal_hip_wins_with_very_large_margin(self):
        """G >= 6: Hip score 0.04 < gaia 0.10 * 0.5 = 0.05 → Hip wins."""
        winner, reason = _choose_matched_winner(
            _gaia_row(0.10, phot_g_mean_mag=8.0),
            _hip_row(0.04, sn=5),
        )
        assert winner == "hip"

    def test_missing_gmag_uses_distance_modulus_fallback(self):
        """Without phot_g_mean_mag, falls back to mag_abs + 5*log10(r_pc/10)."""
        winner, reason = _choose_matched_winner(
            _gaia_row(0.10, mag_abs=-1.0, r_pc=1.0),
            _hip_row(0.05, sn=5),
        )
        # apparent G ≈ -1.0 + 5*log10(0.1) = -6.0 → very bright → margin=1.0
        assert winner == "hip"

    def test_missing_aux_columns_defaults_to_normal_margin(self):
        """Without any magnitude info, defaults to normal (strictest) margin."""
        winner, reason = _choose_matched_winner(
            _gaia_row(0.10),
            _hip_row(0.06),
        )
        assert winner == "gaia"
        assert reason == "gaia_margin"

    def test_missing_sn_no_veto(self):
        """Missing Sn should not trigger multiplicity veto."""
        winner, reason = _choose_matched_winner(
            _gaia_row(0.50, phot_g_mean_mag=2.0),
            _hip_row(0.01),
        )
        assert winner == "hip"


class TestSafeHelpers:
    def test_safe_float_normal(self):
        assert _safe_float(3.14) == pytest.approx(3.14)

    def test_safe_float_none(self):
        assert math.isnan(_safe_float(None))

    def test_safe_float_string(self):
        assert math.isnan(_safe_float("bad"))

    def test_safe_int_normal(self):
        assert _safe_int(5) == 5

    def test_safe_int_float(self):
        assert _safe_int(5.0) == 5

    def test_safe_int_none(self):
        assert _safe_int(None) is None

    def test_safe_int_nan(self):
        assert _safe_int(float("nan")) is None

    def test_safe_score_inf_on_missing(self):
        assert _safe_score(None) == math.inf
        assert _safe_score(float("nan")) == math.inf
