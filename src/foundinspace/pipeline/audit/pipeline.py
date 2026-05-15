"""Audit helpers for supplemental Gaia/HIP matching and review reports."""

from __future__ import annotations

import json
import math
import shutil
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from foundinspace.pipeline.gaia_to_hip.pipeline import (
    GAIA_HIP_MAP_COLS,
    MAPPING_SOURCE_HIPPARCOS2_BEST_NEIGHBOUR,
    empty_gaia_hip_mapping,
    write_gaia_hip_mapping,
)
from foundinspace.pipeline.merge import shards
from foundinspace.pipeline.merge.quality_report import (
    ISSUES_FILENAME,
    run_quality_report,
)

MATCH_EVIDENCE_FILENAME = "match_evidence.parquet"
SUPPLEMENTAL_MAP_FILENAME = "supplemental_gaia_hip_map.parquet"
COMBINED_MAP_FILENAME = "combined_gaia_hip_map.parquet"
MATCH_REPORT_FILENAME = "audit_match_report.json"
OCTREE_REVIEW_FILENAME = "octree_review.parquet"
MANUAL_CANDIDATES_FILENAME = "manual_override_candidates.parquet"
MANUAL_CANDIDATES_CSV_FILENAME = "manual_override_candidates.csv"
AUDIT_REPORT_FILENAME = "audit_report.json"
DISTANCE_HISTOGRAM_PNG_FILENAME = "distance_pct_histogram.png"
DISTANCE_HISTOGRAM_SVG_FILENAME = "distance_pct_histogram.svg"
DISTANCE_HISTOGRAM_BINS_FILENAME = "distance_pct_histogram_bins.csv"
DISTANCE_THRESHOLD_SUMMARY_FILENAME = "distance_threshold_summary.csv"
DISTANCE_THRESHOLD_SUMMARY_JSON_FILENAME = "distance_threshold_summary.json"
DISTANCE_QUALITY_PLOT_PNG_FILENAME = "distance_pct_vs_astrometry_quality.png"
DISTANCE_QUALITY_PLOT_SVG_FILENAME = "distance_pct_vs_astrometry_quality.svg"
DISTANCE_QUALITY_SUMMARY_FILENAME = "distance_quality_summary.csv"

LOCAL_CLOSE_PAIR_MAPPING_SOURCE = "local_close_pair_v1"
BATCH_SIZE = 250_000
OCTREE_REVIEW_MAX_SEP_ARCSEC = 1.0
DISTANCE_HISTOGRAM_BINS = [
    0,
    1,
    2,
    3,
    4,
    5,
    7.5,
    10,
    12.5,
    15,
    20,
    25,
    30,
    40,
    50,
    75,
    100,
]

MATCH_EVIDENCE_COLS = [
    "gaia_source_id",
    "hip_source_id",
    "decision",
    "recommended_action",
    "severity",
    "reasons",
    "separation_arcsec",
    "apparent_mag_delta",
    "distance_ratio",
    "distance_frac_diff",
    "gaia_ra_deg",
    "gaia_dec_deg",
    "hip_ra_deg",
    "hip_dec_deg",
    "gaia_r_pc",
    "hip_r_pc",
    "gaia_mag_abs",
    "hip_mag_abs",
    "gaia_apparent_mag",
    "hip_apparent_mag",
    "gaia_astrometry_quality",
    "hip_astrometry_quality",
    "gaia_photometry_quality",
    "hip_photometry_quality",
    "gaia_ruwe",
    "gaia_phot_g_mean_mag",
    "hip_solution_type",
    "hip_hpmag",
    "gaia_has_official_map",
    "hip_has_official_map",
    "official_conflict",
    "gaia_official_hip_source_id",
    "hip_official_gaia_source_id",
    "gaia_candidate_count",
    "hip_candidate_count",
    "one_to_one",
    "within_auto_thresholds",
    "within_distance_threshold",
    "overridden",
]

OCTREE_REVIEW_COLS = [
    "source",
    "source_id",
    "issue_type",
    "severity",
    "display_action",
    "linked_source",
    "linked_source_id",
    "reasons",
    "ra_deg",
    "dec_deg",
    "r_pc",
    "mag_abs",
    "separation_arcsec",
    "apparent_mag_delta",
]

MANUAL_CANDIDATE_COLS = [
    "issue_type",
    "severity",
    "recommended_action",
    "reasons",
    "source",
    "source_id",
    "gaia_source_id",
    "hip_source_id",
    "label",
    "separation_arcsec",
    "apparent_mag_delta",
    "distance_ratio",
    "distance_frac_diff",
    "gaia_r_pc",
    "hip_r_pc",
    "gaia_mag_abs",
    "hip_mag_abs",
    "merged_r_pc",
    "merged_mag_abs",
    "astrometry_quality",
    "gaia_score",
    "hip_score",
    "gaia_ruwe",
    "hip_solution_type",
]


@dataclass
class AuditMatchReport:
    """JSON summary for the local supplemental crossmatch audit."""

    gaia_dir: str
    hip_path: str
    official_crossmatch_path: str
    overrides_path: str
    audit_dir: str
    match_evidence_path: str
    supplemental_crossmatch_path: str
    combined_crossmatch_path: str
    distance_histogram_png_path: str | None
    distance_histogram_svg_path: str | None
    distance_histogram_bins_path: str
    distance_threshold_summary_path: str
    distance_threshold_summary_json_path: str
    distance_quality_plot_png_path: str | None
    distance_quality_plot_svg_path: str | None
    distance_quality_summary_path: str
    thresholds: dict[str, float]
    evidence_rows: int
    supplemental_rows: int
    official_rows: int
    combined_rows: int
    decision_counts: dict[str, int]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class AuditReport:
    """JSON summary for post-merge audit review artifacts."""

    merge_dir: str
    audit_dir: str
    octree_review_path: str
    manual_candidates_path: str
    manual_candidates_csv_path: str
    octree_review_rows: int
    octree_review_sharded_rows: int
    manual_candidate_rows: int
    manual_counts_by_type: dict[str, int]
    octree_counts_by_action: dict[str, int]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def default_audit_dir(merge_dir: Path) -> Path:
    return Path(merge_dir).expanduser() / "audit"


def run_audit_match(
    *,
    gaia_dir: Path,
    hip_path: Path,
    official_crossmatch_path: Path,
    overrides_path: Path,
    audit_dir: Path,
    max_sep_arcsec: float = 5.0,
    max_mag_delta: float = 0.5,
    auto_sep_arcsec: float = 0.25,
    auto_mag_delta: float = 0.25,
    auto_distance_frac_diff: float = 0.10,
    force: bool = False,
) -> AuditMatchReport:
    """Write local Gaia/HIP match evidence and supplemental crossmatch maps."""
    gaia_dir = Path(gaia_dir).expanduser()
    hip_path = Path(hip_path).expanduser()
    official_crossmatch_path = Path(official_crossmatch_path).expanduser()
    overrides_path = Path(overrides_path).expanduser()
    audit_dir = Path(audit_dir).expanduser()

    if not gaia_dir.is_dir():
        raise FileNotFoundError(str(gaia_dir))
    for path in (hip_path, official_crossmatch_path, overrides_path):
        if not path.is_file():
            raise FileNotFoundError(str(path))

    _prepare_output_dir(audit_dir, force=force)

    evidence_path = audit_dir / MATCH_EVIDENCE_FILENAME
    supplemental_path = audit_dir / SUPPLEMENTAL_MAP_FILENAME
    combined_path = audit_dir / COMBINED_MAP_FILENAME
    report_path = audit_dir / MATCH_REPORT_FILENAME

    override_keys = _load_override_keys(overrides_path)
    official = _read_crossmatch(official_crossmatch_path)
    evidence = build_match_evidence(
        gaia_dir=gaia_dir,
        hip_path=hip_path,
        official_crossmatch=official,
        override_keys=override_keys,
        max_sep_arcsec=max_sep_arcsec,
        max_mag_delta=max_mag_delta,
        auto_sep_arcsec=auto_sep_arcsec,
        auto_mag_delta=auto_mag_delta,
        auto_distance_frac_diff=auto_distance_frac_diff,
    )
    _write_parquet(evidence, evidence_path)

    supplemental = build_supplemental_crossmatch(evidence)
    validate_one_to_one_crossmatch(supplemental, label="supplemental")
    write_gaia_hip_mapping(supplemental, supplemental_path)

    combined = combine_crossmatches(official, supplemental)
    validate_one_to_one_crossmatch(combined, label="combined")
    write_gaia_hip_mapping(combined, combined_path)

    distance_diagnostics = write_distance_threshold_diagnostics(
        evidence,
        audit_dir=audit_dir,
        auto_distance_frac_diff=auto_distance_frac_diff,
    )

    report = AuditMatchReport(
        gaia_dir=str(gaia_dir),
        hip_path=str(hip_path),
        official_crossmatch_path=str(official_crossmatch_path),
        overrides_path=str(overrides_path),
        audit_dir=str(audit_dir),
        match_evidence_path=str(evidence_path),
        supplemental_crossmatch_path=str(supplemental_path),
        combined_crossmatch_path=str(combined_path),
        distance_histogram_png_path=distance_diagnostics["png_path"],
        distance_histogram_svg_path=distance_diagnostics["svg_path"],
        distance_histogram_bins_path=distance_diagnostics["histogram_bins_path"],
        distance_threshold_summary_path=distance_diagnostics["summary_path"],
        distance_threshold_summary_json_path=distance_diagnostics["summary_json_path"],
        distance_quality_plot_png_path=distance_diagnostics["quality_png_path"],
        distance_quality_plot_svg_path=distance_diagnostics["quality_svg_path"],
        distance_quality_summary_path=distance_diagnostics["quality_summary_path"],
        thresholds={
            "max_sep_arcsec": max_sep_arcsec,
            "max_mag_delta": max_mag_delta,
            "auto_sep_arcsec": auto_sep_arcsec,
            "auto_mag_delta": auto_mag_delta,
            "auto_distance_frac_diff": auto_distance_frac_diff,
        },
        evidence_rows=int(len(evidence)),
        supplemental_rows=int(len(supplemental)),
        official_rows=int(len(official)),
        combined_rows=int(len(combined)),
        decision_counts=_value_counts(evidence, "decision"),
    )
    _write_json(report.to_dict(), report_path)
    return report


def build_match_evidence(
    *,
    gaia_dir: Path,
    hip_path: Path,
    official_crossmatch: pd.DataFrame,
    override_keys: set[tuple[str, str]],
    max_sep_arcsec: float,
    max_mag_delta: float,
    auto_sep_arcsec: float,
    auto_mag_delta: float,
    auto_distance_frac_diff: float,
) -> pd.DataFrame:
    """Return local Gaia/HIP close-pair evidence rows."""
    ckdtree = _require_ckdtree()
    gaia = _load_processed_candidates(sorted(Path(gaia_dir).glob("*.parquet")), "gaia")
    hip = _load_processed_candidates([Path(hip_path)], "hip")
    if gaia.empty or hip.empty:
        return _empty_match_evidence()

    gaia_xyz = _unit_vectors(gaia["ra_deg"], gaia["dec_deg"])
    hip_xyz = _unit_vectors(hip["ra_deg"], hip["dec_deg"])
    chord_radius = 2.0 * math.sin(math.radians(max_sep_arcsec / 3600.0) / 2.0)
    neighbour_lists = ckdtree(gaia_xyz).query_ball_tree(
        ckdtree(hip_xyz),
        r=chord_radius,
    )
    if not any(neighbour_lists):
        return _empty_match_evidence()

    official_gaia_to_hip = {
        str(rec.gaia_source_id): str(rec.hip_source_id)
        for rec in official_crossmatch[["gaia_source_id", "hip_source_id"]].itertuples(
            index=False
        )
    }
    official_hip_to_gaia = {
        str(rec.hip_source_id): str(rec.gaia_source_id)
        for rec in official_crossmatch[["gaia_source_id", "hip_source_id"]].itertuples(
            index=False
        )
    }

    rows: list[dict[str, Any]] = []
    for gaia_i, hip_indices in enumerate(neighbour_lists):
        if not hip_indices:
            continue
        gaia_rec = gaia.iloc[gaia_i]
        gaia_vec = gaia_xyz[gaia_i]
        hip_subset = hip.iloc[hip_indices]
        hip_subset_xyz = hip_xyz[hip_indices]
        dots = np.clip(hip_subset_xyz @ gaia_vec, -1.0, 1.0)
        sep_arcsec = np.degrees(np.arccos(dots)) * 3600.0
        mag_delta = np.abs(
            hip_subset["apparent_mag"].to_numpy(dtype=float)
            - float(gaia_rec["apparent_mag"])
        )
        keep = (sep_arcsec <= max_sep_arcsec) & (mag_delta <= max_mag_delta)
        for local_i in np.flatnonzero(keep):
            hip_rec = hip_subset.iloc[int(local_i)]
            gaia_id = str(gaia_rec["source_id"])
            hip_id = str(hip_rec["source_id"])
            if official_gaia_to_hip.get(gaia_id) == hip_id:
                continue
            rows.append(
                _candidate_record(
                    gaia_rec=gaia_rec,
                    hip_rec=hip_rec,
                    sep_arcsec=float(sep_arcsec[local_i]),
                    mag_delta=float(mag_delta[local_i]),
                    official_gaia_to_hip=official_gaia_to_hip,
                    official_hip_to_gaia=official_hip_to_gaia,
                    override_keys=override_keys,
                    auto_sep_arcsec=auto_sep_arcsec,
                    auto_mag_delta=auto_mag_delta,
                    auto_distance_frac_diff=auto_distance_frac_diff,
                )
            )

    if not rows:
        return _empty_match_evidence()
    evidence = pd.DataFrame(rows, columns=MATCH_EVIDENCE_COLS)
    evidence["gaia_candidate_count"] = evidence.groupby("gaia_source_id")[
        "hip_source_id"
    ].transform("nunique")
    evidence["hip_candidate_count"] = evidence.groupby("hip_source_id")[
        "gaia_source_id"
    ].transform("nunique")
    evidence["one_to_one"] = evidence["gaia_candidate_count"].eq(1) & evidence[
        "hip_candidate_count"
    ].eq(1)
    for idx, rec in evidence.iterrows():
        decision, action, severity, reasons = _classify_evidence_row(rec)
        evidence.loc[idx, "decision"] = decision
        evidence.loc[idx, "recommended_action"] = action
        evidence.loc[idx, "severity"] = severity
        evidence.loc[idx, "reasons"] = ";".join(reasons)
    return evidence.sort_values(
        ["decision", "separation_arcsec", "apparent_mag_delta", "gaia_source_id"],
        kind="mergesort",
        ignore_index=True,
    )


def build_supplemental_crossmatch(evidence: pd.DataFrame) -> pd.DataFrame:
    """Build a Gaia-HIP sidecar from auto-match evidence rows."""
    if evidence.empty:
        return empty_gaia_hip_mapping()
    matched = evidence.loc[evidence["decision"].astype(str).eq("auto_match")].copy()
    if matched.empty:
        return empty_gaia_hip_mapping()
    out = pd.DataFrame(
        {
            "gaia_source_id": _parse_uint_series(matched["gaia_source_id"]).astype(
                "uint64"
            ),
            "hip_source_id": _parse_uint_series(matched["hip_source_id"]).astype(
                "uint64"
            ),
            "mapping_source": LOCAL_CLOSE_PAIR_MAPPING_SOURCE,
            "number_of_neighbours": np.int16(1),
            "angular_distance": pd.to_numeric(
                matched["separation_arcsec"], errors="coerce"
            ).astype(np.float32),
        }
    )
    return out[GAIA_HIP_MAP_COLS].sort_values(
        ["gaia_source_id", "hip_source_id"],
        kind="mergesort",
        ignore_index=True,
    )


def combine_crossmatches(
    official: pd.DataFrame,
    supplemental: pd.DataFrame,
) -> pd.DataFrame:
    official = _normalize_crossmatch_frame(official)
    supplemental = _normalize_crossmatch_frame(supplemental)
    if official.empty:
        return supplemental
    if supplemental.empty:
        return official
    combined = pd.concat([official, supplemental], ignore_index=True)
    combined = combined.drop_duplicates(["gaia_source_id", "hip_source_id"])
    return combined.sort_values(
        ["gaia_source_id", "hip_source_id"],
        kind="mergesort",
        ignore_index=True,
    )[GAIA_HIP_MAP_COLS]


def validate_one_to_one_crossmatch(mapping: pd.DataFrame, *, label: str) -> None:
    """Raise if a Gaia-HIP map is not one-to-one."""
    if mapping.empty:
        return
    dup_gaia = mapping["gaia_source_id"].duplicated(keep=False)
    if dup_gaia.any():
        sample = mapping.loc[dup_gaia, "gaia_source_id"].head(5).astype(str).tolist()
        raise ValueError(f"{label} crossmatch has duplicate Gaia IDs: {sample}")
    dup_hip = mapping["hip_source_id"].duplicated(keep=False)
    if dup_hip.any():
        sample = mapping.loc[dup_hip, "hip_source_id"].head(5).astype(str).tolist()
        raise ValueError(f"{label} crossmatch has duplicate HIP IDs: {sample}")


def write_distance_threshold_diagnostics(
    evidence: pd.DataFrame,
    *,
    audit_dir: Path,
    auto_distance_frac_diff: float,
) -> dict[str, str | None]:
    """Write threshold-summary tables and a distance-disagreement histogram."""
    audit_dir = Path(audit_dir)
    work = _distance_diagnostics_frame(evidence)
    clean = work.loc[
        (~work["official_conflict"].astype(bool))
        & (~work["overridden"].astype(bool))
        & work["one_to_one"].astype(bool)
        & np.isfinite(work["distance_frac_diff"])
    ].copy()
    finite = work.loc[np.isfinite(work["distance_frac_diff"])].copy()

    tight = clean["within_auto_thresholds"].astype(bool)
    current = tight | clean["distance_frac_diff"].le(auto_distance_frac_diff)
    pct25 = tight | clean["distance_frac_diff"].le(0.25)
    sep3d1 = tight | clean["separation_3d_pc"].le(1.0)
    sep3d1_strict = clean["separation_3d_pc"].le(1.0)
    current_threshold_label = f"tight or distance <= {auto_distance_frac_diff:.0%}"

    summary = pd.DataFrame(
        [
            {
                "policy": "tight sky/mag only",
                "matched_clean_count": int(tight.sum()),
                "delta_vs_current_distance_threshold": int(
                    tight.sum() - current.sum()
                ),
                "non_tight_added": 0,
            },
            {
                "policy": current_threshold_label,
                "matched_clean_count": int(current.sum()),
                "delta_vs_current_distance_threshold": 0,
                "non_tight_added": int(
                    (
                        ~tight
                        & clean["distance_frac_diff"].le(auto_distance_frac_diff)
                    ).sum()
                ),
            },
            {
                "policy": "tight or distance <= 25%",
                "matched_clean_count": int(pct25.sum()),
                "delta_vs_current_distance_threshold": int(
                    pct25.sum() - current.sum()
                ),
                "non_tight_added": int(
                    (~tight & clean["distance_frac_diff"].le(0.25)).sum()
                ),
            },
            {
                "policy": "tight or 3D separation <= 1 pc",
                "matched_clean_count": int(sep3d1.sum()),
                "delta_vs_current_distance_threshold": int(
                    sep3d1.sum() - current.sum()
                ),
                "non_tight_added": int((~tight & sep3d1_strict).sum()),
            },
        ]
    )
    summary_path = audit_dir / DISTANCE_THRESHOLD_SUMMARY_FILENAME
    summary.to_csv(summary_path, index=False)

    histogram = _distance_histogram_bins(finite, clean)
    histogram_path = audit_dir / DISTANCE_HISTOGRAM_BINS_FILENAME
    histogram.to_csv(histogram_path, index=False)

    quality_summary = _distance_quality_summary(clean)
    quality_summary_path = audit_dir / DISTANCE_QUALITY_SUMMARY_FILENAME
    quality_summary.to_csv(quality_summary_path, index=False)

    summary_json_path = audit_dir / DISTANCE_THRESHOLD_SUMMARY_JSON_FILENAME
    _write_json(
        {
            "rows": {
                "all_finite_distance_candidates": int(len(finite)),
                "clean_auto_eligible_candidates": int(len(clean)),
            },
            "policy_counts_clean_eligible": summary.to_dict(orient="records"),
            "quantiles_clean_pct_diff": _quantiles(
                clean["distance_frac_diff"] * 100.0
            ),
            "quantiles_clean_3d_sep_pc": _quantiles(clean["separation_3d_pc"]),
        },
        summary_json_path,
    )

    png_path: Path | None = None
    svg_path: Path | None = None
    quality_png_path: Path | None = None
    quality_svg_path: Path | None = None
    if not finite.empty or not clean.empty:
        candidate_png_path = audit_dir / DISTANCE_HISTOGRAM_PNG_FILENAME
        candidate_svg_path = audit_dir / DISTANCE_HISTOGRAM_SVG_FILENAME
        wrote_plot = _write_distance_histogram_plot(
            finite=finite,
            clean=clean,
            summary=summary,
            auto_distance_frac_diff=auto_distance_frac_diff,
            png_path=candidate_png_path,
            svg_path=candidate_svg_path,
        )
        if wrote_plot:
            png_path = candidate_png_path
            svg_path = candidate_svg_path
        candidate_quality_png_path = audit_dir / DISTANCE_QUALITY_PLOT_PNG_FILENAME
        candidate_quality_svg_path = audit_dir / DISTANCE_QUALITY_PLOT_SVG_FILENAME
        wrote_quality_plot = _write_distance_quality_plot(
            clean=clean,
            quality_summary=quality_summary,
            auto_distance_frac_diff=auto_distance_frac_diff,
            png_path=candidate_quality_png_path,
            svg_path=candidate_quality_svg_path,
        )
        if wrote_quality_plot:
            quality_png_path = candidate_quality_png_path
            quality_svg_path = candidate_quality_svg_path

    return {
        "png_path": str(png_path) if png_path is not None else None,
        "svg_path": str(svg_path) if svg_path is not None else None,
        "histogram_bins_path": str(histogram_path),
        "summary_path": str(summary_path),
        "summary_json_path": str(summary_json_path),
        "quality_png_path": str(quality_png_path)
        if quality_png_path is not None
        else None,
        "quality_svg_path": str(quality_svg_path)
        if quality_svg_path is not None
        else None,
        "quality_summary_path": str(quality_summary_path),
    }


def run_audit_report(
    *,
    gaia_dir: Path,
    hip_path: Path,
    official_crossmatch_path: Path,
    overrides_path: Path,
    identifiers_path: Path | None,
    merge_dir: Path,
    sidecar_output_dir: Path,
    healpix_order: int,
    audit_dir: Path,
    force: bool = False,
) -> AuditReport:
    """Write octree and manual-review audit reports after a merge."""
    merge_dir = Path(merge_dir).expanduser()
    sidecar_output_dir = Path(sidecar_output_dir).expanduser()
    audit_dir = Path(audit_dir).expanduser()
    evidence_path = audit_dir / MATCH_EVIDENCE_FILENAME
    if not evidence_path.is_file():
        raise FileNotFoundError(str(evidence_path))
    if not (merge_dir / "healpix").is_dir():
        raise FileNotFoundError(str(merge_dir / "healpix"))

    _prepare_report_outputs(audit_dir, sidecar_output_dir, force=force)

    evidence = pd.read_parquet(evidence_path)
    run_quality_report(
        gaia_dir=gaia_dir,
        hip_path=hip_path,
        crossmatch_path=official_crossmatch_path,
        overrides_path=overrides_path,
        merge_dir=merge_dir,
        identifiers_path=identifiers_path,
        output_dir=audit_dir,
        include_close_pairs=False,
        force=force,
    )
    quality_issues = pd.read_parquet(audit_dir / ISSUES_FILENAME)

    octree_review = build_octree_review(evidence, quality_issues)
    octree_path = audit_dir / OCTREE_REVIEW_FILENAME
    _write_parquet(octree_review, octree_path)
    sharded_rows = write_octree_review_sidecar(
        octree_review,
        sidecar_output_dir=sidecar_output_dir,
        healpix_order=healpix_order,
    )

    manual_candidates = build_manual_override_candidates(evidence, quality_issues)
    manual_path = audit_dir / MANUAL_CANDIDATES_FILENAME
    manual_csv_path = audit_dir / MANUAL_CANDIDATES_CSV_FILENAME
    _write_parquet(manual_candidates, manual_path)
    manual_candidates.to_csv(manual_csv_path, index=False)

    report = AuditReport(
        merge_dir=str(merge_dir),
        audit_dir=str(audit_dir),
        octree_review_path=str(octree_path),
        manual_candidates_path=str(manual_path),
        manual_candidates_csv_path=str(manual_csv_path),
        octree_review_rows=int(len(octree_review)),
        octree_review_sharded_rows=int(sharded_rows),
        manual_candidate_rows=int(len(manual_candidates)),
        manual_counts_by_type=_value_counts(manual_candidates, "issue_type"),
        octree_counts_by_action=_value_counts(octree_review, "display_action"),
    )
    _write_json(report.to_dict(), audit_dir / AUDIT_REPORT_FILENAME)
    return report


def build_octree_review(
    evidence: pd.DataFrame,
    quality_issues: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if not evidence.empty:
        for rec in evidence.loc[
            evidence["decision"].astype(str).eq("octree_review")
        ].itertuples(index=False):
            rows.append(
                _octree_record(
                    source="hip",
                    source_id=rec.hip_source_id,
                    issue_type="close_cross_catalog_pair",
                    severity=rec.severity,
                    display_action="suppress_candidate_duplicate",
                    linked_source="gaia",
                    linked_source_id=rec.gaia_source_id,
                    reasons=rec.reasons,
                    ra_deg=rec.hip_ra_deg,
                    dec_deg=rec.hip_dec_deg,
                    r_pc=rec.hip_r_pc,
                    mag_abs=rec.hip_mag_abs,
                    separation_arcsec=rec.separation_arcsec,
                    apparent_mag_delta=rec.apparent_mag_delta,
                )
            )
    if not quality_issues.empty:
        for rec in quality_issues.loc[
            quality_issues["issue_type"].astype(str).eq("merged_row_extreme")
        ].itertuples(index=False):
            rows.append(
                _octree_record(
                    source=rec.source,
                    source_id=rec.source_id,
                    issue_type="merged_row_extreme",
                    severity=rec.severity,
                    display_action="quarantine_suspicious_star",
                    linked_source=pd.NA,
                    linked_source_id=pd.NA,
                    reasons=rec.reasons,
                    ra_deg=rec.merged_ra_deg,
                    dec_deg=rec.merged_dec_deg,
                    r_pc=rec.merged_r_pc,
                    mag_abs=rec.merged_mag_abs,
                    separation_arcsec=pd.NA,
                    apparent_mag_delta=pd.NA,
                )
            )
    return pd.DataFrame(rows, columns=OCTREE_REVIEW_COLS)


def build_manual_override_candidates(
    evidence: pd.DataFrame,
    quality_issues: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if not evidence.empty:
        manual = evidence.loc[evidence["decision"].astype(str).eq("manual_review")]
        for rec in manual.itertuples(index=False):
            rows.append(
                _manual_record(
                    issue_type="close_pair_manual_review",
                    severity=rec.severity,
                    recommended_action=rec.recommended_action,
                    reasons=rec.reasons,
                    source="hip",
                    source_id=rec.hip_source_id,
                    gaia_source_id=rec.gaia_source_id,
                    hip_source_id=rec.hip_source_id,
                    separation_arcsec=rec.separation_arcsec,
                    apparent_mag_delta=rec.apparent_mag_delta,
                    distance_ratio=rec.distance_ratio,
                    distance_frac_diff=rec.distance_frac_diff,
                    gaia_r_pc=rec.gaia_r_pc,
                    hip_r_pc=rec.hip_r_pc,
                    gaia_mag_abs=rec.gaia_mag_abs,
                    hip_mag_abs=rec.hip_mag_abs,
                    gaia_score=rec.gaia_astrometry_quality,
                    hip_score=rec.hip_astrometry_quality,
                    gaia_ruwe=rec.gaia_ruwe,
                    hip_solution_type=rec.hip_solution_type,
                )
            )
    if not quality_issues.empty:
        review_types = {"matched_pair_conflict", "merged_row_extreme"}
        quality_review = quality_issues.loc[
            quality_issues["issue_type"].astype(str).isin(review_types)
        ]
        for rec in quality_review.itertuples(index=False):
            rows.append(
                _manual_record(
                    issue_type=rec.issue_type,
                    severity=rec.severity,
                    recommended_action="create_or_review_override",
                    reasons=rec.reasons,
                    source=rec.source,
                    source_id=rec.source_id,
                    gaia_source_id=rec.gaia_source_id,
                    hip_source_id=rec.hip_source_id,
                    label=rec.label,
                    separation_arcsec=rec.angular_distance_arcsec,
                    apparent_mag_delta=rec.apparent_mag_delta,
                    distance_ratio=rec.distance_ratio,
                    distance_frac_diff=rec.distance_frac_diff,
                    gaia_r_pc=rec.gaia_r_pc,
                    hip_r_pc=rec.hip_r_pc,
                    gaia_mag_abs=rec.gaia_mag_abs,
                    hip_mag_abs=rec.hip_mag_abs,
                    merged_r_pc=rec.merged_r_pc,
                    merged_mag_abs=rec.merged_mag_abs,
                    astrometry_quality=rec.astrometry_quality,
                    gaia_score=rec.gaia_score,
                    hip_score=rec.hip_score,
                    gaia_ruwe=rec.gaia_ruwe,
                    hip_solution_type=rec.hip_solution_type,
                )
            )
    if not rows:
        return pd.DataFrame(columns=MANUAL_CANDIDATE_COLS)
    return pd.DataFrame(rows, columns=MANUAL_CANDIDATE_COLS).sort_values(
        ["severity", "issue_type", "source", "source_id"],
        ascending=[True, True, True, True],
        kind="mergesort",
        ignore_index=True,
    )


def write_octree_review_sidecar(
    octree_review: pd.DataFrame,
    *,
    sidecar_output_dir: Path,
    healpix_order: int,
) -> int:
    if octree_review.empty:
        return 0
    hp = shards._build_healpix(healpix_order)
    work = octree_review.copy()
    work["_shard_ra_deg"] = pd.to_numeric(work["ra_deg"], errors="coerce")
    work["_shard_dec_deg"] = pd.to_numeric(work["dec_deg"], errors="coerce")
    work = work.loc[
        np.isfinite(work["_shard_ra_deg"]) & np.isfinite(work["_shard_dec_deg"])
    ]
    return shards._write_sidecar_shards(
        work,
        hp=hp,
        sidecar_root=Path(sidecar_output_dir),
        sidecar_name="octree_review",
        phase_tag="audit",
        output_cols=OCTREE_REVIEW_COLS,
        seq_by_key={},
    )


def _distance_diagnostics_frame(evidence: pd.DataFrame) -> pd.DataFrame:
    work = evidence.copy()
    for col in [
        "distance_frac_diff",
        "gaia_r_pc",
        "hip_r_pc",
        "separation_arcsec",
        "gaia_astrometry_quality",
        "hip_astrometry_quality",
    ]:
        if col not in work:
            work[col] = np.nan
        work[col] = pd.to_numeric(work[col], errors="coerce")
    for col in [
        "official_conflict",
        "overridden",
        "one_to_one",
        "within_auto_thresholds",
    ]:
        if col not in work:
            work[col] = False
        work[col] = work[col].fillna(False).astype(bool)

    work["distance_pct_diff"] = work["distance_frac_diff"] * 100.0
    work["distance_abs_diff_pc"] = (work["gaia_r_pc"] - work["hip_r_pc"]).abs()
    work["worst_astrometry_quality"] = work[
        ["gaia_astrometry_quality", "hip_astrometry_quality"]
    ].max(axis=1)
    work["best_astrometry_quality"] = work[
        ["gaia_astrometry_quality", "hip_astrometry_quality"]
    ].min(axis=1)
    work["astrometry_quality_ratio"] = (
        work["worst_astrometry_quality"] / work["best_astrometry_quality"]
    )
    theta = np.deg2rad(work["separation_arcsec"] / 3600.0)
    work["separation_3d_pc"] = np.sqrt(
        np.maximum(
            0.0,
            work["gaia_r_pc"] ** 2
            + work["hip_r_pc"] ** 2
            - 2.0 * work["gaia_r_pc"] * work["hip_r_pc"] * np.cos(theta),
        )
    )
    return work


def _distance_quality_summary(clean: pd.DataFrame) -> pd.DataFrame:
    bins = [0, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, np.inf]
    rows = []
    for start, end in zip(bins[:-1], bins[1:], strict=True):
        if np.isinf(end):
            mask = clean["worst_astrometry_quality"].ge(start)
            label = f">={start:g}"
        else:
            mask = clean["worst_astrometry_quality"].ge(start) & clean[
                "worst_astrometry_quality"
            ].lt(end)
            label = f"{start:g}-{end:g}"
        subset = clean.loc[mask]
        rows.append(
            {
                "worst_quality_bin": label,
                "rows": int(len(subset)),
                "distance_pct_le_10": int(subset["distance_frac_diff"].le(0.10).sum()),
                "distance_pct_10_to_25": int(
                    (
                        subset["distance_frac_diff"].gt(0.10)
                        & subset["distance_frac_diff"].le(0.25)
                    ).sum()
                ),
                "distance_pct_gt_25": int(subset["distance_frac_diff"].gt(0.25).sum()),
                "median_distance_pct": _finite_median(
                    subset["distance_frac_diff"] * 100.0
                ),
                "median_gaia_quality": _finite_median(
                    subset["gaia_astrometry_quality"]
                ),
                "median_hip_quality": _finite_median(subset["hip_astrometry_quality"]),
            }
        )
    return pd.DataFrame(rows)


def _distance_histogram_bins(
    finite: pd.DataFrame,
    clean: pd.DataFrame,
) -> pd.DataFrame:
    clean_counts, edges = np.histogram(
        clean["distance_pct_diff"],
        bins=DISTANCE_HISTOGRAM_BINS,
    )
    all_counts, _ = np.histogram(
        finite["distance_pct_diff"],
        bins=DISTANCE_HISTOGRAM_BINS,
    )
    rows = []
    for start, end, clean_count, all_count in zip(
        edges[:-1],
        edges[1:],
        clean_counts,
        all_counts,
        strict=True,
    ):
        rows.append(
            {
                "bin_start_pct": float(start),
                "bin_end_pct": float(end),
                "clean_auto_eligible_count": int(clean_count),
                "all_candidate_count": int(all_count),
            }
        )
    return pd.DataFrame(rows)


def _quantiles(series: pd.Series) -> dict[str, float | None]:
    values = pd.to_numeric(series, errors="coerce")
    values = values[np.isfinite(values)]
    quantiles = [0, 0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 1.0]
    if values.empty:
        return {str(q): None for q in quantiles}
    return {str(q): float(values.quantile(q)) for q in quantiles}


def _finite_median(series: pd.Series) -> float | None:
    values = pd.to_numeric(series, errors="coerce")
    values = values[np.isfinite(values)]
    if values.empty:
        return None
    return float(values.median())


def _write_distance_histogram_plot(
    *,
    finite: pd.DataFrame,
    clean: pd.DataFrame,
    summary: pd.DataFrame,
    auto_distance_frac_diff: float,
    png_path: Path,
    svg_path: Path,
) -> bool:
    try:
        import matplotlib
    except ModuleNotFoundError:
        return False

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import PercentFormatter

    plt.style.use("seaborn-v0_8-whitegrid")
    fig = plt.figure(figsize=(14, 9), constrained_layout=True)
    gs = fig.add_gridspec(2, 2, height_ratios=[3.0, 1.25], width_ratios=[2.4, 1.0])
    ax = fig.add_subplot(gs[0, 0])
    ax_cdf = fig.add_subplot(gs[1, 0])
    ax_table = fig.add_subplot(gs[:, 1])

    ax.hist(
        finite["distance_pct_diff"],
        bins=DISTANCE_HISTOGRAM_BINS,
        color="#c9d3d8",
        edgecolor="white",
        label=f"all broad evidence ({len(finite):,})",
    )
    ax.hist(
        clean["distance_pct_diff"],
        bins=DISTANCE_HISTOGRAM_BINS,
        color="#28785f",
        alpha=0.88,
        edgecolor="white",
        label=f"clean one-to-one eligible ({len(clean):,})",
    )
    threshold_pct = auto_distance_frac_diff * 100.0
    for x, color, label in [
        (threshold_pct, "#b0415a", f"{threshold_pct:.0f}% current"),
        (25.0, "#7048a8", "25% option"),
    ]:
        ax.axvline(x, color=color, lw=2.2, ls="--")
        ymax = ax.get_ylim()[1]
        ax.text(x + 0.6, ymax * 0.93, label, color=color, weight="bold", fontsize=11)

    ax.set_title(
        "Gaia/HIP close-pair distance disagreement",
        fontsize=16,
        weight="bold",
        loc="left",
    )
    ax.set_xlabel("fractional distance disagreement (%)")
    ax.set_ylabel("candidate count")
    ax.legend(frameon=True, loc="upper right")
    ax.set_xlim(0, 100)
    ax.text(
        0.01,
        0.98,
        "distance = abs(Gaia r_pc - HIP r_pc) / max(r_pc) * 100",
        transform=ax.transAxes,
        va="top",
        fontsize=10,
        color="#555555",
    )

    xs = np.sort(clean["distance_pct_diff"].to_numpy(dtype=float))
    if len(xs):
        ys = np.arange(1, len(xs) + 1) / len(xs)
        ax_cdf.plot(xs, ys, color="#28785f", lw=2)
    for x, color in [(threshold_pct, "#b0415a"), (25.0, "#7048a8")]:
        ax_cdf.axvline(x, color=color, lw=1.8, ls="--")
    ax_cdf.set_xlim(0, 100)
    ax_cdf.set_ylim(0, 1)
    ax_cdf.yaxis.set_major_formatter(PercentFormatter(1.0))
    ax_cdf.set_xlabel("fractional distance disagreement (%)")
    ax_cdf.set_ylabel("cumulative share")
    ax_cdf.set_title("Clean eligible cumulative distribution", fontsize=12, loc="left")

    ax_table.axis("off")
    ax_table.set_title("Policy comparison", fontsize=14, weight="bold", loc="left")
    lines = [
        f"Clean eligible candidates: {len(clean):,}",
        "",
    ]
    current_label = f"tight or distance <= {auto_distance_frac_diff:.0%}"
    for row in summary.itertuples(index=False):
        delta = int(row.delta_vs_current_distance_threshold)
        delta_text = "current" if row.policy == current_label else f"{delta:+,}"
        lines.append(str(row.policy))
        lines.append(f"  matched: {int(row.matched_clean_count):,}")
        lines.append(f"  delta: {delta_text}")
        lines.append(f"  non-tight added: {int(row.non_tight_added):,}")
        lines.append("")
    lines.extend(
        [
            "Key read:",
            "25% adds rows whose 3D separation",
            "can still be large.",
        ]
    )
    ax_table.text(
        0.0,
        1.0,
        "\n".join(lines),
        transform=ax_table.transAxes,
        va="top",
        ha="left",
        family="DejaVu Sans Mono",
        fontsize=10.5,
        linespacing=1.35,
    )

    fig.suptitle(
        "Bright-star audit: distance threshold shape",
        fontsize=18,
        weight="bold",
        x=0.03,
        ha="left",
    )
    fig.savefig(png_path, dpi=180, bbox_inches="tight")
    fig.savefig(svg_path, bbox_inches="tight")
    plt.close(fig)
    return True


def _write_distance_quality_plot(
    *,
    clean: pd.DataFrame,
    quality_summary: pd.DataFrame,
    auto_distance_frac_diff: float,
    png_path: Path,
    svg_path: Path,
) -> bool:
    try:
        import matplotlib
    except ModuleNotFoundError:
        return False

    plot = clean.loc[
        np.isfinite(clean["distance_pct_diff"])
        & clean["distance_pct_diff"].gt(0)
        & np.isfinite(clean["worst_astrometry_quality"])
        & clean["worst_astrometry_quality"].gt(0)
    ].copy()
    if plot.empty:
        return False

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    current = auto_distance_frac_diff * 100.0
    tight = plot["within_auto_thresholds"].astype(bool)
    pct_current = plot["distance_pct_diff"].le(current)
    pct25 = plot["distance_pct_diff"].le(25.0)

    categories = [
        (
            "tight sky/mag auto",
            tight,
            "#28785f",
            22,
            0.75,
        ),
        (
            f"distance <= {current:.0f}% extra",
            ~tight & pct_current,
            "#3b82b7",
            18,
            0.55,
        ),
        (
            f"{current:.0f}-25% option",
            ~tight & ~pct_current & pct25,
            "#7048a8",
            18,
            0.55,
        ),
        (
            ">25% distance disagreement",
            ~tight & ~pct25,
            "#b26b3d",
            16,
            0.40,
        ),
    ]

    plt.style.use("seaborn-v0_8-whitegrid")
    fig = plt.figure(figsize=(14, 8.5))
    gs = fig.add_gridspec(2, 2, height_ratios=[2.5, 1.2], width_ratios=[2.2, 1.0])
    ax = fig.add_subplot(gs[:, 0])
    ax_summary = fig.add_subplot(gs[0, 1])
    ax_note = fig.add_subplot(gs[1, 1])

    for label, mask, color, size, alpha in categories:
        subset = plot.loc[mask]
        if subset.empty:
            continue
        ax.scatter(
            subset["worst_astrometry_quality"],
            subset["distance_pct_diff"],
            s=size,
            alpha=alpha,
            color=color,
            edgecolors="none",
            label=f"{label} ({len(subset):,})",
        )

    for y, color, label in [
        (current, "#b0415a", f"{current:.0f}% current"),
        (25.0, "#7048a8", "25% option"),
    ]:
        ax.axhline(y, color=color, lw=2.0, ls="--")
        ax.text(
            plot["worst_astrometry_quality"].min() * 1.2,
            y * 1.06,
            label,
            color=color,
            weight="bold",
            fontsize=11,
        )
    for x, label in [(0.1, "10% quality"), (0.25, "25% quality"), (1.0, "100%")]:
        ax.axvline(x, color="#555555", lw=1.2, ls=":", alpha=0.85)
        ax.text(x * 1.05, 0.0018, label, rotation=90, fontsize=9, color="#444444")

    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlim(
        max(plot["worst_astrometry_quality"].min() * 0.75, 1e-4),
        min(plot["worst_astrometry_quality"].max() * 1.35, 1e3),
    )
    ax.set_ylim(
        max(plot["distance_pct_diff"].min() * 0.75, 1e-3),
        min(max(plot["distance_pct_diff"].max() * 1.15, 120.0), 200.0),
    )
    ax.set_title(
        "Distance disagreement vs worst astrometry quality",
        fontsize=16,
        weight="bold",
        loc="left",
    )
    ax.set_xlabel("worst Gaia/HIP astrometry quality (fractional uncertainty proxy)")
    ax.set_ylabel("fractional distance disagreement (%)")
    ax.legend(loc="upper left", frameon=True, fontsize=9)

    ax_summary.axis("off")
    rows = []
    for rec in quality_summary.itertuples(index=False):
        if int(rec.rows) == 0:
            continue
        rows.append(
            [
                rec.worst_quality_bin,
                f"{int(rec.rows):,}",
                f"{int(rec.distance_pct_le_10):,}",
                f"{int(rec.distance_pct_10_to_25):,}",
                f"{int(rec.distance_pct_gt_25):,}",
            ]
        )
    table = ax_summary.table(
        cellText=rows,
        colLabels=["worst q", "rows", "<=10", "10-25", ">25"],
        loc="center",
        cellLoc="right",
        colLoc="right",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(8.5)
    table.scale(1.0, 1.25)
    ax_summary.set_title("Quality-bin counts", fontsize=13, weight="bold", loc="left")

    log_x = np.log10(plot["worst_astrometry_quality"].to_numpy(dtype=float))
    log_y = np.log10(plot["distance_pct_diff"].to_numpy(dtype=float))
    corr = float(np.corrcoef(log_x, log_y)[0, 1]) if len(plot) > 1 else float("nan")
    ax_note.axis("off")
    ax_note.text(
        0.0,
        1.0,
        "\n".join(
            [
                "Interpretation",
                "",
                "Lower quality values are better.",
                "HIP quality often dominates the",
                "worst-pair score in this sample.",
                "",
                f"log-log correlation: {corr:.2f}",
            ]
        ),
        va="top",
        ha="left",
        fontsize=11,
    )

    fig.suptitle(
        "Bright-star audit: parallax-quality relationship",
        fontsize=18,
        weight="bold",
        x=0.03,
        ha="left",
    )
    fig.subplots_adjust(left=0.07, right=0.97, top=0.90, bottom=0.10, wspace=0.28)
    fig.savefig(png_path, dpi=180, bbox_inches="tight")
    fig.savefig(svg_path, bbox_inches="tight")
    plt.close(fig)
    return True


def _candidate_record(
    *,
    gaia_rec: pd.Series,
    hip_rec: pd.Series,
    sep_arcsec: float,
    mag_delta: float,
    official_gaia_to_hip: dict[str, str],
    official_hip_to_gaia: dict[str, str],
    override_keys: set[tuple[str, str]],
    auto_sep_arcsec: float,
    auto_mag_delta: float,
    auto_distance_frac_diff: float,
) -> dict[str, Any]:
    gaia_id = str(gaia_rec["source_id"])
    hip_id = str(hip_rec["source_id"])
    gaia_official_hip = official_gaia_to_hip.get(gaia_id)
    hip_official_gaia = official_hip_to_gaia.get(hip_id)
    gaia_has_map = gaia_official_hip is not None
    hip_has_map = hip_official_gaia is not None
    gaia_r = _safe_float(gaia_rec["r_pc"])
    hip_r = _safe_float(hip_rec["r_pc"])
    ratio = _distance_ratio(gaia_r, hip_r)
    frac_diff = abs(gaia_r - hip_r) / max(abs(gaia_r), abs(hip_r)) if ratio else pd.NA
    return {
        "gaia_source_id": gaia_id,
        "hip_source_id": hip_id,
        "decision": "reject",
        "recommended_action": "ignore",
        "severity": "low",
        "reasons": "",
        "separation_arcsec": sep_arcsec,
        "apparent_mag_delta": mag_delta,
        "distance_ratio": ratio if ratio is not None else pd.NA,
        "distance_frac_diff": frac_diff,
        "gaia_ra_deg": _safe_float(gaia_rec["ra_deg"]),
        "gaia_dec_deg": _safe_float(gaia_rec["dec_deg"]),
        "hip_ra_deg": _safe_float(hip_rec["ra_deg"]),
        "hip_dec_deg": _safe_float(hip_rec["dec_deg"]),
        "gaia_r_pc": gaia_r,
        "hip_r_pc": hip_r,
        "gaia_mag_abs": _safe_float(gaia_rec["mag_abs"]),
        "hip_mag_abs": _safe_float(hip_rec["mag_abs"]),
        "gaia_apparent_mag": _safe_float(gaia_rec["apparent_mag"]),
        "hip_apparent_mag": _safe_float(hip_rec["apparent_mag"]),
        "gaia_astrometry_quality": _safe_float(gaia_rec["astrometry_quality"]),
        "hip_astrometry_quality": _safe_float(hip_rec["astrometry_quality"]),
        "gaia_photometry_quality": _safe_float(gaia_rec["photometry_quality"]),
        "hip_photometry_quality": _safe_float(hip_rec["photometry_quality"]),
        "gaia_ruwe": _safe_float(gaia_rec.get("ruwe")),
        "gaia_phot_g_mean_mag": _safe_float(gaia_rec.get("phot_g_mean_mag")),
        "hip_solution_type": _safe_float(hip_rec.get("Sn")),
        "hip_hpmag": _safe_float(hip_rec.get("Hpmag")),
        "gaia_has_official_map": gaia_has_map,
        "hip_has_official_map": hip_has_map,
        "official_conflict": gaia_has_map or hip_has_map,
        "gaia_official_hip_source_id": gaia_official_hip or pd.NA,
        "hip_official_gaia_source_id": hip_official_gaia or pd.NA,
        "gaia_candidate_count": 1,
        "hip_candidate_count": 1,
        "one_to_one": True,
        "within_auto_thresholds": sep_arcsec <= auto_sep_arcsec
        and mag_delta <= auto_mag_delta,
        "within_distance_threshold": math.isfinite(_safe_float(frac_diff))
        and _safe_float(frac_diff) <= auto_distance_frac_diff,
        "overridden": ("gaia", gaia_id) in override_keys
        or ("hip", hip_id) in override_keys,
    }


def _classify_evidence_row(rec: pd.Series) -> tuple[str, str, str, list[str]]:
    reasons = ["close_cross_catalog_position", "similar_apparent_magnitude"]
    if bool(rec["overridden"]):
        reasons.append("manual_override_present")
        return "manual_review", "inspect_override_interaction", "high", reasons
    if bool(rec["official_conflict"]):
        reasons.append("official_crossmatch_conflict")
        return "manual_review", "inspect_conflicting_crossmatch", "high", reasons
    if not bool(rec["one_to_one"]):
        reasons.append("ambiguous_many_to_one_candidate")
        return "manual_review", "inspect_ambiguous_close_pair", "high", reasons
    if bool(rec["within_auto_thresholds"]):
        reasons.append("clean_one_to_one_local_match")
        return "auto_match", "add_supplemental_crossmatch", "medium", reasons
    if bool(rec["within_distance_threshold"]):
        reasons.append("distance_agreement_within_10_percent")
        return "auto_match", "add_supplemental_crossmatch", "medium", reasons
    if math.isfinite(_safe_float(rec["distance_frac_diff"])):
        reasons.append("distance_disagreement_display_safe")
        return "reject", "ignore", "low", reasons
    if _safe_float(rec["separation_arcsec"]) <= OCTREE_REVIEW_MAX_SEP_ARCSEC:
        reasons.append("outside_auto_threshold_but_visually_close")
        return (
            "octree_review",
            "suppress_candidate_duplicate_in_display",
            "medium",
            reasons,
        )
    reasons.append("outside_useful_review_threshold")
    return "reject", "ignore", "low", reasons


def _load_processed_candidates(paths: Iterable[Path], source_name: str) -> pd.DataFrame:
    columns = [
        "source",
        "source_id",
        "ra_deg",
        "dec_deg",
        "r_pc",
        "mag_abs",
        "astrometry_quality",
        "photometry_quality",
        "ruwe",
        "phot_g_mean_mag",
        "Sn",
        "Hpmag",
    ]
    chunks: list[pd.DataFrame] = []
    for batch in _iter_parquet_batches(paths, columns):
        source = batch["source"].fillna(source_name).astype(str)
        mask = source.eq(source_name)
        numeric = batch.copy()
        for col in [
            "ra_deg",
            "dec_deg",
            "r_pc",
            "mag_abs",
            "astrometry_quality",
            "photometry_quality",
            "ruwe",
            "phot_g_mean_mag",
            "Sn",
            "Hpmag",
        ]:
            numeric[col] = pd.to_numeric(numeric[col], errors="coerce")
        finite = (
            np.isfinite(numeric["ra_deg"])
            & np.isfinite(numeric["dec_deg"])
            & np.isfinite(numeric["r_pc"])
            & np.isfinite(numeric["mag_abs"])
            & (numeric["r_pc"] > 0)
        )
        out = numeric.loc[mask & finite].copy()
        if out.empty:
            continue
        out["source"] = source.loc[out.index].to_numpy()
        out["source_id"] = batch.loc[out.index, "source_id"].astype(str).to_numpy()
        out["apparent_mag"] = out["mag_abs"] + 5.0 * np.log10(out["r_pc"]) - 5.0
        out = out.loc[np.isfinite(out["apparent_mag"])]
        if not out.empty:
            chunks.append(
                out[
                    [
                        "source",
                        "source_id",
                        "ra_deg",
                        "dec_deg",
                        "r_pc",
                        "mag_abs",
                        "apparent_mag",
                        "astrometry_quality",
                        "photometry_quality",
                        "ruwe",
                        "phot_g_mean_mag",
                        "Sn",
                        "Hpmag",
                    ]
                ]
            )
    if not chunks:
        return pd.DataFrame(
            columns=[
                "source",
                "source_id",
                "ra_deg",
                "dec_deg",
                "r_pc",
                "mag_abs",
                "apparent_mag",
                "astrometry_quality",
                "photometry_quality",
                "ruwe",
                "phot_g_mean_mag",
                "Sn",
                "Hpmag",
            ]
        )
    return pd.concat(chunks, ignore_index=True)


def _read_crossmatch(path: Path) -> pd.DataFrame:
    return _normalize_crossmatch_frame(pq.read_table(path).to_pandas())


def _normalize_crossmatch_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return empty_gaia_hip_mapping()
    out = df.copy()
    if "mapping_source" not in out:
        out["mapping_source"] = MAPPING_SOURCE_HIPPARCOS2_BEST_NEIGHBOUR
    if "number_of_neighbours" not in out:
        out["number_of_neighbours"] = np.int16(0)
    if "angular_distance" not in out:
        out["angular_distance"] = np.float32(np.nan)
    out["gaia_source_id"] = _parse_uint_series(out["gaia_source_id"]).astype("uint64")
    out["hip_source_id"] = _parse_uint_series(out["hip_source_id"]).astype("uint64")
    out["mapping_source"] = out["mapping_source"].fillna("").astype(str)
    out["number_of_neighbours"] = (
        pd.to_numeric(out["number_of_neighbours"], errors="coerce")
        .fillna(0)
        .astype(np.int16)
    )
    out["angular_distance"] = pd.to_numeric(
        out["angular_distance"], errors="coerce"
    ).astype(np.float32)
    return out[GAIA_HIP_MAP_COLS].sort_values(
        ["gaia_source_id", "hip_source_id"],
        kind="mergesort",
        ignore_index=True,
    )


def _iter_parquet_batches(paths: Iterable[Path], columns: list[str]):
    for path in paths:
        parquet = pq.ParquetFile(path)
        present = [c for c in columns if c in parquet.schema_arrow.names]
        if not present:
            continue
        for batch in parquet.iter_batches(columns=present, batch_size=BATCH_SIZE):
            df = batch.to_pandas()
            for col in columns:
                if col not in df:
                    df[col] = pd.NA
            yield df[columns]


def _load_override_keys(path: Path) -> set[tuple[str, str]]:
    df = pq.read_table(path, columns=["source", "source_id", "action"]).to_pandas()
    if df.empty:
        return set()
    return {
        (str(rec.source), str(rec.source_id))
        for rec in df.itertuples(index=False)
        if str(rec.action) in {"replace", "drop", "add"}
    }


def _unit_vectors(ra_deg: pd.Series, dec_deg: pd.Series) -> np.ndarray:
    ra = np.radians(pd.to_numeric(ra_deg, errors="coerce").to_numpy(dtype=float))
    dec = np.radians(pd.to_numeric(dec_deg, errors="coerce").to_numpy(dtype=float))
    cos_dec = np.cos(dec)
    return np.column_stack(
        [
            cos_dec * np.cos(ra),
            cos_dec * np.sin(ra),
            np.sin(dec),
        ]
    )


def _require_ckdtree():
    try:
        from scipy.spatial import cKDTree
    except ImportError as exc:
        raise RuntimeError(
            "Audit matching requires the optional audit dependency group. "
            "Install or run with `uv sync --group audit` or "
            "`uv run --group audit ...`."
        ) from exc
    return cKDTree


def _parse_uint_series(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    values = numeric.to_numpy(dtype=float, na_value=np.nan, copy=False)
    if np.any(~np.isfinite(values)):
        raise ValueError("crossmatch contains non-finite IDs")
    if np.any(values <= 0) or np.any(np.floor(values) != values):
        raise ValueError("crossmatch contains non-positive or non-integer IDs")
    return numeric.astype("uint64")


def _safe_float(value: Any) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return math.nan
    return out if math.isfinite(out) else math.nan


def _distance_ratio(left: float, right: float) -> float | None:
    if not math.isfinite(left) or not math.isfinite(right) or left <= 0 or right <= 0:
        return None
    return max(left, right) / min(left, right)


def _empty_match_evidence() -> pd.DataFrame:
    return pd.DataFrame(columns=MATCH_EVIDENCE_COLS)


def _octree_record(**kwargs: Any) -> dict[str, Any]:
    rec = dict.fromkeys(OCTREE_REVIEW_COLS, pd.NA)
    rec.update(kwargs)
    rec["source"] = str(rec["source"])
    rec["source_id"] = str(rec["source_id"])
    if not pd.isna(rec["linked_source"]):
        rec["linked_source"] = str(rec["linked_source"])
    if not pd.isna(rec["linked_source_id"]):
        rec["linked_source_id"] = str(rec["linked_source_id"])
    return rec


def _manual_record(**kwargs: Any) -> dict[str, Any]:
    rec = dict.fromkeys(MANUAL_CANDIDATE_COLS, pd.NA)
    rec.update(kwargs)
    if not pd.isna(rec["source"]):
        rec["source"] = str(rec["source"])
    if not pd.isna(rec["source_id"]):
        rec["source_id"] = str(rec["source_id"])
    return rec


def _prepare_output_dir(path: Path, *, force: bool) -> None:
    if path.exists():
        if any(path.iterdir()) and not force:
            raise FileExistsError(str(path))
        if force:
            shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def _prepare_report_outputs(
    audit_dir: Path,
    sidecar_output_dir: Path,
    *,
    force: bool,
) -> None:
    audit_dir.mkdir(parents=True, exist_ok=True)
    for path in [
        audit_dir / OCTREE_REVIEW_FILENAME,
        audit_dir / MANUAL_CANDIDATES_FILENAME,
        audit_dir / MANUAL_CANDIDATES_CSV_FILENAME,
        audit_dir / AUDIT_REPORT_FILENAME,
    ]:
        if path.exists():
            if not force:
                raise FileExistsError(str(path))
            path.unlink()
    sidecar_root = sidecar_output_dir / "octree_review"
    if sidecar_root.exists():
        if any(sidecar_root.iterdir()) and not force:
            raise FileExistsError(str(sidecar_root))
        if force:
            shutil.rmtree(sidecar_root)


def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, str(path), compression="zstd")


def _write_json(data: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fp:
        json.dump(data, fp, indent=2, sort_keys=True)


def _value_counts(df: pd.DataFrame, column: str) -> dict[str, int]:
    if df.empty or column not in df:
        return {}
    return {str(key): int(value) for key, value in df[column].value_counts().items()}
