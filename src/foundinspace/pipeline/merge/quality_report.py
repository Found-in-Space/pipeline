"""Inline and retrospective quality audits for suspicious merge results."""

from __future__ import annotations

import json
import math
from collections import Counter
from collections.abc import Iterable, Iterator
from dataclasses import asdict, dataclass
from numbers import Integral
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ISSUES_FILENAME = "merge_quality_issues.parquet"
REPORT_FILENAME = "merge_quality_report.json"

BATCH_SIZE = 250_000
HIP_STANDARD_SOLUTION = 5
DEFAULT_ISSUE_BUFFER_ROWS = 4_096

DEFAULT_DISTANCE_DISAGREEMENT_THRESHOLD = 0.25
DEFAULT_SEVERE_DISTANCE_RATIO = 2.0
DEFAULT_RUWE_THRESHOLD = 1.4
DEFAULT_HIGH_ASTROMETRY_QUALITY = 1.0
DEFAULT_EXTREME_ABS_MAG = -10.0
DEFAULT_LUMINOUS_ABS_MAG = -8.0
DEFAULT_REMOTE_LUMINOUS_PC = 20_000.0

QUALITY_ISSUE_COLS = [
    "issue_type",
    "severity",
    "reasons",
    "source",
    "source_id",
    "label",
    "gaia_source_id",
    "hip_source_id",
    "winner_catalog",
    "winner_source_id",
    "tie_break_reason",
    "distance_ratio",
    "distance_frac_diff",
    "spatial_separation_pc",
    "mag_abs_delta",
    "apparent_mag_delta",
    "gaia_apparent_mag",
    "hip_apparent_mag",
    "gaia_r_pc",
    "hip_r_pc",
    "gaia_mag_abs",
    "hip_mag_abs",
    "gaia_ra_deg",
    "gaia_dec_deg",
    "hip_ra_deg",
    "hip_dec_deg",
    "merged_ra_deg",
    "merged_dec_deg",
    "merged_r_pc",
    "merged_mag_abs",
    "merged_teff",
    "gaia_score",
    "hip_score",
    "astrometry_quality",
    "photometry_quality",
    "gaia_ruwe",
    "hip_solution_type",
    "number_of_neighbours",
    "angular_distance_arcsec",
    "overridden",
]

_QUALITY_STRING_COLS = {
    "issue_type",
    "severity",
    "reasons",
    "source",
    "source_id",
    "label",
    "gaia_source_id",
    "hip_source_id",
    "winner_catalog",
    "winner_source_id",
    "tie_break_reason",
}
_QUALITY_BOOL_COLS = {"overridden"}
QUALITY_ISSUE_SCHEMA = pa.schema(
    [
        pa.field(
            column,
            (
                pa.string()
                if column in _QUALITY_STRING_COLS
                else pa.bool_()
                if column in _QUALITY_BOOL_COLS
                else pa.float64()
            ),
        )
        for column in QUALITY_ISSUE_COLS
    ]
)


@dataclass
class QualityReport:
    """Small JSON summary for the post-merge quality audit."""

    gaia_dir: str
    hip_path: str
    crossmatch_path: str
    overrides_path: str
    merge_dir: str
    issues_path: str
    thresholds: dict[str, float]
    score_decisions: int
    override_decisions: int
    override_targets: int
    matched_pair_issues: int
    merged_row_issues: int
    total_issues: int
    issue_counts_by_type: dict[str, int]
    issue_counts_by_severity: dict[str, int]
    execution_mode: str = "post_merge"
    max_buffered_issues: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class StreamingQualityAudit:
    """Collect merge quality findings without retaining the catalogue or issues."""

    def __init__(
        self,
        *,
        gaia_dir: Path,
        hip_path: Path,
        crossmatch_path: Path,
        overrides_path: Path,
        merge_dir: Path,
        override_keys: set[tuple[str, str]],
        distance_disagreement_threshold: float = (
            DEFAULT_DISTANCE_DISAGREEMENT_THRESHOLD
        ),
        severe_distance_ratio: float = DEFAULT_SEVERE_DISTANCE_RATIO,
        ruwe_threshold: float = DEFAULT_RUWE_THRESHOLD,
        high_astrometry_quality: float = DEFAULT_HIGH_ASTROMETRY_QUALITY,
        extreme_abs_mag: float = DEFAULT_EXTREME_ABS_MAG,
        luminous_abs_mag: float = DEFAULT_LUMINOUS_ABS_MAG,
        remote_luminous_pc: float = DEFAULT_REMOTE_LUMINOUS_PC,
        issue_buffer_rows: int = DEFAULT_ISSUE_BUFFER_ROWS,
    ) -> None:
        if issue_buffer_rows < 1:
            raise ValueError("issue_buffer_rows must be >= 1")

        self.gaia_dir = Path(gaia_dir)
        self.hip_path = Path(hip_path)
        self.crossmatch_path = Path(crossmatch_path)
        self.overrides_path = Path(overrides_path)
        self.merge_dir = Path(merge_dir)
        self.issues_path = self.merge_dir / ISSUES_FILENAME
        self.report_path = self.merge_dir / REPORT_FILENAME
        self.override_keys = override_keys
        self.thresholds = {
            "distance_disagreement_threshold": distance_disagreement_threshold,
            "severe_distance_ratio": severe_distance_ratio,
            "ruwe_threshold": ruwe_threshold,
            "high_astrometry_quality": high_astrometry_quality,
            "extreme_abs_mag": extreme_abs_mag,
            "luminous_abs_mag": luminous_abs_mag,
            "remote_luminous_pc": remote_luminous_pc,
        }
        self.issue_buffer_rows = issue_buffer_rows
        self._buffer: list[dict[str, Any]] = []
        self._writer: pq.ParquetWriter | None = None
        self._counts_by_type: Counter[str] = Counter()
        self._counts_by_severity: Counter[str] = Counter()
        self._score_decisions = 0
        self._matched_pair_issues = 0
        self._merged_row_issues = 0
        self._max_buffered_issues = 0
        self._finalized = False

    def observe_matched_pair(
        self,
        gaia_rec: dict[str, Any],
        hip_rec: dict[str, Any],
        *,
        winner_catalog: str,
        winner_source_id: str,
        tie_break_reason: str,
        number_of_neighbours: int | None,
        angular_distance_arcsec: float,
    ) -> None:
        """Evaluate a pair while both staged candidates are already in memory."""
        self._score_decisions += 1
        issue = _matched_pair_issue_record(
            gaia_rec,
            hip_rec,
            winner_catalog=winner_catalog,
            winner_source_id=winner_source_id,
            tie_break_reason=tie_break_reason,
            number_of_neighbours=number_of_neighbours,
            angular_distance_arcsec=angular_distance_arcsec,
            distance_disagreement_threshold=self.thresholds[
                "distance_disagreement_threshold"
            ],
            severe_distance_ratio=self.thresholds["severe_distance_ratio"],
            ruwe_threshold=self.thresholds["ruwe_threshold"],
        )
        if issue is not None:
            self._matched_pair_issues += 1
            self._append(issue)

    def observe_merged_rows(self, rows: pd.DataFrame) -> None:
        """Evaluate one already-materialized merge output batch before its write."""
        for issue in _iter_merged_row_issue_records(
            rows,
            override_keys=self.override_keys,
            high_astrometry_quality=self.thresholds["high_astrometry_quality"],
            extreme_abs_mag=self.thresholds["extreme_abs_mag"],
            luminous_abs_mag=self.thresholds["luminous_abs_mag"],
            remote_luminous_pc=self.thresholds["remote_luminous_pc"],
        ):
            self._merged_row_issues += 1
            self._append(issue)

    def finalize(
        self,
        *,
        decisions_rows: int,
    ) -> QualityReport:
        """Close the bounded writer and emit the aggregate JSON report."""
        if self._finalized:
            raise RuntimeError("Quality audit has already been finalized")
        self._flush()
        if self._writer is None:
            pq.write_table(
                _issues_to_table([]),
                str(self.issues_path),
                compression="zstd",
            )
        else:
            self._writer.close()
            self._writer = None
        self._finalized = True

        total_issues = self._matched_pair_issues + self._merged_row_issues
        report = QualityReport(
            gaia_dir=str(self.gaia_dir),
            hip_path=str(self.hip_path),
            crossmatch_path=str(self.crossmatch_path),
            overrides_path=str(self.overrides_path),
            merge_dir=str(self.merge_dir),
            issues_path=str(self.issues_path),
            thresholds=self.thresholds,
            score_decisions=self._score_decisions,
            override_decisions=decisions_rows - self._score_decisions,
            override_targets=len(self.override_keys),
            matched_pair_issues=self._matched_pair_issues,
            merged_row_issues=self._merged_row_issues,
            total_issues=total_issues,
            issue_counts_by_type=dict(sorted(self._counts_by_type.items())),
            issue_counts_by_severity=dict(sorted(self._counts_by_severity.items())),
            execution_mode="inline",
            max_buffered_issues=self._max_buffered_issues,
        )
        with self.report_path.open("w", encoding="utf-8") as fp:
            json.dump(report.to_dict(), fp, indent=2, sort_keys=True)
            fp.write("\n")
        return report

    def _append(self, issue: dict[str, Any]) -> None:
        self._counts_by_type[str(issue["issue_type"])] += 1
        self._counts_by_severity[str(issue["severity"])] += 1
        self._buffer.append(issue)
        self._max_buffered_issues = max(
            self._max_buffered_issues,
            len(self._buffer),
        )
        if len(self._buffer) >= self.issue_buffer_rows:
            self._flush()

    def _flush(self) -> None:
        if not self._buffer:
            return
        table = _issues_to_table(self._buffer)
        if self._writer is None:
            self._writer = pq.ParquetWriter(
                str(self.issues_path),
                QUALITY_ISSUE_SCHEMA,
                compression="zstd",
            )
        self._writer.write_table(table)
        self._buffer.clear()


def run_quality_report(
    *,
    gaia_dir: Path,
    hip_path: Path,
    crossmatch_path: Path,
    overrides_path: Path,
    merge_dir: Path,
    identifiers_path: Path | None = None,
    output_dir: Path | None = None,
    distance_disagreement_threshold: float = (DEFAULT_DISTANCE_DISAGREEMENT_THRESHOLD),
    severe_distance_ratio: float = DEFAULT_SEVERE_DISTANCE_RATIO,
    ruwe_threshold: float = DEFAULT_RUWE_THRESHOLD,
    high_astrometry_quality: float = DEFAULT_HIGH_ASTROMETRY_QUALITY,
    extreme_abs_mag: float = DEFAULT_EXTREME_ABS_MAG,
    luminous_abs_mag: float = DEFAULT_LUMINOUS_ABS_MAG,
    remote_luminous_pc: float = DEFAULT_REMOTE_LUMINOUS_PC,
    force: bool = False,
) -> QualityReport:
    """Write a JSON quality summary plus Parquet table of suspicious stars."""
    gaia_dir = Path(gaia_dir).expanduser()
    hip_path = Path(hip_path).expanduser()
    crossmatch_path = Path(crossmatch_path).expanduser()
    overrides_path = Path(overrides_path).expanduser()
    merge_dir = Path(merge_dir).expanduser()
    identifiers_path = Path(identifiers_path).expanduser() if identifiers_path else None
    output_dir = Path(output_dir).expanduser() if output_dir else merge_dir

    decisions_path = merge_dir / "merge_decisions.parquet"
    shards_root = merge_dir / "healpix"
    issues_path = output_dir / ISSUES_FILENAME
    report_path = output_dir / REPORT_FILENAME

    for path in (hip_path, crossmatch_path, overrides_path, decisions_path):
        if not path.is_file():
            raise FileNotFoundError(str(path))
    if not gaia_dir.is_dir():
        raise FileNotFoundError(str(gaia_dir))
    if not shards_root.is_dir():
        raise FileNotFoundError(str(shards_root))

    output_dir.mkdir(parents=True, exist_ok=True)
    for path in (issues_path, report_path):
        if path.exists() and not force:
            raise FileExistsError(str(path))

    override_keys = _load_override_keys(overrides_path)
    labels = _load_labels(identifiers_path)
    decisions = _read_parquet_columns(
        decisions_path,
        [
            "decision_type",
            "gaia_source_id",
            "hip_source_id",
            "winner_catalog",
            "winner_source_id",
            "gaia_score",
            "hip_score",
            "tie_break_reason",
            "number_of_neighbours",
            "angular_distance_arcsec",
            "gaia_ruwe",
            "gaia_phot_g_mean_mag",
            "hip_solution_type",
            "hip_apparent_mag",
        ],
    )

    score_decisions = decisions.loc[
        decisions["decision_type"].astype(str).eq("score")
    ].copy()
    matched_issues = _matched_pair_issues(
        score_decisions,
        gaia_dir=gaia_dir,
        hip_path=hip_path,
        override_keys=override_keys,
        distance_disagreement_threshold=distance_disagreement_threshold,
        severe_distance_ratio=severe_distance_ratio,
        ruwe_threshold=ruwe_threshold,
    )
    merged_issues = _merged_row_issues(
        shards_root,
        override_keys=override_keys,
        high_astrometry_quality=high_astrometry_quality,
        extreme_abs_mag=extreme_abs_mag,
        luminous_abs_mag=luminous_abs_mag,
        remote_luminous_pc=remote_luminous_pc,
    )

    issues = pd.DataFrame(
        [*matched_issues, *merged_issues],
        columns=QUALITY_ISSUE_COLS,
    )
    issues = _attach_labels(issues, labels)
    pq.write_table(
        pa.Table.from_pandas(issues, preserve_index=False),
        str(issues_path),
        compression="zstd",
    )

    report = QualityReport(
        gaia_dir=str(gaia_dir),
        hip_path=str(hip_path),
        crossmatch_path=str(crossmatch_path),
        overrides_path=str(overrides_path),
        merge_dir=str(merge_dir),
        issues_path=str(issues_path),
        thresholds={
            "distance_disagreement_threshold": distance_disagreement_threshold,
            "severe_distance_ratio": severe_distance_ratio,
            "ruwe_threshold": ruwe_threshold,
            "high_astrometry_quality": high_astrometry_quality,
            "extreme_abs_mag": extreme_abs_mag,
            "luminous_abs_mag": luminous_abs_mag,
            "remote_luminous_pc": remote_luminous_pc,
        },
        score_decisions=int(len(score_decisions)),
        override_decisions=int(len(decisions) - len(score_decisions)),
        override_targets=int(len(override_keys)),
        matched_pair_issues=int(len(matched_issues)),
        merged_row_issues=int(len(merged_issues)),
        total_issues=int(len(issues)),
        issue_counts_by_type=_value_counts(issues, "issue_type"),
        issue_counts_by_severity=_value_counts(issues, "severity"),
    )
    with report_path.open("w", encoding="utf-8") as fp:
        json.dump(report.to_dict(), fp, indent=2, sort_keys=True)
    return report


def _matched_pair_issues(
    decisions: pd.DataFrame,
    *,
    gaia_dir: Path,
    hip_path: Path,
    override_keys: set[tuple[str, str]],
    distance_disagreement_threshold: float,
    severe_distance_ratio: float,
    ruwe_threshold: float,
) -> list[dict[str, Any]]:
    if decisions.empty:
        return []

    decisions = decisions.copy()
    decisions["gaia_source_id_num"] = _parse_uint_series(decisions["gaia_source_id"])
    decisions["hip_source_id_num"] = _parse_uint_series(decisions["hip_source_id"])
    decisions = decisions.dropna(subset=["gaia_source_id_num", "hip_source_id_num"])
    gaia_ids = _int_set(decisions["gaia_source_id_num"])
    hip_ids = _int_set(decisions["hip_source_id_num"])

    gaia = _read_matching_rows(
        sorted(gaia_dir.glob("*.parquet")),
        "source_id",
        gaia_ids,
        [
            "source_id",
            "x_icrs_pc",
            "y_icrs_pc",
            "z_icrs_pc",
            "r_pc",
            "mag_abs",
            "astrometry_quality",
            "photometry_quality",
            "ruwe",
            "phot_g_mean_mag",
        ],
    ).rename(
        columns={
            "source_id": "gaia_source_id_num",
            "x_icrs_pc": "gaia_x_icrs_pc",
            "y_icrs_pc": "gaia_y_icrs_pc",
            "z_icrs_pc": "gaia_z_icrs_pc",
            "r_pc": "gaia_r_pc",
            "mag_abs": "gaia_mag_abs",
            "astrometry_quality": "gaia_astrometry_quality",
            "photometry_quality": "gaia_photometry_quality",
            "ruwe": "gaia_ruwe_row",
            "phot_g_mean_mag": "gaia_phot_g_mean_mag_row",
        }
    )
    hip = _read_matching_rows(
        [hip_path],
        "source_id",
        hip_ids,
        [
            "source_id",
            "x_icrs_pc",
            "y_icrs_pc",
            "z_icrs_pc",
            "r_pc",
            "mag_abs",
            "astrometry_quality",
            "photometry_quality",
            "Sn",
            "Hpmag",
        ],
    ).rename(
        columns={
            "source_id": "hip_source_id_num",
            "x_icrs_pc": "hip_x_icrs_pc",
            "y_icrs_pc": "hip_y_icrs_pc",
            "z_icrs_pc": "hip_z_icrs_pc",
            "r_pc": "hip_r_pc",
            "mag_abs": "hip_mag_abs",
            "astrometry_quality": "hip_astrometry_quality",
            "photometry_quality": "hip_photometry_quality",
            "Sn": "hip_solution_type_row",
            "Hpmag": "hip_apparent_mag_row",
        }
    )

    pairs = decisions.merge(gaia, on="gaia_source_id_num", how="left").merge(
        hip,
        on="hip_source_id_num",
        how="left",
    )
    pairs["gaia_score_use"] = _coalesce_numeric(
        pairs.get("gaia_score"),
        pairs.get("gaia_astrometry_quality"),
    )
    pairs["hip_score_use"] = _coalesce_numeric(
        pairs.get("hip_score"),
        pairs.get("hip_astrometry_quality"),
    )
    pairs["gaia_ruwe_use"] = _coalesce_numeric(
        pairs.get("gaia_ruwe"),
        pairs.get("gaia_ruwe_row"),
    )
    pairs["hip_solution_type_use"] = _coalesce_numeric(
        pairs.get("hip_solution_type"),
        pairs.get("hip_solution_type_row"),
    )

    gaia_r = pd.to_numeric(pairs["gaia_r_pc"], errors="coerce")
    hip_r = pd.to_numeric(pairs["hip_r_pc"], errors="coerce")
    nearer = pd.concat([gaia_r, hip_r], axis=1).min(axis=1)
    farther = pd.concat([gaia_r, hip_r], axis=1).max(axis=1)
    pairs["distance_ratio"] = farther / nearer
    pairs["distance_frac_diff"] = pairs["distance_ratio"] - 1.0
    dx = pd.to_numeric(pairs["gaia_x_icrs_pc"], errors="coerce") - pd.to_numeric(
        pairs["hip_x_icrs_pc"], errors="coerce"
    )
    dy = pd.to_numeric(pairs["gaia_y_icrs_pc"], errors="coerce") - pd.to_numeric(
        pairs["hip_y_icrs_pc"], errors="coerce"
    )
    dz = pd.to_numeric(pairs["gaia_z_icrs_pc"], errors="coerce") - pd.to_numeric(
        pairs["hip_z_icrs_pc"], errors="coerce"
    )
    pairs["spatial_separation_pc"] = np.sqrt(dx * dx + dy * dy + dz * dz)
    pairs["mag_abs_delta"] = pd.to_numeric(
        pairs["gaia_mag_abs"], errors="coerce"
    ) - pd.to_numeric(pairs["hip_mag_abs"], errors="coerce")
    gaia_apparent = pd.to_numeric(pairs["gaia_mag_abs"], errors="coerce") + (
        5.0 * np.log10(gaia_r / 10.0)
    )
    hip_apparent = pd.to_numeric(pairs["hip_mag_abs"], errors="coerce") + (
        5.0 * np.log10(hip_r / 10.0)
    )
    pairs["gaia_apparent_mag"] = gaia_apparent
    pairs["hip_apparent_mag"] = hip_apparent
    pairs["apparent_mag_delta"] = gaia_apparent - hip_apparent

    gaia_better = pairs["gaia_score_use"] < pairs["hip_score_use"]
    hip_better = pairs["hip_score_use"] < pairs["gaia_score_use"]
    winner = pairs["winner_catalog"].astype(str)
    pairs["winner_not_lowest_score"] = (gaia_better & ~winner.eq("gaia")) | (
        hip_better & ~winner.eq("hip")
    )
    pairs["gaia_ruwe_high"] = pairs["gaia_ruwe_use"] > ruwe_threshold
    pairs["hip_non_standard"] = pairs["hip_solution_type_use"].notna() & (
        pairs["hip_solution_type_use"] != HIP_STANDARD_SOLUTION
    )
    pairs["override_covered"] = [
        ("gaia", str(int(g))) in override_keys or ("hip", str(int(h))) in override_keys
        for g, h in zip(
            pairs["gaia_source_id_num"],
            pairs["hip_source_id_num"],
            strict=True,
        )
    ]

    distance_disagreement = (
        pairs["distance_frac_diff"] > distance_disagreement_threshold
    )
    both_catalogs_suspect = pairs["gaia_ruwe_high"] & pairs["hip_non_standard"]
    severe_distance = pairs["distance_ratio"] >= severe_distance_ratio
    gaia_winner_suspect = pairs["gaia_ruwe_high"] & (
        pairs["hip_non_standard"] | severe_distance
    )
    mask = (
        distance_disagreement
        & ~pairs["override_covered"]
        & (
            pairs["winner_not_lowest_score"]
            | both_catalogs_suspect
            | gaia_winner_suspect
        )
    )

    issues: list[dict[str, Any]] = []
    for rec in pairs.loc[mask].to_dict(orient="records"):
        reasons = ["distance_disagreement"]
        if rec.get("winner_not_lowest_score"):
            reasons.append("winner_not_lowest_astrometry_score")
        if rec.get("gaia_ruwe_high"):
            reasons.append("gaia_high_ruwe")
        if rec.get("hip_non_standard"):
            reasons.append("hip_non_standard_solution")
        if _safe_float(rec.get("distance_ratio")) >= severe_distance_ratio:
            reasons.append("severe_distance_disagreement")

        severity = "medium"
        if (
            rec.get("winner_not_lowest_score")
            or (rec.get("gaia_ruwe_high") and rec.get("hip_non_standard"))
            or _safe_float(rec.get("distance_ratio")) >= severe_distance_ratio
        ):
            severity = "high"

        issues.append(
            _issue_record(
                issue_type="matched_pair_conflict",
                severity=severity,
                reasons=reasons,
                source="hip",
                source_id=_string_id(rec.get("hip_source_id_num")),
                gaia_source_id=_string_id(rec.get("gaia_source_id_num")),
                hip_source_id=_string_id(rec.get("hip_source_id_num")),
                winner_catalog=rec.get("winner_catalog"),
                winner_source_id=rec.get("winner_source_id"),
                tie_break_reason=rec.get("tie_break_reason"),
                distance_ratio=rec.get("distance_ratio"),
                distance_frac_diff=rec.get("distance_frac_diff"),
                spatial_separation_pc=rec.get("spatial_separation_pc"),
                mag_abs_delta=rec.get("mag_abs_delta"),
                apparent_mag_delta=rec.get("apparent_mag_delta"),
                gaia_apparent_mag=rec.get("gaia_apparent_mag"),
                hip_apparent_mag=rec.get("hip_apparent_mag"),
                gaia_r_pc=rec.get("gaia_r_pc"),
                hip_r_pc=rec.get("hip_r_pc"),
                gaia_mag_abs=rec.get("gaia_mag_abs"),
                hip_mag_abs=rec.get("hip_mag_abs"),
                gaia_score=rec.get("gaia_score_use"),
                hip_score=rec.get("hip_score_use"),
                gaia_ruwe=rec.get("gaia_ruwe_use"),
                hip_solution_type=rec.get("hip_solution_type_use"),
                number_of_neighbours=rec.get("number_of_neighbours"),
                angular_distance_arcsec=rec.get("angular_distance_arcsec"),
            )
        )
    return issues


def _merged_row_issues(
    shards_root: Path,
    *,
    override_keys: set[tuple[str, str]],
    high_astrometry_quality: float,
    extreme_abs_mag: float,
    luminous_abs_mag: float,
    remote_luminous_pc: float,
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    columns = [
        "source",
        "source_id",
        "ra_deg",
        "dec_deg",
        "r_pc",
        "mag_abs",
        "teff",
        "astrometry_quality",
        "photometry_quality",
    ]
    for batch in _iter_parquet_batches(
        sorted(shards_root.glob("*/*.parquet")), columns
    ):
        issues.extend(
            _iter_merged_row_issue_records(
                batch,
                override_keys=override_keys,
                high_astrometry_quality=high_astrometry_quality,
                extreme_abs_mag=extreme_abs_mag,
                luminous_abs_mag=luminous_abs_mag,
                remote_luminous_pc=remote_luminous_pc,
            )
        )
    return issues


def _matched_pair_issue_record(
    gaia_rec: dict[str, Any],
    hip_rec: dict[str, Any],
    *,
    winner_catalog: str,
    winner_source_id: str,
    tie_break_reason: str,
    number_of_neighbours: int | None,
    angular_distance_arcsec: float,
    distance_disagreement_threshold: float,
    severe_distance_ratio: float,
    ruwe_threshold: float,
) -> dict[str, Any] | None:
    gaia_r = _safe_float(gaia_rec.get("r_pc"))
    hip_r = _safe_float(hip_rec.get("r_pc"))
    if math.isfinite(gaia_r) and math.isfinite(hip_r) and gaia_r > 0.0 and hip_r > 0.0:
        distance_ratio = max(gaia_r, hip_r) / min(gaia_r, hip_r)
        distance_frac_diff = distance_ratio - 1.0
    else:
        distance_ratio = math.nan
        distance_frac_diff = math.nan

    gaia_score = _safe_score(gaia_rec.get("astrometry_quality"))
    hip_score = _safe_score(hip_rec.get("astrometry_quality"))
    winner_not_lowest_score = (gaia_score < hip_score and winner_catalog != "gaia") or (
        hip_score < gaia_score and winner_catalog != "hip"
    )
    gaia_ruwe = _safe_float(gaia_rec.get("ruwe"))
    hip_solution_type = _parse_optional_int(hip_rec.get("Sn"))
    gaia_ruwe_high = gaia_ruwe > ruwe_threshold
    hip_non_standard = (
        hip_solution_type is not None and hip_solution_type != HIP_STANDARD_SOLUTION
    )
    severe_distance = distance_ratio >= severe_distance_ratio
    both_catalogs_suspect = gaia_ruwe_high and hip_non_standard
    gaia_winner_suspect = gaia_ruwe_high and (hip_non_standard or severe_distance)
    if not (
        distance_frac_diff > distance_disagreement_threshold
        and (winner_not_lowest_score or both_catalogs_suspect or gaia_winner_suspect)
    ):
        return None

    reasons = ["distance_disagreement"]
    if winner_not_lowest_score:
        reasons.append("winner_not_lowest_astrometry_score")
    if gaia_ruwe_high:
        reasons.append("gaia_high_ruwe")
    if hip_non_standard:
        reasons.append("hip_non_standard_solution")
    if severe_distance:
        reasons.append("severe_distance_disagreement")
    severity = (
        "high"
        if winner_not_lowest_score or both_catalogs_suspect or severe_distance
        else "medium"
    )

    gaia_xyz = [_safe_float(gaia_rec.get(name)) for name in _XYZ_COLUMNS]
    hip_xyz = [_safe_float(hip_rec.get(name)) for name in _XYZ_COLUMNS]
    if all(math.isfinite(value) for value in [*gaia_xyz, *hip_xyz]):
        spatial_separation_pc = math.sqrt(
            sum((gaia - hip) ** 2 for gaia, hip in zip(gaia_xyz, hip_xyz, strict=True))
        )
    else:
        spatial_separation_pc = math.nan

    gaia_mag_abs = _safe_float(gaia_rec.get("mag_abs"))
    hip_mag_abs = _safe_float(hip_rec.get("mag_abs"))
    mag_abs_delta = gaia_mag_abs - hip_mag_abs
    gaia_apparent_mag = _apparent_magnitude(gaia_mag_abs, gaia_r)
    hip_apparent_mag = _apparent_magnitude(hip_mag_abs, hip_r)

    gaia_id = _string_id(gaia_rec.get("source_id"))
    hip_id = _string_id(hip_rec.get("source_id"))
    return _issue_record(
        issue_type="matched_pair_conflict",
        severity=severity,
        reasons=reasons,
        source="hip",
        source_id=hip_id,
        gaia_source_id=gaia_id,
        hip_source_id=hip_id,
        winner_catalog=winner_catalog,
        winner_source_id=winner_source_id,
        tie_break_reason=tie_break_reason or pd.NA,
        distance_ratio=distance_ratio,
        distance_frac_diff=distance_frac_diff,
        spatial_separation_pc=spatial_separation_pc,
        mag_abs_delta=mag_abs_delta,
        apparent_mag_delta=gaia_apparent_mag - hip_apparent_mag,
        gaia_apparent_mag=gaia_apparent_mag,
        hip_apparent_mag=hip_apparent_mag,
        gaia_r_pc=gaia_r,
        hip_r_pc=hip_r,
        gaia_mag_abs=gaia_mag_abs,
        hip_mag_abs=hip_mag_abs,
        gaia_score=gaia_score,
        hip_score=hip_score,
        gaia_ruwe=gaia_ruwe,
        hip_solution_type=hip_solution_type,
        number_of_neighbours=number_of_neighbours,
        angular_distance_arcsec=angular_distance_arcsec,
    )


_XYZ_COLUMNS = ("x_icrs_pc", "y_icrs_pc", "z_icrs_pc")


def _iter_merged_row_issue_records(
    batch: pd.DataFrame,
    *,
    override_keys: set[tuple[str, str]],
    high_astrometry_quality: float,
    extreme_abs_mag: float,
    luminous_abs_mag: float,
    remote_luminous_pc: float,
) -> Iterator[dict[str, Any]]:
    if batch.empty:
        return

    # These fixed-size numeric arrays are bounded by the merge batch. Crucially,
    # the override lookup and Python record creation happen only for candidates,
    # not for every row in the catalogue.
    mag_abs = pd.to_numeric(batch["mag_abs"], errors="coerce").to_numpy(
        dtype=np.float64,
        na_value=np.nan,
    )
    r_pc = pd.to_numeric(batch["r_pc"], errors="coerce").to_numpy(
        dtype=np.float64,
        na_value=np.nan,
    )
    astro_q = pd.to_numeric(batch["astrometry_quality"], errors="coerce").to_numpy(
        dtype=np.float64,
        na_value=np.nan,
    )
    extreme = mag_abs <= extreme_abs_mag
    low_quality_luminous = (mag_abs <= luminous_abs_mag) & (
        astro_q >= high_astrometry_quality
    )
    remote_luminous = (mag_abs <= luminous_abs_mag) & (r_pc >= remote_luminous_pc)

    for position in np.flatnonzero(extreme | low_quality_luminous | remote_luminous):
        pos = int(position)
        source = str(batch["source"].iloc[pos])
        source_id = str(batch["source_id"].iloc[pos])
        if (source, source_id) in override_keys:
            continue

        reasons = []
        if extreme[pos]:
            reasons.append("extreme_absolute_magnitude")
        if low_quality_luminous[pos]:
            reasons.append("low_quality_astrometry_for_luminous_row")
        if remote_luminous[pos]:
            reasons.append("remote_luminous_row")
        yield _issue_record(
            issue_type="merged_row_extreme",
            severity="high" if extreme[pos] else "medium",
            reasons=reasons,
            source=source,
            source_id=source_id,
            merged_ra_deg=batch["ra_deg"].iloc[pos],
            merged_dec_deg=batch["dec_deg"].iloc[pos],
            merged_r_pc=r_pc[pos],
            merged_mag_abs=mag_abs[pos],
            merged_teff=batch["teff"].iloc[pos],
            astrometry_quality=astro_q[pos],
            photometry_quality=batch["photometry_quality"].iloc[pos],
        )


def _issue_record(
    *,
    issue_type: str,
    severity: str,
    reasons: list[str],
    source: Any,
    source_id: Any,
    **kwargs: Any,
) -> dict[str, Any]:
    rec = dict.fromkeys(QUALITY_ISSUE_COLS, pd.NA)
    rec.update(
        {
            "issue_type": issue_type,
            "severity": severity,
            "reasons": ";".join(reasons),
            "source": str(source),
            "source_id": str(source_id),
            "overridden": False,
        }
    )
    rec.update(kwargs)
    return rec


def _issues_to_table(records: list[dict[str, Any]]) -> pa.Table:
    frame = pd.DataFrame(records, columns=QUALITY_ISSUE_COLS)
    for column in _QUALITY_STRING_COLS:
        frame[column] = frame[column].astype("string")
    for column in _QUALITY_BOOL_COLS:
        frame[column] = frame[column].astype("boolean")
    for column in set(QUALITY_ISSUE_COLS) - _QUALITY_STRING_COLS - _QUALITY_BOOL_COLS:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return pa.Table.from_pandas(
        frame,
        schema=QUALITY_ISSUE_SCHEMA,
        preserve_index=False,
        safe=False,
    )


def _safe_score(value: Any) -> float:
    score = _safe_float(value)
    return score if math.isfinite(score) and score >= 0.0 else math.inf


def _parse_optional_int(value: Any) -> int | None:
    parsed = _parse_uint(value)
    return None if pd.isna(parsed) else int(parsed)


def _apparent_magnitude(mag_abs: float, r_pc: float) -> float:
    if not math.isfinite(mag_abs) or not math.isfinite(r_pc) or r_pc <= 0.0:
        return math.nan
    return mag_abs + 5.0 * math.log10(r_pc / 10.0)


def _read_matching_rows(
    paths: Iterable[Path],
    id_col: str,
    ids: set[int],
    columns: list[str],
) -> pd.DataFrame:
    if not ids:
        return pd.DataFrame(columns=columns)
    chunks: list[pd.DataFrame] = []
    for batch in _iter_parquet_batches(paths, columns):
        if batch.empty or id_col not in batch:
            continue
        id_values = _parse_uint_series(batch[id_col])
        matched = batch.loc[id_values.isin(ids)].copy()
        if not matched.empty:
            matched[id_col] = id_values.loc[matched.index].astype("uint64")
            chunks.append(matched)
    if not chunks:
        return pd.DataFrame(columns=columns)
    return pd.concat(chunks, ignore_index=True)[columns]


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


def _read_parquet_columns(path: Path, columns: list[str]) -> pd.DataFrame:
    parquet = pq.ParquetFile(path)
    present = [c for c in columns if c in parquet.schema_arrow.names]
    df = pq.read_table(path, columns=present).to_pandas() if present else pd.DataFrame()
    for col in columns:
        if col not in df:
            df[col] = pd.NA
    return df[columns]


def _load_override_keys(path: Path) -> set[tuple[str, str]]:
    df = _read_parquet_columns(path, ["source", "source_id", "action"])
    if df.empty:
        return set()
    return {
        (str(rec.source), str(rec.source_id))
        for rec in df.itertuples(index=False)
        if str(rec.action) in {"replace", "drop", "add"}
    }


def _load_labels(path: Path | None) -> pd.DataFrame:
    cols = [
        "source",
        "source_id",
        "proper_name",
        "bayer",
        "constellation",
        "hd",
        "hip_id",
    ]
    if path is None or not path.is_file():
        return pd.DataFrame(columns=["source", "source_id", "label"])
    df = _read_parquet_columns(path, cols)
    df["source"] = df["source"].astype(str)
    df["source_id"] = df["source_id"].astype(str)
    label = df["proper_name"].fillna("").astype(str)
    bayer_mask = label.eq("") & df["bayer"].notna()
    label.loc[bayer_mask] = (
        df.loc[bayer_mask, "bayer"].astype(str)
        + " "
        + df.loc[bayer_mask, "constellation"].fillna("").astype(str)
    ).str.strip()
    hd_mask = label.eq("") & df["hd"].notna()
    label.loc[hd_mask] = "HD " + df.loc[hd_mask, "hd"].map(_string_id).astype(str)
    hip_mask = label.eq("") & df["hip_id"].notna()
    label.loc[hip_mask] = "HIP " + df.loc[hip_mask, "hip_id"].map(_string_id).astype(
        str
    )
    out = df[["source", "source_id"]].copy()
    out["label"] = label
    return out.loc[out["label"].ne("")].drop_duplicates(["source", "source_id"])


def _attach_labels(issues: pd.DataFrame, labels: pd.DataFrame) -> pd.DataFrame:
    if issues.empty or labels.empty:
        return issues
    out = issues.drop(columns=["label"]).merge(
        labels,
        on=["source", "source_id"],
        how="left",
    )
    out["label"] = out["label"].fillna("")
    return out[QUALITY_ISSUE_COLS]


def _coalesce_numeric(primary: Any, fallback: Any) -> pd.Series:
    primary_s = pd.to_numeric(primary, errors="coerce")
    fallback_s = pd.to_numeric(fallback, errors="coerce")
    return primary_s.where(primary_s.notna(), fallback_s)


def _int_set(values: pd.Series) -> set[int]:
    out: set[int] = set()
    for value in _parse_uint_series(values).dropna():
        out.add(int(value))
    return out


def _value_counts(df: pd.DataFrame, column: str) -> dict[str, int]:
    if df.empty or column not in df:
        return {}
    return {
        str(key): int(value)
        for key, value in df[column].value_counts(dropna=False).sort_index().items()
    }


def _safe_float(value: Any) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return math.nan
    return out


def _parse_uint_series(values: pd.Series) -> pd.Series:
    return pd.Series(
        [_parse_uint(value) for value in values],
        index=values.index,
        dtype="UInt64",
    )


def _parse_uint(value: Any) -> int | pd.NA:
    if pd.isna(value):
        return pd.NA
    if isinstance(value, Integral):
        return int(value)
    if isinstance(value, float):
        if math.isfinite(value):
            return int(value)
        return pd.NA
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    try:
        return int(text)
    except ValueError:
        return pd.NA


def _string_id(value: Any) -> str:
    if pd.isna(value):
        return ""
    if isinstance(value, Integral):
        return str(int(value))
    if isinstance(value, float):
        if math.isfinite(value):
            return str(int(value))
        return ""
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text
