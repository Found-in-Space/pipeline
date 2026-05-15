"""Post-merge quality audit for suspicious, non-overridden stars."""

from __future__ import annotations

import json
import math
from collections.abc import Iterable
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
CLOSE_PAIR_HIGH_SEP_ARCSEC = 0.25
CLOSE_PAIR_HIGH_MAG_DELTA = 0.1

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
    close_pair_issues: int
    total_issues: int
    issue_counts_by_type: dict[str, int]
    issue_counts_by_severity: dict[str, int]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def run_quality_report(
    *,
    gaia_dir: Path,
    hip_path: Path,
    crossmatch_path: Path,
    overrides_path: Path,
    merge_dir: Path,
    identifiers_path: Path | None = None,
    output_dir: Path | None = None,
    distance_disagreement_threshold: float = 0.25,
    severe_distance_ratio: float = 2.0,
    ruwe_threshold: float = 1.4,
    high_astrometry_quality: float = 1.0,
    extreme_abs_mag: float = -10.0,
    luminous_abs_mag: float = -8.0,
    remote_luminous_pc: float = 20_000.0,
    include_close_pairs: bool = False,
    close_pair_max_sep_arcsec: float = 5.0,
    close_pair_max_mag_delta: float = 0.5,
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
    close_pair_issues = (
        _close_cross_catalog_pair_issues(
            shards_root,
            crossmatch_path=crossmatch_path,
            override_keys=override_keys,
            max_sep_arcsec=close_pair_max_sep_arcsec,
            max_mag_delta=close_pair_max_mag_delta,
        )
        if include_close_pairs
        else []
    )

    issues = pd.DataFrame(
        [*matched_issues, *merged_issues, *close_pair_issues],
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
            "close_pair_max_sep_arcsec": close_pair_max_sep_arcsec,
            "close_pair_max_mag_delta": close_pair_max_mag_delta,
        },
        score_decisions=int(len(score_decisions)),
        override_decisions=int(len(decisions) - len(score_decisions)),
        override_targets=int(len(override_keys)),
        matched_pair_issues=int(len(matched_issues)),
        merged_row_issues=int(len(merged_issues)),
        close_pair_issues=int(len(close_pair_issues)),
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
    pairs["mag_abs_delta"] = pd.to_numeric(
        pairs["gaia_mag_abs"], errors="coerce"
    ) - pd.to_numeric(pairs["hip_mag_abs"], errors="coerce")

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
                mag_abs_delta=rec.get("mag_abs_delta"),
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
        if batch.empty:
            continue
        source = batch["source"].astype(str)
        source_id = batch["source_id"].astype(str)
        override_mask = [
            (src, sid) in override_keys
            for src, sid in zip(source, source_id, strict=True)
        ]
        mag_abs = pd.to_numeric(batch["mag_abs"], errors="coerce")
        r_pc = pd.to_numeric(batch["r_pc"], errors="coerce")
        astro_q = pd.to_numeric(batch["astrometry_quality"], errors="coerce")
        extreme = mag_abs <= extreme_abs_mag
        low_quality_luminous = (mag_abs <= luminous_abs_mag) & (
            astro_q >= high_astrometry_quality
        )
        remote_luminous = (mag_abs <= luminous_abs_mag) & (r_pc >= remote_luminous_pc)
        mask = (extreme | low_quality_luminous | remote_luminous) & ~pd.Series(
            override_mask,
            index=batch.index,
        )
        if not mask.any():
            continue
        for rec in batch.loc[mask].to_dict(orient="records"):
            reasons = []
            if _safe_float(rec.get("mag_abs")) <= extreme_abs_mag:
                reasons.append("extreme_absolute_magnitude")
            if (
                _safe_float(rec.get("mag_abs")) <= luminous_abs_mag
                and _safe_float(rec.get("astrometry_quality"))
                >= high_astrometry_quality
            ):
                reasons.append("low_quality_astrometry_for_luminous_row")
            if (
                _safe_float(rec.get("mag_abs")) <= luminous_abs_mag
                and _safe_float(rec.get("r_pc")) >= remote_luminous_pc
            ):
                reasons.append("remote_luminous_row")
            severity = "high" if "extreme_absolute_magnitude" in reasons else "medium"
            issues.append(
                _issue_record(
                    issue_type="merged_row_extreme",
                    severity=severity,
                    reasons=reasons,
                    source=rec.get("source"),
                    source_id=rec.get("source_id"),
                    merged_ra_deg=rec.get("ra_deg"),
                    merged_dec_deg=rec.get("dec_deg"),
                    merged_r_pc=rec.get("r_pc"),
                    merged_mag_abs=rec.get("mag_abs"),
                    merged_teff=rec.get("teff"),
                    astrometry_quality=rec.get("astrometry_quality"),
                    photometry_quality=rec.get("photometry_quality"),
                )
            )
    return issues


def _close_cross_catalog_pair_issues(
    shards_root: Path,
    *,
    crossmatch_path: Path,
    override_keys: set[tuple[str, str]],
    max_sep_arcsec: float,
    max_mag_delta: float,
) -> list[dict[str, Any]]:
    """Find likely Gaia/Hipparcos duplicate rows left in merged output."""
    ckdtree = _require_ckdtree()
    candidates = _load_close_pair_candidates(shards_root, override_keys)
    gaia = candidates.loc[candidates["source"].eq("gaia")].reset_index(drop=True)
    hip = candidates.loc[candidates["source"].eq("hip")].reset_index(drop=True)
    if gaia.empty or hip.empty:
        return []

    gaia_xyz = _unit_vectors(gaia["ra_deg"], gaia["dec_deg"])
    hip_xyz = _unit_vectors(hip["ra_deg"], hip["dec_deg"])
    chord_radius = 2.0 * math.sin(math.radians(max_sep_arcsec / 3600.0) / 2.0)
    neighbour_lists = ckdtree(gaia_xyz).query_ball_tree(
        ckdtree(hip_xyz),
        r=chord_radius,
    )
    if not any(neighbour_lists):
        return []

    known_pairs = _load_crossmatch_keys(crossmatch_path)
    issues: list[dict[str, Any]] = []
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
            if (gaia_id, hip_id) in known_pairs:
                continue
            reasons = [
                "close_cross_catalog_position",
                "similar_apparent_magnitude",
                "not_in_gaia_hip_crossmatch",
            ]
            severity = (
                "high"
                if sep_arcsec[local_i] <= CLOSE_PAIR_HIGH_SEP_ARCSEC
                and mag_delta[local_i] <= CLOSE_PAIR_HIGH_MAG_DELTA
                else "medium"
            )
            issues.append(
                _issue_record(
                    issue_type="close_cross_catalog_pair",
                    severity=severity,
                    reasons=reasons,
                    source="hip",
                    source_id=hip_id,
                    gaia_source_id=gaia_id,
                    hip_source_id=hip_id,
                    angular_distance_arcsec=float(sep_arcsec[local_i]),
                    apparent_mag_delta=float(mag_delta[local_i]),
                    gaia_apparent_mag=gaia_rec["apparent_mag"],
                    hip_apparent_mag=hip_rec["apparent_mag"],
                    gaia_r_pc=gaia_rec["r_pc"],
                    hip_r_pc=hip_rec["r_pc"],
                    gaia_mag_abs=gaia_rec["mag_abs"],
                    hip_mag_abs=hip_rec["mag_abs"],
                    gaia_ra_deg=gaia_rec["ra_deg"],
                    gaia_dec_deg=gaia_rec["dec_deg"],
                    hip_ra_deg=hip_rec["ra_deg"],
                    hip_dec_deg=hip_rec["dec_deg"],
                    gaia_score=gaia_rec["astrometry_quality"],
                    hip_score=hip_rec["astrometry_quality"],
                )
            )
    return sorted(
        issues,
        key=lambda rec: (
            _safe_float(rec["angular_distance_arcsec"]),
            _safe_float(rec["apparent_mag_delta"]),
        ),
    )


def _load_close_pair_candidates(
    shards_root: Path,
    override_keys: set[tuple[str, str]],
) -> pd.DataFrame:
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
    chunks: list[pd.DataFrame] = []
    for batch in _iter_parquet_batches(
        sorted(shards_root.glob("*/*.parquet")), columns
    ):
        source = batch["source"].astype(str)
        source_id = batch["source_id"].astype(str)
        catalog_mask = source.isin(["gaia", "hip"])
        override_mask = pd.Series(
            [
                (src, sid) in override_keys
                for src, sid in zip(source, source_id, strict=True)
            ],
            index=batch.index,
        )
        numeric = batch.copy()
        for col in [
            "ra_deg",
            "dec_deg",
            "r_pc",
            "mag_abs",
            "astrometry_quality",
            "photometry_quality",
        ]:
            numeric[col] = pd.to_numeric(numeric[col], errors="coerce")
        finite_mask = (
            np.isfinite(numeric["ra_deg"])
            & np.isfinite(numeric["dec_deg"])
            & np.isfinite(numeric["r_pc"])
            & np.isfinite(numeric["mag_abs"])
            & (numeric["r_pc"] > 0)
        )
        out = numeric.loc[catalog_mask & ~override_mask & finite_mask].copy()
        if out.empty:
            continue
        out["source"] = source.loc[out.index].to_numpy()
        out["source_id"] = source_id.loc[out.index].to_numpy()
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
                        "teff",
                        "astrometry_quality",
                        "photometry_quality",
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
                "teff",
                "astrometry_quality",
                "photometry_quality",
            ]
        )
    return pd.concat(chunks, ignore_index=True)


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


def _load_crossmatch_keys(path: Path) -> set[tuple[str, str]]:
    df = _read_parquet_columns(path, ["gaia_source_id", "hip_source_id"])
    if df.empty:
        return set()
    gaia_ids = _parse_uint_series(df["gaia_source_id"])
    hip_ids = _parse_uint_series(df["hip_source_id"])
    return {
        (str(int(gaia_id)), str(int(hip_id)))
        for gaia_id, hip_id in zip(gaia_ids, hip_ids, strict=True)
        if not pd.isna(gaia_id) and not pd.isna(hip_id)
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
            "Close-pair detection requires the optional audit dependency group. "
            "Install or run with `uv sync --group audit` or "
            "`uv run --group audit ...`."
        ) from exc
    return cKDTree


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
