"""Streaming Gaia/Hipparcos merge into HEALPix-partitioned Parquet output."""

from __future__ import annotations

import json
import logging
import math
import shutil
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm.auto import tqdm

from foundinspace.pipeline.common.ids import normalize_compound_key
from foundinspace.pipeline.constants import OUTPUT_COLS
from foundinspace.pipeline.merge import policy, shards, sidecars
from foundinspace.pipeline.merge.decisions import DECISION_COLS, decision_record
from foundinspace.pipeline.merge.overrides import (
    OVERRIDE_REQUIRED_COLS,
    find_pair_override,
    gaia_special_ids_for_overrides,
    split_override_rows,
)

_LOG = logging.getLogger(__name__)
MERGE_BATCH_SIZE = 1_000_000

# Auxiliary columns expected from widened catalog pipelines (gracefully absent).
_GAIA_AUX_COLS = ["ruwe", "phot_g_mean_mag"]
_HIP_AUX_COLS = ["Sn", "Hpmag"]
_CROSS_AUX_COLS = ["number_of_neighbours", "angular_distance", "xm_flag"]
_CROSS_MAPPING_SOURCE_COL = "mapping_source"


@dataclass
class MergeReport:
    """Small aggregate report for one merge run."""

    healpix_order: int
    healpix_nside: int
    gaia_dir: str
    hip_path: str
    crossmatch_path: str
    overrides_path: str
    gaia_files: list[str]
    gaia_rows_total: int
    rows_emitted_total: int
    unmatched_gaia: int
    unmatched_hip: int
    matched_pairs_scored: int
    matched_winner_gaia: int
    matched_winner_hip: int
    hip_with_missing_gaia_partner: int
    override_add_applied: int
    override_replace_applied: int
    override_drop_applied: int
    override_no_effect: int
    decisions_rows: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _output_row(
    payload: dict[str, Any],
    *,
    canonical_source: str | None = None,
    canonical_source_id: str | None = None,
) -> dict[str, Any]:
    """Build one merged row from a payload dict that already satisfies OUTPUT_COLS.

    For matched Gaia↔HIP pairs where Gaia wins astrometry, pass canonical_source='hip'
    and canonical_source_id=str(hip_id) so identity stays Hipparcos-keyed.
    """
    row = {col: payload[col] for col in OUTPUT_COLS}
    if canonical_source is not None:
        row["source"] = canonical_source
    if canonical_source_id is not None:
        row["source_id"] = canonical_source_id
    row["source"] = str(row["source"])
    row["source_id"] = str(row["source_id"])
    return row


def _prepare_gaia_unmatched(df: pd.DataFrame) -> pd.DataFrame:
    out = df[OUTPUT_COLS].copy()
    out["source"] = "gaia"
    out["source_id"] = out["source_id"].astype("uint64").astype("string")
    return out[shards.MERGED_OUTPUT_COLS]


def _read_required_parquet(path: Path, required_cols: list[str]) -> pd.DataFrame:
    df = pq.read_table(path).to_pandas()
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {path}: {missing}")
    return df


def _validate_parquet_columns(path: Path, required_cols: list[str]) -> None:
    schema = pq.ParquetFile(path).schema_arrow
    file_cols = set(schema.names)
    missing = [c for c in required_cols if c not in file_cols]
    if missing:
        raise ValueError(f"Missing required columns in {path}: {missing}")


def _build_crossmatch_maps(df: pd.DataFrame) -> tuple[dict[int, int], dict[int, int]]:
    gaia_to_hip: dict[int, int] = {}
    hip_to_gaia: dict[int, int] = {}
    for rec in df[["gaia_source_id", "hip_source_id"]].itertuples(index=False):
        gaia_id = int(rec.gaia_source_id)
        hip_id = int(rec.hip_source_id)
        prev_hip = gaia_to_hip.get(gaia_id)
        if prev_hip is not None and prev_hip != hip_id:
            raise ValueError(
                f"Crossmatch is not one-to-one: Gaia {gaia_id} maps to {prev_hip} and {hip_id}"
            )
        prev_gaia = hip_to_gaia.get(hip_id)
        if prev_gaia is not None and prev_gaia != gaia_id:
            raise ValueError(
                f"Crossmatch is not one-to-one: HIP {hip_id} maps to {prev_gaia} and {gaia_id}"
            )
        gaia_to_hip[gaia_id] = hip_id
        hip_to_gaia[hip_id] = gaia_id
    return gaia_to_hip, hip_to_gaia


def run_merge(
    *,
    gaia_dir: Path,
    hip_path: Path,
    crossmatch_path: Path,
    overrides_path: Path,
    output_dir: Path,
    sidecar_output_dir: Path | None = None,
    healpix_order: int = 3,
    force: bool = False,
) -> MergeReport:
    """Run streaming merge into HEALPix-partitioned output directories."""
    if healpix_order < 0:
        raise ValueError("healpix_order must be >= 0")

    gaia_dir = Path(gaia_dir).expanduser()
    hip_path = Path(hip_path).expanduser()
    crossmatch_path = Path(crossmatch_path).expanduser()
    overrides_path = Path(overrides_path).expanduser()
    output_dir = Path(output_dir).expanduser()
    sidecar_root = Path(sidecar_output_dir).expanduser() if sidecar_output_dir else None
    shards_root = output_dir / "healpix"
    decisions_path = output_dir / "merge_decisions.parquet"
    report_path = output_dir / "merge_report.json"

    if not gaia_dir.is_dir():
        raise FileNotFoundError(str(gaia_dir))
    for path in (hip_path, crossmatch_path, overrides_path):
        if not path.is_file():
            raise FileNotFoundError(str(path))

    if output_dir.exists():
        has_content = any(output_dir.iterdir())
        if has_content and not force:
            raise FileExistsError(str(output_dir))
        if force:
            shutil.rmtree(output_dir)
    if sidecar_root is not None and sidecar_root.exists():
        has_content = any(sidecar_root.iterdir())
        if has_content and not force:
            raise FileExistsError(str(sidecar_root))
        if force:
            shutil.rmtree(sidecar_root)
    output_dir.mkdir(parents=True, exist_ok=True)
    shards_root.mkdir(parents=True, exist_ok=True)
    if sidecar_root is not None:
        sidecar_root.mkdir(parents=True, exist_ok=True)

    nside = 2**healpix_order
    hp = shards._build_healpix(healpix_order)

    # Load small lookup tables once.
    hip_df = _read_required_parquet(hip_path, OUTPUT_COLS)
    hip_df["source_id"] = pd.to_numeric(hip_df["source_id"], errors="raise").astype(
        "uint64"
    )
    hip_load_cols = OUTPUT_COLS + [c for c in _HIP_AUX_COLS if c in hip_df.columns]
    hip_by_id: dict[int, dict[str, Any]] = {}
    for rec in hip_df[hip_load_cols].to_dict(orient="records"):
        hip_by_id[int(rec["source_id"])] = rec

    cross_df = _read_required_parquet(
        crossmatch_path, ["gaia_source_id", "hip_source_id"]
    )
    gaia_to_hip, hip_to_gaia = _build_crossmatch_maps(cross_df)

    # V2: crossmatch auxiliary data (mapping source, neighbours, angular distance).
    cross_aux_by_gaia: dict[int, dict[str, Any]] = {}
    for col in _CROSS_AUX_COLS:
        if col not in cross_df.columns:
            cross_df[col] = np.nan
    if _CROSS_MAPPING_SOURCE_COL not in cross_df.columns:
        cross_df[_CROSS_MAPPING_SOURCE_COL] = pd.NA
    for rec in cross_df[
        ["gaia_source_id", _CROSS_MAPPING_SOURCE_COL, *_CROSS_AUX_COLS]
    ].itertuples(index=False):
        cross_aux_by_gaia[int(rec.gaia_source_id)] = {
            "mapping_source": rec.mapping_source,
            "number_of_neighbours": policy._safe_int(rec.number_of_neighbours),
            "angular_distance": policy._safe_float(rec.angular_distance),
            "xm_flag": policy._safe_int(rec.xm_flag),
        }

    overrides_df = _read_required_parquet(overrides_path, OVERRIDE_REQUIRED_COLS)
    overrides_by_key, add_overrides = split_override_rows(overrides_df)

    # Small state only (no per-row unmatched storage).
    write_seq_by_pixel: dict[int, int] = {}
    sidecar_seq_by_key: dict[tuple[str, int], int] = {}
    resolved_hip_ids: set[int] = set()
    processed_override_ids: set[str] = set()
    decisions: list[dict[str, Any]] = []

    gaia_files = sorted(gaia_dir.glob("*.parquet"))
    gaia_files_for_report = [str(p) for p in gaia_files]

    gaia_special_ids: set[int] = set(gaia_to_hip.keys())
    gaia_special_ids.update(
        gaia_special_ids_for_overrides(
            overrides_by_key,
            hip_to_gaia=hip_to_gaia,
        )
    )

    report = MergeReport(
        healpix_order=healpix_order,
        healpix_nside=nside,
        gaia_dir=str(gaia_dir),
        hip_path=str(hip_path),
        crossmatch_path=str(crossmatch_path),
        overrides_path=str(overrides_path),
        gaia_files=gaia_files_for_report,
        gaia_rows_total=0,
        rows_emitted_total=0,
        unmatched_gaia=0,
        unmatched_hip=0,
        matched_pairs_scored=0,
        matched_winner_gaia=0,
        matched_winner_hip=0,
        hip_with_missing_gaia_partner=0,
        override_add_applied=0,
        override_replace_applied=0,
        override_drop_applied=0,
        override_no_effect=0,
        decisions_rows=0,
    )

    for gaia_file in tqdm(
        gaia_files,
        total=len(gaia_files),
        desc="Merging Gaia batches",
        unit="file",
        dynamic_ncols=True,
    ):
        special_out_rows: list[dict[str, Any]] = []
        special_sidecar_gaia_rows: list[dict[str, Any]] = []
        special_sidecar_dense_rows: list[dict[str, Any]] = []
        _validate_parquet_columns(gaia_file, OUTPUT_COLS)
        parquet_file = pq.ParquetFile(gaia_file)
        schema_names = parquet_file.schema_arrow.names
        file_col_names = set(schema_names)
        enrichment_cols = sidecars.gaia_enrichment_columns(schema_names)
        gaia_read_cols = list(
            dict.fromkeys(
                OUTPUT_COLS
                + [c for c in _GAIA_AUX_COLS if c in file_col_names]
                + enrichment_cols
            )
        )
        gaia_num_rows = parquet_file.metadata.num_rows
        batch_bar_total = (
            (gaia_num_rows + MERGE_BATCH_SIZE - 1) // MERGE_BATCH_SIZE
            if gaia_num_rows > 0
            else None
        )
        for batch in tqdm(
            parquet_file.iter_batches(
                batch_size=MERGE_BATCH_SIZE,
                columns=gaia_read_cols,
            ),
            desc=f"Batches ({gaia_file.stem})",
            unit="batch",
            total=batch_bar_total,
            leave=False,
            dynamic_ncols=True,
        ):
            gaia_df = batch.to_pandas()
            report.gaia_rows_total += len(gaia_df)
            gaia_df["source_id"] = pd.to_numeric(
                gaia_df["source_id"], errors="raise"
            ).astype("uint64")

            special_mask = gaia_df["source_id"].isin(gaia_special_ids)
            gaia_unmatched_df = gaia_df.loc[~special_mask]
            if not gaia_unmatched_df.empty:
                out = _prepare_gaia_unmatched(gaia_unmatched_df)
                written = shards._write_shards(
                    out,
                    hp=hp,
                    shards_root=shards_root,
                    phase_tag=f"gaia_{gaia_file.stem}",
                    seq_by_pixel=write_seq_by_pixel,
                )
                report.rows_emitted_total += written
                report.unmatched_gaia += written
                if sidecar_root is not None:
                    sidecar_df = sidecars.from_frames(
                        gaia_unmatched_df,
                        out,
                        enrichment_cols=enrichment_cols,
                    )
                    sidecars.write_gaia_sidecars(
                        sidecar_df,
                        hp=hp,
                        sidecar_root=sidecar_root,
                        phase_tag=f"gaia_{gaia_file.stem}",
                        seq_by_key=sidecar_seq_by_key,
                    )

            special_read_cols = [c for c in gaia_read_cols if c in gaia_df.columns]
            special_rows = gaia_df.loc[special_mask, special_read_cols].to_dict(
                orient="records"
            )
            for gaia_rec in special_rows:
                gaia_id = int(gaia_rec["source_id"])
                hip_id = gaia_to_hip.get(gaia_id)
                hip_rec = hip_by_id.get(hip_id) if hip_id is not None else None

                override = find_pair_override(
                    overrides_by_key,
                    gaia_id=gaia_id,
                    hip_id=hip_id,
                )
                if override is not None:
                    cross_aux = cross_aux_by_gaia.get(gaia_id, {})
                    override_id = str(override["override_id"])
                    if override_id in processed_override_ids:
                        # Pair was already resolved by the counterpart route.
                        if hip_id is not None:
                            resolved_hip_ids.add(hip_id)
                        continue
                    processed_override_ids.add(override_id)
                    if hip_id is not None:
                        resolved_hip_ids.add(hip_id)
                    action = str(override["action"])
                    note = ""
                    winner_catalog = ""
                    winner_source_id = ""
                    if action == "replace":
                        dense_row = _output_row(override)
                        special_out_rows.append(dense_row)
                        special_sidecar_gaia_rows.append(gaia_rec)
                        special_sidecar_dense_rows.append(dense_row)
                        report.override_replace_applied += 1
                        winner_catalog = "manual"
                        winner_source_id = str(override["source_id"])
                    elif action == "drop":
                        report.override_drop_applied += 1
                    else:
                        raise ValueError(
                            f"Unsupported override action for pair path: {action}"
                        )

                    if hip_id is None:
                        note = "partner_missing"
                    _n = cross_aux.get("number_of_neighbours")
                    _ang = policy._safe_float(
                        cross_aux.get("angular_distance", math.nan)
                    )
                    _xm = cross_aux.get("xm_flag")
                    decisions.append(
                        decision_record(
                            decision_type="override",
                            gaia_source_id=str(gaia_id),
                            hip_source_id=str(hip_id) if hip_id is not None else pd.NA,
                            winner_catalog=winner_catalog or pd.NA,
                            winner_source_id=winner_source_id or pd.NA,
                            mapping_source=cross_aux.get("mapping_source", pd.NA),
                            gaia_score=policy._safe_score(
                                gaia_rec.get("astrometry_quality")
                            ),
                            hip_score=policy._safe_score(
                                hip_rec.get("astrometry_quality")
                                if hip_rec is not None
                                else np.nan
                            ),
                            override_id=override_id,
                            override_action=action,
                            override_reason=override.get("override_reason", pd.NA),
                            override_policy_version=override.get(
                                "override_policy_version", pd.NA
                            ),
                            number_of_neighbours=_n if _n is not None else pd.NA,
                            angular_distance_arcsec=_ang
                            if math.isfinite(_ang)
                            else pd.NA,
                            crossmatch_xm_flag=_xm if _xm is not None else pd.NA,
                            note=note or pd.NA,
                        )
                    )
                    continue

                if hip_rec is None or hip_id in resolved_hip_ids:
                    dense_row = _output_row(gaia_rec)
                    special_out_rows.append(dense_row)
                    special_sidecar_gaia_rows.append(gaia_rec)
                    special_sidecar_dense_rows.append(dense_row)
                    report.unmatched_gaia += 1
                    continue

                cross_aux = cross_aux_by_gaia.get(gaia_id, {})
                n_neighbours = cross_aux.get("number_of_neighbours")

                winner_catalog, tie_break_reason = policy._choose_matched_winner(
                    gaia_rec,
                    hip_rec,
                    number_of_neighbours=n_neighbours,
                )
                if winner_catalog == "gaia":
                    winner_row = _output_row(
                        gaia_rec,
                        canonical_source="hip",
                        canonical_source_id=str(hip_id),
                    )
                    report.matched_winner_gaia += 1
                else:
                    winner_row = _output_row(hip_rec)
                    report.matched_winner_hip += 1
                special_out_rows.append(winner_row)
                special_sidecar_gaia_rows.append(gaia_rec)
                special_sidecar_dense_rows.append(winner_row)
                resolved_hip_ids.add(int(hip_id))
                report.matched_pairs_scored += 1
                _ruwe = policy._safe_float(gaia_rec.get("ruwe"))
                _gmag = policy._safe_float(gaia_rec.get("phot_g_mean_mag"))
                _sn = policy._safe_int(hip_rec.get("Sn"))
                _hmag = policy._safe_float(hip_rec.get("Hpmag"))
                _ang = policy._safe_float(cross_aux.get("angular_distance", math.nan))
                _xm = cross_aux.get("xm_flag")
                decisions.append(
                    decision_record(
                        decision_type="score",
                        gaia_source_id=str(gaia_id),
                        hip_source_id=str(hip_id),
                        winner_catalog=winner_catalog,
                        winner_source_id=str(
                            gaia_id if winner_catalog == "gaia" else int(hip_id)
                        ),
                        mapping_source=cross_aux.get("mapping_source", pd.NA),
                        gaia_score=policy._safe_score(
                            gaia_rec.get("astrometry_quality")
                        ),
                        hip_score=policy._safe_score(hip_rec.get("astrometry_quality")),
                        tie_break_reason=tie_break_reason or pd.NA,
                        number_of_neighbours=n_neighbours
                        if n_neighbours is not None
                        else pd.NA,
                        angular_distance_arcsec=_ang if math.isfinite(_ang) else pd.NA,
                        crossmatch_xm_flag=_xm if _xm is not None else pd.NA,
                        gaia_ruwe=_ruwe if math.isfinite(_ruwe) else pd.NA,
                        gaia_phot_g_mean_mag=_gmag if math.isfinite(_gmag) else pd.NA,
                        hip_solution_type=_sn if _sn is not None else pd.NA,
                        hip_apparent_mag=_hmag if math.isfinite(_hmag) else pd.NA,
                    )
                )

        if special_out_rows:
            special_df = pd.DataFrame(
                special_out_rows, columns=shards.MERGED_OUTPUT_COLS
            )
            written = shards._write_shards(
                special_df,
                hp=hp,
                shards_root=shards_root,
                phase_tag=f"gaia_special_{gaia_file.stem}",
                seq_by_pixel=write_seq_by_pixel,
            )
            report.rows_emitted_total += written
            if sidecar_root is not None:
                sidecar_df = sidecars.from_records(
                    special_sidecar_gaia_rows,
                    special_sidecar_dense_rows,
                    enrichment_cols=enrichment_cols,
                )
                sidecars.write_gaia_sidecars(
                    sidecar_df,
                    hp=hp,
                    sidecar_root=sidecar_root,
                    phase_tag=f"gaia_special_{gaia_file.stem}",
                    seq_by_key=sidecar_seq_by_key,
                )

    # Flush HIP side, including Gaia-targeted overrides where Gaia row is absent.
    hip_out_rows: list[dict[str, Any]] = []
    for hip_id in tqdm(
        sorted(hip_by_id),
        total=len(hip_by_id),
        desc="Flushing HIP rows",
        unit="row",
        dynamic_ncols=True,
    ):
        if hip_id in resolved_hip_ids:
            continue
        hip_rec = hip_by_id[hip_id]
        gaia_id = hip_to_gaia.get(hip_id)
        override = find_pair_override(
            overrides_by_key,
            gaia_id=gaia_id,
            hip_id=hip_id,
        )
        if override is not None:
            cross_aux = cross_aux_by_gaia.get(gaia_id, {}) if gaia_id is not None else {}
            override_id = str(override["override_id"])
            if override_id in processed_override_ids:
                resolved_hip_ids.add(hip_id)
                continue
            processed_override_ids.add(override_id)
            resolved_hip_ids.add(hip_id)
            action = str(override["action"])
            if action == "replace":
                hip_out_rows.append(_output_row(override))
                report.override_replace_applied += 1
            elif action == "drop":
                report.override_drop_applied += 1
            else:
                raise ValueError(
                    f"Unsupported override action for HIP flush path: {action}"
                )
            _ang = policy._safe_float(cross_aux.get("angular_distance", math.nan))
            _n = cross_aux.get("number_of_neighbours")
            _xm = cross_aux.get("xm_flag")
            _sn = policy._safe_int(hip_rec.get("Sn"))
            _hmag = policy._safe_float(hip_rec.get("Hpmag"))
            decisions.append(
                decision_record(
                    decision_type="override",
                    gaia_source_id=str(gaia_id) if gaia_id is not None else pd.NA,
                    hip_source_id=str(hip_id),
                    winner_catalog="manual" if action == "replace" else pd.NA,
                    winner_source_id=str(override["source_id"])
                    if action == "replace"
                    else pd.NA,
                    mapping_source=cross_aux.get("mapping_source", pd.NA),
                    hip_score=policy._safe_score(hip_rec.get("astrometry_quality")),
                    override_id=override_id,
                    override_action=action,
                    override_reason=override.get("override_reason", pd.NA),
                    override_policy_version=override.get(
                        "override_policy_version", pd.NA
                    ),
                    number_of_neighbours=_n if _n is not None else pd.NA,
                    angular_distance_arcsec=_ang if math.isfinite(_ang) else pd.NA,
                    crossmatch_xm_flag=_xm if _xm is not None else pd.NA,
                    hip_solution_type=_sn if _sn is not None else pd.NA,
                    hip_apparent_mag=_hmag if math.isfinite(_hmag) else pd.NA,
                    note="resolved_in_hip_flush"
                    if gaia_id is not None
                    and ("gaia", gaia_id)
                    == normalize_compound_key(override["source"], override["source_id"])
                    else pd.NA,
                )
            )
            continue

        cross_aux = cross_aux_by_gaia.get(gaia_id, {}) if gaia_id is not None else {}
        _sn = policy._safe_int(hip_rec.get("Sn"))
        _hmag = policy._safe_float(hip_rec.get("Hpmag"))
        if gaia_id is not None:
            _ang = policy._safe_float(cross_aux.get("angular_distance", math.nan))
            _n = cross_aux.get("number_of_neighbours")
            _xm = cross_aux.get("xm_flag")
            report.hip_with_missing_gaia_partner += 1
            decisions.append(
                decision_record(
                    decision_type="missing_gaia_partner",
                    gaia_source_id=str(gaia_id),
                    hip_source_id=str(hip_id),
                    winner_catalog="hip",
                    winner_source_id=str(hip_id),
                    mapping_source=cross_aux.get("mapping_source", pd.NA),
                    hip_score=policy._safe_score(hip_rec.get("astrometry_quality")),
                    note="gaia_partner_absent_from_gaia_stage",
                    number_of_neighbours=_n if _n is not None else pd.NA,
                    angular_distance_arcsec=_ang if math.isfinite(_ang) else pd.NA,
                    crossmatch_xm_flag=_xm if _xm is not None else pd.NA,
                    hip_solution_type=_sn if _sn is not None else pd.NA,
                    hip_apparent_mag=_hmag if math.isfinite(_hmag) else pd.NA,
                )
            )

        hip_out_rows.append(_output_row(hip_rec))
        report.unmatched_hip += 1

    if hip_out_rows:
        hip_out_df = pd.DataFrame(hip_out_rows, columns=shards.MERGED_OUTPUT_COLS)
        written = shards._write_shards(
            hip_out_df,
            hp=hp,
            shards_root=shards_root,
            phase_tag="hip_flush",
            seq_by_pixel=write_seq_by_pixel,
        )
        report.rows_emitted_total += written

    # Add-only overrides.
    add_rows: list[dict[str, Any]] = []
    for ov in tqdm(
        add_overrides,
        total=len(add_overrides),
        desc="Applying add overrides",
        unit="row",
        dynamic_ncols=True,
    ):
        override_id = str(ov["override_id"])
        if override_id in processed_override_ids:
            continue
        processed_override_ids.add(override_id)
        add_rows.append(_output_row(ov))
        report.override_add_applied += 1
        decisions.append(
            decision_record(
                decision_type="override_add",
                winner_catalog="manual",
                winner_source_id=str(ov["source_id"]),
                override_id=override_id,
                override_action="add",
                override_reason=ov.get("override_reason", pd.NA),
                override_policy_version=ov.get("override_policy_version", pd.NA),
            )
        )

    if add_rows:
        add_df = pd.DataFrame(add_rows, columns=shards.MERGED_OUTPUT_COLS)
        written = shards._write_shards(
            add_df,
            hp=hp,
            shards_root=shards_root,
            phase_tag="override_add",
            seq_by_pixel=write_seq_by_pixel,
        )
        report.rows_emitted_total += written

    # Unapplied replace/drop overrides: neither side present.
    for ov in overrides_by_key.values():
        override_id = str(ov["override_id"])
        if override_id in processed_override_ids:
            continue
        report.override_no_effect += 1
        _LOG.warning(
            "Override %s (%s:%s) has no effect: target and crossmatch partner are absent",
            override_id,
            ov.get("source"),
            ov.get("source_id"),
        )
        decisions.append(
            decision_record(
                decision_type="override_no_effect",
                override_id=override_id,
                override_action=ov.get("action", pd.NA),
                override_reason=ov.get("override_reason", pd.NA),
                override_policy_version=ov.get("override_policy_version", pd.NA),
                note="target_and_partner_absent",
            )
        )

    expected_rows_emitted = (
        report.unmatched_gaia
        + report.unmatched_hip
        + report.matched_pairs_scored
        + report.override_replace_applied
        + report.override_add_applied
    )
    if report.rows_emitted_total != expected_rows_emitted:
        raise RuntimeError(
            "Row count consistency check failed: "
            f"rows_emitted_total={report.rows_emitted_total} "
            f"expected={expected_rows_emitted}"
        )

    decisions_df = pd.DataFrame(decisions, columns=DECISION_COLS)
    report.decisions_rows = len(decisions_df)
    if decisions_df.empty:
        decisions_df = pd.DataFrame(columns=DECISION_COLS)
    pq.write_table(
        pa.Table.from_pandas(decisions_df, preserve_index=False),
        str(decisions_path),
        compression="zstd",
    )

    with report_path.open("w", encoding="utf-8") as fp:
        json.dump(report.to_dict(), fp, indent=2, sort_keys=True)
        fp.write("\n")

    return report
