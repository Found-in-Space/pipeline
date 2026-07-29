"""Collect external distance evidence for tracked override targets."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
from astropy.table import Table

from foundinspace.pipeline.common.gaia_credentials import (
    login_gaia_from_environment_if_available,
)

DEFAULT_TRACKER_PATH = (
    Path(__file__).parent / "research" / "distance-resolution-v1.csv"
)
DEFAULT_OUTPUT_DIR = Path("downloads/distance-resolution-v1")
VIZIER_TAP_URL = "https://tapvizier.cds.unistra.fr/TAPVizieR/tap"
STARHORSE_TABLE = '"I/354/starhorse2021"'
SUPPORTED_SERVICES = ("simbad", "gaia", "starhorse")

TRACKER_REQUIRED_COLUMNS = (
    "status",
    "official_source",
    "official_source_id",
    "gaia_source_id",
    "hip_source_id",
    "current_distance_source",
    "reason_for_external_search",
    "next_action",
    "resolved_distance_pc",
    "resolved_distance_error_pc",
    "resolved_reference",
    "resolution_notes",
)
TRACKER_STATUSES = frozenset({"unresolved", "provisional", "resolved"})


def _id_string(value: object) -> str:
    """Return a catalog identifier without allowing float-style corruption."""
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    return text


def _validate_id(value: object, label: str, *, required: bool = False) -> str:
    text = _id_string(value)
    if not text:
        if required:
            raise ValueError(f"{label} must not be blank")
        return ""
    if not text.isdigit() or int(text) <= 0:
        raise ValueError(f"{label} must be a positive integer, got {text!r}")
    return text


def load_distance_tracker(path: Path = DEFAULT_TRACKER_PATH) -> pd.DataFrame:
    """Read and validate the tracked distance-resolution work list."""
    path = Path(path)
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    missing = [name for name in TRACKER_REQUIRED_COLUMNS if name not in frame]
    if missing:
        raise ValueError(f"{path} is missing tracker column(s): {missing}")
    if frame.empty:
        raise ValueError(f"{path} contains no targets")

    frame = frame.copy()
    for index, row in frame.iterrows():
        status = str(row["status"]).strip().lower()
        if status not in TRACKER_STATUSES:
            raise ValueError(
                f"row {index + 2}: status must be unresolved, provisional, "
                f"or resolved, got {status!r}"
            )
        source = str(row["official_source"]).strip().lower()
        if source not in {"gaia", "hip"}:
            raise ValueError(
                f"row {index + 2}: official_source must be gaia or hip"
            )
        official_id = _validate_id(
            row["official_source_id"],
            f"row {index + 2} official_source_id",
            required=True,
        )
        gaia_id = _validate_id(
            row.get("gaia_source_id", ""),
            f"row {index + 2} gaia_source_id",
        )
        hip_id = _validate_id(
            row.get("hip_source_id", ""),
            f"row {index + 2} hip_source_id",
        )
        if source == "gaia" and gaia_id != official_id:
            raise ValueError(
                f"row {index + 2}: Gaia canonical ID does not match gaia_source_id"
            )
        if source == "hip" and hip_id != official_id:
            raise ValueError(
                f"row {index + 2}: HIP canonical ID does not match hip_source_id"
            )
        if not str(row["reason_for_external_search"]).strip():
            raise ValueError(f"row {index + 2}: external-search reason is blank")
        if status != "unresolved":
            for field in (
                "resolved_distance_pc",
                "resolved_distance_error_pc",
                "resolved_reference",
                "resolution_notes",
            ):
                if not str(row[field]).strip():
                    raise ValueError(
                        f"row {index + 2}: {status} target has blank {field}"
                    )
            for field in (
                "resolved_distance_pc",
                "resolved_distance_error_pc",
            ):
                try:
                    value = float(row[field])
                except ValueError as exc:
                    raise ValueError(
                        f"row {index + 2}: {field} must be numeric"
                    ) from exc
                if not np.isfinite(value) or value <= 0:
                    raise ValueError(
                        f"row {index + 2}: {field} must be positive and finite"
                    )
        frame.at[index, "status"] = status
        frame.at[index, "official_source"] = source
        frame.at[index, "official_source_id"] = official_id
        frame.at[index, "gaia_source_id"] = gaia_id
        frame.at[index, "hip_source_id"] = hip_id

    frame["tracker_key"] = (
        frame["official_source"] + ":" + frame["official_source_id"]
    )
    duplicates = frame["tracker_key"].duplicated(keep=False)
    if bool(duplicates.any()):
        values = sorted(frame.loc[duplicates, "tracker_key"].unique())
        raise ValueError(f"{path} contains duplicate target(s): {values}")
    return frame


def build_simbad_aliases(targets: pd.DataFrame) -> pd.DataFrame:
    """Build identifier queries while retaining the target each alias belongs to."""
    records: list[dict[str, str]] = []
    for row in targets.to_dict(orient="records"):
        aliases: list[tuple[str, str]] = []
        gaia_id = _id_string(row.get("gaia_source_id"))
        hip_id = _id_string(row.get("hip_source_id"))
        if gaia_id:
            aliases.extend(
                [
                    ("gaia_dr3", f"Gaia DR3 {gaia_id}"),
                    ("gaia_edr3", f"Gaia EDR3 {gaia_id}"),
                ]
            )
        if hip_id:
            aliases.append(("hip", f"HIP {hip_id}"))
        for kind, column in (
            ("simbad_main_id", "simbad_main_id"),
            ("proper_name", "proper_name"),
        ):
            value = str(row.get(column, "")).strip()
            if value:
                aliases.append((kind, value))

        seen: set[str] = set()
        for alias_kind, query_name in aliases:
            normalized = " ".join(query_name.split()).casefold()
            if normalized in seen:
                continue
            seen.add(normalized)
            records.append(
                {
                    "tracker_key": row["tracker_key"],
                    "alias_kind": alias_kind,
                    "query_name": query_name,
                }
            )
    return pd.DataFrame.from_records(
        records, columns=["tracker_key", "alias_kind", "query_name"]
    )


def _normalise_column_name(value: object) -> str:
    return (
        str(value)
        .strip()
        .lower()
        .replace(".", "_")
        .replace("-", "_")
        .replace("/", "_")
        .replace(" ", "_")
    )


def table_to_frame(table: Table | pd.DataFrame | None) -> pd.DataFrame:
    """Convert an Astroquery result to CSV-safe, normalized columns."""
    if table is None:
        return pd.DataFrame()
    frame = table.copy() if isinstance(table, pd.DataFrame) else table.to_pandas()
    frame.columns = [_normalise_column_name(column) for column in frame.columns]
    for column in frame.columns:
        frame[column] = frame[column].map(
            lambda value: value.decode("utf-8", errors="replace")
            if isinstance(value, bytes)
            else value
        )
    return frame


def _chunks(values: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def query_simbad(
    targets: pd.DataFrame,
    *,
    client=None,
    chunk_size: int = 100,
) -> pd.DataFrame:
    """Query SIMBAD names, best parallax, spectral type, and distance measures."""
    if client is None:
        from astroquery.simbad import Simbad

        client = Simbad()

    # ``distance`` expands to SIMBAD's mesDistance table and preserves method
    # and bibliography; ``parallax`` is the database's selected basic value.
    client.add_votable_fields("parallax", "sp", "otype", "distance", "ids")
    aliases = build_simbad_aliases(targets)
    if aliases.empty:
        return aliases.assign(service=pd.Series(dtype=str))

    results: list[pd.DataFrame] = []
    query_names = aliases["query_name"].drop_duplicates().tolist()
    for names in _chunks(query_names, chunk_size):
        result = table_to_frame(client.query_objects(list(names), async_job=True))
        if "user_specified_id" not in result:
            raise ValueError("SIMBAD result omitted user_specified_id")
        results.append(result)

    evidence = pd.concat(results, ignore_index=True) if results else pd.DataFrame()
    # Astropy preserves the fixed-width upload column and pads shorter names;
    # strip it before joining back to the unpadded tracker aliases.
    evidence["user_specified_id"] = (
        evidence["user_specified_id"].astype(str).str.strip()
    )
    evidence = aliases.merge(
        evidence,
        how="left",
        left_on="query_name",
        right_on="user_specified_id",
        validate="many_to_many",
    )
    main_id = evidence.get("main_id", pd.Series("", index=evidence.index))
    evidence["match_status"] = np.where(
        main_id.fillna("").astype(str).str.strip().ne(""), "found", "not_found"
    )
    parallax = pd.to_numeric(evidence.get("plx_value"), errors="coerce")
    parallax_error = pd.to_numeric(evidence.get("plx_err"), errors="coerce")
    evidence["plx_snr"] = np.where(
        parallax_error > 0, parallax / parallax_error, np.nan
    )
    evidence["service"] = "simbad"
    return evidence


def _numeric_id_list(values: Iterable[object], label: str) -> list[str]:
    return sorted(
        {
            _validate_id(value, label, required=True)
            for value in values
            if _id_string(value)
        },
        key=int,
    )


def build_gaia_hip_xmatch_query(hip_ids: Iterable[object]) -> str:
    """Build an exact HIP-ID query for Gaia's official best-neighbour table."""
    ids = _numeric_id_list(hip_ids, "HIP source ID")
    if not ids:
        raise ValueError("at least one HIP source ID is required")
    return (
        "SELECT\n"
        "  source_id,\n"
        "  original_ext_source_id,\n"
        "  angular_distance,\n"
        "  number_of_neighbours\n"
        "FROM gaiadr3.hipparcos2_best_neighbour\n"
        f"WHERE original_ext_source_id IN ({', '.join(ids)})"
    )


def build_gaia_details_query(gaia_ids: Iterable[object]) -> str:
    """Build an exact Gaia DR3 + Bailer-Jones EDR3 evidence query."""
    ids = _numeric_id_list(gaia_ids, "Gaia source ID")
    if not ids:
        raise ValueError("at least one Gaia source ID is required")
    return (
        "SELECT\n"
        "  g.source_id,\n"
        "  g.ra,\n"
        "  g.dec,\n"
        "  g.parallax,\n"
        "  g.parallax_error,\n"
        "  g.pmra,\n"
        "  g.pmdec,\n"
        "  g.phot_g_mean_mag,\n"
        "  g.phot_bp_mean_mag,\n"
        "  g.phot_rp_mean_mag,\n"
        "  g.ruwe,\n"
        "  g.astrometric_params_solved,\n"
        "  g.visibility_periods_used,\n"
        "  g.astrometric_excess_noise,\n"
        "  g.astrometric_excess_noise_sig,\n"
        "  g.ipd_frac_multi_peak,\n"
        "  g.ipd_gof_harmonic_amplitude,\n"
        "  g.duplicated_source,\n"
        "  g.non_single_star,\n"
        "  d.r_med_geo,\n"
        "  d.r_lo_geo,\n"
        "  d.r_hi_geo,\n"
        "  d.r_med_photogeo,\n"
        "  d.r_lo_photogeo,\n"
        "  d.r_hi_photogeo\n"
        "FROM gaiadr3.gaia_source AS g\n"
        "LEFT JOIN external.gaiaedr3_distance AS d\n"
        "  ON d.source_id = g.source_id\n"
        f"WHERE g.source_id IN ({', '.join(ids)})"
    )


def _gaia_results(query: str, client) -> pd.DataFrame:
    login_gaia_from_environment_if_available(client)
    return table_to_frame(client.launch_job_async(query).get_results())


def query_gaia(
    targets: pd.DataFrame,
    *,
    simbad_evidence: pd.DataFrame | None = None,
    client=None,
) -> pd.DataFrame:
    """Resolve HIP aliases and fetch Gaia/Bailer-Jones evidence by exact ID."""
    if client is None:
        from astroquery.gaia import Gaia

        client = Gaia

    mappings: list[dict[str, object]] = []
    for row in targets.to_dict(orient="records"):
        gaia_id = _id_string(row.get("gaia_source_id"))
        if gaia_id:
            mappings.append(
                {
                    "tracker_key": row["tracker_key"],
                    "source_id": gaia_id,
                    "match_kind": "tracker_gaia_id",
                    "hip_source_id": _id_string(row.get("hip_source_id")),
                    "angular_distance": np.nan,
                    "number_of_neighbours": np.nan,
                }
            )

    if simbad_evidence is not None and not simbad_evidence.empty:
        resolved = simbad_evidence.loc[
            simbad_evidence.get("match_status", "").eq("found")
            & simbad_evidence.get("ids", "").fillna("").astype(str).ne(""),
            ["tracker_key", "ids"],
        ].drop_duplicates("tracker_key")
        for row in resolved.to_dict(orient="records"):
            match = pd.Series([row["ids"]]).str.extract(r"Gaia DR3 (\d+)").iloc[0, 0]
            gaia_id = _id_string(match)
            if gaia_id:
                mappings.append(
                    {
                        "tracker_key": row["tracker_key"],
                        "source_id": gaia_id,
                        "match_kind": "simbad_gaia_dr3_id",
                        "hip_source_id": "",
                        "angular_distance": np.nan,
                        "number_of_neighbours": np.nan,
                    }
                )

    hip_targets = targets.loc[targets["hip_source_id"].ne("")]
    if not hip_targets.empty:
        xmatch = _gaia_results(
            build_gaia_hip_xmatch_query(hip_targets["hip_source_id"]), client
        )
        hip_to_key = dict(
            zip(
                hip_targets["hip_source_id"].map(_id_string),
                hip_targets["tracker_key"],
                strict=True,
            )
        )
        for row in xmatch.to_dict(orient="records"):
            hip_id = _id_string(row.get("original_ext_source_id"))
            gaia_id = _id_string(row.get("source_id"))
            tracker_key = hip_to_key.get(hip_id)
            if tracker_key and gaia_id:
                mappings.append(
                    {
                        "tracker_key": tracker_key,
                        "source_id": gaia_id,
                        "match_kind": "hipparcos2_best_neighbour",
                        "hip_source_id": hip_id,
                        "angular_distance": row.get("angular_distance"),
                        "number_of_neighbours": row.get("number_of_neighbours"),
                    }
                )

    mapping = pd.DataFrame.from_records(mappings)
    if mapping.empty:
        return pd.DataFrame(
            columns=["tracker_key", "source_id", "match_kind", "service"]
        )
    mapping["source_id"] = mapping["source_id"].map(_id_string)
    mapping = mapping.drop_duplicates(
        subset=["tracker_key", "source_id"], keep="first"
    )

    details = _gaia_results(build_gaia_details_query(mapping["source_id"]), client)
    details["source_id"] = details["source_id"].map(_id_string)
    evidence = mapping.merge(details, how="left", on="source_id", validate="many_to_one")
    parallax = pd.to_numeric(evidence.get("parallax"), errors="coerce")
    parallax_error = pd.to_numeric(evidence.get("parallax_error"), errors="coerce")
    evidence["parallax_snr"] = np.where(
        parallax_error > 0, parallax / parallax_error, np.nan
    )
    evidence["match_status"] = np.where(
        evidence.get("ra").notna(), "found", "not_found"
    )
    evidence["service"] = "gaia"
    return evidence


def build_starhorse_query(gaia_ids: Iterable[object]) -> str:
    """Build an indexed exact-ID query for the StarHorse 2021 table."""
    ids = _numeric_id_list(gaia_ids, "Gaia source ID")
    if not ids:
        raise ValueError("at least one Gaia source ID is required")
    return (
        "SELECT sh.*\n"
        f"FROM {STARHORSE_TABLE} AS sh\n"
        f'WHERE sh."Source" IN ({", ".join(ids)})'
    )


def _gaia_ids_for_starhorse(
    targets: pd.DataFrame, gaia_evidence: pd.DataFrame
) -> pd.DataFrame:
    records = [
        {
            "tracker_key": row["tracker_key"],
            "gaia_source_id": _id_string(row["gaia_source_id"]),
        }
        for row in targets.to_dict(orient="records")
        if _id_string(row.get("gaia_source_id"))
    ]
    if not gaia_evidence.empty:
        records.extend(
            {
                "tracker_key": str(row["tracker_key"]),
                "gaia_source_id": _id_string(row["source_id"]),
            }
            for row in gaia_evidence.to_dict(orient="records")
            if _id_string(row.get("source_id"))
        )
    return pd.DataFrame.from_records(
        records, columns=["tracker_key", "gaia_source_id"]
    ).drop_duplicates()


def query_starhorse(
    targets: pd.DataFrame,
    gaia_evidence: pd.DataFrame,
    *,
    client=None,
) -> pd.DataFrame:
    """Query StarHorse 2021 by exact Gaia EDR3/DR3 source identifier."""
    identifiers = _gaia_ids_for_starhorse(targets, gaia_evidence)
    if identifiers.empty:
        return identifiers.assign(service=pd.Series(dtype=str))

    if client is None:
        from astroquery.utils.tap.core import TapPlus

        client = TapPlus(url=VIZIER_TAP_URL)

    job = client.launch_job(build_starhorse_query(identifiers["gaia_source_id"]))
    matches = table_to_frame(job.get_results())
    if "source" not in matches:
        raise ValueError("StarHorse result omitted Source")
    matches["source"] = matches["source"].map(_id_string)
    evidence = identifiers.merge(
        matches,
        how="left",
        left_on="gaia_source_id",
        right_on="source",
        validate="many_to_one",
    ).rename(columns={"gaia_source_id": "requested_gaia_source_id"})
    for percentile in ("dist05", "dist16", "dist50", "dist84", "dist95"):
        if percentile in evidence:
            evidence[f"{percentile}_pc"] = (
                pd.to_numeric(evidence[percentile], errors="coerce") * 1000.0
            )
    match_column = evidence.get("source", pd.Series("", index=evidence.index))
    evidence["match_status"] = np.where(
        match_column.fillna("").astype(str).str.strip().ne(""),
        "found",
        "not_found",
    )
    evidence["service"] = "starhorse"
    return evidence


def _best_row(
    evidence: pd.DataFrame,
    *,
    numeric_score: str | None = None,
    ascending: bool = False,
) -> pd.DataFrame:
    if evidence.empty:
        return evidence
    ranked = evidence.copy()
    ranked["_found"] = ranked.get("match_status", "").eq("found")
    sort_columns = ["tracker_key", "_found"]
    ascending_flags = [True, False]
    if numeric_score and numeric_score in ranked:
        ranked["_score"] = pd.to_numeric(ranked[numeric_score], errors="coerce")
        sort_columns.append("_score")
        ascending_flags.append(ascending)
    ranked = ranked.sort_values(
        sort_columns,
        ascending=ascending_flags,
        na_position="last",
        kind="mergesort",
    )
    return ranked.drop_duplicates("tracker_key", keep="first").drop(
        columns=["_found", "_score"], errors="ignore"
    )


def _select_prefixed(
    frame: pd.DataFrame,
    columns: Sequence[str],
    *,
    prefix: str,
) -> pd.DataFrame:
    if "tracker_key" not in frame:
        return pd.DataFrame(columns=["tracker_key"])
    selected = ["tracker_key", *(name for name in columns if name in frame)]
    out = frame.loc[:, selected].copy()
    return out.rename(
        columns={name: f"{prefix}_{name}" for name in selected if name != "tracker_key"}
    )


def build_evidence_summary(
    targets: pd.DataFrame,
    *,
    simbad: pd.DataFrame,
    gaia: pd.DataFrame,
    starhorse: pd.DataFrame,
) -> pd.DataFrame:
    """Build one comparison row per tracker target without auto-promoting data."""
    summary_columns = [
        "tracker_key",
        "status",
        "official_source",
        "official_source_id",
        "gaia_source_id",
        "hip_source_id",
        "proper_name",
        "simbad_main_id",
        "current_distance_source",
        "reason_for_external_search",
        "next_action",
    ]
    summary = targets.loc[
        :, [column for column in summary_columns if column in targets]
    ].copy()

    simbad_best = _best_row(simbad, numeric_score="plx_snr")
    summary = summary.merge(
        _select_prefixed(
            simbad_best,
            (
                "match_status",
                "query_name",
                "alias_kind",
                "main_id",
                "ids",
                "otype",
                "otype_txt",
                "sp_type",
                "plx_value",
                "plx_err",
                "plx_snr",
                "plx_bibcode",
                "mesdistance_dist",
                "mesdistance_unit",
                "mesdistance_minus_err",
                "mesdistance_plus_err",
                "mesdistance_method",
                "mesdistance_bibcode",
            ),
            prefix="simbad_result",
        ),
        how="left",
        on="tracker_key",
        validate="one_to_one",
    )

    gaia_best = gaia.copy()
    if not gaia_best.empty:
        gaia_best["_exact"] = gaia_best["match_kind"].eq("tracker_gaia_id")
        gaia_best["_angle"] = pd.to_numeric(
            gaia_best.get("angular_distance"), errors="coerce"
        )
        gaia_best = gaia_best.sort_values(
            ["tracker_key", "_exact", "_angle"],
            ascending=[True, False, True],
            na_position="last",
            kind="mergesort",
        ).drop_duplicates("tracker_key", keep="first")
    summary = summary.merge(
        _select_prefixed(
            gaia_best,
            (
                "match_status",
                "source_id",
                "match_kind",
                "angular_distance",
                "number_of_neighbours",
                "parallax",
                "parallax_error",
                "parallax_snr",
                "ruwe",
                "duplicated_source",
                "non_single_star",
                "r_med_geo",
                "r_lo_geo",
                "r_hi_geo",
                "r_med_photogeo",
                "r_lo_photogeo",
                "r_hi_photogeo",
            ),
            prefix="gaia_result",
        ),
        how="left",
        on="tracker_key",
        validate="one_to_one",
    )

    starhorse_best = _best_row(starhorse)
    summary = summary.merge(
        _select_prefixed(
            starhorse_best,
            (
                "match_status",
                "requested_gaia_source_id",
                "source",
                "dist05_pc",
                "dist16_pc",
                "dist50_pc",
                "dist84_pc",
                "dist95_pc",
                "fidelity",
                "flagout",
            ),
            prefix="starhorse_result",
        ),
        how="left",
        on="tracker_key",
        validate="one_to_one",
    )

    simbad_found = summary.get(
        "simbad_result_match_status", pd.Series("", index=summary.index)
    ).eq("found")
    starhorse_found = summary.get(
        "starhorse_result_match_status", pd.Series("", index=summary.index)
    ).eq("found")
    gaia_found = summary.get(
        "gaia_result_match_status", pd.Series("", index=summary.index)
    ).eq("found")
    summary["evidence_status"] = np.select(
        [
            simbad_found & starhorse_found,
            starhorse_found,
            simbad_found,
            gaia_found,
        ],
        [
            "simbad_and_starhorse",
            "starhorse_only",
            "simbad_only",
            "gaia_bailer_jones_only",
        ],
        default="no_catalog_distance",
    )
    summary["automatic_promotion_allowed"] = False
    return summary


def _read_cached(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def _write_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def run_distance_research(
    tracker_path: Path = DEFAULT_TRACKER_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    *,
    services: Sequence[str] = SUPPORTED_SERVICES,
    force: bool = False,
    limit: int | None = None,
) -> dict[str, object]:
    """Run selected services, cache evidence, and write a per-target summary."""
    unknown = sorted(set(services) - set(SUPPORTED_SERVICES))
    if unknown:
        raise ValueError(f"unknown research service(s): {unknown}")
    tracker_path = Path(tracker_path)
    output_dir = Path(output_dir)
    targets = load_distance_tracker(tracker_path)
    if limit is not None:
        if limit <= 0:
            raise ValueError("limit must be positive")
        targets = targets.head(limit).copy()
    output_dir.mkdir(parents=True, exist_ok=True)

    evidence: dict[str, pd.DataFrame] = {
        service: pd.DataFrame() for service in SUPPORTED_SERVICES
    }
    errors: list[dict[str, str]] = []
    file_names = {
        "simbad": "simbad-evidence.csv",
        "gaia": "gaia-evidence.csv",
        "starhorse": "starhorse-evidence.csv",
    }

    for service in SUPPORTED_SERVICES:
        path = output_dir / file_names[service]
        if service not in services:
            if path.is_file():
                evidence[service] = _read_cached(path)
            continue
        if path.is_file() and not force:
            evidence[service] = _read_cached(path)
            continue
        try:
            if service == "simbad":
                result = query_simbad(targets)
            elif service == "gaia":
                result = query_gaia(targets, simbad_evidence=evidence["simbad"])
            else:
                result = query_starhorse(targets, evidence["gaia"])
            evidence[service] = result
            _write_csv(result, path)
        except Exception as exc:  # keep independent services useful
            errors.append(
                {
                    "service": service,
                    "error_type": type(exc).__name__,
                    "message": str(exc),
                }
            )
            if path.is_file():
                evidence[service] = _read_cached(path)

    summary = build_evidence_summary(
        targets,
        simbad=evidence["simbad"],
        gaia=evidence["gaia"],
        starhorse=evidence["starhorse"],
    )
    summary_path = output_dir / "distance-evidence-summary.csv"
    error_path = output_dir / "service-errors.csv"
    manifest_path = output_dir / "manifest.json"
    _write_csv(summary, summary_path)
    _write_csv(
        pd.DataFrame.from_records(
            errors, columns=["service", "error_type", "message"]
        ),
        error_path,
    )

    tracker_sha256 = hashlib.sha256(tracker_path.read_bytes()).hexdigest()
    manifest = {
        "created_at_utc": datetime.now(UTC).isoformat(),
        "tracker": str(tracker_path.resolve()),
        "tracker_sha256": tracker_sha256,
        "target_count": len(targets),
        "requested_services": list(services),
        "service_row_counts": {
            service: len(frame) for service, frame in evidence.items()
        },
        "error_count": len(errors),
        "summary": str(summary_path.resolve()),
        "automatic_override_promotion": False,
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest
