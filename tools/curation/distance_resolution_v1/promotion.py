"""Promote reviewed distance resolutions into an explicit override YAML."""

from __future__ import annotations

import hashlib
import math
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from foundinspace.pipeline.overrides.loader import load_parsed_override_documents
from tools.curation.distance_resolution_v1.research import (
    DEFAULT_TRACKER_PATH,
    load_distance_tracker,
)

DEFAULT_PREFLIGHT_DIR = Path("downloads/official-upper-level-preflight")
DEFAULT_RESOLVED_OVERRIDE_PATH = Path(__file__).with_name(
    "distance-resolution-v1-resolved.yaml"
)

_PREFLIGHT_FILES = {
    "candidate_review": "external-distance-required.parquet",
    "pipeline_candidates": "current-pipeline-candidates.parquet",
    "gaia_staged": "gaia-staged.parquet",
    "hip_staged": "hip-staged.parquet",
}
_PAYLOAD_POLICY_VERSION = "distance_resolution_v1"
_OVERRIDE_REASON = "reviewed_poor_distance_replacement"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _id_string(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    if text.endswith(".0") and text[:-2].isdigit():
        return text[:-2]
    return text


def _key(source: object, source_id: object) -> str:
    return f"{str(source).strip().lower()}:{_id_string(source_id)}"


def _require_unique_keys(
    frame: pd.DataFrame,
    *,
    source_column: str,
    id_column: str,
    label: str,
) -> pd.DataFrame:
    out = frame.copy()
    out["_promotion_key"] = [
        _key(source, source_id)
        for source, source_id in zip(
            out[source_column],
            out[id_column],
            strict=True,
        )
    ]
    duplicates = out["_promotion_key"].duplicated(keep=False)
    if bool(duplicates.any()):
        values = sorted(out.loc[duplicates, "_promotion_key"].unique())
        raise ValueError(f"{label} contains duplicate target(s): {values}")
    return out.set_index("_promotion_key", drop=False)


def _require_exact_key_set(
    actual: set[str],
    expected: set[str],
    *,
    label: str,
) -> None:
    if actual == expected:
        return
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    raise ValueError(
        f"{label} target set differs from tracker; missing={missing}, extra={extra}"
    )


def _finite_float(value: object, label: str) -> float:
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{label} must be finite, got {value!r}")
    return result


def _stage_index(frame: pd.DataFrame, source: str) -> pd.DataFrame:
    ids = frame["source_id"].map(_id_string)
    if bool(ids.eq("").any()):
        raise ValueError(f"{source} staged data contains a blank source_id")
    if bool(ids.duplicated(keep=False).any()):
        duplicates = sorted(ids.loc[ids.duplicated(keep=False)].unique())
        raise ValueError(f"{source} staged data contains duplicate IDs: {duplicates}")
    out = frame.copy()
    out["_source_id"] = ids
    return out.set_index("_source_id", drop=False)


def _donor_row(
    candidate: pd.Series,
    staged: dict[str, pd.DataFrame],
    donor_kind: str,
    tracker_key: str,
) -> tuple[str, str, pd.Series]:
    source = str(candidate[f"{donor_kind}_donor"]).strip().lower()
    if source not in staged:
        raise ValueError(
            f"{tracker_key}: unsupported {donor_kind} donor source {source!r}"
        )
    source_id = _id_string(candidate.get(f"{source}_source_id"))
    if not source_id or source_id not in staged[source].index:
        raise ValueError(
            f"{tracker_key}: {donor_kind} donor {source}:{source_id} is absent "
            "from staged data"
        )
    return source, source_id, staged[source].loc[source_id]


def _identifier_mapping(row: pd.Series) -> dict[str, object]:
    identifiers: dict[str, object] = {}
    gaia_id = _id_string(row.get("gaia_source_id"))
    hip_id = _id_string(row.get("hip_source_id"))
    proper_name = str(row.get("proper_name", "")).strip()
    simbad_main_id = str(row.get("simbad_main_id", "")).strip()
    if gaia_id:
        identifiers["gaia_source_id"] = gaia_id
    if hip_id:
        identifiers["hip_id"] = hip_id
    if proper_name:
        identifiers["proper_name"] = proper_name
    if simbad_main_id:
        identifiers["simbad_main_id"] = simbad_main_id
    return identifiers


def build_resolved_distance_override_document(
    *,
    tracker_path: Path = DEFAULT_TRACKER_PATH,
    preflight_dir: Path = DEFAULT_PREFLIGHT_DIR,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build and cross-check the resolved-row override YAML document."""
    tracker_path = Path(tracker_path)
    preflight_dir = Path(preflight_dir)
    input_paths = {
        "distance_resolution_tracker": tracker_path,
        **{
            label: preflight_dir / filename
            for label, filename in _PREFLIGHT_FILES.items()
        },
    }
    missing = [str(path) for path in input_paths.values() if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"override promotion input(s) missing: {missing}")

    tracker = load_distance_tracker(tracker_path)
    tracker = _require_unique_keys(
        tracker,
        source_column="official_source",
        id_column="official_source_id",
        label="distance tracker",
    )
    tracker_keys = set(tracker.index)

    review = _require_unique_keys(
        pd.read_parquet(input_paths["candidate_review"]),
        source_column="official_source",
        id_column="official_source_id",
        label="candidate review",
    )
    candidates = _require_unique_keys(
        pd.read_parquet(input_paths["pipeline_candidates"]),
        source_column="official_source",
        id_column="official_source_id",
        label="pipeline candidates",
    )
    _require_exact_key_set(set(review.index), tracker_keys, label="candidate review")
    missing_candidates = tracker_keys - set(candidates.index)
    if missing_candidates:
        raise ValueError(
            f"pipeline candidates omit tracked target(s): {sorted(missing_candidates)}"
        )

    staged = {
        "gaia": _stage_index(
            pd.read_parquet(input_paths["gaia_staged"]),
            "gaia",
        ),
        "hip": _stage_index(
            pd.read_parquet(input_paths["hip_staged"]),
            "hip",
        ),
    }

    stars: list[dict[str, Any]] = []
    max_apparent_magnitude_delta = 0.0
    resolved = tracker.loc[tracker["status"].eq("resolved")]
    provisional = tracker.loc[tracker["status"].eq("provisional")]
    for tracker_key, row in resolved.iterrows():
        candidate = candidates.loc[tracker_key]
        astrometry_source, astrometry_id, astrometry = _donor_row(
            candidate,
            staged,
            "astrometry",
            tracker_key,
        )
        photometry_source, photometry_id, photometry = _donor_row(
            candidate,
            staged,
            "photometry",
            tracker_key,
        )
        teff_source, teff_id, temperature = _donor_row(
            candidate,
            staged,
            "teff",
            tracker_key,
        )

        selected_distance = _finite_float(
            row["resolved_distance_pc"],
            f"{tracker_key} resolved distance",
        )
        selected_error = _finite_float(
            row["resolved_distance_error_pc"],
            f"{tracker_key} resolved distance error",
        )
        if selected_distance <= 0 or selected_error <= 0:
            raise ValueError(
                f"{tracker_key}: resolved distance and error must be positive"
            )

        apparent_magnitude = _finite_float(
            candidate["candidate_apparent_mag_reconstructed"],
            f"{tracker_key} candidate apparent magnitude",
        )
        donor_apparent_magnitude = (
            _finite_float(
                photometry["mag_abs"],
                f"{tracker_key} photometry donor absolute magnitude",
            )
            + 5.0
            * math.log10(
                _finite_float(
                    photometry["r_pc"],
                    f"{tracker_key} photometry donor distance",
                )
            )
            - 5.0
        )
        apparent_delta = abs(apparent_magnitude - donor_apparent_magnitude)
        max_apparent_magnitude_delta = max(
            max_apparent_magnitude_delta,
            apparent_delta,
        )
        if apparent_delta > 1e-4:
            raise ValueError(
                f"{tracker_key}: candidate apparent magnitude differs from "
                f"photometry donor by {apparent_delta:.6g} mag"
            )

        new_mag_abs = apparent_magnitude - 5.0 * math.log10(selected_distance) + 5.0
        ra_deg = _finite_float(
            astrometry["ra_deg"],
            f"{tracker_key} astrometry donor RA",
        )
        dec_deg = _finite_float(
            astrometry["dec_deg"],
            f"{tracker_key} astrometry donor Dec",
        )
        teff = _finite_float(
            temperature["teff"],
            f"{tracker_key} temperature donor Teff",
        )

        source, source_id = tracker_key.split(":", 1)
        stars.append(
            {
                "override_id": f"distance.{source}.{source_id}.replace.v1",
                "action": "replace",
                "source": source,
                "source_id": source_id,
                "override_reason": _OVERRIDE_REASON,
                "override_policy_version": _PAYLOAD_POLICY_VERSION,
                "ra_deg": ra_deg,
                "dec_deg": dec_deg,
                "r_pc": selected_distance,
                "mag_abs": new_mag_abs,
                "teff": teff,
                "identifiers": _identifier_mapping(row),
                "provenance": {
                    "schema_version": 1,
                    "tracker_key": tracker_key,
                    "review_status": "resolved",
                    "prior_distance_source": str(row["current_distance_source"]),
                    "reason_for_external_search": str(
                        row["reason_for_external_search"]
                    ),
                    "selected_distance": {
                        "value_pc": selected_distance,
                        "symmetric_error_pc": selected_error,
                        "reference": str(row["resolved_reference"]),
                        "notes": str(row["resolution_notes"]),
                    },
                    "preserved_payload": {
                        "apparent_magnitude": apparent_magnitude,
                        "magnitude_rebase_formula": (
                            "mag_abs = apparent_mag - 5*log10(r_pc) + 5"
                        ),
                        "astrometry_donor": (f"{astrometry_source}:{astrometry_id}"),
                        "photometry_donor": (f"{photometry_source}:{photometry_id}"),
                        "temperature_donor": f"{teff_source}:{teff_id}",
                    },
                },
            }
        )

    input_records = {
        label: {
            "filename": path.name,
            "sha256": _sha256(path),
        }
        for label, path in input_paths.items()
    }
    document = {
        "description": (
            "Reviewed distance replacements for the resolved subset of the "
            "81-star poor-distance candidate cohort. Provisional rows are "
            "deliberately excluded."
        ),
        "dataset": {
            "schema_version": 1,
            "dataset_id": "fis.distance-resolution-v1.resolved",
            "override_policy_version": _PAYLOAD_POLICY_VERSION,
            "selection": "status == resolved",
            "resolved_row_count": len(stars),
            "excluded_provisional_row_count": len(provisional),
            "inputs": input_records,
        },
        "stars": stars,
    }
    report = {
        "tracker_rows": len(tracker),
        "resolved_tracker_rows": len(resolved),
        "provisional_tracker_rows": len(provisional),
        "yaml_rows": len(stars),
        "candidate_review_identity_match": True,
        "pipeline_candidate_rows_matched": len(tracker),
        "payload_rows_cross_checked": len(stars),
        "max_apparent_magnitude_delta": max_apparent_magnitude_delta,
        "input_sha256": {
            label: record["sha256"] for label, record in input_records.items()
        },
    }
    return document, report


def write_resolved_distance_override_yaml(
    output_path: Path = DEFAULT_RESOLVED_OVERRIDE_PATH,
    *,
    tracker_path: Path = DEFAULT_TRACKER_PATH,
    preflight_dir: Path = DEFAULT_PREFLIGHT_DIR,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Generate the reviewed override YAML and return its cross-check report."""
    output_path = Path(output_path)
    if output_path.exists() and not overwrite:
        raise FileExistsError(str(output_path))
    document, report = build_resolved_distance_override_document(
        tracker_path=tracker_path,
        preflight_dir=preflight_dir,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        yaml.safe_dump(
            document,
            sort_keys=False,
            allow_unicode=True,
            width=100,
        ),
        encoding="utf-8",
    )
    report["output"] = str(output_path.resolve())
    report["output_sha256"] = _sha256(output_path)
    return report


def validate_resolved_override_against_tracker(
    override_path: Path = DEFAULT_RESOLVED_OVERRIDE_PATH,
    *,
    tracker_path: Path = DEFAULT_TRACKER_PATH,
) -> dict[str, Any]:
    """Validate YAML identities and reviewed evidence against the tracker."""
    override_path = Path(override_path)
    tracker = load_distance_tracker(tracker_path)
    expected = tracker.loc[tracker["status"].eq("resolved")].copy()
    expected["tracker_key"] = (
        expected["official_source"] + ":" + expected["official_source_id"]
    )
    expected = expected.set_index("tracker_key")

    documents = load_parsed_override_documents(source_paths=(override_path,))
    if len(documents) != 1:
        raise ValueError(f"{override_path} must contain exactly one YAML document")
    stars = documents[0].get("stars")
    if not isinstance(stars, list):
        raise ValueError(f"{override_path} does not contain a stars list")

    actual: dict[str, dict[str, Any]] = {}
    for star in stars:
        if not isinstance(star, dict):
            raise ValueError("override stars must be mappings")
        tracker_key = _key(star.get("source"), star.get("source_id"))
        if tracker_key in actual:
            raise ValueError(f"duplicate override target: {tracker_key}")
        actual[tracker_key] = star
    _require_exact_key_set(
        set(actual),
        set(expected.index),
        label="resolved override YAML",
    )

    for tracker_key, row in expected.iterrows():
        star = actual[tracker_key]
        if star.get("action") != "replace":
            raise ValueError(f"{tracker_key}: action must be replace")
        provenance = star.get("provenance")
        if not isinstance(provenance, dict):
            raise ValueError(f"{tracker_key}: provenance is missing")
        selected = provenance.get("selected_distance")
        if not isinstance(selected, dict):
            raise ValueError(f"{tracker_key}: selected-distance provenance is missing")
        comparisons = {
            "r_pc": (
                float(star["r_pc"]),
                float(row["resolved_distance_pc"]),
            ),
            "symmetric_error_pc": (
                float(selected["symmetric_error_pc"]),
                float(row["resolved_distance_error_pc"]),
            ),
        }
        for label, (actual_value, expected_value) in comparisons.items():
            if not math.isclose(
                actual_value,
                expected_value,
                rel_tol=0.0,
                abs_tol=1e-12,
            ):
                raise ValueError(
                    f"{tracker_key}: {label} differs from tracker "
                    f"({actual_value} != {expected_value})"
                )
        if str(selected.get("reference", "")) != str(row["resolved_reference"]):
            raise ValueError(f"{tracker_key}: reference differs from tracker")
        if str(selected.get("notes", "")) != str(row["resolution_notes"]):
            raise ValueError(f"{tracker_key}: resolution notes differ from tracker")

    return {
        "tracker_rows": len(tracker),
        "resolved_tracker_rows": len(expected),
        "provisional_tracker_rows": int(tracker["status"].eq("provisional").sum()),
        "yaml_rows": len(stars),
        "identity_match": True,
        "distance_match": True,
        "uncertainty_match": True,
        "reference_match": True,
        "notes_match": True,
        "output_sha256": _sha256(override_path),
    }
