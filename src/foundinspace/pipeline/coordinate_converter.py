"""Utilities for converting ad hoc astrometric rows to project coordinates."""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from typing import Any

import numpy as np
import pandas as pd
from astropy import units as u
from astropy.coordinates import Angle

from foundinspace.pipeline.common.coords import calculate_coordinates_fast
from foundinspace.pipeline.common.photometry import (
    TEFF_LOG8_SENTINEL,
    encode_teff_log8,
)
from foundinspace.pipeline.constants import CANONICAL_EPOCH_JYEAR

_COLUMN_STRIP_RE = re.compile(r"[^a-z0-9]+")
_UNICODE_MINUS_TRANSLATION = str.maketrans(
    {
        "\N{MINUS SIGN}": "-",
        "\N{EN DASH}": "-",
        "\N{EM DASH}": "-",
        "\N{SMALL HYPHEN-MINUS}": "-",
        "\N{FULLWIDTH HYPHEN-MINUS}": "-",
    }
)

IDENTITY_ALIASES = {
    "name": ("name", "label", "object", "star", "designation"),
    "source": ("source",),
    "source_id": ("source_id", "sourceid", "id"),
}

RA_DEG_ALIASES = ("ra_deg", "radeg", "ra_degrees", "ra_degree")
RA_HOUR_ALIASES = ("ra_hours", "ra_hour", "rahours", "rahour", "ra_h")
RA_SEXAGESIMAL_ALIASES = ("ra_hms", "rahms")
RA_GENERIC_ALIASES = ("ra",)

DEC_DEG_ALIASES = ("dec_deg", "decdeg", "dec_degrees", "dec_degree")
DEC_SEXAGESIMAL_ALIASES = ("dec_dms", "decdms")
DEC_GENERIC_ALIASES = ("dec", "declination")

DISTANCE_ALIASES = ("distance_pc", "distance", "r_pc", "rpc")
PARALLAX_ALIASES = ("parallax_mas", "parallax", "plx_mas", "plx")
PMRA_ALIASES = (
    "pmra_masyr",
    "pmra",
    "pm_ra",
    "pmRA",
    "pmRA*",
    "pmra_star",
    "pmra_use_masyr",
)
PMDEC_ALIASES = (
    "pmdec_masyr",
    "pmdec",
    "pm_dec",
    "pmDE",
    "pmdec_use_masyr",
)
EPOCH_ALIASES = ("epoch_yr", "epoch", "source_epoch_yr", "source_epoch")
TEFF_ALIASES = ("teff_k", "teff", "temperature_k", "temperature")

OUTPUT_COLUMNS = [
    "name",
    "source",
    "source_id",
    "x_icrs_pc",
    "y_icrs_pc",
    "z_icrs_pc",
    "ra_deg",
    "dec_deg",
    "r_pc",
    "teff_k",
    "teff_log8",
    "teff_log8_is_sentinel",
    "ra_source_deg",
    "dec_source_deg",
    "distance_source",
    "epoch_yr",
    "pmra_masyr",
    "pmdec_masyr",
    "parallax_mas",
]


def _normalize_column(name: str) -> str:
    return _COLUMN_STRIP_RE.sub("", name.strip().lower())


def _column_lookup(columns: Iterable[str]) -> dict[str, str]:
    return {_normalize_column(str(column)): str(column) for column in columns}


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    return bool(pd.isna(value))


def _normalize_text(value: Any) -> str:
    return str(value).strip().translate(_UNICODE_MINUS_TRANSLATION)


def _first_present(
    row: Mapping[str, Any], lookup: Mapping[str, str], aliases: Iterable[str]
) -> Any | None:
    for alias in aliases:
        key = lookup.get(_normalize_column(alias))
        if key is None:
            continue
        value = row[key]
        if not _is_missing(value):
            return value
    return None


def _float_or_none(value: Any) -> float | None:
    if _is_missing(value):
        return None
    if isinstance(value, str):
        value = _normalize_text(value).replace(",", "")
    return float(value)


def _looks_sexagesimal(text: str) -> bool:
    lower = text.lower()
    return (
        ":" in text
        or len(text.split()) >= 2
        or any(mark in lower for mark in ("h", "m", "s", "d", "deg", "°"))
    )


def parse_ra_deg(value: Any, *, unit_hint: str | None = None) -> float:
    """Parse RA as degrees.

    Generic sexagesimal values such as ``08 55 10.8317`` are interpreted as
    hourangle. Decimal generic values are interpreted as degrees.
    """
    if _is_missing(value):
        raise ValueError("RA is required")
    text = _normalize_text(value)
    if unit_hint == "deg":
        return float(text)
    if unit_hint == "hour":
        return (
            Angle(text, unit=u.hourangle).degree
            if _looks_sexagesimal(text)
            else float(text) * 15.0
        )
    if _looks_sexagesimal(text):
        if "d" in text.lower() or "deg" in text.lower() or "°" in text:
            return Angle(text, unit=u.deg).degree
        return Angle(text, unit=u.hourangle).degree
    return float(text)


def parse_dec_deg(value: Any) -> float:
    """Parse Dec as degrees."""
    if _is_missing(value):
        raise ValueError("Dec is required")
    text = _normalize_text(value)
    if not _looks_sexagesimal(text):
        return float(text)
    return Angle(text, unit=u.deg).degree


def _parse_ra_from_row(row: Mapping[str, Any], lookup: Mapping[str, str]) -> float:
    value = _first_present(row, lookup, RA_DEG_ALIASES)
    if value is not None:
        return parse_ra_deg(value, unit_hint="deg")
    value = _first_present(row, lookup, RA_HOUR_ALIASES)
    if value is not None:
        return parse_ra_deg(value, unit_hint="hour")
    value = _first_present(row, lookup, RA_SEXAGESIMAL_ALIASES)
    if value is not None:
        return parse_ra_deg(value, unit_hint="hour")
    value = _first_present(row, lookup, RA_GENERIC_ALIASES)
    return parse_ra_deg(value)


def _parse_dec_from_row(row: Mapping[str, Any], lookup: Mapping[str, str]) -> float:
    value = _first_present(row, lookup, DEC_DEG_ALIASES)
    if value is not None:
        return parse_dec_deg(value)
    value = _first_present(row, lookup, DEC_SEXAGESIMAL_ALIASES)
    if value is not None:
        return parse_dec_deg(value)
    value = _first_present(row, lookup, DEC_GENERIC_ALIASES)
    return parse_dec_deg(value)


def _distance_from_row(
    row: Mapping[str, Any], lookup: Mapping[str, str]
) -> tuple[float, float | None, str]:
    distance = _float_or_none(_first_present(row, lookup, DISTANCE_ALIASES))
    parallax = _float_or_none(_first_present(row, lookup, PARALLAX_ALIASES))
    if distance is not None:
        if distance <= 0:
            raise ValueError("distance_pc must be positive")
        return distance, parallax, "distance_pc"
    if parallax is None:
        raise ValueError("Either distance_pc or parallax_mas is required")
    if parallax <= 0:
        raise ValueError("parallax_mas must be positive")
    return 1000.0 / parallax, parallax, "parallax_mas"


def _epoch_from_row(
    row: Mapping[str, Any],
    lookup: Mapping[str, str],
    *,
    pmra_masyr: float,
    pmdec_masyr: float,
) -> float:
    epoch = _float_or_none(_first_present(row, lookup, EPOCH_ALIASES))
    if epoch is not None:
        return epoch
    if pmra_masyr != 0.0 or pmdec_masyr != 0.0:
        raise ValueError("epoch_yr is required when proper motion is non-zero")
    return CANONICAL_EPOCH_JYEAR


def convert_coordinate_table(input_df: pd.DataFrame) -> pd.DataFrame:
    """Convert ad hoc coordinate rows into project and viewer-friendly columns."""
    if input_df.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    lookup = _column_lookup(input_df.columns)
    work_rows: list[dict[str, Any]] = []
    output_rows: list[dict[str, Any]] = []

    for row_number, (_, row) in enumerate(input_df.iterrows(), start=1):
        try:
            ra_source_deg = _parse_ra_from_row(row, lookup)
            dec_source_deg = _parse_dec_from_row(row, lookup)
            distance_pc, parallax_mas, distance_source = _distance_from_row(row, lookup)
            pmra_masyr = (
                _float_or_none(_first_present(row, lookup, PMRA_ALIASES)) or 0.0
            )
            pmdec_masyr = (
                _float_or_none(_first_present(row, lookup, PMDEC_ALIASES)) or 0.0
            )
            epoch_yr = _epoch_from_row(
                row, lookup, pmra_masyr=pmra_masyr, pmdec_masyr=pmdec_masyr
            )
            teff_k = _float_or_none(_first_present(row, lookup, TEFF_ALIASES))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Row {row_number}: {exc}") from exc

        work_rows.append(
            {
                "ra_use_deg": ra_source_deg,
                "dec_use_deg": dec_source_deg,
                "distance_use_pc": distance_pc,
                "pmra_use_masyr": pmra_masyr,
                "pmdec_use_masyr": pmdec_masyr,
                "epoch_yr": epoch_yr,
            }
        )

        identity = {
            key: _first_present(row, lookup, aliases)
            for key, aliases in IDENTITY_ALIASES.items()
        }
        output_rows.append(
            {
                **identity,
                "teff_k": teff_k if teff_k is not None else np.nan,
                "ra_source_deg": ra_source_deg,
                "dec_source_deg": dec_source_deg,
                "distance_source": distance_source,
                "epoch_yr": epoch_yr,
                "pmra_masyr": pmra_masyr,
                "pmdec_masyr": pmdec_masyr,
                "parallax_mas": parallax_mas if parallax_mas is not None else np.nan,
            }
        )

    calculated = calculate_coordinates_fast(pd.DataFrame(work_rows))
    out = pd.DataFrame(output_rows)
    out["x_icrs_pc"] = calculated["x_icrs_pc"].to_numpy()
    out["y_icrs_pc"] = calculated["y_icrs_pc"].to_numpy()
    out["z_icrs_pc"] = calculated["z_icrs_pc"].to_numpy()
    out["ra_deg"] = calculated["ra_deg"].to_numpy()
    out["dec_deg"] = calculated["dec_deg"].to_numpy()
    out["r_pc"] = calculated["r_pc"].to_numpy()
    out["teff_log8"] = encode_teff_log8(out["teff_k"].to_numpy(dtype=float))
    out["teff_log8_is_sentinel"] = out["teff_log8"] == TEFF_LOG8_SENTINEL

    for column in OUTPUT_COLUMNS:
        if column not in out:
            out[column] = pd.NA
    return out[OUTPUT_COLUMNS]


def to_viewer_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Return compact objects useful for dynamic viewer-side insertion."""
    records: list[dict[str, Any]] = []
    for row in df.to_dict(orient="records"):
        record = {
            "positionPc": [
                float(row["x_icrs_pc"]),
                float(row["y_icrs_pc"]),
                float(row["z_icrs_pc"]),
            ],
            "teffLog8": int(row["teff_log8"]),
        }
        for key in ("name", "source", "source_id"):
            value = row.get(key)
            if not _is_missing(value):
                record[key] = value
        teff_k = row.get("teff_k")
        if not _is_missing(teff_k):
            record["teffK"] = float(teff_k)
        records.append(record)
    return records


def dataframe_to_json_records(df: pd.DataFrame) -> str:
    """Serialize conversion output as indented JSON records."""
    records = json.loads(df.to_json(orient="records"))
    return json.dumps(records, indent=2)
