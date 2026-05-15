from __future__ import annotations

import hashlib
from dataclasses import dataclass

from foundinspace.pipeline.gaia.download.fieldsets import GaiaCarryField

HP3_DIVISOR = 9_007_199_254_740_992

GAIA_SOURCE_CORE_COLS = (
    "source_id",
    "ra",
    "dec",
    "parallax",
    "parallax_error",
    "pmra",
    "pmdec",
    "phot_g_mean_mag",
    "phot_bp_mean_mag",
    "phot_rp_mean_mag",
    "ruwe",
)

DISTANCE_CORE_COLS = (
    "r_med_geo",
    "r_lo_geo",
    "r_hi_geo",
    "r_med_photogeo",
    "r_lo_photogeo",
    "r_hi_photogeo",
)

ASTROPHYS_CORE_COLS = (
    "mg_gspphot",
    "mg_gspphot_lower",
    "mg_gspphot_upper",
    "ag_gspphot",
    "teff_esphs",
    "teff_gspspec",
    "teff_espucd",
    "teff_gspphot",
    "teff_gspphot_lower",
    "teff_gspphot_upper",
    "logg_esphs",
    "logg_gspspec",
    "logg_gspphot",
    "logg_gspphot_lower",
    "logg_gspphot_upper",
)

CORE_OUTPUT_NAMES = (
    *GAIA_SOURCE_CORE_COLS,
    *DISTANCE_CORE_COLS,
    *ASTROPHYS_CORE_COLS,
)


@dataclass(frozen=True, slots=True)
class GaiaQuerySpec:
    mode: str
    mag_limit: float | None
    carry_fields: tuple[GaiaCarryField, ...]

    @property
    def requires_aps(self) -> bool:
        return any("aps" in field.query_aliases for field in self.carry_fields)


def _select_columns(alias: str, columns: tuple[str, ...]) -> list[str]:
    return [f"  {alias}.{column} AS {column}" for column in columns]


def _format_float(value: float) -> str:
    return format(float(value), ".12g")


def _where_clauses(
    spec: GaiaQuerySpec,
    *,
    hp3_values: tuple[int, ...] | list[int] | None = None,
) -> list[str]:
    clauses = [
        "g.astrometric_params_solved IN (31, 95)",
        "(d.r_med_photogeo IS NOT NULL OR d.r_med_geo IS NOT NULL)",
    ]
    if spec.mag_limit is not None:
        clauses.append(f"g.phot_g_mean_mag <= {_format_float(spec.mag_limit)}")
    if hp3_values is not None:
        if not hp3_values:
            raise ValueError("hp3_values must not be empty when provided")
        values = ", ".join(str(int(value)) for value in sorted(hp3_values))
        clauses.append(f"(g.source_id / {HP3_DIVISOR}) IN ({values})")
    return clauses


def _format_where(clauses: list[str]) -> str:
    return "\nWHERE\n  " + "\n  AND ".join(clauses)


def validate_query_spec(spec: GaiaQuerySpec) -> None:
    core_names = set(CORE_OUTPUT_NAMES)
    for field in spec.carry_fields:
        if field.source == "query" and field.name in core_names:
            raise ValueError(
                f"Gaia carry field {field.name!r} duplicates a core query column"
            )
        if "aps" in field.query_aliases and not spec.requires_aps:
            raise ValueError(
                f"Internal error: aps field not reflected in spec: {field.name}"
            )


def build_count_query(spec: GaiaQuerySpec) -> str:
    validate_query_spec(spec)
    return (
        "SELECT\n"
        f"  (g.source_id / {HP3_DIVISOR}) AS hp3,\n"
        "  COUNT(*) AS n\n"
        "FROM gaiadr3.gaia_source AS g\n"
        "JOIN external.gaiaedr3_distance AS d\n"
        "  ON d.source_id = g.source_id"
        f"{_format_where(_where_clauses(spec))}\n"
        "GROUP BY 1\n"
        "ORDER BY n DESC"
    )


def build_download_query(
    spec: GaiaQuerySpec,
    *,
    hp3_values: tuple[int, ...] | list[int] | None = None,
) -> str:
    validate_query_spec(spec)
    select_lines: list[str] = []
    select_lines.extend(_select_columns("g", GAIA_SOURCE_CORE_COLS))
    select_lines.extend(_select_columns("d", DISTANCE_CORE_COLS))
    select_lines.extend(_select_columns("ap", ASTROPHYS_CORE_COLS))
    for field in spec.carry_fields:
        if field.source == "query":
            select_lines.append(f"  {field.expression} AS {field.name}")

    joins = [
        "FROM gaiadr3.gaia_source AS g",
        "JOIN external.gaiaedr3_distance AS d",
        "  ON d.source_id = g.source_id",
        "LEFT JOIN gaiadr3.astrophysical_parameters AS ap",
        "  ON ap.source_id = g.source_id",
    ]
    if spec.requires_aps:
        joins.extend(
            [
                "LEFT JOIN gaiadr3.astrophysical_parameters_supp AS aps",
                "  ON aps.source_id = g.source_id",
            ]
        )

    return (
        "SELECT\n"
        + ",\n".join(select_lines)
        + "\n"
        + "\n".join(joins)
        + _format_where(_where_clauses(spec, hp3_values=hp3_values))
    )


def query_hash(query: str) -> str:
    return hashlib.sha256(query.encode("utf-8")).hexdigest()
