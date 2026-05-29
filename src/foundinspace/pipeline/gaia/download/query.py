from __future__ import annotations

import hashlib
from dataclasses import dataclass

from foundinspace.pipeline.gaia.download.fieldsets import GaiaCarryField
from foundinspace.pipeline.gaia.engines import (
    DEFAULT_GAIA_CALCULATION_ENGINE,
    get_gaia_calculation_engine,
)

HP3_DIVISOR = 9_007_199_254_740_992


@dataclass(frozen=True, slots=True)
class GaiaQuerySpec:
    mode: str
    mag_limit: float | None
    carry_fields: tuple[GaiaCarryField, ...]
    source_where_adql: str | None = None
    calculation_engine: str = DEFAULT_GAIA_CALCULATION_ENGINE

    @property
    def requires_aps(self) -> bool:
        return any("aps" in field.query_aliases for field in self.carry_fields)


def _select_columns(alias: str, columns: tuple[str, ...]) -> list[str]:
    return [f"  {alias}.{column} AS {column}" for column in columns]


def _format_float(value: float) -> str:
    return format(float(value), ".12g")


def _normalize_source_where_adql(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    normalized = fragment.strip()
    if not normalized:
        raise ValueError("gaia_download.source_where_adql must not be empty")
    if ";" in normalized:
        raise ValueError("gaia_download.source_where_adql must not contain semicolons")
    return normalized


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
    source_where = _normalize_source_where_adql(spec.source_where_adql)
    if source_where is not None:
        clauses.append(f"({source_where})")
    if hp3_values is not None:
        if not hp3_values:
            raise ValueError("hp3_values must not be empty when provided")
        values = ", ".join(str(int(value)) for value in sorted(hp3_values))
        clauses.append(f"(g.source_id / {HP3_DIVISOR}) IN ({values})")
    return clauses


def _format_where(clauses: list[str]) -> str:
    return "\nWHERE\n  " + "\n  AND ".join(clauses)


def validate_query_spec(spec: GaiaQuerySpec) -> None:
    _normalize_source_where_adql(spec.source_where_adql)
    engine = get_gaia_calculation_engine(spec.calculation_engine)
    core_names = set(engine.core_output_names)
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
    engine = get_gaia_calculation_engine(spec.calculation_engine)
    if not engine.requires_distance_table:
        raise ValueError(
            f"Gaia calculation engine {engine.name!r} does not support the "
            "current count query distance eligibility filter"
        )
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
    engine = get_gaia_calculation_engine(spec.calculation_engine)
    select_lines: list[str] = []
    select_lines.extend(_select_columns("g", engine.gaia_source_columns))
    select_lines.extend(_select_columns("d", engine.distance_columns))
    select_lines.extend(_select_columns("ap", engine.astrophysical_parameter_columns))
    for field in spec.carry_fields:
        if field.source == "query":
            select_lines.append(f"  {field.expression} AS {field.name}")

    joins = [
        "FROM gaiadr3.gaia_source AS g",
        "JOIN external.gaiaedr3_distance AS d",
        "  ON d.source_id = g.source_id",
    ]
    if engine.requires_astrophysical_parameters:
        joins.extend(
            [
                "LEFT JOIN gaiadr3.astrophysical_parameters AS ap",
                "  ON ap.source_id = g.source_id",
            ]
        )
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
