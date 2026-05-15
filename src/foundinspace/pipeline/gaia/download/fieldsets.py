from __future__ import annotations

import re
from dataclasses import dataclass
from importlib import metadata
from pathlib import Path
from typing import Any

import tomllib

_DIST_NAME = "found-in-space-pipeline"
_IDENTIFIER_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_ALIAS_RE = re.compile(r"\b([A-Za-z][A-Za-z0-9_]*)\.")
_VALID_DTYPES = {"float64", "int64", "uint64", "string", "bool"}
_VALID_QUERY_ALIASES = {"g", "d", "ap", "aps"}
_VALID_STAGE_SOURCES = {"input", "stage"}


@dataclass(frozen=True, slots=True)
class GaiaCarryField:
    """One optional Gaia value carried through staging into sidecars."""

    name: str
    dtype: str
    sidecar: str
    source: str
    column: str | None = None
    expression: str | None = None
    field_set: str = ""
    unit: str | None = None

    @property
    def output_column(self) -> str:
        return f"gaia_{self.name}"

    @property
    def input_column(self) -> str:
        return self.column or self.name

    @property
    def query_aliases(self) -> set[str]:
        if self.expression is None:
            return set()
        return set(_ALIAS_RE.findall(self.expression))


def _field_set_path(name: str) -> Path:
    filename = f"{name}.toml"
    repo_path = Path(__file__).resolve().parents[5] / "field_sets" / "gaia" / filename
    if repo_path.is_file():
        return repo_path

    try:
        dist_files = metadata.files(_DIST_NAME)
    except metadata.PackageNotFoundError as exc:
        raise FileNotFoundError(f"Gaia field set not found: {name}") from exc

    if dist_files is not None:
        expected = f"field_sets/gaia/{filename}"
        for dist_file in dist_files:
            if str(dist_file).replace("\\", "/") == expected:
                located = Path(dist_file.locate())
                if located.is_file():
                    return located

    raise FileNotFoundError(f"Gaia field set not found: {name}")


def _require_string(raw: dict[str, Any], key: str, *, label: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label}.{key} must be a non-empty string")
    return value.strip()


def _parse_field(raw: dict[str, Any], *, field_set: str, index: int) -> GaiaCarryField:
    label = f"{field_set}.fields[{index}]"
    name = _require_string(raw, "name", label=label)
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"{label}.name must be a lowercase identifier")
    if name.startswith("gaia_"):
        raise ValueError(f"{label}.name must not include the gaia_ output prefix")

    dtype = _require_string(raw, "dtype", label=label)
    if dtype not in _VALID_DTYPES:
        raise ValueError(f"{label}.dtype must be one of {', '.join(sorted(_VALID_DTYPES))}")

    sidecar = _require_string(raw, "sidecar", label=label)
    if not _IDENTIFIER_RE.match(sidecar):
        raise ValueError(f"{label}.sidecar must be a lowercase identifier")

    expression = raw.get("expression")
    if expression is not None:
        if not isinstance(expression, str) or not expression.strip():
            raise ValueError(f"{label}.expression must be a non-empty string")
        field = GaiaCarryField(
            name=name,
            dtype=dtype,
            sidecar=sidecar,
            source="query",
            expression=expression.strip(),
            field_set=field_set,
            unit=raw.get("unit") if isinstance(raw.get("unit"), str) else None,
        )
        unknown_aliases = sorted(field.query_aliases - _VALID_QUERY_ALIASES)
        if unknown_aliases:
            raise ValueError(
                f"{label}.expression uses unknown alias(es): {', '.join(unknown_aliases)}"
            )
        return field

    source = _require_string(raw, "source", label=label)
    if source not in _VALID_STAGE_SOURCES:
        raise ValueError(f"{label}.source must be one of input, stage")
    column = _require_string(raw, "column", label=label)
    if not _IDENTIFIER_RE.match(column):
        raise ValueError(f"{label}.column must be a lowercase identifier")
    return GaiaCarryField(
        name=name,
        dtype=dtype,
        sidecar=sidecar,
        source=source,
        column=column,
        field_set=field_set,
        unit=raw.get("unit") if isinstance(raw.get("unit"), str) else None,
    )


def load_gaia_field_set(name: str) -> tuple[GaiaCarryField, ...]:
    field_set = name.strip()
    if not _IDENTIFIER_RE.match(field_set):
        raise ValueError(f"Gaia field set name must be a lowercase identifier: {name!r}")

    path = _field_set_path(field_set)
    with path.open("rb") as fp:
        raw = tomllib.load(fp)
    fields_raw = raw.get("fields")
    if not isinstance(fields_raw, list) or not fields_raw:
        raise ValueError(f"Gaia field set {field_set!r} must define [[fields]]")

    fields: list[GaiaCarryField] = []
    seen: set[str] = set()
    for index, item in enumerate(fields_raw):
        if not isinstance(item, dict):
            raise ValueError(f"{field_set}.fields[{index}] must be a TOML table")
        field = _parse_field(item, field_set=field_set, index=index)
        if field.name in seen:
            raise ValueError(f"Gaia field set {field_set!r} duplicates field {field.name!r}")
        seen.add(field.name)
        fields.append(field)
    return tuple(fields)


def load_gaia_field_sets(names: tuple[str, ...] | list[str]) -> tuple[GaiaCarryField, ...]:
    fields: list[GaiaCarryField] = []
    seen_fields: dict[str, str] = {}
    for name in names:
        for field in load_gaia_field_set(name):
            existing = seen_fields.get(field.name)
            if existing is not None:
                raise ValueError(
                    f"Gaia carry field {field.name!r} is defined by both "
                    f"{existing!r} and {field.field_set!r}"
                )
            seen_fields[field.name] = field.field_set
            fields.append(field)
    return tuple(fields)


def gaia_enrichment_columns(fields: tuple[GaiaCarryField, ...]) -> list[str]:
    return [field.output_column for field in fields]


def fields_by_sidecar(
    fields: tuple[GaiaCarryField, ...],
) -> dict[str, tuple[GaiaCarryField, ...]]:
    grouped: dict[str, list[GaiaCarryField]] = {}
    for field in fields:
        grouped.setdefault(field.sidecar, []).append(field)
    return {name: tuple(items) for name, items in sorted(grouped.items())}
