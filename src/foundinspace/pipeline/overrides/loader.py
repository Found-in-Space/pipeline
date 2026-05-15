"""Load manual override source files from the overrides data package."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, MutableMapping
from pathlib import Path
from typing import Any

import numpy as np
import yaml

_YAML_SUFFIXES = (".yaml", ".yml")

# Packaged default: sibling `data/` next to this module (editable + wheel with packaged YAML).
_PACKAGE_DATA_DIR = Path(__file__).resolve().parent / "data"


def icrs_spherical_to_cartesian_pc(
    ra_deg: float, dec_deg: float, r_pc: float
) -> tuple[float, float, float]:
    """ICRS (RA, Dec, distance) to sun-centered Cartesian parsecs."""
    ra = np.deg2rad(ra_deg)
    dec = np.deg2rad(dec_deg)
    cos_d = np.cos(dec)
    x = r_pc * cos_d * np.cos(ra)
    y = r_pc * cos_d * np.sin(ra)
    z = r_pc * np.sin(dec)
    return float(x), float(y), float(z)


def _sorted_yaml_paths(paths: Iterable[Path]) -> Iterator[Path]:
    """Yield YAML paths in deterministic name order."""
    yield from sorted(
        (p for p in paths if p.is_file() and p.suffix.lower() in _YAML_SUFFIXES),
        key=lambda p: p.name,
    )


def _default_data_dir() -> Path:
    return _PACKAGE_DATA_DIR


def iter_override_source_files(data_dir: Path | None = None) -> list[Path]:
    """Return all override YAML files in deterministic order.

    If `data_dir` is provided, files are scanned from that filesystem path.
    Otherwise, the package `data` directory next to this module is used.
    """
    root = data_dir if data_dir is not None else _default_data_dir()
    return list(_sorted_yaml_paths(root.iterdir()))


def load_override_source_texts(data_dir: Path | None = None) -> dict[str, str]:
    """Load all override YAML files as text keyed by filename."""
    out: dict[str, str] = {}
    for path in iter_override_source_files(data_dir=data_dir):
        out[path.name] = path.read_text(encoding="utf-8")
    return out


def _parse_yaml_file(path: Path) -> Any:
    text = path.read_text(encoding="utf-8")
    return yaml.safe_load(text)


def _has_full_xyz(star: Mapping[str, Any]) -> bool:
    keys = ("x_icrs_pc", "y_icrs_pc", "z_icrs_pc")
    return all(k in star and star[k] is not None for k in keys)


def _ensure_cartesian(star: MutableMapping[str, Any]) -> None:
    """Set x_icrs_pc, y_icrs_pc, z_icrs_pc from spherical if not all three provided."""
    if _has_full_xyz(star):
        return
    for k in ("x_icrs_pc", "y_icrs_pc", "z_icrs_pc"):
        if k in star and star[k] is not None:
            raise ValueError(
                "Override star must provide all of x_icrs_pc, y_icrs_pc, z_icrs_pc "
                "or none; partial Cartesian is not allowed"
            )
    try:
        ra = float(star["ra_deg"])
        dec = float(star["dec_deg"])
        r = float(star["r_pc"])
    except KeyError as e:
        raise ValueError(
            "Non-drop overrides require ra_deg, dec_deg, and r_pc when Cartesian is omitted"
        ) from e
    x, y, z = icrs_spherical_to_cartesian_pc(ra, dec, r)
    star["x_icrs_pc"] = x
    star["y_icrs_pc"] = y
    star["z_icrs_pc"] = z


def _normalize_star_dict(star: MutableMapping[str, Any]) -> None:
    action = star.get("action")
    if action == "drop":
        required = (
            "override_id",
            "action",
            "source",
            "source_id",
            "override_reason",
            "override_policy_version",
        )
        missing = [k for k in required if k not in star or star[k] is None]
        if missing:
            raise ValueError(f"drop override missing required keys: {missing}")
        return
    for k in ("ra_deg", "dec_deg", "r_pc"):
        if k not in star or star[k] is None:
            raise ValueError(f"Override with action={action!r} requires {k}")
    _ensure_cartesian(star)


def load_parsed_override_documents(
    data_dir: Path | None = None,
) -> list[dict[str, Any]]:
    """Load and parse every YAML file as a document dict (may contain `description`, `stars`)."""
    docs: list[dict[str, Any]] = []
    for path in iter_override_source_files(data_dir=data_dir):
        raw = _parse_yaml_file(path)
        if raw is None:
            continue
        if not isinstance(raw, dict):
            raise ValueError(f"Expected mapping at root of {path}, got {type(raw)}")
        doc = dict(raw)
        doc["_source_file"] = path.name
        docs.append(doc)
    return docs


def load_normalized_override_stars(
    data_dir: Path | None = None,
) -> list[dict[str, Any]]:
    """Load all stars from all YAML files, with Cartesian coordinates filled when omitted.

    Each item is a flat dict including override fields, optional extras from YAML,
    plus ``_source_file`` and ``_file_description`` (if the file had ``description``).
    """
    out: list[dict[str, Any]] = []
    for path in iter_override_source_files(data_dir=data_dir):
        raw = _parse_yaml_file(path)
        if raw is None:
            continue
        if not isinstance(raw, dict):
            raise ValueError(f"Expected mapping at root of {path}, got {type(raw)}")
        description = raw.get("description")
        stars = raw.get("stars")
        if stars is None:
            continue
        if not isinstance(stars, list):
            raise ValueError(f"`stars` must be a list in {path}")
        for i, star in enumerate(stars):
            if not isinstance(star, dict):
                raise ValueError(f"stars[{i}] must be a mapping in {path}")
            row: dict[str, Any] = dict(star)
            row["_source_file"] = path.name
            if description is not None:
                row["_file_description"] = description
            _normalize_star_dict(row)
            out.append(row)
    return out
