from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import tomllib

FORMAT_VERSION = 1

_GAIA_KEYS = {
    "output_dir",
    "mag_limit",
    "download_counts_csv",
    "download_plan_csv",
    "download_manifest_yaml",
    "download_votable_dir",
    "target_rows_per_batch",
    "max_archive_inflight_queries",
    "max_concurrent_downloads",
    "poll_interval_seconds",
}
_GAIA_TO_HIP_KEYS = {"download_ecsv", "output_parquet"}
_HIP_KEYS = {"download_ecsv", "output_parquet"}
_IDENTIFIERS_KEYS = {
    "hip_hd_ecsv",
    "iv27a_catalog_ecsv",
    "iv27a_proper_names_ecsv",
    "output_parquet",
}
_OVERRIDES_KEYS = {"output_parquet", "data_dir"}
_MERGE_KEYS = {"output_dir", "healpix_order"}


def _reject_unknown_keys(
    raw: dict[str, Any], *, allowed: set[str], table_name: str
) -> None:
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise ValueError(f"Unknown key(s) in [{table_name}]: {', '.join(unknown)}")


def _reject_env_expansion(value: str, *, field_name: str) -> None:
    if "$" in value:
        raise ValueError(
            f"{field_name} must not contain environment-variable syntax: {value!r}"
        )


def _resolve_path(project_dir: Path, value: str, *, field_name: str) -> Path:
    _reject_env_expansion(value, field_name=field_name)
    raw_path = Path(value)
    if raw_path.is_absolute():
        return raw_path
    return project_dir / raw_path


def _require_str(raw: dict[str, Any], key: str, *, field_name: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def _require_int(raw: dict[str, Any], key: str, *, field_name: str) -> int:
    value = raw.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer")
    return value


class _SectionAccessor:
    def __init__(
        self, section_name: str, raw: dict[str, Any] | None, project_dir: Path
    ) -> None:
        self._section = section_name
        self._raw = raw
        self._project_dir = project_dir

    @property
    def is_configured(self) -> bool:
        """True if this section was present in the project file."""
        return self._raw is not None

    def _require_path(self, key: str) -> Path:
        if self._raw is None:
            raise ValueError(f"Missing [{self._section}] table in project file")
        value = _require_str(self._raw, key, field_name=f"{self._section}.{key}")
        return _resolve_path(
            self._project_dir, value, field_name=f"{self._section}.{key}"
        )

    def _optional_path(self, key: str) -> Path | None:
        if self._raw is None or key not in self._raw:
            return None
        value = _require_str(self._raw, key, field_name=f"{self._section}.{key}")
        return _resolve_path(
            self._project_dir, value, field_name=f"{self._section}.{key}"
        )

    def _require_int_field(self, key: str) -> int:
        if self._raw is None:
            raise ValueError(f"Missing [{self._section}] table in project file")
        return _require_int(self._raw, key, field_name=f"{self._section}.{key}")

    def _optional_float_field(self, key: str) -> float | None:
        if self._raw is None or key not in self._raw:
            return None
        value = self._raw.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{self._section}.{key} must be a number")
        value_f = float(value)
        if not math.isfinite(value_f):
            raise ValueError(f"{self._section}.{key} must be finite")
        return value_f


class GaiaConfig(_SectionAccessor):
    @property
    def output_dir(self) -> Path:
        return self._require_path("output_dir")

    @property
    def mag_limit(self) -> float | None:
        return self._optional_float_field("mag_limit")

    @property
    def download_counts_csv(self) -> Path:
        return self._require_path("download_counts_csv")

    @property
    def download_plan_csv(self) -> Path:
        return self._require_path("download_plan_csv")

    @property
    def download_manifest_yaml(self) -> Path:
        return self._require_path("download_manifest_yaml")

    @property
    def download_votable_dir(self) -> Path:
        return self._require_path("download_votable_dir")

    @property
    def target_rows_per_batch(self) -> int:
        value = self._require_int_field("target_rows_per_batch")
        if value <= 0:
            raise ValueError("gaia.target_rows_per_batch must be > 0")
        return value

    @property
    def max_archive_inflight_queries(self) -> int:
        value = self._require_int_field("max_archive_inflight_queries")
        if value <= 0:
            raise ValueError("gaia.max_archive_inflight_queries must be > 0")
        return value

    @property
    def max_concurrent_downloads(self) -> int:
        value = self._require_int_field("max_concurrent_downloads")
        if value <= 0:
            raise ValueError("gaia.max_concurrent_downloads must be > 0")
        return value

    @property
    def poll_interval_seconds(self) -> float:
        value = self._optional_float_field("poll_interval_seconds")
        if value is None:
            return 10.0
        if value <= 0:
            raise ValueError("gaia.poll_interval_seconds must be > 0")
        return value


class GaiaToHipConfig(_SectionAccessor):
    @property
    def download_ecsv(self) -> Path:
        return self._require_path("download_ecsv")

    @property
    def output_parquet(self) -> Path:
        return self._require_path("output_parquet")


class HipConfig(_SectionAccessor):
    @property
    def download_ecsv(self) -> Path:
        return self._require_path("download_ecsv")

    @property
    def output_parquet(self) -> Path:
        return self._require_path("output_parquet")


class IdentifiersConfig(_SectionAccessor):
    @property
    def hip_hd_ecsv(self) -> Path:
        return self._require_path("hip_hd_ecsv")

    @property
    def iv27a_catalog_ecsv(self) -> Path:
        return self._require_path("iv27a_catalog_ecsv")

    @property
    def iv27a_proper_names_ecsv(self) -> Path:
        return self._require_path("iv27a_proper_names_ecsv")

    @property
    def output_parquet(self) -> Path:
        return self._require_path("output_parquet")


class OverridesConfig(_SectionAccessor):
    @property
    def output_parquet(self) -> Path:
        return self._require_path("output_parquet")

    @property
    def data_dir(self) -> Path | None:
        return self._optional_path("data_dir")


class MergeConfig(_SectionAccessor):
    @property
    def output_dir(self) -> Path:
        return self._require_path("output_dir")

    @property
    def healpix_order(self) -> int:
        value = self._require_int_field("healpix_order")
        if value < 0:
            raise ValueError("merge.healpix_order must be >= 0")
        return value


@dataclass(frozen=True, slots=True)
class PipelineProject:
    project_path: Path
    gaia: GaiaConfig
    gaia_to_hip: GaiaToHipConfig
    hip: HipConfig
    identifiers: IdentifiersConfig
    overrides: OverridesConfig
    merge: MergeConfig

    def require(self, *section_names: str) -> None:
        """Raise ValueError listing all missing required sections at once.

        Call this at the start of a command to fail fast with a complete list
        of missing sections rather than surfacing one error per re-run.
        """
        known: dict[str, _SectionAccessor] = {
            self.gaia._section: self.gaia,
            self.gaia_to_hip._section: self.gaia_to_hip,
            self.hip._section: self.hip,
            self.identifiers._section: self.identifiers,
            self.overrides._section: self.overrides,
            self.merge._section: self.merge,
        }
        unknown = sorted(set(section_names) - set(known))
        if unknown:
            raise ValueError(
                f"require() called with unknown section name(s): {', '.join(unknown)}"
            )
        missing = [n for n in section_names if not known[n].is_configured]
        if missing:
            raise ValueError(
                "Missing required config sections: "
                + ", ".join(f"[{n}]" for n in missing)
            )


def _validate_section(
    raw: dict[str, Any],
    key: str,
    allowed_keys: set[str],
) -> dict[str, Any] | None:
    section = raw.get(key)
    if section is None:
        return None
    if not isinstance(section, dict):
        raise ValueError(f"Invalid [{key}] table in project file")
    _reject_unknown_keys(section, allowed=allowed_keys, table_name=key)
    return section


def load_project(project_path: Path) -> PipelineProject:
    resolved_project_path = project_path.expanduser().resolve()
    with resolved_project_path.open("rb") as fp:
        raw = tomllib.load(fp)

    if not isinstance(raw, dict):
        raise ValueError("Project file root must be a TOML table")

    format_version = raw.get("format_version")
    if format_version != FORMAT_VERSION:
        raise ValueError(
            f"format_version must be {FORMAT_VERSION}, got {format_version!r}"
        )

    project_dir = resolved_project_path.parent

    gaia_raw = _validate_section(raw, "gaia", _GAIA_KEYS)
    gaia_to_hip_raw = _validate_section(raw, "gaia-to-hip", _GAIA_TO_HIP_KEYS)
    hip_raw = _validate_section(raw, "hip", _HIP_KEYS)
    identifiers_raw = _validate_section(raw, "identifiers", _IDENTIFIERS_KEYS)
    overrides_raw = _validate_section(raw, "overrides", _OVERRIDES_KEYS)
    merge_raw = _validate_section(raw, "merge", _MERGE_KEYS)

    return PipelineProject(
        project_path=resolved_project_path,
        gaia=GaiaConfig("gaia", gaia_raw, project_dir),
        gaia_to_hip=GaiaToHipConfig("gaia-to-hip", gaia_to_hip_raw, project_dir),
        hip=HipConfig("hip", hip_raw, project_dir),
        identifiers=IdentifiersConfig("identifiers", identifiers_raw, project_dir),
        overrides=OverridesConfig("overrides", overrides_raw, project_dir),
        merge=MergeConfig("merge", merge_raw, project_dir),
    )


def render_project_template() -> str:
    return (
        f"format_version = {FORMAT_VERSION}\n\n"
        "[gaia]\n"
        'output_dir = "data/processed/gaia"\n'
        "# mag_limit = 15.0\n"
        'download_counts_csv = "data/catalogs/gaia_l3_counts.csv"\n'
        'download_plan_csv = "data/catalogs/gaia_batch_plan.csv"\n'
        'download_manifest_yaml = "data/catalogs/gaia_download_manifest.yaml"\n'
        'download_votable_dir = "data/catalogs/gaia_batches"\n'
        "target_rows_per_batch = 55000000\n"
        "max_archive_inflight_queries = 2\n"
        "max_concurrent_downloads = 2\n"
        "poll_interval_seconds = 10\n\n"
        "[gaia-to-hip]\n"
        'download_ecsv = "data/catalogs/gaia_hipparcos2_best_neighbour.ecsv"\n'
        'output_parquet = "data/processed/gaia_hip_map.parquet"\n\n'
        "[hip]\n"
        'download_ecsv = "data/catalogs/hipparcos2.ecsv"\n'
        'output_parquet = "data/processed/hip_stars.parquet"\n\n'
        "[identifiers]\n"
        'hip_hd_ecsv = "data/catalogs/hip_hd.ecsv"\n'
        'iv27a_catalog_ecsv = "data/catalogs/iv27a_catalog.ecsv"\n'
        'iv27a_proper_names_ecsv = "data/catalogs/iv27a_proper_names.ecsv"\n'
        'output_parquet = "data/processed/identifiers_map.parquet"\n\n'
        "[overrides]\n"
        'output_parquet = "data/processed/overrides.parquet"\n'
        '# data_dir = "path/to/overrides"\n\n'
        "[merge]\n"
        'output_dir = "data/processed/merged"\n'
        "healpix_order = 3\n"
    )
