from __future__ import annotations

import math
import re
from dataclasses import dataclass
from importlib import metadata
from pathlib import Path
from typing import Any

import tomllib

from foundinspace.pipeline.gaia.engines import (
    DEFAULT_GAIA_CALCULATION_ENGINE,
    GAIA_CALCULATION_ENGINES,
)

FORMAT_VERSION = 1
PROJECT_PROFILES = ("full", "small")
GAIA_INPUT_MODES = ("download-state", "directory")

_RUN_KEYS = {"id", "provenance_dir"}
_GAIA_KEYS = {
    "input_dir",
    "input_mode",
    "output_dir",
    "mag_limit",
    "calculation_engine",
}
_GAIA_DOWNLOAD_KEYS = {
    "mode",
    "access",
    "mag_limit",
    "source_where_adql",
    "state_db",
    "row_cap",
    "max_active_jobs",
    "remote_quota_bytes",
    "remote_min_free_bytes",
    "remote_result_bytes_per_row",
    "remote_max_result_bytes_per_job",
    "carry_field_sets",
}
_GAIA_TO_HIP_KEYS = {"download_ecsv", "output_parquet", "supplemental_parquets"}
_HIP_KEYS = {"download_ecsv", "output_parquet"}
_IDENTIFIERS_KEYS = {
    "hip_hd_ecsv",
    "iv27a_catalog_ecsv",
    "iv27a_proper_names_ecsv",
    "output_parquet",
}
_OVERRIDE_ZENODO_KEYS = {
    "zenodo_version_doi",
    "zenodo_archive",
    "zenodo_archive_sha256",
    "zenodo_download_dir",
}
_OVERRIDES_KEYS = {
    "output_parquet",
    "data_dir",
    "source_paths",
    *_OVERRIDE_ZENODO_KEYS,
}
_MERGE_KEYS = {"output_dir", "healpix_order", "sidecar_output_dir"}
_DIST_NAME = "found-in-space-pipeline"
_RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_ZENODO_VERSION_DOI_RE = re.compile(r"^10\.5281/zenodo\.[1-9][0-9]*$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

GAIA_DOWNLOAD_MODES = ("small", "full")
GAIA_DOWNLOAD_ACCESS_MODES = ("auto", "anonymous", "authenticated")
GAIA_DOWNLOAD_DEFAULT_ROW_CAP = 55_000_000
GAIA_DOWNLOAD_DEFAULT_MAX_ACTIVE_JOBS = 4
GAIA_DOWNLOAD_DEFAULT_REMOTE_QUOTA_BYTES = 20_000_000_000
GAIA_DOWNLOAD_DEFAULT_REMOTE_MIN_FREE_BYTES = 5_000_000_000
GAIA_DOWNLOAD_DEFAULT_REMOTE_RESULT_BYTES_PER_ROW = 400
GAIA_DOWNLOAD_DEFAULT_REMOTE_MAX_RESULT_BYTES_PER_JOB = 5_000_000_000
GAIA_DOWNLOAD_DEFAULT_CARRY_FIELD_SETS: tuple[str, ...] = ()


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


def _normalize_choice(
    value: str,
    *,
    choices: tuple[str, ...],
    field_name: str,
) -> str:
    normalized = value.strip().lower()
    if normalized not in choices:
        raise ValueError(f"{field_name} must be one of {', '.join(choices)}")
    return normalized


class _SectionAccessor:
    def __init__(
        self, section_name: str, raw: dict[str, Any] | None, project_dir: Path
    ) -> None:
        self._section = section_name
        self._raw = raw
        self._project_dir = project_dir

    @property
    def is_configured(self) -> bool:
        """True when this section was present in the project file."""
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

    def _optional_path_list(self, key: str) -> tuple[Path, ...] | None:
        if self._raw is None or key not in self._raw:
            return None
        value = self._raw.get(key)
        if not isinstance(value, list):
            raise ValueError(f"{self._section}.{key} must be a list of paths")
        out: list[Path] = []
        seen: set[str] = set()
        for item in value:
            if not isinstance(item, str) or not item.strip():
                raise ValueError(f"{self._section}.{key} must be a list of paths")
            resolved = _resolve_path(
                self._project_dir,
                item.strip(),
                field_name=f"{self._section}.{key}",
            )
            duplicate_key = str(resolved)
            if duplicate_key in seen:
                raise ValueError(
                    f"{self._section}.{key} contains duplicate path {item!r}"
                )
            seen.add(duplicate_key)
            out.append(resolved)
        return tuple(out)

    def _require_int_field(self, key: str) -> int:
        if self._raw is None:
            raise ValueError(f"Missing [{self._section}] table in project file")
        return _require_int(self._raw, key, field_name=f"{self._section}.{key}")

    def _optional_int_field(self, key: str, default: int) -> int:
        if self._raw is None or key not in self._raw:
            return default
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

    def _optional_str_field(self, key: str) -> str | None:
        if self._raw is None or key not in self._raw:
            return None
        return _require_str(self._raw, key, field_name=f"{self._section}.{key}")

    def _optional_str_choice(
        self,
        key: str,
        *,
        default: str,
        choices: tuple[str, ...],
    ) -> str:
        if self._raw is None:
            raise ValueError(f"Missing [{self._section}] table in project file")
        if key not in self._raw:
            return default
        value = _require_str(self._raw, key, field_name=f"{self._section}.{key}")
        return _normalize_choice(
            value,
            choices=choices,
            field_name=f"{self._section}.{key}",
        )

    def _require_str_choice(
        self,
        key: str,
        *,
        choices: tuple[str, ...],
    ) -> str:
        if self._raw is None:
            raise ValueError(f"Missing [{self._section}] table in project file")
        value = _require_str(self._raw, key, field_name=f"{self._section}.{key}")
        return _normalize_choice(
            value,
            choices=choices,
            field_name=f"{self._section}.{key}",
        )

    def _optional_str_list(self, key: str, default: tuple[str, ...]) -> tuple[str, ...]:
        if self._raw is None:
            raise ValueError(f"Missing [{self._section}] table in project file")
        if key not in self._raw:
            return default
        value = self._raw.get(key)
        if not isinstance(value, list):
            raise ValueError(f"{self._section}.{key} must be a list of strings")
        out: list[str] = []
        seen: set[str] = set()
        for item in value:
            if not isinstance(item, str) or not item.strip():
                raise ValueError(f"{self._section}.{key} must be a list of strings")
            normalized = item.strip()
            if normalized in seen:
                raise ValueError(
                    f"{self._section}.{key} contains duplicate {normalized!r}"
                )
            seen.add(normalized)
            out.append(normalized)
        return tuple(out)


class GaiaConfig(_SectionAccessor):
    @property
    def input_dir(self) -> Path:
        return self._require_path("input_dir")

    @property
    def output_dir(self) -> Path:
        return self._require_path("output_dir")

    @property
    def input_mode(self) -> str | None:
        value = self._optional_str_field("input_mode")
        if value is None:
            return None
        return _normalize_choice(
            value,
            choices=GAIA_INPUT_MODES,
            field_name="gaia.input_mode",
        )

    @property
    def mag_limit(self) -> float | None:
        return self._optional_float_field("mag_limit")

    @property
    def calculation_engine(self) -> str:
        return self._optional_str_choice(
            "calculation_engine",
            default=DEFAULT_GAIA_CALCULATION_ENGINE,
            choices=GAIA_CALCULATION_ENGINES,
        )


class GaiaDownloadConfig(_SectionAccessor):
    @property
    def configured(self) -> bool:
        return self.is_configured

    @property
    def mode(self) -> str:
        return self._require_str_choice("mode", choices=GAIA_DOWNLOAD_MODES)

    @property
    def access(self) -> str:
        return self._optional_str_choice(
            "access",
            default="auto",
            choices=GAIA_DOWNLOAD_ACCESS_MODES,
        )

    @property
    def mag_limit(self) -> float | None:
        return self._optional_float_field("mag_limit")

    @property
    def source_where_adql(self) -> str | None:
        value = self._optional_str_field("source_where_adql")
        if value is None:
            return None
        if ";" in value:
            raise ValueError(
                "gaia_download.source_where_adql must not contain semicolons"
            )
        return value

    @property
    def state_db(self) -> Path:
        return self._require_path("state_db")

    @property
    def row_cap(self) -> int:
        value = self._optional_int_field("row_cap", GAIA_DOWNLOAD_DEFAULT_ROW_CAP)
        if value <= 0:
            raise ValueError("gaia_download.row_cap must be > 0")
        return value

    @property
    def max_active_jobs(self) -> int:
        value = self._optional_int_field(
            "max_active_jobs",
            GAIA_DOWNLOAD_DEFAULT_MAX_ACTIVE_JOBS,
        )
        if value <= 0:
            raise ValueError("gaia_download.max_active_jobs must be > 0")
        return value

    @property
    def remote_quota_bytes(self) -> int:
        value = self._optional_int_field(
            "remote_quota_bytes",
            GAIA_DOWNLOAD_DEFAULT_REMOTE_QUOTA_BYTES,
        )
        if value <= 0:
            raise ValueError("gaia_download.remote_quota_bytes must be > 0")
        return value

    @property
    def remote_min_free_bytes(self) -> int:
        value = self._optional_int_field(
            "remote_min_free_bytes",
            GAIA_DOWNLOAD_DEFAULT_REMOTE_MIN_FREE_BYTES,
        )
        if value < 0:
            raise ValueError("gaia_download.remote_min_free_bytes must be >= 0")
        return value

    @property
    def remote_result_bytes_per_row(self) -> int:
        value = self._optional_int_field(
            "remote_result_bytes_per_row",
            GAIA_DOWNLOAD_DEFAULT_REMOTE_RESULT_BYTES_PER_ROW,
        )
        if value <= 0:
            raise ValueError("gaia_download.remote_result_bytes_per_row must be > 0")
        return value

    @property
    def remote_max_result_bytes_per_job(self) -> int:
        value = self._optional_int_field(
            "remote_max_result_bytes_per_job",
            GAIA_DOWNLOAD_DEFAULT_REMOTE_MAX_RESULT_BYTES_PER_JOB,
        )
        if value <= 0:
            raise ValueError(
                "gaia_download.remote_max_result_bytes_per_job must be > 0"
            )
        return value

    @property
    def carry_field_sets(self) -> tuple[str, ...]:
        return self._optional_str_list(
            "carry_field_sets",
            GAIA_DOWNLOAD_DEFAULT_CARRY_FIELD_SETS,
        )


class GaiaToHipConfig(_SectionAccessor):
    @property
    def download_ecsv(self) -> Path:
        return self._require_path("download_ecsv")

    @property
    def output_parquet(self) -> Path:
        return self._require_path("output_parquet")

    @property
    def supplemental_parquets(self) -> tuple[Path, ...]:
        return self._optional_path_list("supplemental_parquets") or ()


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

    @property
    def source_paths(self) -> tuple[Path, ...] | None:
        return self._optional_path_list("source_paths")

    @property
    def zenodo_configured(self) -> bool:
        return self._raw is not None and bool(
            _OVERRIDE_ZENODO_KEYS.intersection(self._raw)
        )

    @property
    def zenodo_version_doi(self) -> str | None:
        value = self._optional_str_field("zenodo_version_doi")
        if value is not None and not _ZENODO_VERSION_DOI_RE.fullmatch(value):
            raise ValueError(
                "overrides.zenodo_version_doi must be a canonical Zenodo DOI "
                "such as '10.5281/zenodo.21703732'"
            )
        return value

    @property
    def zenodo_archive(self) -> str | None:
        value = self._optional_str_field("zenodo_archive")
        if value is not None and (
            Path(value).name != value or not value.lower().endswith(".zip")
        ):
            raise ValueError(
                "overrides.zenodo_archive must be a ZIP filename without a path"
            )
        return value

    @property
    def zenodo_archive_sha256(self) -> str | None:
        value = self._optional_str_field("zenodo_archive_sha256")
        if value is not None:
            value = value.lower()
            if not _SHA256_RE.fullmatch(value):
                raise ValueError(
                    "overrides.zenodo_archive_sha256 must be 64 hexadecimal characters"
                )
        return value

    @property
    def zenodo_download_dir(self) -> Path | None:
        return self._optional_path("zenodo_download_dir")


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

    @property
    def sidecar_output_dir(self) -> Path:
        explicit = self._optional_path("sidecar_output_dir")
        if explicit is not None:
            return explicit
        return self.output_dir.parent / "sidecars"


class RunConfig(_SectionAccessor):
    @property
    def id(self) -> str:
        if self._raw is None:
            raise ValueError("Missing [run] table in project file")
        value = _require_str(self._raw, "id", field_name="run.id")
        if not _RUN_ID_RE.fullmatch(value):
            raise ValueError(
                "run.id must start with an alphanumeric character and contain "
                "only letters, numbers, '.', '_' or '-'"
            )
        return value

    @property
    def provenance_dir(self) -> Path:
        explicit = self._optional_path("provenance_dir")
        if explicit is not None:
            return explicit
        return self._project_dir / "provenance"


@dataclass(frozen=True, slots=True)
class PipelineProject:
    project_path: Path
    run: RunConfig
    gaia: GaiaConfig
    gaia_download: GaiaDownloadConfig
    gaia_to_hip: GaiaToHipConfig
    hip: HipConfig
    identifiers: IdentifiersConfig
    overrides: OverridesConfig
    merge: MergeConfig

    def require(self, *section_names: str) -> None:
        """Raise ValueError listing all missing required sections at once."""
        known: dict[str, _SectionAccessor] = {
            self.run._section: self.run,
            self.gaia._section: self.gaia,
            self.gaia_download._section: self.gaia_download,
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
        missing = [name for name in section_names if not known[name].is_configured]
        if missing:
            raise ValueError(
                "Missing required config sections: "
                + ", ".join(f"[{name}]" for name in missing)
            )

    def resolved_paths(self) -> dict[str, str | list[str]]:
        """Return every configured filesystem path using stable config labels."""
        paths: dict[str, str | list[str]] = {}

        def add(section: _SectionAccessor, key: str, value: Path) -> None:
            if section.is_configured:
                paths[f"{section._section}.{key}"] = str(value.resolve())

        if self.run.is_configured:
            add(self.run, "provenance_dir", self.run.provenance_dir)
        if self.gaia.is_configured:
            add(self.gaia, "input_dir", self.gaia.input_dir)
            add(self.gaia, "output_dir", self.gaia.output_dir)
        if self.gaia_download.is_configured:
            add(self.gaia_download, "state_db", self.gaia_download.state_db)
        if self.gaia_to_hip.is_configured:
            add(self.gaia_to_hip, "download_ecsv", self.gaia_to_hip.download_ecsv)
            add(self.gaia_to_hip, "output_parquet", self.gaia_to_hip.output_parquet)
            paths["gaia-to-hip.supplemental_parquets"] = [
                str(path.resolve()) for path in self.gaia_to_hip.supplemental_parquets
            ]
        if self.hip.is_configured:
            add(self.hip, "download_ecsv", self.hip.download_ecsv)
            add(self.hip, "output_parquet", self.hip.output_parquet)
        if self.identifiers.is_configured:
            add(self.identifiers, "hip_hd_ecsv", self.identifiers.hip_hd_ecsv)
            add(
                self.identifiers,
                "iv27a_catalog_ecsv",
                self.identifiers.iv27a_catalog_ecsv,
            )
            add(
                self.identifiers,
                "iv27a_proper_names_ecsv",
                self.identifiers.iv27a_proper_names_ecsv,
            )
            add(
                self.identifiers,
                "output_parquet",
                self.identifiers.output_parquet,
            )
        if self.overrides.is_configured:
            add(self.overrides, "output_parquet", self.overrides.output_parquet)
            if self.overrides.data_dir is not None:
                add(self.overrides, "data_dir", self.overrides.data_dir)
            if self.overrides.source_paths is not None:
                paths["overrides.source_paths"] = [
                    str(path.resolve()) for path in self.overrides.source_paths
                ]
            if self.overrides.zenodo_download_dir is not None:
                add(
                    self.overrides,
                    "zenodo_download_dir",
                    self.overrides.zenodo_download_dir,
                )
        if self.merge.is_configured:
            add(self.merge, "output_dir", self.merge.output_dir)
            add(self.merge, "sidecar_output_dir", self.merge.sidecar_output_dir)
        return paths


CONTROLLED_SECTIONS = (
    "run",
    "gaia",
    "gaia_download",
    "gaia-to-hip",
    "hip",
    "identifiers",
    "overrides",
    "merge",
)


def validate_controlled_project(project: PipelineProject) -> None:
    """Validate the explicit configuration required for a traceable full run."""
    project.require(*CONTROLLED_SECTIONS)
    run_id = project.run.id
    _ = project.run.provenance_dir
    if project.project_path.name != "pipeline.toml":
        raise ValueError("Controlled project file must be named pipeline.toml")
    if project.project_path.parent.name != run_id:
        raise ValueError(
            "run.id must match the directory containing pipeline.toml: "
            f"{run_id!r} != {project.project_path.parent.name!r}"
        )

    gaia_input_mode = project.gaia.input_mode
    gaia_mag_limit = project.gaia.mag_limit
    if gaia_input_mode is None:
        raise ValueError(
            "Controlled projects must set gaia.input_mode to "
            "'download-state' or 'directory'"
        )
    _ = (
        project.gaia.input_dir,
        project.gaia.output_dir,
        project.gaia.calculation_engine,
    )

    download_mode = project.gaia_download.mode
    download_access = project.gaia_download.access
    download_mag_limit = project.gaia_download.mag_limit
    remote_quota_bytes = project.gaia_download.remote_quota_bytes
    remote_min_free_bytes = project.gaia_download.remote_min_free_bytes
    remote_result_bytes_per_row = project.gaia_download.remote_result_bytes_per_row
    remote_max_result_bytes_per_job = (
        project.gaia_download.remote_max_result_bytes_per_job
    )
    carry_field_sets = project.gaia_download.carry_field_sets
    _ = (
        project.gaia_download.source_where_adql,
        project.gaia_download.state_db,
        project.gaia_download.row_cap,
        project.gaia_download.max_active_jobs,
    )
    if download_mode == "small" and download_mag_limit is None:
        raise ValueError("gaia_download.mag_limit is required when mode = 'small'")
    if download_mode == "full" and download_mag_limit is not None:
        raise ValueError("gaia_download.mag_limit must be omitted when mode = 'full'")
    if download_mode == "full" and download_access == "anonymous":
        raise ValueError(
            "gaia_download.access = 'anonymous' is not valid for full mode"
        )
    if (
        download_mag_limit is not None
        and gaia_mag_limit is not None
        and abs(gaia_mag_limit - download_mag_limit) > 1e-9
    ):
        raise ValueError(
            "gaia.mag_limit must match gaia_download.mag_limit or be omitted"
        )
    if remote_quota_bytes <= remote_min_free_bytes:
        raise ValueError(
            "gaia_download.remote_quota_bytes must exceed "
            "gaia_download.remote_min_free_bytes"
        )
    if remote_max_result_bytes_per_job < remote_result_bytes_per_row:
        raise ValueError(
            "gaia_download.remote_max_result_bytes_per_job must be at least "
            "gaia_download.remote_result_bytes_per_row"
        )

    from foundinspace.pipeline.gaia.download.fieldsets import load_gaia_field_sets

    load_gaia_field_sets(carry_field_sets)

    _ = (
        project.gaia_to_hip.download_ecsv,
        project.gaia_to_hip.output_parquet,
        project.gaia_to_hip.supplemental_parquets,
        project.hip.download_ecsv,
        project.hip.output_parquet,
        project.identifiers.hip_hd_ecsv,
        project.identifiers.iv27a_catalog_ecsv,
        project.identifiers.iv27a_proper_names_ecsv,
        project.identifiers.output_parquet,
        project.overrides.output_parquet,
        project.overrides.data_dir,
        project.merge.output_dir,
        project.merge.sidecar_output_dir,
        project.merge.healpix_order,
    )
    override_sources = project.overrides.source_paths
    if override_sources is None:
        raise ValueError(
            "Controlled projects must set overrides.source_paths explicitly; "
            "use [] to select no overrides"
        )
    if project.overrides.zenodo_configured:
        _ = (
            project.overrides.zenodo_version_doi,
            project.overrides.zenodo_archive,
            project.overrides.zenodo_archive_sha256,
            project.overrides.zenodo_download_dir,
        )
    project.resolved_paths()


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

    run_raw = _validate_section(raw, "run", _RUN_KEYS)
    gaia_raw = _validate_section(raw, "gaia", _GAIA_KEYS)
    gaia_download_raw = _validate_section(raw, "gaia_download", _GAIA_DOWNLOAD_KEYS)
    gaia_to_hip_raw = _validate_section(raw, "gaia-to-hip", _GAIA_TO_HIP_KEYS)
    hip_raw = _validate_section(raw, "hip", _HIP_KEYS)
    identifiers_raw = _validate_section(raw, "identifiers", _IDENTIFIERS_KEYS)
    overrides_raw = _validate_section(raw, "overrides", _OVERRIDES_KEYS)
    merge_raw = _validate_section(raw, "merge", _MERGE_KEYS)

    if (
        overrides_raw is not None
        and "data_dir" in overrides_raw
        and "source_paths" in overrides_raw
    ):
        raise ValueError(
            "overrides.data_dir and overrides.source_paths are mutually exclusive"
        )
    if overrides_raw is not None:
        configured_zenodo_keys = _OVERRIDE_ZENODO_KEYS.intersection(overrides_raw)
        if configured_zenodo_keys and configured_zenodo_keys != _OVERRIDE_ZENODO_KEYS:
            missing = sorted(_OVERRIDE_ZENODO_KEYS - configured_zenodo_keys)
            raise ValueError(
                "Incomplete Zenodo override configuration; missing "
                + ", ".join(f"overrides.{key}" for key in missing)
            )

    return PipelineProject(
        project_path=resolved_project_path,
        run=RunConfig("run", run_raw, project_dir),
        gaia=GaiaConfig("gaia", gaia_raw, project_dir),
        gaia_download=GaiaDownloadConfig(
            "gaia_download", gaia_download_raw, project_dir
        ),
        gaia_to_hip=GaiaToHipConfig("gaia-to-hip", gaia_to_hip_raw, project_dir),
        hip=HipConfig("hip", hip_raw, project_dir),
        identifiers=IdentifiersConfig("identifiers", identifiers_raw, project_dir),
        overrides=OverridesConfig("overrides", overrides_raw, project_dir),
        merge=MergeConfig("merge", merge_raw, project_dir),
    )


def _profile_template_path(profile_name: str) -> Path:
    name = f"{profile_name}.toml"
    repo_profile = Path(__file__).resolve().parents[3] / "profiles" / name
    if repo_profile.is_file():
        return repo_profile

    try:
        dist_files = metadata.files(_DIST_NAME)
    except metadata.PackageNotFoundError as exc:
        raise FileNotFoundError(f"Profile template not found: {name}") from exc

    if dist_files is not None:
        for dist_file in dist_files:
            if str(dist_file).replace("\\", "/") == f"profiles/{name}":
                located = dist_file.locate()
                if located.is_file():
                    return Path(located)

    raise FileNotFoundError(f"Profile template not found: {name}")


def render_project_template(profile: str = "full") -> str:
    profile_name = profile.strip().lower()
    if profile_name not in PROJECT_PROFILES:
        raise ValueError(
            f"Unknown project profile {profile!r}; expected one of {', '.join(PROJECT_PROFILES)}"
        )
    return _profile_template_path(profile_name).read_text(encoding="utf-8")


def render_run_project_template(*, run_id: str, profile: str = "full") -> str:
    """Render a profile with controlled-run metadata."""
    if not _RUN_ID_RE.fullmatch(run_id):
        raise ValueError(
            "run_id must start with an alphanumeric character and contain only "
            "letters, numbers, '.', '_' or '-'"
        )
    rendered = render_project_template(profile=profile)
    marker = f"format_version = {FORMAT_VERSION}\n"
    run_table = f'\n[run]\nid = "{run_id}"\nprovenance_dir = "provenance"\n'
    return rendered.replace(marker, marker + run_table, 1)
