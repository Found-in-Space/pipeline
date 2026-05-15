from __future__ import annotations

import math
from dataclasses import dataclass
from importlib import metadata
from pathlib import Path
from typing import Any

import tomllib

FORMAT_VERSION = 1
PROJECT_PROFILES = ("full", "small")

_SECTION_NAMES = {
    "gaia",
    "gaia-to-hip",
    "hip",
    "identifiers",
    "overrides",
    "merge",
    "gaia_download",
}
_TOP_LEVEL_KEYS = {"format_version"} | _SECTION_NAMES

_GAIA_KEYS = {"input_dir", "output_dir", "mag_limit"}
_GAIA_DOWNLOAD_KEYS = {
    "mode",
    "access",
    "mag_limit",
    "state_db",
    "row_cap",
    "max_active_jobs",
    "carry_field_sets",
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
_MERGE_KEYS = {"output_dir", "healpix_order", "sidecar_output_dir"}
_DIST_NAME = "found-in-space-pipeline"

GAIA_DOWNLOAD_MODES = ("small", "full")
GAIA_DOWNLOAD_ACCESS_MODES = ("auto", "anonymous", "authenticated")
GAIA_DOWNLOAD_DEFAULT_ROW_CAP = 55_000_000
GAIA_DOWNLOAD_DEFAULT_MAX_ACTIVE_JOBS = 4
GAIA_DOWNLOAD_DEFAULT_CARRY_FIELD_SETS: tuple[str, ...] = ()


def _reject_unknown_keys(raw: dict[str, Any], *, allowed: set[str], table_name: str) -> None:
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
    def __init__(self, section_name: str, raw: dict[str, Any] | None, project_dir: Path) -> None:
        self._section = section_name
        self._raw = raw
        self._project_dir = project_dir

    def _require_path(self, key: str) -> Path:
        if self._raw is None:
            raise ValueError(f"Missing [{self._section}] table in project file")
        value = _require_str(self._raw, key, field_name=f"{self._section}.{key}")
        return _resolve_path(self._project_dir, value, field_name=f"{self._section}.{key}")

    def _optional_path(self, key: str) -> Path | None:
        if self._raw is None or key not in self._raw:
            return None
        value = _require_str(self._raw, key, field_name=f"{self._section}.{key}")
        return _resolve_path(self._project_dir, value, field_name=f"{self._section}.{key}")

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
                raise ValueError(f"{self._section}.{key} contains duplicate {normalized!r}")
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
    def mag_limit(self) -> float | None:
        return self._optional_float_field("mag_limit")


class GaiaDownloadConfig(_SectionAccessor):
    @property
    def configured(self) -> bool:
        return self._raw is not None

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

    @property
    def sidecar_output_dir(self) -> Path:
        explicit = self._optional_path("sidecar_output_dir")
        if explicit is not None:
            return explicit
        return self.output_dir.parent / "sidecars"


@dataclass(frozen=True, slots=True)
class PipelineProject:
    project_path: Path
    gaia: GaiaConfig
    gaia_download: GaiaDownloadConfig
    gaia_to_hip: GaiaToHipConfig
    hip: HipConfig
    identifiers: IdentifiersConfig
    overrides: OverridesConfig
    merge: MergeConfig


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

    _reject_unknown_keys(raw, allowed=_TOP_LEVEL_KEYS, table_name="root")

    format_version = raw.get("format_version")
    if format_version != FORMAT_VERSION:
        raise ValueError(
            f"format_version must be {FORMAT_VERSION}, got {format_version!r}"
        )

    project_dir = resolved_project_path.parent

    gaia_raw = _validate_section(raw, "gaia", _GAIA_KEYS)
    gaia_download_raw = _validate_section(raw, "gaia_download", _GAIA_DOWNLOAD_KEYS)
    gaia_to_hip_raw = _validate_section(raw, "gaia-to-hip", _GAIA_TO_HIP_KEYS)
    hip_raw = _validate_section(raw, "hip", _HIP_KEYS)
    identifiers_raw = _validate_section(raw, "identifiers", _IDENTIFIERS_KEYS)
    overrides_raw = _validate_section(raw, "overrides", _OVERRIDES_KEYS)
    merge_raw = _validate_section(raw, "merge", _MERGE_KEYS)

    return PipelineProject(
        project_path=resolved_project_path,
        gaia=GaiaConfig("gaia", gaia_raw, project_dir),
        gaia_download=GaiaDownloadConfig("gaia_download", gaia_download_raw, project_dir),
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
