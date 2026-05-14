from __future__ import annotations

from pathlib import Path

import pytest
import tomllib
from click.testing import CliRunner

from foundinspace.pipeline.project import (
    FORMAT_VERSION,
    PROJECT_PROFILES,
    load_project,
    render_project_template,
)
from foundinspace.pipeline.project_cli import cli as project_cli


def _project_text() -> str:
    return """
format_version = 1

[gaia]
output_dir = "data/processed/gaia"

[gaia-to-hip]
download_ecsv = "data/catalogs/gaia_hipparcos2_best_neighbour.ecsv"
output_parquet = "data/processed/gaia_hip_map.parquet"

[hip]
download_ecsv = "data/catalogs/hipparcos2.ecsv"
output_parquet = "data/processed/hip_stars.parquet"

[identifiers]
hip_hd_ecsv = "data/catalogs/hip_hd.ecsv"
iv27a_catalog_ecsv = "data/catalogs/iv27a_catalog.ecsv"
iv27a_proper_names_ecsv = "data/catalogs/iv27a_proper_names.ecsv"
output_parquet = "data/processed/identifiers_map.parquet"

[overrides]
output_parquet = "data/processed/overrides.parquet"

[merge]
output_dir = "data/processed/merged"
healpix_order = 3
""".strip() + "\n"


def test_load_project_resolves_relative_paths_from_project_file_dir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir("/")
    project_dir = tmp_path / "run"
    project_dir.mkdir()
    project_path = project_dir / "project.toml"
    project_path.write_text(_project_text(), encoding="utf-8")

    project = load_project(project_path)

    assert project.gaia.output_dir == project_dir / "data" / "processed" / "gaia"
    assert project.gaia.mag_limit is None
    assert project.hip.output_parquet == project_dir / "data" / "processed" / "hip_stars.parquet"
    assert project.gaia_to_hip.download_ecsv == project_dir / "data" / "catalogs" / "gaia_hipparcos2_best_neighbour.ecsv"
    assert project.merge.healpix_order == 3
    assert project.merge.output_dir == project_dir / "data" / "processed" / "merged"


def test_load_project_rejects_env_style_path_strings(tmp_path: Path) -> None:
    project_path = tmp_path / "project.toml"
    project_path.write_text(
        _project_text().replace(
            'output_dir = "data/processed/gaia"',
            'output_dir = "${FIS_PROCESSED_DIR}/gaia"',
        ),
        encoding="utf-8",
    )

    project = load_project(project_path)
    with pytest.raises(ValueError, match="environment-variable syntax"):
        _ = project.gaia.output_dir


def test_load_project_rejects_unknown_keys_in_section(tmp_path: Path) -> None:
    project_path = tmp_path / "project.toml"
    project_path.write_text(
        _project_text().replace(
            'output_dir = "data/processed/gaia"\n',
            'output_dir = "data/processed/gaia"\nextra_path = "x"\n',
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"Unknown key\(s\) in \[gaia\]"):
        load_project(project_path)


def test_load_project_gaia_mag_limit_reads_optional_number(tmp_path: Path) -> None:
    project_path = tmp_path / "project.toml"
    project_path.write_text(
        _project_text().replace(
            'output_dir = "data/processed/gaia"\n',
            'output_dir = "data/processed/gaia"\nmag_limit = 15.5\n',
        ),
        encoding="utf-8",
    )

    project = load_project(project_path)
    assert project.gaia.mag_limit == 15.5


def test_load_project_gaia_mag_limit_rejects_non_numeric(tmp_path: Path) -> None:
    project_path = tmp_path / "project.toml"
    project_path.write_text(
        _project_text().replace(
            'output_dir = "data/processed/gaia"\n',
            'output_dir = "data/processed/gaia"\nmag_limit = "bright"\n',
        ),
        encoding="utf-8",
    )

    project = load_project(project_path)
    with pytest.raises(ValueError, match="gaia.mag_limit must be a number"):
        _ = project.gaia.mag_limit


def test_load_project_rejects_unknown_keys_in_merge(tmp_path: Path) -> None:
    project_path = tmp_path / "project.toml"
    project_path.write_text(
        _project_text().replace(
            "healpix_order = 3\n",
            "healpix_order = 3\nunexpected = 1\n",
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"Unknown key\(s\) in \[merge\]"):
        load_project(project_path)


def test_load_project_missing_required_key_raises_on_access(tmp_path: Path) -> None:
    project_path = tmp_path / "project.toml"
    project_path.write_text(
        _project_text().replace(
            'output_parquet = "data/processed/hip_stars.parquet"\n',
            "",
            1,
        ),
        encoding="utf-8",
    )

    project = load_project(project_path)
    with pytest.raises(ValueError, match="hip.output_parquet"):
        _ = project.hip.output_parquet


def test_load_project_missing_section_raises_on_access(tmp_path: Path) -> None:
    project_path = tmp_path / "project.toml"
    text = _project_text()
    without_merge = text.split("[merge]")[0].rstrip() + "\n"
    project_path.write_text(without_merge, encoding="utf-8")

    project = load_project(project_path)
    with pytest.raises(ValueError, match=r"Missing \[merge\] table"):
        _ = project.merge.healpix_order


def test_load_project_optional_section_absent_returns_none_on_optional_fields(
    tmp_path: Path,
) -> None:
    project_path = tmp_path / "project.toml"
    text = _project_text()
    without_overrides = text.replace(
        '[overrides]\noutput_parquet = "data/processed/overrides.parquet"\n\n',
        "",
    )
    project_path.write_text(without_overrides, encoding="utf-8")

    project = load_project(project_path)
    assert project.overrides.data_dir is None
    with pytest.raises(ValueError, match=r"Missing \[overrides\] table"):
        _ = project.overrides.output_parquet


def test_load_project_requires_single_supported_format_version(tmp_path: Path) -> None:
    project_path = tmp_path / "project.toml"
    project_path.write_text(
        _project_text().replace("format_version = 1", "format_version = 99"),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match=f"format_version must be {FORMAT_VERSION}",
    ):
        load_project(project_path)


def test_load_project_sections_are_independent(tmp_path: Path) -> None:
    """Only the accessed section needs to be present — other missing sections don't fail."""
    project_path = tmp_path / "project.toml"
    project_path.write_text(
        'format_version = 1\n\n[gaia]\noutput_dir = "data/processed/gaia"\n',
        encoding="utf-8",
    )

    project = load_project(project_path)
    assert project.gaia.output_dir == tmp_path / "data" / "processed" / "gaia"
    with pytest.raises(ValueError, match=r"Missing \[hip\] table"):
        _ = project.hip.output_parquet


def test_render_project_template_is_valid_toml_with_all_sections() -> None:
    rendered = render_project_template()
    parsed = tomllib.loads(rendered)

    assert parsed["format_version"] == FORMAT_VERSION

    assert "output_dir" in parsed["gaia"]
    assert "mag_limit" not in parsed["gaia"]

    assert "download_ecsv" in parsed["gaia-to-hip"]
    assert "output_parquet" in parsed["gaia-to-hip"]

    assert "download_ecsv" in parsed["hip"]
    assert "output_parquet" in parsed["hip"]

    assert "hip_hd_ecsv" in parsed["identifiers"]
    assert "iv27a_catalog_ecsv" in parsed["identifiers"]
    assert "iv27a_proper_names_ecsv" in parsed["identifiers"]
    assert "output_parquet" in parsed["identifiers"]

    assert "output_parquet" in parsed["overrides"]

    assert "output_dir" in parsed["merge"]
    assert parsed["merge"]["healpix_order"] == 3

    assert "FIS_CATALOGS_DIR" not in rendered
    assert "FIS_PROCESSED_DIR" not in rendered


@pytest.mark.parametrize("profile", PROJECT_PROFILES)
def test_render_project_profiles_are_valid_toml(profile: str) -> None:
    rendered = render_project_template(profile=profile)
    parsed = tomllib.loads(rendered)

    assert parsed["format_version"] == FORMAT_VERSION
    assert parsed["gaia"]["input_dir"]
    assert parsed["gaia"]["output_dir"]
    assert parsed["merge"]["healpix_order"] == 3


@pytest.mark.parametrize("profile", PROJECT_PROFILES)
def test_checked_in_profile_matches_renderer(profile: str) -> None:
    profile_path = Path(__file__).resolve().parents[1] / "profiles" / f"{profile}.toml"

    assert profile_path.read_text(encoding="utf-8") == render_project_template(profile)


def test_render_project_template_small_profile_sets_mag_limit() -> None:
    parsed = tomllib.loads(render_project_template(profile="small"))

    assert parsed["gaia"]["input_dir"] == "data/catalogs/gaia-small"
    assert parsed["gaia"]["mag_limit"] == 8.0
    assert parsed["merge"]["output_dir"] == "data/processed/merged-small"


def test_render_project_template_rejects_unknown_profile() -> None:
    with pytest.raises(ValueError, match="Unknown project profile"):
        render_project_template(profile="tiny")


@pytest.mark.parametrize("profile", PROJECT_PROFILES)
def test_project_init_writes_selected_profile(tmp_path: Path, profile: str) -> None:
    runner = CliRunner()
    project_path = tmp_path / f"{profile}.toml"

    result = runner.invoke(
        project_cli,
        ["init", "--profile", profile, str(project_path)],
    )

    assert result.exit_code == 0, result.output
    assert project_path.read_text(encoding="utf-8") == render_project_template(profile)
    project = load_project(project_path)
    assert project.project_path == project_path.resolve()
