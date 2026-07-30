from __future__ import annotations

import json
from pathlib import Path

import pytest
import tomllib
from click.testing import CliRunner

from foundinspace.pipeline.gaia.engines import DEFAULT_GAIA_CALCULATION_ENGINE
from foundinspace.pipeline.project import (
    FORMAT_VERSION,
    PROJECT_PROFILES,
    load_project,
    render_project_template,
    render_run_project_template,
    validate_controlled_project,
)
from foundinspace.pipeline.project_cli import cli as project_cli


def _project_text() -> str:
    return (
        """
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
""".strip()
        + "\n"
    )


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
    assert project.gaia.calculation_engine == DEFAULT_GAIA_CALCULATION_ENGINE
    assert (
        project.hip.output_parquet
        == project_dir / "data" / "processed" / "hip_stars.parquet"
    )
    assert (
        project.gaia_to_hip.download_ecsv
        == project_dir / "data" / "catalogs" / "gaia_hipparcos2_best_neighbour.ecsv"
    )
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


def test_load_project_gaia_calculation_engine_reads_optional_choice(
    tmp_path: Path,
) -> None:
    project_path = tmp_path / "project.toml"
    project_path.write_text(
        _project_text().replace(
            'output_dir = "data/processed/gaia"\n',
            'output_dir = "data/processed/gaia"\n'
            'calculation_engine = "DR3_AP_BJ_CORE"\n',
        ),
        encoding="utf-8",
    )

    project = load_project(project_path)

    assert project.gaia.calculation_engine == DEFAULT_GAIA_CALCULATION_ENGINE


def test_load_project_gaia_calculation_engine_rejects_unknown(
    tmp_path: Path,
) -> None:
    project_path = tmp_path / "project.toml"
    project_path.write_text(
        _project_text().replace(
            'output_dir = "data/processed/gaia"\n',
            'output_dir = "data/processed/gaia"\ncalculation_engine = "thin"\n',
        ),
        encoding="utf-8",
    )

    project = load_project(project_path)
    with pytest.raises(ValueError, match="gaia.calculation_engine"):
        _ = project.gaia.calculation_engine


def test_load_project_gaia_download_config_and_merge_sidecar_default(
    tmp_path: Path,
) -> None:
    project_path = tmp_path / "project.toml"
    project_path.write_text(
        _project_text()
        + """
[gaia_download]
mode = "small"
access = "auto"
mag_limit = 9.0
state_db = "data/processed/gaia-download.sqlite"
row_cap = 123
max_active_jobs = 2
remote_quota_bytes = 30000000000
remote_min_free_bytes = 5000000000
remote_result_bytes_per_row = 900
remote_max_result_bytes_per_job = 4000000000
carry_field_sets = ["motion", "mass"]
source_where_adql = '''
g.phot_g_mean_mag <= 15
OR d.r_med_geo <= 100
'''
""",
        encoding="utf-8",
    )

    project = load_project(project_path)

    assert project.gaia_download.configured
    assert project.gaia_download.mode == "small"
    assert project.gaia_download.access == "auto"
    assert project.gaia_download.mag_limit == 9.0
    assert (
        project.gaia_download.state_db
        == tmp_path / "data/processed/gaia-download.sqlite"
    )
    assert project.gaia_download.row_cap == 123
    assert project.gaia_download.max_active_jobs == 2
    assert project.gaia_download.remote_quota_bytes == 30_000_000_000
    assert project.gaia_download.remote_min_free_bytes == 5_000_000_000
    assert project.gaia_download.remote_result_bytes_per_row == 900
    assert project.gaia_download.remote_max_result_bytes_per_job == 4_000_000_000
    assert project.gaia_download.carry_field_sets == ("motion", "mass")
    assert (
        project.gaia_download.source_where_adql
        == "g.phot_g_mean_mag <= 15\nOR d.r_med_geo <= 100"
    )
    assert project.merge.sidecar_output_dir == tmp_path / "data/processed/sidecars"


def test_load_project_resolves_explicit_crossmatch_and_override_paths(
    tmp_path: Path,
) -> None:
    project_path = tmp_path / "project.toml"
    project_path.write_text(
        _project_text()
        .replace(
            'output_parquet = "data/processed/gaia_hip_map.parquet"\n',
            (
                'output_parquet = "data/processed/gaia_hip_map.parquet"\n'
                'supplemental_parquets = ["../../publications/20260515.1/catalog/fis_gaia_hip_supplemental_display_map.parquet"]\n'
            ),
            1,
        )
        .replace(
            '[overrides]\noutput_parquet = "data/processed/overrides.parquet"\n',
            (
                "[overrides]\n"
                'output_parquet = "data/processed/overrides.parquet"\n'
                'source_paths = ["overrides/sun.yaml", "overrides/alpha_cen.yaml"]\n'
            ),
        ),
        encoding="utf-8",
    )

    project = load_project(project_path)

    assert project.gaia_to_hip.supplemental_parquets == (
        tmp_path
        / "../../publications/20260515.1/catalog/fis_gaia_hip_supplemental_display_map.parquet",
    )
    assert project.overrides.source_paths == (
        tmp_path / "overrides/sun.yaml",
        tmp_path / "overrides/alpha_cen.yaml",
    )


def test_load_project_rejects_override_data_dir_with_source_paths(
    tmp_path: Path,
) -> None:
    project_path = tmp_path / "project.toml"
    project_path.write_text(
        _project_text().replace(
            '[overrides]\noutput_parquet = "data/processed/overrides.parquet"\n',
            (
                "[overrides]\n"
                'output_parquet = "data/processed/overrides.parquet"\n'
                'data_dir = "overrides"\n'
                'source_paths = ["overrides/sun.yaml"]\n'
            ),
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="mutually exclusive"):
        load_project(project_path)


def test_load_project_resolves_version_pinned_zenodo_override_release(
    tmp_path: Path,
) -> None:
    project_path = tmp_path / "project.toml"
    project_path.write_text(
        _project_text().replace(
            '[overrides]\noutput_parquet = "data/processed/overrides.parquet"\n',
            (
                "[overrides]\n"
                'output_parquet = "data/processed/overrides.parquet"\n'
                'zenodo_version_doi = "10.5281/zenodo.21703732"\n'
                'zenodo_archive = "fis-overrides-20260730.2.zip"\n'
                'zenodo_archive_sha256 = "'
                "1f4a7544fe1257d50d7f5a00e9626404a05ea077bc1a305c72fb9eee6448981f"
                '"\n'
                'zenodo_download_dir = "data/catalogs/fis-overrides-20260730.2"\n'
                "source_paths = [\n"
                '  "data/catalogs/fis-overrides-20260730.2/catalog/alpha_cen.yaml",\n'
                '  "data/catalogs/fis-overrides-20260730.2/catalog/'
                'distance_resolution_v1_resolved.yaml",\n'
                "]\n"
            ),
        ),
        encoding="utf-8",
    )

    project = load_project(project_path)

    assert project.overrides.zenodo_configured
    assert project.overrides.zenodo_version_doi == "10.5281/zenodo.21703732"
    assert project.overrides.zenodo_archive == "fis-overrides-20260730.2.zip"
    assert (
        project.overrides.zenodo_download_dir
        == tmp_path / "data/catalogs/fis-overrides-20260730.2"
    )


def test_load_project_rejects_partial_zenodo_override_configuration(
    tmp_path: Path,
) -> None:
    project_path = tmp_path / "project.toml"
    project_path.write_text(
        _project_text().replace(
            '[overrides]\noutput_parquet = "data/processed/overrides.parquet"\n',
            (
                "[overrides]\n"
                'output_parquet = "data/processed/overrides.parquet"\n'
                'zenodo_version_doi = "10.5281/zenodo.21703732"\n'
            ),
            1,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Incomplete Zenodo override configuration"):
        load_project(project_path)


def test_load_project_gaia_download_defaults_to_no_carry_fields(tmp_path: Path) -> None:
    project_path = tmp_path / "project.toml"
    project_path.write_text(
        _project_text()
        + """
[gaia_download]
mode = "small"
mag_limit = 9.0
state_db = "data/processed/gaia-download.sqlite"
""",
        encoding="utf-8",
    )

    project = load_project(project_path)

    assert project.gaia_download.carry_field_sets == ()
    assert project.gaia_download.source_where_adql is None
    assert project.gaia_download.remote_quota_bytes == 20_000_000_000
    assert project.gaia_download.remote_min_free_bytes == 5_000_000_000
    assert project.gaia_download.remote_result_bytes_per_row == 400
    assert project.gaia_download.remote_max_result_bytes_per_job == 5_000_000_000


def test_load_project_gaia_download_rejects_source_where_semicolon(
    tmp_path: Path,
) -> None:
    project_path = tmp_path / "project.toml"
    project_path.write_text(
        _project_text()
        + """
[gaia_download]
mode = "small"
mag_limit = 9.0
state_db = "data/processed/gaia-download.sqlite"
source_where_adql = "g.phot_g_mean_mag <= 15; DROP TABLE x"
""",
        encoding="utf-8",
    )

    project = load_project(project_path)
    with pytest.raises(ValueError, match="source_where_adql"):
        _ = project.gaia_download.source_where_adql


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


def test_load_project_rejects_unknown_keys_in_gaia_download(tmp_path: Path) -> None:
    project_path = tmp_path / "project.toml"
    project_path.write_text(
        _project_text()
        + """
[gaia_download]
mode = "small"
state_db = "state.sqlite"
unexpected = true
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"Unknown key\(s\) in \[gaia_download\]"):
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


def test_load_project_ignores_unknown_top_level_sections(tmp_path: Path) -> None:
    project_path = tmp_path / "project.toml"
    project_path.write_text(
        "format_version = 1\n\n"
        '[gaia]\noutput_dir = "data/processed/gaia"\n\n'
        '[rezaei2024]\ncatalog_gz = "data/catalogs/finalmap.dat.gz"\n'
        'output_bin = "data/processed/dust_map.bin"\n',
        encoding="utf-8",
    )

    project = load_project(project_path)

    assert project.gaia.output_dir == tmp_path / "data" / "processed" / "gaia"


def test_section_is_configured_tracks_section_presence(tmp_path: Path) -> None:
    project_path = tmp_path / "project.toml"
    project_path.write_text(
        'format_version = 1\n\n[gaia]\noutput_dir = "data/processed/gaia"\n',
        encoding="utf-8",
    )

    project = load_project(project_path)

    assert project.gaia.is_configured is True
    assert project.gaia_download.is_configured is False
    assert project.gaia_download.configured is False
    assert project.hip.is_configured is False
    assert project.merge.is_configured is False


def test_require_succeeds_when_all_sections_present(tmp_path: Path) -> None:
    project_path = tmp_path / "project.toml"
    project_path.write_text(_project_text(), encoding="utf-8")

    project = load_project(project_path)

    project.require("gaia", "hip", "merge")


def test_require_raises_listing_missing_sections(tmp_path: Path) -> None:
    project_path = tmp_path / "project.toml"
    project_path.write_text(
        'format_version = 1\n\n[gaia]\noutput_dir = "data/processed/gaia"\n',
        encoding="utf-8",
    )

    project = load_project(project_path)

    with pytest.raises(ValueError, match=r"\[hip\].*\[merge\]"):
        project.require("gaia", "hip", "merge")


def test_require_raises_for_unknown_section_name(tmp_path: Path) -> None:
    project_path = tmp_path / "project.toml"
    project_path.write_text(_project_text(), encoding="utf-8")

    project = load_project(project_path)

    with pytest.raises(ValueError, match="unknown section name"):
        project.require("rezaei2024")


def test_render_project_template_is_valid_toml_with_all_sections() -> None:
    rendered = render_project_template()
    parsed = tomllib.loads(rendered)

    assert parsed["format_version"] == FORMAT_VERSION

    assert "output_dir" in parsed["gaia"]
    assert parsed["gaia"]["input_mode"] == "download-state"
    assert parsed["gaia"]["calculation_engine"] == DEFAULT_GAIA_CALCULATION_ENGINE
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
    assert parsed["overrides"]["zenodo_version_doi"] == "10.5281/zenodo.21703732"
    assert parsed["overrides"]["zenodo_archive"] == "fis-overrides-20260730.2.zip"
    assert parsed["overrides"]["source_paths"] == [
        "data/catalogs/fis-overrides-20260730.2/catalog/alpha_cen.yaml",
        (
            "data/catalogs/fis-overrides-20260730.2/catalog/"
            "distance_resolution_v1_resolved.yaml"
        ),
    ]

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
    assert parsed["gaia"]["calculation_engine"] == DEFAULT_GAIA_CALCULATION_ENGINE
    assert parsed["merge"]["healpix_order"] == 3


@pytest.mark.parametrize("profile", PROJECT_PROFILES)
def test_checked_in_profile_matches_renderer(profile: str) -> None:
    profile_path = Path(__file__).resolve().parents[1] / "profiles" / f"{profile}.toml"

    assert profile_path.read_text(encoding="utf-8") == render_project_template(profile)


def test_render_project_template_small_profile_sets_mag_limit() -> None:
    parsed = tomllib.loads(render_project_template(profile="small"))

    assert parsed["gaia"]["input_dir"] == "data/catalogs/gaia-small"
    assert parsed["gaia"]["mag_limit"] == 9.0
    assert parsed["gaia_download"]["mode"] == "small"
    assert parsed["gaia_download"]["mag_limit"] == 9.0
    assert parsed["gaia_download"]["carry_field_sets"] == []
    assert parsed["merge"]["output_dir"] == "data/processed/merged-small"


def test_render_project_template_full_profile_uses_core_gaia_fields() -> None:
    parsed = tomllib.loads(render_project_template(profile="full"))

    assert parsed["gaia_download"]["mode"] == "full"
    assert parsed["gaia_download"]["carry_field_sets"] == []
    assert parsed["gaia_download"]["remote_result_bytes_per_row"] == 400


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


def test_render_run_project_template_adds_controlled_run_metadata() -> None:
    parsed = tomllib.loads(
        render_run_project_template(run_id="stars-20260730-a1b2c3", profile="full")
    )

    assert parsed["format_version"] == FORMAT_VERSION
    assert parsed["run"] == {
        "id": "stars-20260730-a1b2c3",
        "provenance_dir": "provenance",
    }


def test_project_init_run_creates_named_run_and_refuses_overwrite(
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    run_dir = tmp_path / "stars-20260730-a1b2c3"

    result = runner.invoke(
        project_cli,
        ["init-run", "--profile", "small", str(run_dir)],
    )

    assert result.exit_code == 0, result.output
    assert (run_dir / "RUN_ID").read_text(encoding="utf-8") == (
        "stars-20260730-a1b2c3\n"
    )
    assert (run_dir / "provenance").is_dir()
    project = load_project(run_dir / "pipeline.toml")
    assert project.run.id == run_dir.name
    assert project.run.provenance_dir == run_dir / "provenance"
    validate_controlled_project(project)

    repeated = runner.invoke(
        project_cli,
        ["init-run", "--profile", "small", str(run_dir)],
    )
    assert repeated.exit_code != 0
    assert "already exists" in repeated.output


def test_controlled_validation_requires_explicit_gaia_and_override_inputs(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "run-1"
    run_dir.mkdir()
    project_path = run_dir / "pipeline.toml"
    project_path.write_text(
        _project_text().replace(
            "format_version = 1",
            'format_version = 1\n\n[run]\nid = "run-1"',
        )
        + '\n[gaia_download]\nmode = "full"\nstate_db = "download.sqlite"\n',
        encoding="utf-8",
    )
    project = load_project(project_path)

    with pytest.raises(ValueError, match="gaia.input_mode"):
        validate_controlled_project(project)

    with_explicit_gaia = project_path.read_text(encoding="utf-8").replace(
        '[gaia]\noutput_dir = "data/processed/gaia"',
        '[gaia]\ninput_mode = "directory"\n'
        'input_dir = "data/catalogs/gaia"\n'
        'output_dir = "data/processed/gaia"',
    )
    project_path.write_text(
        with_explicit_gaia,
        encoding="utf-8",
    )
    project = load_project(project_path)

    with pytest.raises(ValueError, match="overrides.source_paths"):
        validate_controlled_project(project)

    project_path.write_text(
        with_explicit_gaia.replace(
            '[overrides]\noutput_parquet = "data/processed/overrides.parquet"',
            '[overrides]\noutput_parquet = "data/processed/overrides.parquet"\n'
            "source_paths = []",
        ),
        encoding="utf-8",
    )
    validate_controlled_project(load_project(project_path))


@pytest.mark.parametrize(
    ("old", "new", "message"),
    [
        (
            'calculation_engine = "dr3_ap_bj_core"',
            'calculation_engine = "unknown"',
            "gaia.calculation_engine",
        ),
        (
            'input_mode = "download-state"',
            'input_mode = "unknown"',
            "gaia.input_mode",
        ),
        ('mode = "small"', 'mode = "unknown"', "gaia_download.mode"),
        ('access = "auto"', 'access = "unknown"', "gaia_download.access"),
        (
            "row_cap = 55000000",
            "row_cap = 0",
            "gaia_download.row_cap",
        ),
        (
            "max_active_jobs = 4",
            "max_active_jobs = 0",
            "gaia_download.max_active_jobs",
        ),
        (
            "max_active_jobs = 4",
            "max_active_jobs = 4\nremote_min_free_bytes = -1",
            "gaia_download.remote_min_free_bytes",
        ),
        (
            "remote_result_bytes_per_row = 400",
            "remote_result_bytes_per_row = 0",
            "gaia_download.remote_result_bytes_per_row",
        ),
        (
            "remote_max_result_bytes_per_job = 5000000000",
            "remote_max_result_bytes_per_job = 0",
            "gaia_download.remote_max_result_bytes_per_job",
        ),
        (
            "carry_field_sets = []",
            'carry_field_sets = ["not_installed"]',
            "Gaia field set not found",
        ),
        (
            ('"data/catalogs/fis-overrides-20260730.2/catalog/alpha_cen.yaml",'),
            "1,",
            "overrides.source_paths",
        ),
        (
            '# supplemental_parquets = ["path/to/pre_resolved_gaia_hip_map.parquet"]',
            "supplemental_parquets = [1]",
            "gaia-to-hip.supplemental_parquets",
        ),
        ("healpix_order = 3", "healpix_order = -1", "merge.healpix_order"),
    ],
)
def test_controlled_validation_checks_every_configured_setting(
    tmp_path: Path,
    old: str,
    new: str,
    message: str,
) -> None:
    run_dir = tmp_path / "run-settings"
    run_dir.mkdir()
    project_path = run_dir / "pipeline.toml"
    project_path.write_text(
        render_run_project_template(run_id=run_dir.name, profile="small").replace(
            old,
            new,
            1,
        ),
        encoding="utf-8",
    )

    with pytest.raises((OSError, ValueError), match=message):
        validate_controlled_project(load_project(project_path))


@pytest.mark.parametrize(
    ("old", "new", "message"),
    [
        (
            "mag_limit = 9.0",
            "mag_limit = 8.0",
            "gaia.mag_limit must match",
        ),
        (
            "remote_result_bytes_per_row = 400",
            "remote_result_bytes_per_row = 6000000000",
            "remote_max_result_bytes_per_job",
        ),
    ],
)
def test_controlled_validation_checks_cross_setting_limits(
    tmp_path: Path,
    old: str,
    new: str,
    message: str,
) -> None:
    run_dir = tmp_path / "run-limits"
    run_dir.mkdir()
    project_path = run_dir / "pipeline.toml"
    project_path.write_text(
        render_run_project_template(run_id=run_dir.name, profile="small").replace(
            old,
            new,
            1,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=message):
        validate_controlled_project(load_project(project_path))


def test_project_validate_records_controlled_configuration(tmp_path: Path) -> None:
    runner = CliRunner()
    run_dir = tmp_path / "run-validated"
    created = runner.invoke(project_cli, ["init-run", str(run_dir)])
    assert created.exit_code == 0, created.output

    result = runner.invoke(
        project_cli,
        ["validate", "--project", str(run_dir / "pipeline.toml")],
    )

    assert result.exit_code == 0, result.output
    event_paths = list((run_dir / "provenance").glob("*.json"))
    assert len(event_paths) == 1
    event = json.loads(event_paths[0].read_text(encoding="utf-8"))
    assert event["command"] == "project validate"
    assert event["status"] == "succeeded"
    assert event["project"]["resolved_paths"]["gaia.input_dir"].endswith(
        "/data/catalogs/gaia"
    )


def test_project_validate_reports_malformed_run_metadata_without_event(
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    run_dir = tmp_path / "run-invalid"
    run_dir.mkdir()
    provenance_dir = run_dir / "provenance"
    provenance_dir.mkdir()
    project_path = run_dir / "pipeline.toml"
    project_path.write_text(
        render_run_project_template(run_id=run_dir.name).replace(
            'id = "run-invalid"',
            'id = "../invalid"',
        ),
        encoding="utf-8",
    )

    result = runner.invoke(
        project_cli,
        ["validate", "--project", str(project_path)],
    )

    assert result.exit_code != 0
    assert "Error: run.id must start with an alphanumeric" in result.output
    assert "Traceback" not in result.output
    assert not list(provenance_dir.iterdir())


def test_project_validate_records_non_run_validation_failure(
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    run_dir = tmp_path / "run-invalid-engine"
    run_dir.mkdir()
    provenance_dir = run_dir / "provenance"
    provenance_dir.mkdir()
    project_path = run_dir / "pipeline.toml"
    project_path.write_text(
        render_run_project_template(run_id=run_dir.name).replace(
            'calculation_engine = "dr3_ap_bj_core"',
            'calculation_engine = "invalid"',
        ),
        encoding="utf-8",
    )

    result = runner.invoke(
        project_cli,
        ["validate", "--project", str(project_path)],
    )

    assert result.exit_code != 0
    assert "gaia.calculation_engine" in result.output
    event_path = next(provenance_dir.glob("*.json"))
    event = json.loads(event_path.read_text(encoding="utf-8"))
    assert event["status"] == "failed"
    assert event["error"]["type"] == "ClickException"
