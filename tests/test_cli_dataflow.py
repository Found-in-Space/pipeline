from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

import foundinspace.pipeline.gaia.cli as gaia_cli_module
import foundinspace.pipeline.hipparcos.cli as hip_cli_module
import foundinspace.pipeline.identifiers.cli as identifiers_cli_module
import foundinspace.pipeline.identifiers.download as identifiers_download
import foundinspace.pipeline.merge.cli as merge_cli_module
from foundinspace.pipeline.gaia.cli import cli as gaia_cli
from foundinspace.pipeline.gaia_to_hip.cli import cli as gaia_to_hip_cli
from foundinspace.pipeline.hipparcos.cli import cli as hip_cli
from foundinspace.pipeline.identifiers.cli import cli as identifiers_cli
from foundinspace.pipeline.merge.cli import cli as merge_cli


def _write_project(path: Path, body: str) -> Path:
    path.write_text(f"format_version = 1\n\n{body.strip()}\n", encoding="utf-8")
    return path


def test_hip_build_requires_downloaded_input_and_never_acquires(
    tmp_path: Path,
) -> None:
    project_path = _write_project(
        tmp_path / "project.toml",
        """
[hip]
download_ecsv = "raw/hip.ecsv"
output_parquet = "derived/hip.parquet"
""",
    )

    result = CliRunner().invoke(
        hip_cli,
        ["build", "--project", str(project_path)],
    )

    assert result.exit_code != 0
    assert "Run the matching download command first" in result.output
    assert not (tmp_path / "raw/hip.ecsv").exists()


def test_limited_hip_build_requires_noncanonical_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = tmp_path / "raw/hip.ecsv"
    raw.parent.mkdir()
    raw.write_text("fixture\n", encoding="utf-8")
    project_path = _write_project(
        tmp_path / "project.toml",
        """
[hip]
download_ecsv = "raw/hip.ecsv"
output_parquet = "derived/hip.parquet"
""",
    )
    calls: list[tuple[Path, Path, int | None]] = []

    def fake_build(
        input_path: Path,
        output_path: Path,
        *,
        skip_if_exists: bool,
        limit: int | None,
    ) -> None:
        calls.append((input_path, output_path, limit))
        output_path.write_text(str(skip_if_exists), encoding="utf-8")

    monkeypatch.setattr(hip_cli_module, "main", fake_build)
    runner = CliRunner()
    rejected = runner.invoke(
        hip_cli,
        ["build", "--project", str(project_path), "--limit", "1"],
    )

    assert rejected.exit_code != 0
    assert "--limit requires --output" in rejected.output
    assert not calls

    sample_output = tmp_path / "samples/hip-one.parquet"
    accepted = runner.invoke(
        hip_cli,
        [
            "build",
            "--project",
            str(project_path),
            "--limit",
            "1",
            "--output",
            str(sample_output),
        ],
    )
    assert accepted.exit_code == 0, accepted.output
    assert calls == [(raw, sample_output, 1)]
    assert not (tmp_path / "derived/hip.parquet").exists()


def test_gaia_to_hip_build_requires_downloaded_input(tmp_path: Path) -> None:
    project_path = _write_project(
        tmp_path / "project.toml",
        """
[gaia-to-hip]
download_ecsv = "raw/best-neighbour.ecsv"
output_parquet = "derived/map.parquet"
""",
    )

    result = CliRunner().invoke(
        gaia_to_hip_cli,
        ["build", "--project", str(project_path)],
    )

    assert result.exit_code != 0
    assert "Run the matching download command first" in result.output
    assert not (tmp_path / "raw/best-neighbour.ecsv").exists()


def test_identifier_download_fetches_only_missing_catalog(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hip_hd = tmp_path / "hip-hd.ecsv"
    names = tmp_path / "names.ecsv"
    hip_hd.write_text("existing\n", encoding="utf-8")
    names.write_text("existing\n", encoding="utf-8")
    catalog = tmp_path / "catalog.ecsv"
    fetched: list[Path] = []

    def fake_fetch(*, catalog, columns, output_path, overwrite):
        fetched.append(output_path)
        output_path.write_text(f"{catalog}:{columns}:{overwrite}\n", encoding="utf-8")
        return output_path

    monkeypatch.setattr(identifiers_download, "_fetch_catalog_to_ecsv", fake_fetch)
    outputs = identifiers_download.ensure_identifier_catalogs(
        hip_hd_output=hip_hd,
        iv27a_catalog_output=catalog,
        iv27a_proper_names_output=names,
    )

    assert fetched == [catalog]
    assert outputs["hip_hd"].read_text(encoding="utf-8") == "existing\n"
    assert outputs["iv27a_proper_names"].read_text(encoding="utf-8") == "existing\n"


def test_identifiers_build_requires_configured_crossmatch_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    for name in ("hip-hd.ecsv", "catalog.ecsv", "names.ecsv"):
        (raw_dir / name).write_text("fixture\n", encoding="utf-8")
    project_path = _write_project(
        tmp_path / "project.toml",
        """
[gaia-to-hip]
download_ecsv = "raw/best-neighbour.ecsv"
output_parquet = "derived/map.parquet"

[identifiers]
hip_hd_ecsv = "raw/hip-hd.ecsv"
iv27a_catalog_ecsv = "raw/catalog.ecsv"
iv27a_proper_names_ecsv = "raw/names.ecsv"
output_parquet = "derived/identifiers.parquet"
""",
    )
    called = False

    def fake_prepare(*args, **kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(
        identifiers_cli_module,
        "prepare_identifiers_sidecar",
        fake_prepare,
    )
    result = CliRunner().invoke(
        identifiers_cli,
        ["build", "--project", str(project_path)],
    )

    assert result.exit_code != 0
    assert "[gaia-to-hip] output_parquet" in result.output
    assert not called


def test_identifiers_build_revalidates_configured_zenodo_override_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    for name in ("hip-hd.ecsv", "catalog.ecsv", "names.ecsv"):
        (raw_dir / name).write_text("fixture\n", encoding="utf-8")
    derived_dir = tmp_path / "derived"
    derived_dir.mkdir()
    (derived_dir / "map.parquet").write_text("fixture\n", encoding="utf-8")
    release_dir = tmp_path / "release"
    (release_dir / "catalog").mkdir(parents=True)
    (release_dir / "catalog/alpha_cen.yaml").write_text(
        "stars: []\n",
        encoding="utf-8",
    )
    (release_dir / "catalog/distance_resolution_v1_resolved.yaml").write_text(
        "stars: []\n",
        encoding="utf-8",
    )
    project_path = _write_project(
        tmp_path / "project.toml",
        """
[gaia-to-hip]
download_ecsv = "raw/best-neighbour.ecsv"
output_parquet = "derived/map.parquet"

[identifiers]
hip_hd_ecsv = "raw/hip-hd.ecsv"
iv27a_catalog_ecsv = "raw/catalog.ecsv"
iv27a_proper_names_ecsv = "raw/names.ecsv"
output_parquet = "derived/identifiers.parquet"

[overrides]
output_parquet = "derived/overrides.parquet"
zenodo_version_doi = "10.5281/zenodo.21703732"
zenodo_archive = "fis-overrides-20260730.2.zip"
zenodo_archive_sha256 = "1111111111111111111111111111111111111111111111111111111111111111"
zenodo_download_dir = "release"
source_paths = [
  "release/catalog/alpha_cen.yaml",
  "release/catalog/distance_resolution_v1_resolved.yaml",
]
""",
    )
    prepared = False

    def reject_release(*args, **kwargs):
        raise ValueError("tampered override release")

    def fake_prepare(*args, **kwargs):
        nonlocal prepared
        prepared = True

    monkeypatch.setattr(
        identifiers_cli_module,
        "validate_cached_override_release",
        reject_release,
    )
    monkeypatch.setattr(
        identifiers_cli_module,
        "prepare_identifiers_sidecar",
        fake_prepare,
    )

    result = CliRunner().invoke(
        identifiers_cli,
        ["build", "--project", str(project_path)],
    )

    assert result.exit_code != 0
    assert "tampered override release" in result.output
    assert not prepared


def test_gaia_input_mode_controls_state_use(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_dir = tmp_path / "raw"
    input_dir.mkdir()
    votable = input_dir / "manual.vot"
    votable.write_text("fixture\n", encoding="utf-8")
    body = """
[gaia]
input_dir = "raw"
input_mode = "{input_mode}"
output_dir = "derived"

[gaia_download]
mode = "small"
state_db = "missing.sqlite"
carry_field_sets = []
"""
    calls: list[Path] = []

    def fake_build(input_path: Path, output_path: Path, **kwargs) -> None:
        calls.append(input_path)

    monkeypatch.setattr(gaia_cli_module, "main", fake_build)
    runner = CliRunner()
    directory_project = _write_project(
        tmp_path / "directory.toml",
        body.format(input_mode="directory"),
    )
    directory_result = runner.invoke(
        gaia_cli,
        ["build", "--project", str(directory_project)],
    )
    assert directory_result.exit_code == 0, directory_result.output
    assert calls == [votable]

    calls.clear()
    state_project = _write_project(
        tmp_path / "state.toml",
        body.format(input_mode="download-state"),
    )
    state_result = runner.invoke(
        gaia_cli,
        ["build", "--project", str(state_project)],
    )
    assert state_result.exit_code != 0
    assert "state_db is missing or contains no batches" in state_result.output
    assert not calls


def test_merge_crossmatch_override_must_equal_project_value(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_path = _write_project(
        tmp_path / "project.toml",
        """
[gaia]
input_dir = "raw"
output_dir = "gaia"

[gaia-to-hip]
download_ecsv = "raw/map.ecsv"
output_parquet = "accepted/map.parquet"

[hip]
download_ecsv = "raw/hip.ecsv"
output_parquet = "hip.parquet"

[overrides]
output_parquet = "overrides.parquet"

[merge]
output_dir = "merged"
healpix_order = 3
""",
    )
    other_map = tmp_path / "other-map.parquet"
    other_map.write_text("fixture\n", encoding="utf-8")
    called = False

    def fake_merge(**kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(merge_cli_module, "run_merge", fake_merge)
    result = CliRunner().invoke(
        merge_cli,
        [
            "build",
            "--project",
            str(project_path),
            "--crossmatch-path",
            str(other_map),
        ],
    )

    assert result.exit_code != 0
    assert "may no longer override" in result.output
    assert not called
