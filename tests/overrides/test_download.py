from __future__ import annotations

import hashlib
import json
import zipfile
from pathlib import Path

import pytest
from click.testing import CliRunner

import foundinspace.pipeline.overrides.cli as overrides_cli_module
import foundinspace.pipeline.overrides.download as download_module
from foundinspace.pipeline.overrides.cli import cli as overrides_cli
from foundinspace.pipeline.overrides.download import (
    OverrideReleaseError,
    ensure_zenodo_override_release,
    zenodo_record_id_from_version_doi,
)

VERSION_DOI = "10.5281/zenodo.21703732"
CONCEPT_DOI = "10.5281/zenodo.21703731"
ARCHIVE_NAME = "fis-overrides-20260730.2.zip"
RELEASE = "20260730.2"
CATALOG_MEMBERS = (
    "catalog/alpha_cen.yaml",
    "catalog/distance_resolution_v1_resolved.yaml",
)


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _build_release_archive(tmp_path: Path) -> tuple[bytes, str]:
    release_dir = tmp_path / "release"
    (release_dir / "catalog").mkdir(parents=True)
    (release_dir / CATALOG_MEMBERS[0]).write_text(
        "stars: []\n",
        encoding="utf-8",
    )
    (release_dir / CATALOG_MEMBERS[1]).write_text(
        "stars: []\n",
        encoding="utf-8",
    )
    (release_dir / "manifest.toml").write_text(
        f"""
format_version = 1
release = "{RELEASE}"
series_id = "fis.overrides"

[lifecycle]
version_doi = "{VERSION_DOI}"

[outputs]
alpha_cen_catalog = "{CATALOG_MEMBERS[0]}"
distance_resolution_catalog = "{CATALOG_MEMBERS[1]}"

[consumer_contract]
selection_must_be_explicit = true
all_two_catalog_paths_required = true
""".lstrip(),
        encoding="utf-8",
    )
    checksummed = sorted(
        path
        for path in release_dir.rglob("*")
        if path.is_file() and path.name != "checksums.sha256"
    )
    (release_dir / "checksums.sha256").write_text(
        "".join(
            f"{_sha256(path.read_bytes())}  "
            f"{path.relative_to(release_dir).as_posix()}\n"
            for path in checksummed
        ),
        encoding="utf-8",
    )

    archive_path = tmp_path / ARCHIVE_NAME
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path in sorted(release_dir.rglob("*")):
            if path.is_file():
                zf.write(path, path.relative_to(release_dir).as_posix())
    payload = archive_path.read_bytes()
    return payload, _sha256(payload)


def _record(payload: bytes, *, doi: str = VERSION_DOI, record_id: int = 21703732):
    return {
        "id": record_id,
        "doi": doi,
        "conceptdoi": CONCEPT_DOI,
        "status": "published",
        "metadata": {
            "version": RELEASE,
            "access_right": "open",
        },
        "files": [
            {
                "key": ARCHIVE_NAME,
                "checksum": f"md5:{hashlib.md5(payload).hexdigest()}",
                "links": {
                    "self": (
                        "https://zenodo.org/api/records/21703732/files/"
                        f"{ARCHIVE_NAME}/content"
                    )
                },
            }
        ],
    }


def _source_paths(download_dir: Path) -> tuple[Path, ...]:
    return tuple(download_dir / member for member in CATALOG_MEMBERS)


def test_version_doi_encodes_exact_record_id() -> None:
    assert zenodo_record_id_from_version_doi(VERSION_DOI) == 21703732

    with pytest.raises(OverrideReleaseError, match="canonical Zenodo Version DOI"):
        zenodo_record_id_from_version_doi("https://doi.org/10.5281/zenodo.21703732")


def test_download_refuses_filesystem_root_as_cache() -> None:
    with pytest.raises(OverrideReleaseError, match="broad override download target"):
        ensure_zenodo_override_release(
            Path("/"),
            version_doi=VERSION_DOI,
            archive_name=ARCHIVE_NAME,
            archive_sha256="0" * 64,
            source_paths=(),
        )


def test_download_verifies_and_extracts_published_format(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload, archive_sha256 = _build_release_archive(tmp_path)
    download_dir = tmp_path / "cached"
    calls: list[str] = []

    def fake_record(url: str, *, timeout: float = 60.0):
        calls.append(url)
        return _record(payload)

    def fake_download(url: str, target: Path, *, timeout: float = 60.0) -> None:
        calls.append(url)
        target.write_bytes(payload)

    monkeypatch.setattr(download_module, "_read_url_json", fake_record)
    monkeypatch.setattr(download_module, "_download_url", fake_download)

    out = ensure_zenodo_override_release(
        download_dir,
        version_doi=VERSION_DOI,
        archive_name=ARCHIVE_NAME,
        archive_sha256=archive_sha256,
        source_paths=_source_paths(download_dir),
    )

    assert out == download_dir
    assert (download_dir / ARCHIVE_NAME).read_bytes() == payload
    assert all(path.is_file() for path in _source_paths(download_dir))
    receipt = json.loads((download_dir / "zenodo-record.json").read_text())
    assert receipt["doi"] == VERSION_DOI
    assert calls[0].endswith("/api/records/21703732")
    assert calls[1].endswith(f"/{ARCHIVE_NAME}/content")

    def unexpected(*args, **kwargs):
        raise AssertionError("valid cached releases must not contact Zenodo")

    monkeypatch.setattr(download_module, "_read_url_json", unexpected)
    monkeypatch.setattr(download_module, "_download_url", unexpected)
    assert (
        ensure_zenodo_override_release(
            download_dir,
            version_doi=VERSION_DOI,
            archive_name=ARCHIVE_NAME,
            archive_sha256=archive_sha256,
            source_paths=_source_paths(download_dir),
        )
        == download_dir
    )


def test_concept_doi_cannot_be_used_as_version_pin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload, archive_sha256 = _build_release_archive(tmp_path)
    monkeypatch.setattr(
        download_module,
        "_read_url_json",
        lambda url, timeout=60.0: _record(
            payload,
            doi=VERSION_DOI,
            record_id=21703731,
        ),
    )

    with pytest.raises(OverrideReleaseError, match="Concept DOI"):
        ensure_zenodo_override_release(
            tmp_path / "cached",
            version_doi=CONCEPT_DOI,
            archive_name=ARCHIVE_NAME,
            archive_sha256=archive_sha256,
            source_paths=_source_paths(tmp_path / "cached"),
        )


def test_download_rejects_sha256_mismatch_before_extraction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload, _ = _build_release_archive(tmp_path)
    monkeypatch.setattr(
        download_module,
        "_read_url_json",
        lambda url, timeout=60.0: _record(payload),
    )
    monkeypatch.setattr(
        download_module,
        "_download_url",
        lambda url, target, timeout=60.0: target.write_bytes(payload),
    )
    download_dir = tmp_path / "cached"

    with pytest.raises(OverrideReleaseError, match="SHA-256 mismatch"):
        ensure_zenodo_override_release(
            download_dir,
            version_doi=VERSION_DOI,
            archive_name=ARCHIVE_NAME,
            archive_sha256="0" * 64,
            source_paths=_source_paths(download_dir),
        )

    assert not download_dir.exists()


def test_download_requires_manifest_catalog_paths_in_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload, archive_sha256 = _build_release_archive(tmp_path)
    monkeypatch.setattr(
        download_module,
        "_read_url_json",
        lambda url, timeout=60.0: _record(payload),
    )
    monkeypatch.setattr(
        download_module,
        "_download_url",
        lambda url, target, timeout=60.0: target.write_bytes(payload),
    )
    download_dir = tmp_path / "cached"

    with pytest.raises(OverrideReleaseError, match="manifest order"):
        ensure_zenodo_override_release(
            download_dir,
            version_doi=VERSION_DOI,
            archive_name=ARCHIVE_NAME,
            archive_sha256=archive_sha256,
            source_paths=tuple(reversed(_source_paths(download_dir))),
        )


def test_safe_extraction_rejects_parent_traversal(tmp_path: Path) -> None:
    archive_path = tmp_path / "unsafe.zip"
    with zipfile.ZipFile(archive_path, "w") as zf:
        zf.writestr("../escape.yaml", "stars: []\n")

    with pytest.raises(OverrideReleaseError, match="Unsafe path"):
        download_module._safe_extract_zip(archive_path, tmp_path / "out")


def test_download_cli_uses_version_doi_configuration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    download_dir = tmp_path / "release"
    project_path = tmp_path / "project.toml"
    project_path.write_text(
        f"""
format_version = 1

[overrides]
output_parquet = "overrides.parquet"
zenodo_version_doi = "{VERSION_DOI}"
zenodo_archive = "{ARCHIVE_NAME}"
zenodo_archive_sha256 = "{"1" * 64}"
zenodo_download_dir = "release"
source_paths = [
  "release/{CATALOG_MEMBERS[0]}",
  "release/{CATALOG_MEMBERS[1]}",
]
""".lstrip(),
        encoding="utf-8",
    )
    captured: dict[str, object] = {}

    def fake_ensure(target: Path, **kwargs):
        captured["target"] = target
        captured.update(kwargs)
        target.mkdir()
        return target

    monkeypatch.setattr(
        overrides_cli_module,
        "ensure_zenodo_override_release",
        fake_ensure,
    )

    result = CliRunner().invoke(
        overrides_cli,
        ["download", "--project", str(project_path)],
    )

    assert result.exit_code == 0, result.output
    assert captured["target"] == download_dir
    assert captured["version_doi"] == VERSION_DOI
    assert captured["source_paths"] == _source_paths(download_dir)


def test_build_revalidates_zenodo_release_before_loading_yaml(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload, archive_sha256 = _build_release_archive(tmp_path)
    download_dir = tmp_path / "cached"
    monkeypatch.setattr(
        download_module,
        "_read_url_json",
        lambda url, timeout=60.0: _record(payload),
    )
    monkeypatch.setattr(
        download_module,
        "_download_url",
        lambda url, target, timeout=60.0: target.write_bytes(payload),
    )
    ensure_zenodo_override_release(
        download_dir,
        version_doi=VERSION_DOI,
        archive_name=ARCHIVE_NAME,
        archive_sha256=archive_sha256,
        source_paths=_source_paths(download_dir),
    )
    _source_paths(download_dir)[0].write_text(
        "stars:\n  - override_id: tampered\n",
        encoding="utf-8",
    )

    project_path = tmp_path / "project.toml"
    project_path.write_text(
        f"""
format_version = 1

[overrides]
output_parquet = "overrides.parquet"
zenodo_version_doi = "{VERSION_DOI}"
zenodo_archive = "{ARCHIVE_NAME}"
zenodo_archive_sha256 = "{archive_sha256}"
zenodo_download_dir = "cached"
source_paths = [
  "cached/{CATALOG_MEMBERS[0]}",
  "cached/{CATALOG_MEMBERS[1]}",
]
""".lstrip(),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        overrides_cli,
        ["build", "--project", str(project_path)],
    )

    assert result.exit_code != 0
    assert "Checksum mismatch" in result.output
    assert not (tmp_path / "overrides.parquet").exists()
