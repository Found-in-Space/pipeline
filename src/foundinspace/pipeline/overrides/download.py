"""Acquire a version-pinned override publication from Zenodo."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import stat
import tempfile
import uuid
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import tomllib

ZENODO_API_ROOT = "https://zenodo.org/api/records"
ZENODO_VERSION_DOI_RE = re.compile(r"^10\.5281/zenodo\.([1-9][0-9]*)$")
CHECKSUM_LINE_RE = re.compile(r"^([0-9a-f]{64}) [ *](.+)$")
RECORD_RECEIPT = "zenodo-record.json"
REQUIRED_SERIES_ID = "fis.overrides"
REQUIRED_CATALOG_OUTPUTS = (
    "alpha_cen_catalog",
    "distance_resolution_catalog",
)


class OverrideReleaseError(ValueError):
    """Raised when a remote or cached override release fails validation."""


def _hash_file(path: Path, algorithm: str) -> str:
    digest = hashlib.new(algorithm)
    with path.open("rb") as fp:
        for block in iter(lambda: fp.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_url_json(url: str, *, timeout: float = 60.0) -> dict[str, Any]:
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "found-in-space-pipeline/zenodo-overrides",
        },
    )
    try:
        with urlopen(request, timeout=timeout) as response:  # noqa: S310
            payload = json.load(response)
    except (HTTPError, URLError, OSError, json.JSONDecodeError) as exc:
        raise OverrideReleaseError(
            f"Could not retrieve Zenodo record metadata from {url}: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise OverrideReleaseError("Zenodo record metadata must be a JSON object")
    return payload


def _download_url(url: str, target: Path, *, timeout: float = 60.0) -> None:
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.hostname != "zenodo.org":
        raise OverrideReleaseError(f"Zenodo returned an unexpected artifact URL: {url}")
    request = Request(
        url,
        headers={"User-Agent": "found-in-space-pipeline/zenodo-overrides"},
    )
    try:
        with (
            urlopen(request, timeout=timeout) as response,  # noqa: S310
            target.open("wb") as fp,
        ):
            shutil.copyfileobj(response, fp, length=1024 * 1024)
    except (HTTPError, URLError, OSError) as exc:
        raise OverrideReleaseError(
            f"Could not download Zenodo override artifact from {url}: {exc}"
        ) from exc


def zenodo_record_id_from_version_doi(version_doi: str) -> int:
    """Return the record ID encoded by a canonical Zenodo Version DOI."""
    match = ZENODO_VERSION_DOI_RE.fullmatch(version_doi)
    if match is None:
        raise OverrideReleaseError(
            "Expected a canonical Zenodo Version DOI such as '10.5281/zenodo.21703732'"
        )
    return int(match.group(1))


def _validated_record(
    *,
    version_doi: str,
    archive_name: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    record_id = zenodo_record_id_from_version_doi(version_doi)
    record_url = f"{ZENODO_API_ROOT}/{record_id}"
    record = _read_url_json(record_url)

    if record.get("id") != record_id:
        raise OverrideReleaseError(
            f"Zenodo record ID does not match Version DOI {version_doi}"
        )
    if record.get("doi") != version_doi:
        raise OverrideReleaseError(
            f"{version_doi} is not the Version DOI returned by record {record_id}; "
            "a Concept DOI or mismatched record is not allowed"
        )
    concept_doi = record.get("conceptdoi")
    if not isinstance(concept_doi, str) or concept_doi == version_doi:
        raise OverrideReleaseError(
            "Zenodo record does not expose a distinct Concept DOI and Version DOI"
        )
    if record.get("status") != "published":
        raise OverrideReleaseError(f"Zenodo record {record_id} is not published")

    metadata = record.get("metadata")
    if not isinstance(metadata, dict):
        raise OverrideReleaseError("Zenodo record is missing metadata")
    if metadata.get("access_right") != "open":
        raise OverrideReleaseError("Zenodo override artifact must be open access")
    version = metadata.get("version")
    if not isinstance(version, str) or not version.strip():
        raise OverrideReleaseError("Zenodo record is missing its release version")

    files = record.get("files")
    if not isinstance(files, list):
        raise OverrideReleaseError("Zenodo record is missing its file list")
    matches = [
        item
        for item in files
        if isinstance(item, dict) and item.get("key") == archive_name
    ]
    if len(matches) != 1:
        raise OverrideReleaseError(
            f"Zenodo record must contain exactly one artifact named {archive_name!r}"
        )
    artifact = matches[0]
    links = artifact.get("links")
    if not isinstance(links, dict) or not isinstance(links.get("self"), str):
        raise OverrideReleaseError(
            f"Zenodo artifact {archive_name!r} has no content link"
        )
    return record, artifact


def _safe_extract_zip(archive_path: Path, destination: Path) -> None:
    try:
        archive = zipfile.ZipFile(archive_path)
    except (OSError, zipfile.BadZipFile) as exc:
        raise OverrideReleaseError(
            f"Override artifact is not a readable ZIP archive: {archive_path}"
        ) from exc

    with archive:
        seen: set[str] = set()
        for member in archive.infolist():
            raw_name = member.filename
            member_path = PurePosixPath(raw_name)
            if (
                not raw_name
                or "\\" in raw_name
                or member_path.is_absolute()
                or ".." in member_path.parts
            ):
                raise OverrideReleaseError(
                    f"Unsafe path in override archive: {raw_name!r}"
                )
            normalized = member_path.as_posix().rstrip("/")
            if normalized in seen:
                raise OverrideReleaseError(
                    f"Duplicate path in override archive: {normalized!r}"
                )
            seen.add(normalized)
            if normalized in {archive_path.name, RECORD_RECEIPT}:
                raise OverrideReleaseError(
                    f"Reserved path in override archive: {normalized!r}"
                )

            mode = member.external_attr >> 16
            if stat.S_ISLNK(mode):
                raise OverrideReleaseError(
                    f"Symbolic links are not allowed in override archive: {raw_name!r}"
                )

            output_path = destination.joinpath(*member_path.parts)
            if member.is_dir():
                output_path.mkdir(parents=True, exist_ok=True)
                continue
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(member, "r") as source, output_path.open("xb") as output:
                shutil.copyfileobj(source, output, length=1024 * 1024)


def _release_checksums(release_dir: Path) -> dict[str, str]:
    checksum_path = release_dir / "checksums.sha256"
    if not checksum_path.is_file():
        raise OverrideReleaseError("Override release is missing checksums.sha256")

    checksums: dict[str, str] = {}
    for line_number, raw_line in enumerate(
        checksum_path.read_text(encoding="utf-8").splitlines(),
        start=1,
    ):
        if not raw_line:
            continue
        match = CHECKSUM_LINE_RE.fullmatch(raw_line)
        if match is None:
            raise OverrideReleaseError(
                f"Invalid checksums.sha256 line {line_number}: {raw_line!r}"
            )
        checksum, relative = match.groups()
        relative_path = PurePosixPath(relative)
        if relative_path.is_absolute() or ".." in relative_path.parts:
            raise OverrideReleaseError(
                f"Unsafe checksums.sha256 path on line {line_number}: {relative!r}"
            )
        if relative in checksums:
            raise OverrideReleaseError(f"Duplicate checksums.sha256 path: {relative!r}")
        checksums[relative] = checksum
    return checksums


def _verify_release_files(
    release_dir: Path,
    *,
    archive_name: str,
) -> None:
    checksums = _release_checksums(release_dir)
    payload_files = {
        path.relative_to(release_dir).as_posix()
        for path in release_dir.rglob("*")
        if path.is_file()
        and path.relative_to(release_dir).as_posix()
        not in {archive_name, RECORD_RECEIPT, "checksums.sha256"}
    }
    expected_files = set(checksums)
    if payload_files != expected_files:
        missing = sorted(expected_files - payload_files)
        untracked = sorted(payload_files - expected_files)
        details: list[str] = []
        if missing:
            details.append("missing: " + ", ".join(missing))
        if untracked:
            details.append("not checksummed: " + ", ".join(untracked))
        raise OverrideReleaseError(
            "Override release contents do not match checksums.sha256 ("
            + "; ".join(details)
            + ")"
        )
    for relative, expected in checksums.items():
        actual = _hash_file(release_dir / relative, "sha256")
        if actual != expected:
            raise OverrideReleaseError(
                f"Checksum mismatch for override release file {relative!r}: "
                f"expected {expected}, got {actual}"
            )


def _validate_release_manifest(
    release_dir: Path,
    *,
    version_doi: str,
    record_version: str,
    source_paths: tuple[Path, ...],
    final_download_dir: Path,
) -> None:
    manifest_path = release_dir / "manifest.toml"
    try:
        with manifest_path.open("rb") as fp:
            manifest = tomllib.load(fp)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise OverrideReleaseError(
            "Override release manifest.toml is missing or invalid"
        ) from exc

    if manifest.get("format_version") != 1:
        raise OverrideReleaseError("Unsupported override manifest format_version")
    if manifest.get("series_id") != REQUIRED_SERIES_ID:
        raise OverrideReleaseError(
            f"Expected override series_id {REQUIRED_SERIES_ID!r}"
        )
    if manifest.get("release") != record_version:
        raise OverrideReleaseError(
            "Override manifest release does not match Zenodo record version"
        )

    lifecycle = manifest.get("lifecycle")
    if not isinstance(lifecycle, dict) or lifecycle.get("version_doi") != version_doi:
        raise OverrideReleaseError(
            "Override manifest does not identify the configured Version DOI"
        )

    outputs = manifest.get("outputs")
    if not isinstance(outputs, dict):
        raise OverrideReleaseError("Override manifest is missing [outputs]")
    catalog_members: list[str] = []
    for key in REQUIRED_CATALOG_OUTPUTS:
        value = outputs.get(key)
        if not isinstance(value, str) or not value:
            raise OverrideReleaseError(f"Override manifest is missing outputs.{key}")
        catalog_members.append(value)

    configured_sources = tuple(path.resolve() for path in source_paths)
    expected_sources = tuple(
        (final_download_dir / member).resolve() for member in catalog_members
    )
    if configured_sources != expected_sources:
        expected_text = ", ".join(str(path) for path in expected_sources)
        raise OverrideReleaseError(
            "overrides.source_paths must select the complete published catalog "
            f"in manifest order: {expected_text}"
        )
    for member in catalog_members:
        if not (release_dir / member).is_file():
            raise OverrideReleaseError(
                f"Override release is missing catalog member {member!r}"
            )

    consumer_contract = manifest.get("consumer_contract")
    if not isinstance(consumer_contract, dict) or not all(
        consumer_contract.get(key) is True
        for key in (
            "selection_must_be_explicit",
            "all_two_catalog_paths_required",
        )
    ):
        raise OverrideReleaseError(
            "Override manifest does not declare the required explicit "
            "two-catalog consumer contract"
        )


def validate_cached_override_release(
    download_dir: Path,
    *,
    version_doi: str,
    archive_name: str,
    archive_sha256: str,
    source_paths: tuple[Path, ...],
    record: dict[str, Any] | None = None,
) -> Path:
    """Validate a fully downloaded and extracted override release."""
    download_dir = Path(download_dir)
    if not download_dir.is_dir():
        raise OverrideReleaseError(
            f"Override download directory does not exist: {download_dir}"
        )
    archive_path = download_dir / archive_name
    if not archive_path.is_file():
        raise OverrideReleaseError(
            f"Override download is missing archive {archive_name!r}"
        )
    actual_archive_sha256 = _hash_file(archive_path, "sha256")
    if actual_archive_sha256 != archive_sha256:
        raise OverrideReleaseError(
            f"Override archive SHA-256 mismatch: expected {archive_sha256}, "
            f"got {actual_archive_sha256}"
        )

    if record is None:
        receipt_path = download_dir / RECORD_RECEIPT
        try:
            record = json.loads(receipt_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise OverrideReleaseError(
                f"Cached override release is missing a valid {RECORD_RECEIPT}"
            ) from exc
    if not isinstance(record, dict) or record.get("doi") != version_doi:
        raise OverrideReleaseError(
            "Cached Zenodo record receipt does not match the configured Version DOI"
        )
    metadata = record.get("metadata")
    if not isinstance(metadata, dict) or not isinstance(metadata.get("version"), str):
        raise OverrideReleaseError(
            "Cached Zenodo record receipt is missing its release version"
        )

    _verify_release_files(download_dir, archive_name=archive_name)
    _validate_release_manifest(
        download_dir,
        version_doi=version_doi,
        record_version=metadata["version"],
        source_paths=source_paths,
        final_download_dir=download_dir,
    )
    return download_dir


def _replace_directory(staged: Path, target: Path) -> None:
    if not target.exists():
        os.replace(staged, target)
        return
    if not target.is_dir():
        raise OverrideReleaseError(
            f"Override download target exists but is not a directory: {target}"
        )

    backup = target.with_name(f".{target.name}.backup-{uuid.uuid4().hex}")
    os.replace(target, backup)
    try:
        os.replace(staged, target)
    except BaseException:
        os.replace(backup, target)
        raise
    else:
        shutil.rmtree(backup)


def ensure_zenodo_override_release(
    download_dir: Path,
    *,
    version_doi: str,
    archive_name: str,
    archive_sha256: str,
    source_paths: tuple[Path, ...],
    force: bool = False,
) -> Path:
    """Download, verify, and extract one exact Zenodo override release."""
    download_dir = Path(download_dir).expanduser()
    resolved_download_dir = download_dir.resolve()
    filesystem_root = Path(resolved_download_dir.anchor)
    if resolved_download_dir in {filesystem_root, Path.home().resolve()}:
        raise OverrideReleaseError(
            f"Refusing to use a broad override download target: {download_dir}"
        )
    if (resolved_download_dir / ".git").exists():
        raise OverrideReleaseError(
            f"Refusing to replace a Git worktree as an override cache: {download_dir}"
        )
    archive_sha256 = archive_sha256.lower()

    if download_dir.exists() and not force:
        try:
            return validate_cached_override_release(
                download_dir,
                version_doi=version_doi,
                archive_name=archive_name,
                archive_sha256=archive_sha256,
                source_paths=source_paths,
            )
        except OverrideReleaseError as exc:
            raise OverrideReleaseError(
                f"Existing override download is invalid: {exc}. "
                "Use --force to reacquire it."
            ) from exc

    record, artifact = _validated_record(
        version_doi=version_doi,
        archive_name=archive_name,
    )
    content_url = artifact["links"]["self"]

    download_dir.parent.mkdir(parents=True, exist_ok=True)
    staged = Path(
        tempfile.mkdtemp(
            prefix=f".{download_dir.name}.download-",
            dir=download_dir.parent,
        )
    )
    try:
        archive_path = staged / archive_name
        _download_url(content_url, archive_path)
        actual_sha256 = _hash_file(archive_path, "sha256")
        if actual_sha256 != archive_sha256:
            raise OverrideReleaseError(
                f"Downloaded override archive SHA-256 mismatch: expected "
                f"{archive_sha256}, got {actual_sha256}"
            )

        zenodo_checksum = artifact.get("checksum")
        if isinstance(zenodo_checksum, str) and zenodo_checksum.startswith("md5:"):
            expected_md5 = zenodo_checksum.removeprefix("md5:")
            actual_md5 = _hash_file(archive_path, "md5")
            if actual_md5 != expected_md5:
                raise OverrideReleaseError(
                    f"Downloaded override archive does not match Zenodo's "
                    f"checksum: expected {expected_md5}, got {actual_md5}"
                )

        _safe_extract_zip(archive_path, staged)
        _verify_release_files(staged, archive_name=archive_name)
        metadata = record["metadata"]
        _validate_release_manifest(
            staged,
            version_doi=version_doi,
            record_version=metadata["version"],
            source_paths=source_paths,
            final_download_dir=download_dir,
        )
        (staged / RECORD_RECEIPT).write_text(
            json.dumps(record, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        _replace_directory(staged, download_dir)
    finally:
        if staged.exists():
            shutil.rmtree(staged)
    return download_dir
