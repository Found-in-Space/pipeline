from pathlib import Path

import click

from foundinspace.pipeline.cli_support import (
    load_project_or_die,
    project_option,
    record_command_or_die,
    require_input_file,
)
from foundinspace.pipeline.overrides.download import (
    ensure_zenodo_override_release,
    validate_cached_override_release,
)
from foundinspace.pipeline.overrides.pipeline import prepare_overrides_parquet


@click.group(name="overrides")
def cli():
    """Manual override YAML → processed Parquet for merger."""


@cli.command(name="download")
@project_option
@click.option(
    "--force",
    "-f",
    is_flag=True,
    default=False,
    help="Re-download and replace the cached release after full verification.",
)
def download(project_path: Path, force: bool) -> None:
    """Acquire the configured version-pinned Zenodo override release."""
    project = load_project_or_die(project_path, "overrides")
    overrides = project.overrides
    if not overrides.zenodo_configured:
        raise click.ClickException(
            "[overrides] does not configure a Zenodo Version DOI release"
        )

    version_doi = overrides.zenodo_version_doi
    archive_name = overrides.zenodo_archive
    archive_sha256 = overrides.zenodo_archive_sha256
    download_dir = overrides.zenodo_download_dir
    source_paths = overrides.source_paths
    if (
        version_doi is None
        or archive_name is None
        or archive_sha256 is None
        or download_dir is None
        or source_paths is None
    ):
        raise click.ClickException(
            "[overrides] Zenodo acquisition requires a Version DOI, archive, "
            "SHA-256, download directory, and explicit source_paths"
        )

    try:
        with record_command_or_die(
            project,
            command="overrides download",
            parameters={
                "force": force,
                "zenodo_version_doi": version_doi,
                "zenodo_archive": archive_name,
                "zenodo_archive_sha256": archive_sha256,
            },
            outputs={"override_release": download_dir},
        ):
            out = ensure_zenodo_override_release(
                download_dir,
                version_doi=version_doi,
                archive_name=archive_name,
                archive_sha256=archive_sha256,
                source_paths=source_paths,
                force=force,
            )
    except (OSError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Override release {version_doi} ready at {out.resolve()}")


@cli.command(name="build")
@project_option
@click.option("--force", "-f", is_flag=True, default=False)
def build(project_path: Path, force: bool) -> None:
    """Write OUTPUT_COLS + override metadata to Parquet (zstd)."""
    project = load_project_or_die(project_path, "overrides")
    overrides = project.overrides
    data_dir = overrides.data_dir
    source_paths = overrides.source_paths
    inputs: dict[str, Path | tuple[Path, ...]] = {}
    if data_dir is not None:
        inputs["override_data_dir"] = data_dir
    if source_paths is not None:
        inputs["override_sources"] = source_paths
    if overrides.zenodo_download_dir is not None:
        inputs["override_release"] = overrides.zenodo_download_dir
    try:
        with record_command_or_die(
            project,
            command="overrides build",
            parameters={"force": force},
            inputs=inputs,
            outputs={"overrides_parquet": overrides.output_parquet},
        ):
            if data_dir is not None and not data_dir.is_dir():
                raise click.ClickException(
                    f"[overrides] data_dir does not exist: {data_dir}"
                )
            if source_paths is not None:
                for source_path in source_paths:
                    require_input_file(
                        source_path,
                        label="[overrides] source_paths entry",
                    )
            if overrides.zenodo_configured:
                version_doi = overrides.zenodo_version_doi
                archive_name = overrides.zenodo_archive
                archive_sha256 = overrides.zenodo_archive_sha256
                download_dir = overrides.zenodo_download_dir
                if (
                    version_doi is None
                    or archive_name is None
                    or archive_sha256 is None
                    or download_dir is None
                    or source_paths is None
                ):
                    raise ValueError("[overrides] has an incomplete Zenodo release pin")
                validate_cached_override_release(
                    download_dir,
                    version_doi=version_doi,
                    archive_name=archive_name,
                    archive_sha256=archive_sha256,
                    source_paths=source_paths,
                )
            out = prepare_overrides_parquet(
                overrides.output_parquet,
                data_dir=data_dir,
                source_paths=source_paths,
                overwrite=force,
            )
    except (OSError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Wrote overrides table to {out.resolve()}")
