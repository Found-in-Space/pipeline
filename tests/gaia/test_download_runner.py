from __future__ import annotations

import gzip
from pathlib import Path

from click.testing import CliRunner

import foundinspace.pipeline.gaia.download.runner as runner_module
from foundinspace.pipeline.gaia.download.archive import ArchiveJobInfo
from foundinspace.pipeline.gaia.download.cli import cli as download_cli
from foundinspace.pipeline.gaia.download.planner import HealpixCount
from foundinspace.pipeline.gaia.download.runner import (
    ANONYMOUS_QUOTA_ERROR,
    _friendly_archive_error,
    plan_gaia_download,
    run_gaia_download,
    write_browser_queries,
)
from foundinspace.pipeline.gaia.download.state import GaiaDownloadState
from foundinspace.pipeline.project import load_project


class _FakeArchive:
    def __init__(self) -> None:
        self.submitted: list[str] = []
        self.deleted: list[str] = []

    def run_count_query(self, query: str, *, access_mode: str, job_name: str):
        assert "COUNT(*) AS n" in query
        return [HealpixCount(5, 100), HealpixCount(4, 200)]

    def submit_download_job(self, query: str, *, job_name: str, access_mode: str):
        assert "SELECT" in query
        job_id = f"job-{len(self.submitted) + 1}"
        self.submitted.append(job_id)
        return ArchiveJobInfo(
            job_id=job_id, job_url=f"https://example.test/{job_id}", phase="PENDING"
        )

    def poll_phase(self, job_id: str, *, access_mode: str):
        return "COMPLETED"

    def job_error(self, job_id: str, *, access_mode: str):
        return "failed"

    def download_result(self, job_id: str, output_path: Path, *, access_mode: str):
        with gzip.open(output_path, "wb") as fp:
            fp.write(b'<?xml version="1.0"?><VOTABLE></VOTABLE>')

    def delete_job(self, job_id: str, *, access_mode: str):
        self.deleted.append(job_id)


class _SlowArchive(_FakeArchive):
    def __init__(self) -> None:
        super().__init__()
        self.phases = ["EXECUTING", "EXECUTING", "COMPLETED"]

    def poll_phase(self, job_id: str, *, access_mode: str):
        return self.phases.pop(0)


class _DeleteFailsArchive(_FakeArchive):
    def delete_job(self, job_id: str, *, access_mode: str):
        self.deleted.append(job_id)
        raise RuntimeError("temporary delete failure")


def _write_project(
    tmp_path: Path, *, mode: str = "small", access: str = "auto"
) -> Path:
    project_path = tmp_path / "project.toml"
    mag_limit = "mag_limit = 9.0\n" if mode == "small" else ""
    project_path.write_text(
        f"""
format_version = 1

[gaia]
input_dir = "data/catalogs/gaia"
output_dir = "data/processed/gaia"
{mag_limit}

[gaia_download]
mode = "{mode}"
access = "{access}"
{mag_limit}state_db = "data/processed/gaia-download.sqlite"
row_cap = 1000
max_active_jobs = 2
carry_field_sets = ["motion", "mass"]

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
""",
        encoding="utf-8",
    )
    return project_path


def test_plan_and_run_small_anonymous_download_with_fake_archive(
    tmp_path: Path,
) -> None:
    project = load_project(_write_project(tmp_path))
    archive = _FakeArchive()

    plan = plan_gaia_download(project, archive_client=archive, echo=lambda _msg: None)
    run = run_gaia_download(
        project,
        archive_client=archive,
        poll_seconds=0,
        echo=lambda _msg: None,
    )

    assert plan.access_mode == "anonymous"
    assert plan.batch_count == 1
    assert len(archive.submitted) == 1
    assert run.downloaded_batches == 1
    assert sorted(project.gaia.input_dir.glob("*.vot.gz"))


def test_authenticated_run_deletes_remote_job_after_download(tmp_path: Path) -> None:
    project = load_project(
        _write_project(tmp_path, mode="full", access="authenticated")
    )
    archive = _FakeArchive()

    plan = plan_gaia_download(project, archive_client=archive, echo=lambda _msg: None)
    run = run_gaia_download(
        project,
        archive_client=archive,
        poll_seconds=0,
        echo=lambda _msg: None,
    )

    assert plan.access_mode == "authenticated"
    assert run.deleted_remote_batches == 1
    assert archive.deleted == ["job-1"]


def test_authenticated_delete_pending_is_retried_on_resume(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project = load_project(
        _write_project(tmp_path, mode="full", access="authenticated")
    )
    failing_archive = _DeleteFailsArchive()
    messages: list[str] = []

    def interrupt_sleep(_seconds: float) -> None:
        raise KeyboardInterrupt

    plan_gaia_download(project, archive_client=failing_archive, echo=lambda _msg: None)
    monkeypatch.setattr(runner_module.time, "sleep", interrupt_sleep)
    try:
        run_gaia_download(
            project,
            archive_client=failing_archive,
            poll_seconds=30,
            echo=messages.append,
        )
    except KeyboardInterrupt:
        pass
    else:  # pragma: no cover - protects against accidentally losing the interrupt path
        raise AssertionError("Expected delete-pending run to be interrupted")

    with GaiaDownloadState(project.gaia_download.state_db) as state:
        batches = state.read_batches()
    assert batches[0].state == "delete_pending"
    assert batches[0].job_id == "job-1"
    assert any("remote delete pending" in message for message in messages)

    resume_archive = _FakeArchive()
    summary = run_gaia_download(
        project,
        archive_client=resume_archive,
        poll_seconds=0,
        echo=lambda _msg: None,
    )

    assert summary.deleted_remote_batches == 1
    assert resume_archive.deleted == ["job-1"]
    assert resume_archive.submitted == []


def test_run_echoes_wait_status_when_poll_phase_is_unchanged(tmp_path: Path) -> None:
    project = load_project(_write_project(tmp_path))
    archive = _SlowArchive()
    messages: list[str] = []

    plan_gaia_download(project, archive_client=archive, echo=lambda _msg: None)
    run_gaia_download(
        project,
        archive_client=archive,
        poll_seconds=0,
        echo=messages.append,
    )

    assert any("phase=EXECUTING" in message for message in messages)
    assert any("waiting 0s before next poll" in message for message in messages)
    assert any("job_id=job-1" in message for message in messages)


def test_write_browser_queries_for_small_profile(tmp_path: Path) -> None:
    project = load_project(_write_project(tmp_path))

    summary = write_browser_queries(project, echo=lambda _msg: None)

    assert summary.count_query_path.is_file()
    assert summary.download_query_path.is_file()
    assert summary.gaia_input_dir == project.gaia.input_dir
    assert len(summary.download_query_hash) == 64

    download_query = summary.download_query_path.read_text(encoding="utf-8")
    count_query = summary.count_query_path.read_text(encoding="utf-8")
    assert "g.phot_g_mean_mag <= 9" in download_query
    assert "(g.source_id / 9007199254740992) IN" not in download_query
    assert "COUNT(*) AS n" in count_query


def test_browser_queries_cli_writes_adql_files(tmp_path: Path) -> None:
    project_path = _write_project(tmp_path)
    runner = CliRunner()

    result = runner.invoke(download_cli, ["queries", "--project", str(project_path)])

    assert result.exit_code == 0, result.output
    assert "download.adql" in result.output
    query_dir = tmp_path / "data/processed/gaia-download-browser-queries"
    assert (query_dir / "count.adql").is_file()
    assert (query_dir / "download.adql").is_file()


def test_anonymous_quota_error_gets_actionable_advice() -> None:
    error = _friendly_archive_error(
        f"Code: -1, msg: {ANONYMOUS_QUOTA_ERROR} (Currently using 19 GB)",
        access_mode="anonymous",
    )

    assert "anonymous async storage is currently full" in error
    assert "GAIA_USER/GAIA_PASS" in error
    assert (
        _friendly_archive_error(ANONYMOUS_QUOTA_ERROR, access_mode="authenticated")
        == ANONYMOUS_QUOTA_ERROR
    )
