from __future__ import annotations

import gzip
from pathlib import Path

from click.testing import CliRunner

import foundinspace.pipeline.gaia.download.runner as runner_module
from foundinspace.pipeline.gaia.download.archive import (
    ArchiveJobInfo,
    RemoteStorageInfo,
)
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
        self.count_submitted: list[str] = []
        self.submitted: list[str] = []
        self.deleted: list[str] = []
        self.remote_storage = RemoteStorageInfo(
            known_result_bytes=0,
            known_result_job_ids=frozenset(),
            unknown_result_job_ids=(),
            active_job_ids=(),
            failed_job_ids=(),
        )

    def submit_count_job(self, query: str, *, access_mode: str, job_name: str):
        assert "COUNT(*) AS n" in query
        job_id = f"count-job-{len(self.count_submitted) + 1}"
        self.count_submitted.append(job_id)
        return ArchiveJobInfo(
            job_id=job_id, job_url=f"https://example.test/{job_id}", phase="PENDING"
        )

    def fetch_count_results(self, job_id: str, *, access_mode: str):
        assert job_id.startswith("count-job-")
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

    def remote_async_storage(self, *, access_mode: str):
        return self.remote_storage


class _SizedCountsArchive(_FakeArchive):
    def __init__(self, counts: list[HealpixCount]) -> None:
        super().__init__()
        self.counts = counts

    def fetch_count_results(self, job_id: str, *, access_mode: str):
        assert job_id.startswith("count-job-")
        return self.counts


class _SlowArchive(_FakeArchive):
    def __init__(self) -> None:
        super().__init__()
        self.phases = ["EXECUTING", "EXECUTING", "COMPLETED"]

    def poll_phase(self, job_id: str, *, access_mode: str):
        if job_id.startswith("count-job-"):
            return "COMPLETED"
        return self.phases.pop(0)


class _PollFailsThenCompletesArchive(_FakeArchive):
    def __init__(self) -> None:
        super().__init__()
        self.polls = 0

    def poll_phase(self, job_id: str, *, access_mode: str):
        if job_id.startswith("count-job-"):
            return "COMPLETED"
        self.polls += 1
        if self.polls == 1:
            raise RuntimeError("temporary 503")
        return "COMPLETED"


class _DeleteFailsArchive(_FakeArchive):
    def delete_job(self, job_id: str, *, access_mode: str):
        self.deleted.append(job_id)
        raise RuntimeError("temporary delete failure")


class _ErrorArchive(_FakeArchive):
    def poll_phase(self, job_id: str, *, access_mode: str):
        if job_id.startswith("count-job-"):
            return "COMPLETED"
        return "ERROR"

    def job_error(self, job_id: str, *, access_mode: str):
        return "archive failed"


class _TransientJobErrorThenCompletesArchive(_FakeArchive):
    def poll_phase(self, job_id: str, *, access_mode: str):
        if job_id.startswith("count-job-"):
            return "COMPLETED"
        if job_id == "job-1":
            return "ERROR"
        return "COMPLETED"

    def job_error(self, job_id: str, *, access_mode: str):
        return "Could not open JPA EntityManager for transaction"


def _write_project(
    tmp_path: Path,
    *,
    mode: str = "small",
    access: str = "auto",
    source_where_adql: str | None = None,
    remote_quota_bytes: int | None = None,
    remote_result_bytes_per_row: int | None = None,
    remote_max_result_bytes_per_job: int | None = None,
) -> Path:
    project_path = tmp_path / "project.toml"
    mag_limit = "mag_limit = 9.0\n" if mode == "small" else ""
    source_where = (
        f'source_where_adql = """\n{source_where_adql}\n"""\n'
        if source_where_adql is not None
        else ""
    )
    remote_quota = (
        f"remote_quota_bytes = {remote_quota_bytes}\n"
        if remote_quota_bytes is not None
        else ""
    )
    remote_result_bytes = (
        f"remote_result_bytes_per_row = {remote_result_bytes_per_row}\n"
        if remote_result_bytes_per_row is not None
        else ""
    )
    remote_max_result = (
        f"remote_max_result_bytes_per_job = {remote_max_result_bytes_per_job}\n"
        if remote_max_result_bytes_per_job is not None
        else ""
    )
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
{remote_quota}{remote_result_bytes}{remote_max_result}remote_min_free_bytes = 5000000000
{source_where}\
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

    plan = plan_gaia_download(
        project,
        archive_client=archive,
        poll_seconds=0,
        echo=lambda _msg: None,
    )
    run = run_gaia_download(
        project,
        archive_client=archive,
        poll_seconds=0,
        echo=lambda _msg: None,
    )

    assert plan.access_mode == "anonymous"
    assert plan.batch_count == 1
    assert archive.count_submitted == ["count-job-1"]
    assert len(archive.submitted) == 1
    assert run.downloaded_batches == 1
    assert sorted(project.gaia.input_dir.glob("*.vot.gz"))


def test_authenticated_run_deletes_remote_job_after_download(tmp_path: Path) -> None:
    project = load_project(
        _write_project(tmp_path, mode="full", access="authenticated")
    )
    archive = _FakeArchive()

    plan = plan_gaia_download(
        project,
        archive_client=archive,
        poll_seconds=0,
        echo=lambda _msg: None,
    )
    run = run_gaia_download(
        project,
        archive_client=archive,
        poll_seconds=0,
        echo=lambda _msg: None,
    )

    assert plan.access_mode == "authenticated"
    assert run.deleted_remote_batches == 1
    assert archive.deleted == ["count-job-1", "job-1"]


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

    plan_gaia_download(
        project,
        archive_client=failing_archive,
        poll_seconds=0,
        echo=lambda _msg: None,
    )
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


def test_authenticated_error_job_is_deleted_before_terminal(
    tmp_path: Path,
) -> None:
    project = load_project(
        _write_project(tmp_path, mode="full", access="authenticated")
    )
    archive = _ErrorArchive()
    messages: list[str] = []

    plan_gaia_download(
        project,
        archive_client=archive,
        poll_seconds=0,
        echo=lambda _msg: None,
    )
    summary = run_gaia_download(
        project,
        archive_client=archive,
        poll_seconds=0,
        echo=messages.append,
    )

    assert summary.failed_batches == 1
    assert archive.deleted == ["count-job-1", "job-1"]
    assert any("deleted failed remote job job-1" in message for message in messages)
    with GaiaDownloadState(project.gaia_download.state_db) as state:
        batch = state.read_batches()[0]
    assert batch.state == "failed"
    assert batch.remote_deleted_at is not None


def test_transient_archive_error_is_requeued_after_remote_delete(
    tmp_path: Path,
) -> None:
    project = load_project(
        _write_project(tmp_path, mode="full", access="authenticated")
    )
    archive = _TransientJobErrorThenCompletesArchive()
    messages: list[str] = []

    plan_gaia_download(
        project,
        archive_client=archive,
        poll_seconds=0,
        echo=lambda _msg: None,
    )
    summary = run_gaia_download(
        project,
        archive_client=archive,
        poll_seconds=0,
        echo=messages.append,
    )

    assert summary.failed_batches == 0
    assert summary.deleted_remote_batches == 1
    assert archive.submitted == ["job-1", "job-2"]
    assert archive.deleted == ["count-job-1", "job-1", "job-2"]
    assert any("retrying transient archive failure" in message for message in messages)
    with GaiaDownloadState(project.gaia_download.state_db) as state:
        batch = state.read_batches()[0]
    assert batch.state == "deleted_remote"
    assert batch.retry_count == 1


def test_authenticated_remote_quota_guard_blocks_insufficient_quota(
    tmp_path: Path,
) -> None:
    project = load_project(
        _write_project(
            tmp_path,
            mode="full",
            access="authenticated",
            remote_quota_bytes=5_000_010_000,
        )
    )
    archive = _FakeArchive()

    plan_gaia_download(
        project,
        archive_client=archive,
        poll_seconds=0,
        echo=lambda _msg: None,
    )

    try:
        run_gaia_download(
            project,
            archive_client=archive,
            poll_seconds=0,
            echo=lambda _msg: None,
        )
    except ValueError as exc:
        assert "Gaia remote quota guard blocked submission" in str(exc)
        assert "required free 5.00 GB" in str(exc)
    else:  # pragma: no cover - protects the guard against accidental removal
        raise AssertionError("Expected remote quota guard to block submission")
    assert archive.submitted == []


def test_authenticated_plan_caps_batches_by_estimated_remote_result_size(
    tmp_path: Path,
) -> None:
    project = load_project(
        _write_project(
            tmp_path,
            mode="full",
            access="authenticated",
            remote_result_bytes_per_row=100,
            remote_max_result_bytes_per_job=1_000,
        )
    )
    archive = _SizedCountsArchive(
        [HealpixCount(1, 6), HealpixCount(2, 5), HealpixCount(3, 4)]
    )

    summary = plan_gaia_download(
        project,
        archive_client=archive,
        poll_seconds=0,
        echo=lambda _msg: None,
    )

    assert summary.batch_count == 2
    with GaiaDownloadState(project.gaia_download.state_db) as state:
        batches = state.read_batches()
    assert [batch.expected_rows for batch in batches] == [10, 5]
    assert [batch.estimated_result_bytes for batch in batches] == [1_000, 500]


def test_authenticated_remote_quota_guard_blocks_external_active_jobs(
    tmp_path: Path,
) -> None:
    project = load_project(
        _write_project(tmp_path, mode="full", access="authenticated")
    )
    archive = _FakeArchive()
    archive.remote_storage = RemoteStorageInfo(
        known_result_bytes=0,
        known_result_job_ids=frozenset(),
        unknown_result_job_ids=(),
        active_job_ids=("external-running",),
        failed_job_ids=(),
    )

    plan_gaia_download(
        project,
        archive_client=archive,
        poll_seconds=0,
        echo=lambda _msg: None,
    )

    try:
        run_gaia_download(
            project,
            archive_client=archive,
            poll_seconds=0,
            echo=lambda _msg: None,
        )
    except ValueError as exc:
        assert "active jobs outside this download state" in str(exc)
        assert "external-running" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected external active jobs to block submission")
    assert archive.submitted == []


def test_authenticated_remote_quota_guard_blocks_failed_remote_jobs(
    tmp_path: Path,
) -> None:
    project = load_project(
        _write_project(tmp_path, mode="full", access="authenticated")
    )
    archive = _FakeArchive()
    archive.remote_storage = RemoteStorageInfo(
        known_result_bytes=0,
        known_result_job_ids=frozenset(),
        unknown_result_job_ids=(),
        active_job_ids=(),
        failed_job_ids=("external-failed",),
    )

    plan_gaia_download(
        project,
        archive_client=archive,
        poll_seconds=0,
        echo=lambda _msg: None,
    )

    try:
        run_gaia_download(
            project,
            archive_client=archive,
            poll_seconds=0,
            echo=lambda _msg: None,
        )
    except ValueError as exc:
        assert "unknown storage remain" in str(exc)
        assert "external-failed" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected failed remote jobs to block submission")
    assert archive.submitted == []


def test_run_echoes_wait_status_when_poll_phase_is_unchanged(tmp_path: Path) -> None:
    project = load_project(_write_project(tmp_path))
    archive = _SlowArchive()
    messages: list[str] = []

    plan_gaia_download(
        project,
        archive_client=archive,
        poll_seconds=0,
        echo=lambda _msg: None,
    )
    run_gaia_download(
        project,
        archive_client=archive,
        poll_seconds=0,
        echo=messages.append,
    )

    assert any("phase=EXECUTING" in message for message in messages)
    assert any("waiting 0s before next poll" in message for message in messages)
    assert any("job_id=job-1" in message for message in messages)


def test_poll_failure_is_retried_without_failing_batch(tmp_path: Path) -> None:
    project = load_project(
        _write_project(tmp_path, mode="full", access="authenticated")
    )
    archive = _PollFailsThenCompletesArchive()
    messages: list[str] = []

    plan_gaia_download(
        project,
        archive_client=archive,
        poll_seconds=0,
        echo=lambda _msg: None,
    )
    summary = run_gaia_download(
        project,
        archive_client=archive,
        poll_seconds=0,
        echo=messages.append,
    )

    assert summary.deleted_remote_batches == 1
    assert summary.failed_batches == 0
    assert any("poll failed; will retry" in message for message in messages)
    assert archive.deleted == ["count-job-1", "job-1"]


def test_plan_resumes_background_count_job(tmp_path: Path) -> None:
    project = load_project(_write_project(tmp_path))
    archive = _FakeArchive()
    messages: list[str] = []

    with GaiaDownloadState(project.gaia_download.state_db) as state:
        state.set_metadata("count_query_hash", "stale")
        state.set_metadata("count_job_id", "count-job-stale")

    first = plan_gaia_download(
        project,
        archive_client=archive,
        poll_seconds=0,
        echo=messages.append,
    )
    assert first.batch_count == 1
    assert archive.count_submitted == ["count-job-1"]

    second = plan_gaia_download(
        project,
        archive_client=archive,
        poll_seconds=0,
        echo=messages.append,
    )
    assert second.batch_count == 1
    assert archive.count_submitted == ["count-job-1"]
    assert any("Reusing Gaia HEALPix count table" in message for message in messages)


def test_write_browser_queries_for_small_profile(tmp_path: Path) -> None:
    project = load_project(
        _write_project(
            tmp_path,
            source_where_adql=(
                "g.phot_g_mean_mag <= 15\n"
                "OR d.r_med_photogeo <= 100\n"
                "OR d.r_med_geo <= 100"
            ),
        )
    )

    summary = write_browser_queries(project, echo=lambda _msg: None)

    assert summary.count_query_path.is_file()
    assert summary.download_query_path.is_file()
    assert summary.gaia_input_dir == project.gaia.input_dir
    assert len(summary.download_query_hash) == 64

    download_query = summary.download_query_path.read_text(encoding="utf-8")
    count_query = summary.count_query_path.read_text(encoding="utf-8")
    assert "g.phot_g_mean_mag <= 9" in download_query
    assert "OR d.r_med_photogeo <= 100" in download_query
    assert "OR d.r_med_geo <= 100" in download_query
    assert "OR d.r_med_photogeo <= 100" in count_query
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
