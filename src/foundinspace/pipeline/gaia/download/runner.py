from __future__ import annotations

import gzip
import re
import shutil
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path

from foundinspace.pipeline.gaia.download.archive import GaiaArchiveClient
from foundinspace.pipeline.gaia.download.fieldsets import load_gaia_field_sets
from foundinspace.pipeline.gaia.download.planner import (
    ANONYMOUS_ROW_CAP,
    DownloadBatch,
    HealpixCount,
    plan_download_batches,
)
from foundinspace.pipeline.gaia.download.query import (
    GaiaQuerySpec,
    build_count_query,
    build_download_query,
    query_hash,
)
from foundinspace.pipeline.gaia.download.state import BatchState, GaiaDownloadState
from foundinspace.pipeline.project import PipelineProject

Echo = Callable[[str], None]

ANONYMOUS_QUOTA_ERROR = "Filesystem quota exceeded for user anonymous"
ACQUISITION_SPEC_HASH_METADATA_KEY = "acquisition_spec_hash"
DOWNLOAD_QUERY_HASH_METADATA_KEY = "download_query_hash"
DOWNLOADER_OUTPUT_RE = re.compile(r"^gaia_(?:small|full)_b\d{4}\.vot\.gz$")
REMOTE_GB = 1_000_000_000
MAX_TRANSIENT_BATCH_RETRIES = 3
TRANSIENT_ARCHIVE_ERROR_MARKERS = (
    "503 Service Unavailable",
    "temporarily unable to service",
    "maintenance downtime",
    "capacity problems",
    "Could not open JPA EntityManager",
    "Connection reset",
    "Read timed out",
)


@dataclass(frozen=True, slots=True)
class PlanSummary:
    state_db: Path
    count_query_hash: str
    total_rows: int
    access_mode: str
    batch_count: int
    over_cap_batches: int


@dataclass(frozen=True, slots=True)
class BrowserQuerySummary:
    query_dir: Path
    count_query_path: Path
    download_query_path: Path
    gaia_input_dir: Path
    count_query_hash: str
    download_query_hash: str


@dataclass(frozen=True, slots=True)
class RunSummary:
    state_db: Path
    downloaded_batches: int
    deleted_remote_batches: int
    failed_batches: int


def _require_gaia_download(project: PipelineProject) -> None:
    if not project.gaia_download.configured:
        raise ValueError("Missing [gaia_download] table in project file")


def query_spec_from_project(project: PipelineProject) -> GaiaQuerySpec:
    _require_gaia_download(project)
    mode = project.gaia_download.mode
    download_mag_limit = project.gaia_download.mag_limit
    if mode == "small" and download_mag_limit is None:
        raise ValueError("gaia_download.mag_limit is required when mode = 'small'")
    if mode == "full" and download_mag_limit is not None:
        raise ValueError("gaia_download.mag_limit must be omitted when mode = 'full'")
    if (
        download_mag_limit is not None
        and project.gaia.mag_limit is not None
        and abs(project.gaia.mag_limit - download_mag_limit) > 1e-9
    ):
        raise ValueError(
            "gaia.mag_limit must match gaia_download.mag_limit or be omitted"
        )
    fields = load_gaia_field_sets(project.gaia_download.carry_field_sets)
    return GaiaQuerySpec(
        mode=mode,
        mag_limit=download_mag_limit,
        carry_fields=fields,
        source_where_adql=project.gaia_download.source_where_adql,
        calculation_engine=project.gaia.calculation_engine,
    )


def _count_access_mode(project: PipelineProject) -> str:
    access = project.gaia_download.access
    if access == "authenticated" or project.gaia_download.mode == "full":
        return "authenticated"
    return "anonymous"


def _resolve_access_mode(project: PipelineProject, total_rows: int) -> str:
    access = project.gaia_download.access
    mode = project.gaia_download.mode
    if mode == "full":
        if access == "anonymous":
            raise ValueError(
                "gaia_download.access = 'anonymous' is not valid for full mode"
            )
        return "authenticated"
    if access == "authenticated":
        return "authenticated"
    if access == "anonymous":
        if total_rows > ANONYMOUS_ROW_CAP:
            raise ValueError(
                f"Anonymous Gaia downloads are capped at {ANONYMOUS_ROW_CAP:,} rows; "
                f"this plan has {total_rows:,}"
            )
        return "anonymous"
    return "anonymous" if total_rows <= ANONYMOUS_ROW_CAP else "authenticated"


def _query_dir(state_db: Path) -> Path:
    return state_db.parent / f"{state_db.stem}-queries"


def _browser_query_dir(state_db: Path) -> Path:
    return state_db.parent / f"{state_db.stem}-browser-queries"


def write_browser_queries(
    project: PipelineProject,
    *,
    output_dir: Path | None = None,
    echo: Echo = print,
) -> BrowserQuerySummary:
    spec = query_spec_from_project(project)
    if project.gaia_download.mode != "small":
        raise ValueError(
            "Browser query export is only supported for small Gaia profiles"
        )

    query_dir = output_dir or _browser_query_dir(project.gaia_download.state_db)
    query_dir.mkdir(parents=True, exist_ok=True)
    count_query = build_count_query(spec)
    download_query = build_download_query(spec)
    count_query_path = query_dir / "count.adql"
    download_query_path = query_dir / "download.adql"
    count_query_path.write_text(count_query + "\n", encoding="utf-8")
    download_query_path.write_text(download_query + "\n", encoding="utf-8")

    echo(f"Wrote Gaia browser count query: {count_query_path}")
    echo(f"Wrote Gaia browser download query: {download_query_path}")
    echo(f"Save the Gaia result as VOTable gzip under: {project.gaia.input_dir}")
    return BrowserQuerySummary(
        query_dir=query_dir,
        count_query_path=count_query_path,
        download_query_path=download_query_path,
        gaia_input_dir=project.gaia.input_dir,
        count_query_hash=query_hash(count_query),
        download_query_hash=query_hash(download_query),
    )


def _output_path_for_batch(project: PipelineProject, batch: DownloadBatch) -> Path:
    return (
        project.gaia.input_dir
        / f"gaia_{project.gaia_download.mode}_{batch.batch_id}.vot.gz"
    )


def _job_name(project: PipelineProject, batch: DownloadBatch, batch_hash: str) -> str:
    return f"fis-gaia-{project.gaia_download.mode}-{batch.batch_id}-{batch_hash[:8]}"


def is_downloader_output_file(path: Path) -> bool:
    return DOWNLOADER_OUTPUT_RE.fullmatch(path.name.lower()) is not None


def downloader_output_files(input_dir: Path) -> list[Path]:
    if not input_dir.is_dir():
        return []
    return sorted(
        path
        for path in input_dir.iterdir()
        if path.is_file() and is_downloader_output_file(path)
    )


def _acquisition_spec_hash(count_query: str, download_query: str) -> str:
    return query_hash(
        "gaia-download-acquisition-v1\n"
        "-- count query --\n"
        f"{count_query}\n"
        "-- download query --\n"
        f"{download_query}\n"
    )


def _acquisition_hash_for_project(project: PipelineProject) -> tuple[str, str, str]:
    spec = query_spec_from_project(project)
    count_query = build_count_query(spec)
    download_query = build_download_query(spec)
    return (
        count_query,
        download_query,
        _acquisition_spec_hash(count_query, download_query),
    )


def gaia_download_input_files_for_build(project: PipelineProject) -> list[Path] | None:
    """Return current downloader outputs when populated state should drive build."""
    if (
        not project.gaia_download.configured
        or not project.gaia_download.state_db.exists()
    ):
        return None

    with GaiaDownloadState(project.gaia_download.state_db) as state:
        batches = state.read_batches()
        if not batches:
            return None
        _require_current_acquisition_spec(state, project)
        incomplete = [
            f"{batch.batch_id} state={batch.state}"
            for batch in batches
            if batch.state not in {"downloaded", "deleted_remote"}
        ]
        if incomplete:
            sample = "; ".join(incomplete[:5])
            suffix = "" if len(incomplete) <= 5 else "; ..."
            raise ValueError(
                "Gaia download state is not complete; cannot build from "
                f"{state.path}: {sample}{suffix}"
            )
        expected_paths = [batch.output_path for batch in batches]

    expected_set = set(expected_paths)
    extras = [
        path
        for path in downloader_output_files(project.gaia.input_dir)
        if path not in expected_set
    ]
    if extras:
        sample = ", ".join(str(path) for path in extras[:5])
        suffix = "" if len(extras) <= 5 else ", ..."
        raise ValueError(
            "Found Gaia downloader-shaped VOTable files that are not in the "
            f"current download state: {sample}{suffix}. Use a fresh input_dir "
            "or remove stale downloader outputs before building."
        )

    for path in expected_paths:
        _verify_votable_gzip(path)
    return expected_paths


def _require_current_acquisition_spec(
    state: GaiaDownloadState,
    project: PipelineProject,
) -> None:
    acquisition_hash = _acquisition_hash_for_project(project)[2]
    stored_hash = state.get_metadata(ACQUISITION_SPEC_HASH_METADATA_KEY)
    if stored_hash != acquisition_hash:
        raise ValueError(
            "Gaia download state does not match the current acquisition spec. "
            f"State DB: {state.path}. Use a fresh state_db/input_dir pair or "
            "intentionally clean up stale downloader outputs before building."
        )


def _guard_acquisition_spec(
    state: GaiaDownloadState,
    project: PipelineProject,
    acquisition_hash: str,
    *,
    echo: Echo,
) -> None:
    stored_hash = state.get_metadata(ACQUISITION_SPEC_HASH_METADATA_KEY)
    if stored_hash == acquisition_hash:
        return

    blockers = _acquisition_spec_blockers(state, project)
    if blockers:
        raise ValueError(
            "Gaia download acquisition spec changed, and existing downloader "
            "state or files cannot be safely reused: "
            + "; ".join(blockers)
            + ". Use a fresh state_db/input_dir pair or intentionally clean up "
            "stale downloader outputs before re-planning."
        )

    if stored_hash is not None or state.has_batches():
        state.clear_download_plan()
        echo("Gaia acquisition spec changed; cleared unstarted download plan")


def _acquisition_spec_blockers(
    state: GaiaDownloadState,
    project: PipelineProject,
) -> list[str]:
    batches = state.read_batches()
    blockers: list[str] = []

    active = [
        batch.batch_id
        for batch in batches
        if batch.state
        in {"submitted", "running", "completed_remote", "downloading", "delete_pending"}
        or (
            batch.state == "failed" and batch.job_id and batch.remote_deleted_at is None
        )
    ]
    if active:
        blockers.append(f"active or cleanup-pending jobs: {_sample_join(active)}")

    successful = [
        batch.batch_id
        for batch in batches
        if batch.state in {"downloaded", "deleted_remote"}
        or batch.downloaded_at is not None
        or batch.downloaded_bytes is not None
    ]
    if successful:
        blockers.append(f"downloaded batches: {_sample_join(successful)}")

    downloaded_counts = sum(count.downloaded for count in state.read_counts())
    if downloaded_counts:
        blockers.append(f"{downloaded_counts:,} downloaded HEALPix count rows")

    existing_outputs = downloader_output_files(project.gaia.input_dir)
    if existing_outputs:
        blockers.append(
            "existing downloader output files: "
            + _sample_join(str(path) for path in existing_outputs)
        )

    return blockers


def _sample_join(values: Iterable[object]) -> str:
    items = list(values)
    sample = ", ".join(str(value) for value in items[:5])
    return sample if len(items) <= 5 else f"{sample}, ..."


def plan_gaia_download(
    project: PipelineProject,
    *,
    archive_client: GaiaArchiveClient | None = None,
    refresh_counts: bool = False,
    poll_seconds: float = 30.0,
    echo: Echo = print,
) -> PlanSummary:
    spec = query_spec_from_project(project)
    count_query = build_count_query(spec)
    full_download_query = build_download_query(spec)
    count_hash = query_hash(count_query)
    download_hash = query_hash(full_download_query)
    acquisition_hash = _acquisition_spec_hash(count_query, full_download_query)
    archive = archive_client or GaiaArchiveClient()

    with GaiaDownloadState(project.gaia_download.state_db) as state:
        _guard_acquisition_spec(state, project, acquisition_hash, echo=echo)
        query_dir = _query_dir(project.gaia_download.state_db)
        query_dir.mkdir(parents=True, exist_ok=True)
        count_query_path = query_dir / "count.adql"
        count_query_path.write_text(count_query + "\n", encoding="utf-8")

        counts_are_current = (
            state.has_counts()
            and state.get_metadata("count_query_hash") == count_hash
            and not refresh_counts
        )
        if counts_are_current:
            counts = state.read_counts()
            echo(f"Reusing Gaia HEALPix count table from {state.path}")
        else:
            count_access = _count_access_mode(project)
            count_job_name = (
                f"fis-gaia-{project.gaia_download.mode}-count-{count_hash[:8]}"
            )
            counts = _run_count_query_job(
                state,
                archive,
                count_query,
                access_mode=count_access,
                job_name=count_job_name,
                count_hash=count_hash,
                refresh_counts=refresh_counts,
                poll_seconds=poll_seconds,
                echo=echo,
            )
            state.replace_counts(counts)
            state.set_metadata("count_query_hash", count_hash)
            state.set_metadata("count_query_path", str(count_query_path))
            echo(f"Stored {len(counts):,} HEALPix count rows in {state.path}")

        total_rows = sum(count.count for count in counts if not count.downloaded)
        access_mode = _resolve_access_mode(project, total_rows)
        effective_row_cap = _effective_authenticated_row_cap(
            project,
            access_mode=access_mode,
        )
        if effective_row_cap < project.gaia_download.row_cap:
            echo(
                "Gaia download row cap reduced by remote quota estimate: "
                f"configured={project.gaia_download.row_cap:,}, "
                f"effective={effective_row_cap:,}, "
                f"bytes_per_row={project.gaia_download.remote_result_bytes_per_row:,}, "
                "max_result_bytes_per_job="
                f"{_format_bytes(project.gaia_download.remote_max_result_bytes_per_job)}"
            )
        batches = plan_download_batches(
            counts,
            mode=project.gaia_download.mode,
            access_mode=access_mode,
            row_cap=effective_row_cap,
        )
        batches = _avoid_finished_batch_id_collisions(batches, state.read_batches())

        query_paths: dict[str, Path] = {}
        output_paths: dict[str, Path] = {}
        query_hashes: dict[str, str] = {}
        job_names: dict[str, str] = {}
        for batch in batches:
            download_query = build_download_query(spec, hp3_values=batch.hp3_values)
            batch_hash = query_hash(download_query)
            query_path = query_dir / f"{batch.batch_id}.adql"
            query_path.write_text(download_query + "\n", encoding="utf-8")
            query_paths[batch.batch_id] = query_path
            output_paths[batch.batch_id] = _output_path_for_batch(project, batch)
            query_hashes[batch.batch_id] = batch_hash
            job_names[batch.batch_id] = _job_name(project, batch, batch_hash)

        state.upsert_batches(
            batches,
            query_text_paths=query_paths,
            output_paths=output_paths,
            access_mode=access_mode,
            query_hashes=query_hashes,
            job_names=job_names,
            estimated_result_bytes_per_row=project.gaia_download.remote_result_bytes_per_row,
        )
        state.set_metadata(ACQUISITION_SPEC_HASH_METADATA_KEY, acquisition_hash)
        state.set_metadata(DOWNLOAD_QUERY_HASH_METADATA_KEY, download_hash)

        over_cap = sum(1 for batch in batches if batch.over_cap)
        echo(
            "Gaia download plan: "
            f"rows={total_rows:,}, access={access_mode}, "
            f"batches={len(batches):,}, over_cap={over_cap:,}"
        )
        return PlanSummary(
            state_db=state.path,
            count_query_hash=count_hash,
            total_rows=total_rows,
            access_mode=access_mode,
            batch_count=len(batches),
            over_cap_batches=over_cap,
        )


def _effective_authenticated_row_cap(
    project: PipelineProject,
    *,
    access_mode: str,
) -> int:
    configured_cap = project.gaia_download.row_cap
    if access_mode != "authenticated":
        return configured_cap

    bytes_per_row = project.gaia_download.remote_result_bytes_per_row
    remote_budget = (
        project.gaia_download.remote_quota_bytes
        - project.gaia_download.remote_min_free_bytes
    )
    if remote_budget <= 0:
        raise ValueError(
            "gaia_download.remote_quota_bytes must exceed "
            "gaia_download.remote_min_free_bytes"
        )
    row_caps = [
        configured_cap,
        project.gaia_download.remote_max_result_bytes_per_job // bytes_per_row,
        remote_budget // bytes_per_row,
    ]
    return max(1, min(int(cap) for cap in row_caps))


def _run_count_query_job(
    state: GaiaDownloadState,
    archive: GaiaArchiveClient,
    query: str,
    *,
    access_mode: str,
    job_name: str,
    count_hash: str,
    refresh_counts: bool,
    poll_seconds: float,
    echo: Echo,
) -> list[HealpixCount]:
    metadata_hash = state.get_metadata("count_query_hash")
    job_id = state.get_metadata("count_job_id")
    if refresh_counts or metadata_hash != count_hash:
        job_id = None

    if job_id:
        echo(f"Resuming Gaia HEALPix count job_id={job_id} ({access_mode})")
    else:
        echo(f"Submitting Gaia HEALPix count query ({access_mode})")
        job = archive.submit_count_job(
            query,
            access_mode=access_mode,
            job_name=job_name,
        )
        job_id = job.job_id
        state.set_metadata("count_query_hash", count_hash)
        state.set_metadata("count_job_id", job_id)
        if job.job_url is not None:
            state.set_metadata("count_job_url", job.job_url)
        if job.phase is not None:
            state.set_metadata("count_job_phase", job.phase)
        echo(f"Gaia count: submitted job_id={job_id}")

    while True:
        try:
            phase = archive.poll_phase(job_id, access_mode=access_mode)
        except Exception as exc:
            raise RuntimeError(f"Gaia count job poll failed: {exc}") from exc
        state.set_metadata("count_job_phase", phase)
        if phase == "COMPLETED":
            echo(f"Gaia count: completed upstream job_id={job_id}")
            counts = archive.fetch_count_results(job_id, access_mode=access_mode)
            state.replace_counts(counts)
            if access_mode == "authenticated":
                try:
                    archive.delete_job(job_id, access_mode=access_mode)
                except Exception as exc:
                    state.set_metadata("count_job_delete_error", str(exc))
                    echo(f"Gaia count: remote delete failed ({exc})")
                else:
                    state.set_metadata("count_job_phase", "deleted_remote")
                    echo(f"Gaia count: deleted remote job {job_id}")
            return counts
        if phase in {"ERROR", "ABORTED"}:
            error = _friendly_archive_error(
                archive.job_error(job_id, access_mode=access_mode),
                access_mode=access_mode,
            )
            raise RuntimeError(f"Gaia count job {phase.lower()}: {error}")

        seconds = max(0.0, poll_seconds)
        wait = f"{seconds:.1f}s" if seconds and seconds < 10 else f"{seconds:.0f}s"
        echo(f"Gaia count: phase={phase}, waiting {wait}; job_id={job_id}")
        time.sleep(seconds)


def _avoid_finished_batch_id_collisions(
    batches: list[DownloadBatch],
    existing_batches: list[BatchState],
) -> list[DownloadBatch]:
    if not batches or not existing_batches:
        return batches
    active_existing = [
        batch
        for batch in existing_batches
        if batch.state
        in {"submitted", "running", "completed_remote", "downloading", "delete_pending"}
    ]
    if active_existing:
        raise ValueError(
            "Cannot re-plan while Gaia jobs are active; run or resolve them first"
        )
    finished_ids = [
        _batch_number(batch.batch_id)
        for batch in existing_batches
        if batch.state in {"downloaded", "deleted_remote"}
    ]
    if not finished_ids:
        return batches
    next_number = (
        max([_batch_number(batch.batch_id) for batch in existing_batches] + [0]) + 1
    )
    renumbered: list[DownloadBatch] = []
    for offset, batch in enumerate(batches):
        renumbered.append(
            DownloadBatch(
                batch_id=f"b{next_number + offset:04d}",
                hp3_values=batch.hp3_values,
                expected_rows=batch.expected_rows,
                over_cap=batch.over_cap,
            )
        )
    return renumbered


def _batch_number(batch_id: str) -> int:
    try:
        return int(batch_id.removeprefix("b"))
    except ValueError:
        return 0


def run_gaia_download(
    project: PipelineProject,
    *,
    archive_client: GaiaArchiveClient | None = None,
    poll_seconds: float = 30.0,
    echo: Echo = print,
) -> RunSummary:
    _require_gaia_download(project)
    archive = archive_client or GaiaArchiveClient()

    with GaiaDownloadState(project.gaia_download.state_db) as state:
        try:
            if not state.has_batches():
                echo("No Gaia download plan found; planning first")
                plan_gaia_download(project, archive_client=archive, echo=echo)

            while True:
                progress = _delete_failed_authenticated(state, archive, echo)
                batches = state.read_batches()
                if all(_is_terminal(batch) for batch in batches):
                    break

                progress |= _delete_pending_authenticated(state, archive, echo)
                progress |= _download_completed_batches(state, archive, echo)
                progress |= _poll_submitted_batches(state, archive, echo)
                if not _has_authenticated_cleanup_pending(state):
                    progress |= _submit_planned_batches(
                        project,
                        state,
                        archive,
                        echo=echo,
                    )

                if not progress:
                    active = [
                        batch for batch in state.read_batches() if _is_active(batch)
                    ]
                    if not active:
                        break
                    _echo_wait_status(active, poll_seconds=poll_seconds, echo=echo)
                    time.sleep(max(0.0, poll_seconds))
        except KeyboardInterrupt:
            echo(
                f"Interrupted. Gaia download state is saved in {state.path}; rerun to resume."
            )
            raise

        final_batches = state.read_batches()
        return RunSummary(
            state_db=state.path,
            downloaded_batches=sum(
                batch.state == "downloaded" for batch in final_batches
            ),
            deleted_remote_batches=sum(
                batch.state == "deleted_remote" for batch in final_batches
            ),
            failed_batches=sum(batch.state == "failed" for batch in final_batches),
        )


def _is_terminal(batch: BatchState) -> bool:
    if batch.state == "failed":
        if batch.access_mode == "authenticated" and batch.job_id:
            return batch.remote_deleted_at is not None
        return True
    if batch.access_mode == "authenticated":
        return batch.state == "deleted_remote"
    return batch.state == "downloaded"


def _is_active(batch: BatchState) -> bool:
    return batch.state in {
        "submitted",
        "running",
        "completed_remote",
        "downloading",
        "downloaded",
        "delete_pending",
    }


def _delete_pending_authenticated(
    state: GaiaDownloadState,
    archive: GaiaArchiveClient,
    echo: Echo,
) -> bool:
    progress = False
    for batch in state.read_batches():
        if batch.access_mode != "authenticated":
            continue
        if batch.state not in {"downloaded", "delete_pending"} or not batch.job_id:
            continue
        try:
            archive.delete_job(batch.job_id, access_mode=batch.access_mode)
        except Exception as exc:
            state.mark_delete_pending(batch.batch_id, error_message=str(exc))
            echo(f"Gaia {batch.batch_id}: remote delete pending ({exc})")
        else:
            state.mark_deleted_remote(batch.batch_id)
            echo(f"Gaia {batch.batch_id}: deleted remote job {batch.job_id}")
            progress = True
    return progress


def _delete_failed_authenticated(
    state: GaiaDownloadState,
    archive: GaiaArchiveClient,
    echo: Echo,
) -> bool:
    progress = False
    for batch in state.read_batches():
        if batch.access_mode != "authenticated":
            continue
        if (
            batch.state != "failed"
            or not batch.job_id
            or batch.remote_deleted_at is not None
        ):
            continue
        try:
            archive.delete_job(batch.job_id, access_mode=batch.access_mode)
        except Exception as exc:
            state.mark_failed(
                batch.batch_id,
                phase=batch.phase,
                error_message=f"{batch.error_message or ''} Remote delete failed: {exc}",
            )
            echo(f"Gaia {batch.batch_id}: failed remote delete pending ({exc})")
        else:
            state.mark_remote_deleted(batch.batch_id)
            if _should_retry_failed_batch(batch):
                state.mark_retry_planned(batch.batch_id)
                echo(
                    f"Gaia {batch.batch_id}: retrying transient archive failure "
                    f"after remote delete ({batch.retry_count}/"
                    f"{MAX_TRANSIENT_BATCH_RETRIES})"
                )
            else:
                echo(f"Gaia {batch.batch_id}: deleted failed remote job {batch.job_id}")
            progress = True
    return progress


def _has_authenticated_cleanup_pending(state: GaiaDownloadState) -> bool:
    cleanup_states = {"completed_remote", "downloading", "downloaded", "delete_pending"}
    for batch in state.read_batches():
        if batch.access_mode != "authenticated":
            continue
        if batch.state in cleanup_states:
            return True
        if batch.state == "failed" and batch.job_id and batch.remote_deleted_at is None:
            return True
    return False


def _should_retry_failed_batch(batch: BatchState) -> bool:
    if batch.retry_count > MAX_TRANSIENT_BATCH_RETRIES:
        return False
    error = batch.error_message or ""
    return any(marker in error for marker in TRANSIENT_ARCHIVE_ERROR_MARKERS)


def _download_completed_batches(
    state: GaiaDownloadState,
    archive: GaiaArchiveClient,
    echo: Echo,
) -> bool:
    progress = False
    for batch in state.read_batches():
        if batch.state not in {"completed_remote", "downloading"} or not batch.job_id:
            continue
        tmp_path = batch.output_path.with_name(batch.output_path.name + ".tmp")
        if batch.output_path.exists():
            _verify_votable_gzip(batch.output_path)
            state.mark_downloaded(
                batch, downloaded_bytes=batch.output_path.stat().st_size
            )
            echo(f"Gaia {batch.batch_id}: existing output verified")
            progress = True
            continue
        if tmp_path.exists():
            tmp_path.unlink()
            echo(f"Gaia {batch.batch_id}: removed stale partial download")
        state.mark_downloading(batch.batch_id)
        echo(f"Gaia {batch.batch_id}: downloading to {batch.output_path}")
        try:
            archive.download_result(
                batch.job_id,
                tmp_path,
                access_mode=batch.access_mode,
            )
            _verify_votable_gzip(tmp_path)
            batch.output_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path.replace(batch.output_path)
            downloaded_bytes = batch.output_path.stat().st_size
            state.mark_downloaded(batch, downloaded_bytes=downloaded_bytes)
            echo(f"Gaia {batch.batch_id}: downloaded {downloaded_bytes:,} bytes")
        except Exception as exc:
            if tmp_path.exists():
                tmp_path.unlink()
            state.mark_failed(batch.batch_id, phase=batch.phase, error_message=str(exc))
            echo(f"Gaia {batch.batch_id}: download failed ({exc})")
        progress = True
    return progress


def _echo_wait_status(
    active_batches: list[BatchState],
    *,
    poll_seconds: float,
    echo: Echo,
) -> None:
    seconds = max(0.0, poll_seconds)
    wait = f"{seconds:.1f}s" if seconds and seconds < 10 else f"{seconds:.0f}s"
    details = "; ".join(_batch_wait_fragment(batch) for batch in active_batches)
    echo(f"Gaia waiting {wait} before next poll; active: {details}")


def _batch_wait_fragment(batch: BatchState) -> str:
    phase = batch.phase or "-"
    job = f", job_id={batch.job_id}" if batch.job_id else ""
    return (
        f"{batch.batch_id} state={batch.state}, phase={phase}, "
        f"rows={batch.expected_rows:,}{job}"
    )


def _poll_submitted_batches(
    state: GaiaDownloadState,
    archive: GaiaArchiveClient,
    echo: Echo,
) -> bool:
    progress = False
    for batch in state.read_batches():
        if batch.state not in {"submitted", "running"} or not batch.job_id:
            continue
        try:
            phase = archive.poll_phase(batch.job_id, access_mode=batch.access_mode)
        except Exception as exc:
            state.mark_poll_error(
                batch.batch_id,
                phase=batch.phase,
                error_message=str(exc),
            )
            echo(f"Gaia {batch.batch_id}: poll failed; will retry ({exc})")
            continue

        if phase == "COMPLETED":
            state.mark_completed_remote(batch.batch_id, phase=phase)
            echo(f"Gaia {batch.batch_id}: completed upstream")
            progress = True
        elif phase in {"ERROR", "ABORTED"}:
            error = _friendly_archive_error(
                archive.job_error(batch.job_id, access_mode=batch.access_mode),
                access_mode=batch.access_mode,
            )
            state.mark_failed(batch.batch_id, phase=phase, error_message=error)
            echo(f"Gaia {batch.batch_id}: {phase.lower()} ({error})")
            progress = True
        else:
            new_state = "running" if phase in {"QUEUED", "EXECUTING"} else "submitted"
            state.mark_phase(batch.batch_id, phase=phase, state=new_state)
            if phase != batch.phase:
                echo(f"Gaia {batch.batch_id}: phase={phase}")
                progress = True
    return progress


def _submit_planned_batches(
    project: PipelineProject,
    state: GaiaDownloadState,
    archive: GaiaArchiveClient,
    *,
    echo: Echo,
) -> bool:
    progress = False
    for batch in state.read_batches():
        if batch.state != "planned":
            continue
        if not _can_submit_batch(
            project,
            state,
            archive,
            batch,
            echo=echo,
        ):
            continue
        query = batch.query_text_path.read_text(encoding="utf-8")
        batch.output_path.parent.mkdir(parents=True, exist_ok=True)
        echo(
            f"Gaia {batch.batch_id}: submitting {batch.expected_rows:,} rows "
            f"({batch.access_mode})"
        )
        try:
            job = archive.submit_download_job(
                query,
                job_name=batch.job_name,
                access_mode=batch.access_mode,
            )
        except Exception as exc:
            error = _friendly_archive_error(str(exc), access_mode=batch.access_mode)
            state.mark_failed(batch.batch_id, phase=None, error_message=error)
            echo(f"Gaia {batch.batch_id}: submit failed ({error})")
        else:
            state.mark_submitted(
                batch.batch_id,
                job_id=job.job_id,
                job_url=job.job_url,
                phase=job.phase,
            )
            echo(f"Gaia {batch.batch_id}: submitted job_id={job.job_id}")
        progress = True
    return progress


def _can_submit_batch(
    project: PipelineProject,
    state: GaiaDownloadState,
    archive: GaiaArchiveClient,
    batch: BatchState,
    *,
    echo: Echo,
) -> bool:
    if batch.output_path.exists():
        try:
            _verify_votable_gzip(batch.output_path)
        except Exception as exc:
            raise ValueError(
                f"Existing Gaia output is not valid: {batch.output_path}"
            ) from exc
        state.mark_downloaded(batch, downloaded_bytes=batch.output_path.stat().st_size)
        echo(f"Gaia {batch.batch_id}: existing output verified")
        return False

    if batch.access_mode != "authenticated":
        return True

    current = state.read_batches()
    active = [
        item
        for item in current
        if item.access_mode == "authenticated" and _is_active(item)
    ]
    if len(active) >= project.gaia_download.max_active_jobs:
        return False
    _check_remote_quota(
        project,
        state,
        archive,
        batch,
    )

    batch.output_path.parent.mkdir(parents=True, exist_ok=True)
    free_bytes = shutil.disk_usage(batch.output_path.parent).free
    if free_bytes < int(batch.estimated_result_bytes * 1.2):
        raise OSError(
            f"Not enough local free space for {batch.output_path}: "
            f"need about {int(batch.estimated_result_bytes * 1.2):,} bytes, "
            f"free {free_bytes:,}"
        )
    return True


def _check_remote_quota(
    project: PipelineProject,
    state: GaiaDownloadState,
    archive: GaiaArchiveClient,
    batch: BatchState,
) -> None:
    storage = archive.remote_async_storage(access_mode=batch.access_mode)
    local_job_ids = {
        item.job_id for item in state.read_batches() if item.job_id is not None
    }
    external_active_ids = [
        job_id for job_id in storage.active_job_ids if job_id not in local_job_ids
    ]
    if external_active_ids:
        sample = ", ".join(external_active_ids[:3])
        suffix = "" if len(external_active_ids) <= 3 else ", ..."
        raise ValueError(
            "Gaia remote quota guard cannot estimate active jobs outside this "
            f"download state: {sample}{suffix}. Wait for or delete those jobs "
            "before submitting more result jobs."
        )

    blocking_ids = storage.unknown_result_job_ids + storage.failed_job_ids
    if blocking_ids:
        sample = ", ".join(blocking_ids[:3])
        suffix = "" if len(blocking_ids) <= 3 else ", ..."
        raise ValueError(
            "Gaia remote quota guard cannot verify free space while completed "
            "or failed jobs with unknown storage remain in the account: "
            f"{sample}{suffix}. Delete those Gaia Archive jobs before retrying."
        )

    active_estimate = sum(
        item.estimated_result_bytes
        for item in state.read_batches()
        if item.access_mode == "authenticated"
        and _is_active(item)
        and (item.job_id is None or item.job_id not in storage.known_result_job_ids)
    )
    projected_bytes = (
        storage.known_result_bytes + active_estimate + batch.estimated_result_bytes
    )
    free_after = project.gaia_download.remote_quota_bytes - projected_bytes
    if free_after < project.gaia_download.remote_min_free_bytes:
        raise ValueError(
            "Gaia remote quota guard blocked submission: "
            f"known remote results {_format_bytes(storage.known_result_bytes)}, "
            f"active estimate {_format_bytes(active_estimate)}, "
            f"next batch estimate {_format_bytes(batch.estimated_result_bytes)}, "
            f"quota {_format_bytes(project.gaia_download.remote_quota_bytes)}, "
            f"free after submit {_format_bytes(free_after)}, required free "
            f"{_format_bytes(project.gaia_download.remote_min_free_bytes)}."
        )


def _format_bytes(value: int) -> str:
    return f"{value / REMOTE_GB:.2f} GB"


def _verify_votable_gzip(path: Path) -> None:
    if not path.is_file() or path.stat().st_size <= 0:
        raise ValueError(f"Gaia result file is empty or missing: {path}")
    try:
        with gzip.open(path, "rb") as fp:
            sample = fp.read(4096)
    except OSError:
        sample = path.read_bytes()[:4096]
    if b"VOTABLE" not in sample.upper():
        raise ValueError(f"Gaia result does not look like a VOTable: {path}")


def _friendly_archive_error(error_message: str, *, access_mode: str) -> str:
    if access_mode != "anonymous" or ANONYMOUS_QUOTA_ERROR not in error_message:
        return error_message
    advice = (
        "Gaia anonymous async storage is currently full. Retry later, or use "
        "authenticated access with GAIA_CREDENTIALS_FILE or GAIA_USER/GAIA_PASS."
    )
    if advice in error_message:
        return error_message
    return f"{error_message} {advice}"
