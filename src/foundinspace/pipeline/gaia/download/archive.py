from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from foundinspace.pipeline.common.gaia_credentials import (
    GAIA_CREDENTIALS_MESSAGE,
    login_gaia_from_environment_if_available,
)
from foundinspace.pipeline.gaia.download.planner import HealpixCount


class GaiaCredentialsError(RuntimeError):
    """Raised when authenticated Gaia access is requested without credentials."""


@dataclass(frozen=True, slots=True)
class ArchiveJobInfo:
    job_id: str
    job_url: str | None
    phase: str | None


@dataclass(frozen=True, slots=True)
class RemoteStorageInfo:
    known_result_bytes: int
    known_result_job_ids: frozenset[str]
    unknown_result_job_ids: tuple[str, ...]
    active_job_ids: tuple[str, ...]
    failed_job_ids: tuple[str, ...]


class GaiaArchiveClient:
    """Small testable boundary around astroquery's Gaia TAP client."""

    def __init__(self) -> None:
        self._logged_in = False

    def _gaia(self):
        from astroquery.gaia import Gaia

        return Gaia

    def login_from_environment(self) -> None:
        if self._logged_in:
            return
        gaia = self._gaia()
        if login_gaia_from_environment_if_available(gaia):
            self._logged_in = True
            return

        raise GaiaCredentialsError(
            f"Authenticated Gaia downloads require {GAIA_CREDENTIALS_MESSAGE}"
        )

    def run_count_query(
        self,
        query: str,
        *,
        access_mode: str,
        job_name: str = "fis-gaia-count",
        poll_seconds: float = 30.0,
    ) -> list[HealpixCount]:
        job = self.submit_count_job(
            query,
            access_mode=access_mode,
            job_name=job_name,
        )
        while True:
            phase = self.poll_phase(job.job_id, access_mode=access_mode)
            if phase == "COMPLETED":
                return self.fetch_count_results(job.job_id, access_mode=access_mode)
            if phase in {"ERROR", "ABORTED"}:
                error = self.job_error(job.job_id, access_mode=access_mode)
                raise RuntimeError(f"Gaia count job {phase.lower()}: {error}")
            time.sleep(max(0.0, poll_seconds))

    def submit_count_job(
        self,
        query: str,
        *,
        access_mode: str,
        job_name: str,
    ) -> ArchiveJobInfo:
        if access_mode == "authenticated":
            self.login_from_environment()
        gaia = self._gaia()
        job = gaia.launch_job_async(
            query,
            name=job_name,
            output_format="votable_gzip",
            background=True,
        )
        return ArchiveJobInfo(
            job_id=str(job.jobid),
            job_url=getattr(job, "remoteLocation", None),
            phase=getattr(job, "_phase", None),
        )

    def fetch_count_results(
        self,
        job_id: str,
        *,
        access_mode: str,
    ) -> list[HealpixCount]:
        if access_mode == "authenticated":
            self.login_from_environment()
        job = self._gaia().load_async_job(jobid=job_id, load_results=False)
        if job is None:
            raise RuntimeError(f"Gaia async job not found: {job_id}")
        table = job.get_results()
        return _counts_from_table(table)

    def submit_download_job(
        self,
        query: str,
        *,
        job_name: str,
        access_mode: str,
    ) -> ArchiveJobInfo:
        if access_mode == "authenticated":
            self.login_from_environment()
        gaia = self._gaia()
        job = gaia.launch_job_async(
            query,
            name=job_name,
            output_format="votable_gzip",
            background=True,
        )
        return ArchiveJobInfo(
            job_id=str(job.jobid),
            job_url=getattr(job, "remoteLocation", None),
            phase=getattr(job, "_phase", None),
        )

    def poll_phase(self, job_id: str, *, access_mode: str) -> str:
        if access_mode == "authenticated":
            self.login_from_environment()
        job = self._gaia().load_async_job(jobid=job_id, load_results=False)
        if job is None:
            raise RuntimeError(f"Gaia async job not found: {job_id}")
        phase = job.get_phase(update=True)
        return str(phase).upper()

    def job_error(self, job_id: str, *, access_mode: str) -> str:
        if access_mode == "authenticated":
            self.login_from_environment()
        job = self._gaia().load_async_job(jobid=job_id, load_results=False)
        if job is None:
            return f"Gaia async job not found: {job_id}"
        try:
            error = job.get_error(verbose=False)
        except (
            Exception
        ) as exc:  # pragma: no cover - depends on archive response object
            return str(exc)
        return str(error)

    def download_result(
        self,
        job_id: str,
        output_path: Path,
        *,
        access_mode: str,
    ) -> None:
        if access_mode == "authenticated":
            self.login_from_environment()
        job = self._gaia().load_async_job(jobid=job_id, load_results=False)
        if job is None:
            raise RuntimeError(f"Gaia async job not found: {job_id}")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        job.outputFileUser = str(output_path)
        job.outputFile = str(output_path)
        job.save_results(verbose=False)

    def delete_job(self, job_id: str, *, access_mode: str) -> None:
        if access_mode == "authenticated":
            self.login_from_environment()
        self._gaia().remove_jobs([job_id])

    def remote_async_storage(self, *, access_mode: str) -> RemoteStorageInfo:
        if access_mode == "authenticated":
            self.login_from_environment()
        gaia = self._gaia()
        known_bytes = 0
        known_ids: set[str] = set()
        unknown_ids: list[str] = []
        active_ids: list[str] = []
        failed_ids: list[str] = []
        for job in gaia.list_async_jobs(verbose=False):
            job_id = str(job.jobid)
            phase = str(getattr(job, "_phase", "") or "").upper()
            if phase == "COMPLETED":
                content_length = self._remote_result_content_length(job_id)
                if content_length is None:
                    unknown_ids.append(job_id)
                else:
                    known_bytes += content_length
                    known_ids.add(job_id)
            elif phase in {"PENDING", "QUEUED", "EXECUTING", "HELD", "SUSPENDED"}:
                active_ids.append(job_id)
            elif phase in {"ERROR", "ABORTED"}:
                failed_ids.append(job_id)
        return RemoteStorageInfo(
            known_result_bytes=known_bytes,
            known_result_job_ids=frozenset(known_ids),
            unknown_result_job_ids=tuple(unknown_ids),
            active_job_ids=tuple(active_ids),
            failed_job_ids=tuple(failed_ids),
        )

    def _remote_result_content_length(self, job_id: str) -> int | None:
        conn = self._gaia()._Tap__connHandler
        response = conn.execute_tapget(f"async/{job_id}/results/result")
        try:
            for _ in range(20):
                if response.status not in {302, 303}:
                    break
                location = conn.find_header(response.getheaders(), "location")
                response.close()
                if not location:
                    return None
                response = conn.execute_tapget(location)

            if response.status != 200:
                return None
            content_length = conn.find_header(response.getheaders(), "Content-Length")
            if content_length is None:
                return None
            try:
                return int(content_length)
            except ValueError:
                return None
        finally:
            response.close()


def _counts_from_table(table: Any) -> list[HealpixCount]:
    counts: list[HealpixCount] = []
    for row in table:
        hp3 = int(row["hp3"])
        n = int(row["n"])
        counts.append(HealpixCount(hp3=hp3, count=n))
    counts.sort(key=lambda item: (-item.count, item.hp3))
    return counts
