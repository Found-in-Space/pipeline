from __future__ import annotations

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
    ) -> list[HealpixCount]:
        if access_mode == "authenticated":
            self.login_from_environment()
        gaia = self._gaia()
        job = gaia.launch_job_async(
            query,
            name=job_name,
            output_format="votable_gzip",
            background=False,
        )
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


def _counts_from_table(table: Any) -> list[HealpixCount]:
    counts: list[HealpixCount] = []
    for row in table:
        hp3 = int(row["hp3"])
        n = int(row["n"])
        counts.append(HealpixCount(hp3=hp3, count=n))
    counts.sort(key=lambda item: (-item.count, item.hp3))
    return counts
