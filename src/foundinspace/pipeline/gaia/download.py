"""Gaia DR3 batched archive downloader.

This module implements a resilient, interruptible async download workflow that:
- builds/refreshes HEALPix level-3 counts from Gaia Archive,
- packs pending tiles into batch plans near a target row budget,
- submits archive async jobs with bounded in-flight concurrency,
- downloads completed jobs concurrently,
- deletes remote archive jobs after verified download to free row quota.
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import click
import pandas as pd
import yaml
from astroquery.gaia import Gaia
from decouple import config
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

from foundinspace.pipeline.project import load_project

COUNT_QUERY = """
SELECT
  (g.source_id / 9007199254740992) AS hp3,
  COUNT(*) AS n
FROM gaiadr3.gaia_source AS g
JOIN external.gaiaedr3_distance AS d
  ON d.source_id = g.source_id
WHERE
  g.astrometric_params_solved IN (31, 95)
  AND (
    d.r_med_photogeo IS NOT NULL
    OR d.r_med_geo IS NOT NULL
  )
GROUP BY 1
ORDER BY n DESC
"""

BATCH_QUERY_TEMPLATE = """
SELECT
  g.source_id,
  g.ra,
  g.dec,
  g.parallax,
  g.parallax_error,
  g.pmra,
  g.pmdec,
  g.phot_g_mean_mag,
  g.phot_bp_mean_mag,
  g.phot_rp_mean_mag,
  g.ruwe,
  g.mg_gspphot,
  g.ag_gspphot,
  g.mg_gspphot_upper,
  g.mg_gspphot_lower,
  g.teff_esphs,
  g.teff_gspspec,
  g.teff_espucd,
  g.teff_gspphot,
  d.r_med_geo,
  d.r_lo_geo,
  d.r_hi_geo,
  d.r_med_photogeo,
  d.r_lo_photogeo,
  d.r_hi_photogeo
FROM gaiadr3.gaia_source AS g
JOIN external.gaiaedr3_distance AS d
  ON d.source_id = g.source_id
WHERE
  g.astrometric_params_solved IN (31, 95)
  AND (
    d.r_med_photogeo IS NOT NULL
    OR d.r_med_geo IS NOT NULL
  )
  AND g.source_id / 9007199254740992 IN ({formatted_levels})
"""


@dataclass(slots=True)
class BatchState:
    label: str
    hp3: list[int]
    expected_rows: int
    output_path: Path
    status: str = "planned"
    job_id: str | None = None
    retries: int = 0
    last_error: str | None = None


async def _retry_api_call(fn, *args, retries: int = 6, context: str = "", **kwargs):
    delay = 1.0
    for attempt in range(retries + 1):
        try:
            return await asyncio.to_thread(fn, *args, **kwargs)
        except Exception:
            if attempt >= retries:
                raise
            await asyncio.sleep(delay + random.uniform(0, 0.3 * delay))
            delay = min(30.0, delay * 2.0)


def _normalise_counts_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    if "downloaded" not in out.columns:
        out["downloaded"] = ""
    out["hp3"] = pd.to_numeric(out["hp3"], errors="coerce").astype("Int64")
    out["count"] = pd.to_numeric(
        out["n"].astype(str).str.replace(r"[,\s]+", "", regex=True), errors="coerce"
    ).astype("Int64")
    out["downloaded"] = out["downloaded"].fillna("").astype(str)
    out = out.dropna(subset=["hp3", "count"]).copy()
    out["hp3"] = out["hp3"].astype(int)
    out["count"] = out["count"].astype(int)
    return out[["hp3", "count", "downloaded"]]


def best_subset_under_limit(values: list[int], limit: int) -> tuple[list[int], int]:
    reachable: dict[int, list[int]] = {0: []}
    for i, value in enumerate(values):
        nxt = dict(reachable)
        for total, idxs in reachable.items():
            new_total = total + int(value)
            if new_total <= limit and new_total not in nxt:
                nxt[new_total] = idxs + [i]
        reachable = nxt
    best_sum = max(reachable)
    return reachable[best_sum], best_sum


def batch_under_limit(values: list[int], limit: int) -> list[list[int]]:
    remaining = list(enumerate(values))
    batches: list[list[int]] = []
    while remaining:
        local_values = [v for _, v in remaining]
        chosen_local, _ = best_subset_under_limit(local_values, limit)
        if not chosen_local:
            max_i = max(range(len(local_values)), key=lambda i: local_values[i])
            chosen_local = [max_i]
        chosen_set = set(chosen_local)
        batches.append([remaining[i][0] for i in chosen_local])
        remaining = [x for i, x in enumerate(remaining) if i not in chosen_set]
    return batches


def build_plan_from_counts(
    counts_csv: Path,
    plan_csv: Path,
    *,
    target_rows_per_batch: int,
    overwrite: bool,
) -> pd.DataFrame:
    if plan_csv.exists() and not overwrite:
        return pd.read_csv(plan_csv)

    counts_df = _normalise_counts_df(pd.read_csv(counts_csv))
    pending = (
        counts_df[counts_df["downloaded"].eq("")][["hp3", "count"]]
        .sort_values("count", ascending=True)
        .reset_index(drop=True)
    )
    if pending.empty:
        out = pending.assign(batch="")
        out.to_csv(plan_csv, index=False)
        return out

    batches = batch_under_limit(pending["count"].astype(int).tolist(), target_rows_per_batch)
    pending["batch"] = ""
    for i, members in enumerate(batches, start=1):
        pending.loc[members, "batch"] = f"b{i}"

    plan_csv.parent.mkdir(parents=True, exist_ok=True)
    pending.to_csv(plan_csv, index=False)
    return pending


def _load_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": 1, "jobs": {}}
    with path.open("r", encoding="utf-8") as fp:
        data = yaml.safe_load(fp) or {}
    if not isinstance(data, dict):
        return {"version": 1, "jobs": {}}
    if "jobs" not in data or not isinstance(data["jobs"], dict):
        data["jobs"] = {}
    return data


def _save_manifest(path: Path, manifest: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fp:
        yaml.safe_dump(manifest, fp, sort_keys=True)


def _build_batch_query(hp3_values: list[int]) -> str:
    formatted_levels = ",".join(str(int(v)) for v in hp3_values)
    return BATCH_QUERY_TEMPLATE.format(formatted_levels=formatted_levels)


async def _build_counts_file(counts_csv: Path) -> None:
    job = await _retry_api_call(
        Gaia.launch_job_async,
        COUNT_QUERY,
        output_format="csv",
        background=False,
        context="count query",
    )
    table = await _retry_api_call(job.get_results, context="count query results")
    df = table.to_pandas()
    out = _normalise_counts_df(df.rename(columns={"n": "n"}))
    out = out.rename(columns={"count": "n"})
    out["downloaded"] = ""
    counts_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(counts_csv, index=False)


async def _submit_batch(batch: BatchState) -> Any:
    query = _build_batch_query(batch.hp3)
    return await _retry_api_call(
        Gaia.launch_job_async,
        query,
        output_format="votable_gzip",
        background=True,
        context=f"submit {batch.label}",
    )


async def _job_phase(job: Any) -> str:
    phase = await _retry_api_call(job.get_phase, context="job phase")
    return str(phase).upper()


async def _download_batch(job: Any, output_path: Path) -> None:
    table = await _retry_api_call(job.get_results, context="download results")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    await _retry_api_call(
        table.write,
        output_path,
        format="votable",
        overwrite=True,
        context="write votable",
    )


async def _delete_job(job_id: str) -> None:
    try:
        await _retry_api_call(Gaia.remove_jobs, [job_id], context=f"delete job {job_id}")
    except Exception:
        await _retry_api_call(Gaia.remove_jobs, job_id, context=f"delete job {job_id}")


def _require_credentials() -> tuple[str, str]:
    username = config("GAIA_ARCHIVE_USERNAME")
    password = config("GAIA_ARCHIVE_PASSWORD")
    return username, password


def _hydrate_batches(plan_df: pd.DataFrame, download_dir: Path, manifest: dict[str, Any]) -> list[BatchState]:
    jobs = manifest.setdefault("jobs", {})
    by_batch = plan_df.groupby("batch", sort=False)
    states: list[BatchState] = []
    for label, rows in by_batch:
        hp3_values = [int(v) for v in rows["hp3"].tolist()]
        expected_rows = int(rows["count"].sum())
        output_path = download_dir / f"gaia_batch_{label}.vot.gz"
        prior = jobs.get(label, {})
        states.append(
            BatchState(
                label=label,
                hp3=hp3_values,
                expected_rows=expected_rows,
                output_path=output_path,
                status=str(prior.get("status", "planned")),
                job_id=prior.get("job_id"),
                retries=int(prior.get("retries", 0)),
                last_error=prior.get("last_error"),
            )
        )
    return states


def _persist_batches(manifest: dict[str, Any], manifest_path: Path, batches: list[BatchState]) -> None:
    jobs = manifest.setdefault("jobs", {})
    for b in batches:
        jobs[b.label] = {
            "hp3": [int(v) for v in b.hp3],
            "expected_rows": int(b.expected_rows),
            "output_path": str(b.output_path),
            "status": b.status,
            "job_id": b.job_id,
            "retries": int(b.retries),
            "last_error": b.last_error,
            "updated_at_unix": int(time.time()),
        }
    _save_manifest(manifest_path, manifest)


async def run_downloads(
    plan_df: pd.DataFrame,
    *,
    manifest_path: Path,
    download_dir: Path,
    max_inflight_jobs: int,
    max_concurrent_downloads: int,
    poll_interval_seconds: float,
) -> None:
    manifest = _load_manifest(manifest_path)
    batches = _hydrate_batches(plan_df, download_dir, manifest)
    labels = {b.label for b in batches}
    for old_label in set(manifest.get("jobs", {})) - labels:
        del manifest["jobs"][old_label]

    pending_downloads: dict[str, asyncio.Task[None]] = {}
    jobs_by_label: dict[str, Any] = {}
    download_semaphore = asyncio.Semaphore(max_concurrent_downloads)
    console = Console()

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task_id = progress.add_task("Gaia download orchestration", total=None)
        while True:
            done_count = sum(1 for b in batches if b.status == "done")
            failed = [b for b in batches if b.status == "failed"]
            if failed:
                details = ", ".join(f"{b.label}: {b.last_error}" for b in failed[:3])
                raise RuntimeError(f"One or more batches failed: {details}")
            if done_count == len(batches):
                progress.update(task_id, description="All batches completed")
                break

            inflight = [b for b in batches if b.status in {"submitted", "running", "ready", "downloading"}]
            available = max(0, max_inflight_jobs - len(inflight))
            if available > 0:
                for batch in [b for b in batches if b.status == "planned"][:available]:
                    try:
                        job = await _submit_batch(batch)
                        batch.job_id = str(getattr(job, "jobid", ""))
                        batch.status = "submitted"
                        jobs_by_label[batch.label] = job
                    except Exception as exc:
                        batch.retries += 1
                        batch.last_error = str(exc)
                        if batch.retries > 5:
                            batch.status = "failed"
                    _persist_batches(manifest, manifest_path, batches)

            for batch in batches:
                if batch.status not in {"submitted", "running", "ready"}:
                    continue
                if batch.label not in jobs_by_label and batch.job_id:
                    try:
                        jobs_by_label[batch.label] = await _retry_api_call(
                            Gaia.load_async_job,
                            batch.job_id,
                            context=f"load job {batch.job_id}",
                        )
                    except Exception as exc:
                        batch.retries += 1
                        batch.last_error = str(exc)
                        if batch.retries > 5:
                            batch.status = "failed"
                        continue
                job = jobs_by_label.get(batch.label)
                if job is None:
                    continue
                try:
                    phase = await _job_phase(job)
                except Exception as exc:
                    batch.retries += 1
                    batch.last_error = str(exc)
                    if batch.retries > 5:
                        batch.status = "failed"
                    continue

                if phase in {"COMPLETED"}:
                    batch.status = "ready"
                elif phase in {"EXECUTING", "QUEUED", "PENDING", "RUNNING"}:
                    batch.status = "running"
                elif phase in {"ERROR", "ABORTED", "FAILED"}:
                    batch.status = "failed"
                    batch.last_error = f"Gaia job phase={phase}"

            for batch in batches:
                if batch.status != "ready" or batch.label in pending_downloads:
                    continue

                async def _runner(b: BatchState) -> None:
                    async with download_semaphore:
                        b.status = "downloading"
                        _persist_batches(manifest, manifest_path, batches)
                        job = jobs_by_label[b.label]
                        await _download_batch(job, b.output_path)
                        if not b.output_path.exists() or b.output_path.stat().st_size == 0:
                            raise RuntimeError(f"Downloaded file missing/empty: {b.output_path}")
                        await _delete_job(str(b.job_id))
                        b.status = "done"
                        _persist_batches(manifest, manifest_path, batches)

                pending_downloads[batch.label] = asyncio.create_task(_runner(batch))

            finished_labels: list[str] = []
            for label, task in pending_downloads.items():
                if not task.done():
                    continue
                try:
                    await task
                except Exception as exc:
                    batch = next(b for b in batches if b.label == label)
                    batch.retries += 1
                    batch.last_error = str(exc)
                    if batch.retries > 5:
                        batch.status = "failed"
                    else:
                        batch.status = "ready"
                    _persist_batches(manifest, manifest_path, batches)
                finished_labels.append(label)
            for label in finished_labels:
                del pending_downloads[label]

            done_count = sum(1 for b in batches if b.status == "done")
            progress.update(
                task_id,
                description=(
                    f"done={done_count}/{len(batches)} "
                    f"inflight={len(inflight)} downloading={len(pending_downloads)}"
                ),
            )
            _persist_batches(manifest, manifest_path, batches)
            await asyncio.sleep(max(0.5, poll_interval_seconds))


@click.command(name="download")
@click.option(
    "--project",
    "project_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to pipeline project TOML.",
)
@click.option("--refresh-counts", is_flag=True, default=False, help="Re-run counts query even if counts CSV exists.")
@click.option("--rebuild-plan", is_flag=True, default=False, help="Rebuild batch plan CSV even if it exists.")
def main(project_path: Path, refresh_counts: bool, rebuild_plan: bool) -> None:
    """Download Gaia DR3 data in resilient HEALPix L3 batches."""
    try:
        project = load_project(project_path)
        project.require("gaia")
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc

    counts_csv = project.gaia.download_counts_csv
    plan_csv = project.gaia.download_plan_csv
    manifest_yaml = project.gaia.download_manifest_yaml
    download_dir = project.gaia.download_votable_dir

    target_rows = project.gaia.target_rows_per_batch
    max_inflight = project.gaia.max_archive_inflight_queries
    max_dl = project.gaia.max_concurrent_downloads
    poll_seconds = project.gaia.poll_interval_seconds

    username, password = _require_credentials()

    async def _run() -> None:
        await _retry_api_call(Gaia.login, user=username, password=password, context="Gaia login")
        if refresh_counts or not counts_csv.exists():
            await _build_counts_file(counts_csv)
        plan_df = build_plan_from_counts(
            counts_csv,
            plan_csv,
            target_rows_per_batch=target_rows,
            overwrite=rebuild_plan,
        )
        if plan_df.empty:
            click.echo("No pending HEALPix tiles left to download.")
            return

        await run_downloads(
            plan_df,
            manifest_path=manifest_yaml,
            download_dir=download_dir,
            max_inflight_jobs=max_inflight,
            max_concurrent_downloads=max_dl,
            poll_interval_seconds=poll_seconds,
        )

    try:
        asyncio.run(_run())
    except KeyboardInterrupt as exc:
        raise click.ClickException("Interrupted. Progress saved to manifest YAML.") from exc
