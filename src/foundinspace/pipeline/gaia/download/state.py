from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from foundinspace.pipeline.gaia.download.planner import DownloadBatch, HealpixCount


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass(frozen=True, slots=True)
class BatchState:
    batch_id: str
    hp3_values: tuple[int, ...]
    expected_rows: int
    estimated_result_bytes: int
    over_cap: bool
    query_hash: str
    query_text_path: Path
    output_path: Path
    access_mode: str
    job_name: str
    job_id: str | None
    job_url: str | None
    phase: str | None
    state: str
    submitted_at: str | None
    last_polled_at: str | None
    completed_at: str | None
    download_started_at: str | None
    downloaded_at: str | None
    remote_deleted_at: str | None
    downloaded_bytes: int | None
    error_message: str | None
    retry_count: int


class GaiaDownloadState:
    def __init__(self, path: Path) -> None:
        self.path = Path(path).expanduser()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path)
        self._conn.row_factory = sqlite3.Row
        self._create_schema()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> GaiaDownloadState:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def _create_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS metadata (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS counts (
              hp3 INTEGER PRIMARY KEY,
              count INTEGER NOT NULL,
              batch_id TEXT,
              downloaded INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS batches (
              batch_id TEXT PRIMARY KEY,
              hp3_values TEXT NOT NULL,
              expected_rows INTEGER NOT NULL,
              estimated_result_bytes INTEGER NOT NULL,
              over_cap INTEGER NOT NULL DEFAULT 0,
              query_hash TEXT NOT NULL,
              query_text_path TEXT NOT NULL,
              output_path TEXT NOT NULL,
              access_mode TEXT NOT NULL,
              job_name TEXT NOT NULL,
              job_id TEXT,
              job_url TEXT,
              phase TEXT,
              state TEXT NOT NULL,
              submitted_at TEXT,
              last_polled_at TEXT,
              completed_at TEXT,
              download_started_at TEXT,
              downloaded_at TEXT,
              remote_deleted_at TEXT,
              downloaded_bytes INTEGER,
              error_message TEXT,
              retry_count INTEGER NOT NULL DEFAULT 0
            );
            """
        )
        self._conn.commit()

    def get_metadata(self, key: str) -> str | None:
        row = self._conn.execute(
            "SELECT value FROM metadata WHERE key = ?",
            (key,),
        ).fetchone()
        return None if row is None else str(row["value"])

    def set_metadata(self, key: str, value: str) -> None:
        self._conn.execute(
            """
            INSERT INTO metadata (key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )
        self._conn.commit()

    def replace_counts(self, counts: list[HealpixCount]) -> None:
        self._conn.execute("DELETE FROM counts")
        self._conn.executemany(
            """
            INSERT INTO counts (hp3, count, downloaded)
            VALUES (?, ?, ?)
            """,
            [(item.hp3, item.count, int(item.downloaded)) for item in counts],
        )
        self._conn.commit()

    def read_counts(self) -> list[HealpixCount]:
        rows = self._conn.execute(
            "SELECT hp3, count, downloaded FROM counts ORDER BY count DESC, hp3"
        ).fetchall()
        return [
            HealpixCount(
                hp3=int(row["hp3"]),
                count=int(row["count"]),
                downloaded=bool(row["downloaded"]),
            )
            for row in rows
        ]

    def has_counts(self) -> bool:
        row = self._conn.execute("SELECT COUNT(*) AS n FROM counts").fetchone()
        return int(row["n"]) > 0

    def upsert_batches(
        self,
        batches: list[DownloadBatch],
        *,
        query_text_paths: dict[str, Path],
        output_paths: dict[str, Path],
        access_mode: str,
        query_hashes: dict[str, str],
        job_names: dict[str, str],
    ) -> None:
        valid_ids = {batch.batch_id for batch in batches}
        for batch in batches:
            existing = self._conn.execute(
                "SELECT query_hash, state FROM batches WHERE batch_id = ?",
                (batch.batch_id,),
            ).fetchone()
            hp3_json = json.dumps(list(batch.hp3_values), separators=(",", ":"))
            estimate = estimate_result_bytes(batch.expected_rows)
            if (
                existing is not None
                and existing["query_hash"] == query_hashes[batch.batch_id]
                and existing["state"] != "failed"
            ):
                self._conn.execute(
                    """
                    UPDATE batches
                    SET hp3_values = ?,
                        expected_rows = ?,
                        estimated_result_bytes = ?,
                        over_cap = ?,
                        query_text_path = ?,
                        output_path = ?,
                        access_mode = ?,
                        job_name = ?
                    WHERE batch_id = ?
                    """,
                    (
                        hp3_json,
                        batch.expected_rows,
                        estimate,
                        int(batch.over_cap),
                        str(query_text_paths[batch.batch_id]),
                        str(output_paths[batch.batch_id]),
                        access_mode,
                        job_names[batch.batch_id],
                        batch.batch_id,
                    ),
                )
            else:
                self._conn.execute(
                    """
                    INSERT INTO batches (
                      batch_id, hp3_values, expected_rows, estimated_result_bytes,
                      over_cap, query_hash, query_text_path, output_path,
                      access_mode, job_name, state
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'planned')
                    ON CONFLICT(batch_id) DO UPDATE SET
                      hp3_values = excluded.hp3_values,
                      expected_rows = excluded.expected_rows,
                      estimated_result_bytes = excluded.estimated_result_bytes,
                      over_cap = excluded.over_cap,
                      query_hash = excluded.query_hash,
                      query_text_path = excluded.query_text_path,
                      output_path = excluded.output_path,
                      access_mode = excluded.access_mode,
                      job_name = excluded.job_name,
                      job_id = NULL,
                      job_url = NULL,
                      phase = NULL,
                      state = 'planned',
                      submitted_at = NULL,
                      last_polled_at = NULL,
                      completed_at = NULL,
                      download_started_at = NULL,
                      downloaded_at = NULL,
                      remote_deleted_at = NULL,
                      downloaded_bytes = NULL,
                      error_message = NULL,
                      retry_count = 0
                    """,
                    (
                        batch.batch_id,
                        hp3_json,
                        batch.expected_rows,
                        estimate,
                        int(batch.over_cap),
                        query_hashes[batch.batch_id],
                        str(query_text_paths[batch.batch_id]),
                        str(output_paths[batch.batch_id]),
                        access_mode,
                        job_names[batch.batch_id],
                    ),
                )

            self._conn.executemany(
                "UPDATE counts SET batch_id = ? WHERE hp3 = ?",
                [(batch.batch_id, hp3) for hp3 in batch.hp3_values],
            )

        self._conn.execute(
            """
            DELETE FROM batches
            WHERE state = 'planned'
              AND batch_id NOT IN ({})
            """.format(",".join("?" for _ in valid_ids) or "''"),
            tuple(valid_ids),
        )
        self._conn.commit()

    def read_batches(self) -> list[BatchState]:
        rows = self._conn.execute("SELECT * FROM batches ORDER BY batch_id").fetchall()
        return [self._batch_from_row(row) for row in rows]

    def _batch_from_row(self, row: sqlite3.Row) -> BatchState:
        return BatchState(
            batch_id=str(row["batch_id"]),
            hp3_values=tuple(int(value) for value in json.loads(row["hp3_values"])),
            expected_rows=int(row["expected_rows"]),
            estimated_result_bytes=int(row["estimated_result_bytes"]),
            over_cap=bool(row["over_cap"]),
            query_hash=str(row["query_hash"]),
            query_text_path=Path(str(row["query_text_path"])),
            output_path=Path(str(row["output_path"])),
            access_mode=str(row["access_mode"]),
            job_name=str(row["job_name"]),
            job_id=row["job_id"],
            job_url=row["job_url"],
            phase=row["phase"],
            state=str(row["state"]),
            submitted_at=row["submitted_at"],
            last_polled_at=row["last_polled_at"],
            completed_at=row["completed_at"],
            download_started_at=row["download_started_at"],
            downloaded_at=row["downloaded_at"],
            remote_deleted_at=row["remote_deleted_at"],
            downloaded_bytes=row["downloaded_bytes"],
            error_message=row["error_message"],
            retry_count=int(row["retry_count"]),
        )

    def has_batches(self) -> bool:
        row = self._conn.execute("SELECT COUNT(*) AS n FROM batches").fetchone()
        return int(row["n"]) > 0

    def mark_submitted(
        self,
        batch_id: str,
        *,
        job_id: str,
        job_url: str | None,
        phase: str | None,
    ) -> None:
        self._conn.execute(
            """
            UPDATE batches
            SET job_id = ?, job_url = ?, phase = ?, state = 'submitted',
                submitted_at = ?
            WHERE batch_id = ?
            """,
            (job_id, job_url, phase, utc_now_iso(), batch_id),
        )
        self._conn.commit()

    def mark_phase(
        self, batch_id: str, *, phase: str, state: str | None = None
    ) -> None:
        updates: dict[str, Any] = {
            "phase": phase,
            "last_polled_at": utc_now_iso(),
        }
        if state is not None:
            updates["state"] = state
        self._update(batch_id, updates)

    def mark_completed_remote(self, batch_id: str, *, phase: str = "COMPLETED") -> None:
        self._update(
            batch_id,
            {
                "phase": phase,
                "state": "completed_remote",
                "completed_at": utc_now_iso(),
                "last_polled_at": utc_now_iso(),
            },
        )

    def mark_downloading(self, batch_id: str) -> None:
        self._update(
            batch_id,
            {"state": "downloading", "download_started_at": utc_now_iso()},
        )

    def mark_downloaded(self, batch: BatchState, *, downloaded_bytes: int) -> None:
        now = utc_now_iso()
        self._conn.execute(
            """
            UPDATE batches
            SET state = 'downloaded',
                downloaded_at = ?,
                downloaded_bytes = ?
            WHERE batch_id = ?
            """,
            (now, int(downloaded_bytes), batch.batch_id),
        )
        self._conn.executemany(
            "UPDATE counts SET downloaded = 1 WHERE hp3 = ?",
            [(hp3,) for hp3 in batch.hp3_values],
        )
        self._conn.commit()

    def mark_deleted_remote(self, batch_id: str) -> None:
        self._update(
            batch_id,
            {"state": "deleted_remote", "remote_deleted_at": utc_now_iso()},
        )

    def mark_delete_pending(self, batch_id: str, *, error_message: str) -> None:
        self._update(
            batch_id,
            {
                "state": "delete_pending",
                "error_message": error_message,
                "retry_count": sqlite3_retry_increment(self, batch_id),
            },
        )

    def mark_failed(
        self, batch_id: str, *, phase: str | None, error_message: str
    ) -> None:
        self._update(
            batch_id,
            {
                "state": "failed",
                "phase": phase,
                "error_message": error_message,
                "retry_count": sqlite3_retry_increment(self, batch_id),
            },
        )

    def _update(self, batch_id: str, values: dict[str, Any]) -> None:
        assignments = ", ".join(f"{key} = ?" for key in values)
        self._conn.execute(
            f"UPDATE batches SET {assignments} WHERE batch_id = ?",
            (*values.values(), batch_id),
        )
        self._conn.commit()


def estimate_result_bytes(expected_rows: int) -> int:
    # A deliberately coarse gzip-VOTable estimate. It is for scheduling guard
    # rails only; the completed local file size is recorded after download.
    return int(expected_rows) * 80


def sqlite3_retry_increment(state: GaiaDownloadState, batch_id: str) -> int:
    row = state._conn.execute(
        "SELECT retry_count FROM batches WHERE batch_id = ?",
        (batch_id,),
    ).fetchone()
    return 1 if row is None else int(row["retry_count"]) + 1
