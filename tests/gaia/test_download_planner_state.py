from __future__ import annotations

from pathlib import Path

from foundinspace.pipeline.gaia.download.planner import (
    HealpixCount,
    best_subset_under_limit,
    plan_download_batches,
)
from foundinspace.pipeline.gaia.download.state import GaiaDownloadState


def test_best_subset_under_limit_is_not_simple_greedy() -> None:
    chosen, total = best_subset_under_limit([8, 6, 4, 4], 10)

    assert total == 10
    assert chosen in ([1, 2], [1, 3])


def test_plan_download_batches_handles_small_and_over_cap() -> None:
    small = plan_download_batches(
        [HealpixCount(2, 100), HealpixCount(1, 200)],
        mode="small",
        access_mode="anonymous",
        row_cap=250,
    )
    assert len(small) == 1
    assert small[0].hp3_values == (1, 2)
    assert small[0].expected_rows == 300

    over_cap = plan_download_batches(
        [HealpixCount(1, 12), HealpixCount(2, 4)],
        mode="full",
        access_mode="authenticated",
        row_cap=10,
    )
    assert over_cap[0].over_cap
    assert over_cap[0].hp3_values == (1,)


def test_download_state_preserves_job_progress_on_replanning(tmp_path: Path) -> None:
    state_path = tmp_path / "state.sqlite"
    batch = plan_download_batches(
        [HealpixCount(1, 100), HealpixCount(2, 100)],
        mode="small",
        access_mode="anonymous",
        row_cap=1_000,
    )[0]

    with GaiaDownloadState(state_path) as state:
        state.replace_counts([HealpixCount(1, 100), HealpixCount(2, 100)])
        state.upsert_batches(
            [batch],
            query_text_paths={batch.batch_id: tmp_path / "b0001.adql"},
            output_paths={batch.batch_id: tmp_path / "b0001.vot.gz"},
            access_mode="anonymous",
            query_hashes={batch.batch_id: "abc"},
            job_names={batch.batch_id: "job"},
        )
        state.mark_submitted(
            batch.batch_id,
            job_id="123",
            job_url="https://example.test/123",
            phase="EXECUTING",
        )
        state.upsert_batches(
            [batch],
            query_text_paths={batch.batch_id: tmp_path / "b0001.adql"},
            output_paths={batch.batch_id: tmp_path / "b0001.vot.gz"},
            access_mode="anonymous",
            query_hashes={batch.batch_id: "abc"},
            job_names={batch.batch_id: "job"},
        )

        saved = state.read_batches()[0]

    assert saved.job_id == "123"
    assert saved.state == "submitted"
