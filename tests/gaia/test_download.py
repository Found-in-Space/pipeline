from pathlib import Path

import pytest

from foundinspace.pipeline.gaia.download import (
    batch_under_limit,
    best_subset_under_limit,
    build_plan_from_counts,
)
from foundinspace.pipeline.project import load_project


def test_best_subset_under_limit_picks_exact_sum_when_available() -> None:
    idxs, total = best_subset_under_limit([4, 7, 3, 2], 9)
    assert total == 9
    assert sorted(idxs) in ([0, 2, 3], [1, 3])


def test_batch_under_limit_partitions_all_indices() -> None:
    values = [10, 11, 12, 3]
    batches = batch_under_limit(values, limit=15)
    flattened = sorted(i for batch in batches for i in batch)
    assert flattened == [0, 1, 2, 3]
    for batch in batches:
        total = sum(values[i] for i in batch)
        # A single value can exceed the limit in fallback mode.
        assert total <= 15 or len(batch) == 1


def test_build_plan_from_counts_skips_downloaded_and_writes_csv(tmp_path: Path) -> None:
    counts_csv = tmp_path / "counts.csv"
    plan_csv = tmp_path / "plan.csv"
    counts_csv.write_text(
        "hp3,n,downloaded\n1,10,\n2,20,b1\n3,5,\n",
        encoding="utf-8",
    )

    out = build_plan_from_counts(
        counts_csv,
        plan_csv,
        target_rows_per_batch=12,
        overwrite=True,
    )

    assert set(out["hp3"].tolist()) == {1, 3}
    assert all(label.startswith("b") for label in out["batch"].tolist())
    assert plan_csv.exists()


def test_project_gaia_download_fields_are_read(tmp_path: Path) -> None:
    project_path = tmp_path / "project.toml"
    project_path.write_text(
        """
format_version = 1

[gaia]
output_dir = "data/processed/gaia"
download_counts_csv = "data/catalogs/gaia_l3_counts.csv"
download_plan_csv = "data/catalogs/gaia_batch_plan.csv"
download_manifest_yaml = "data/catalogs/gaia_download_manifest.yaml"
download_votable_dir = "data/catalogs/gaia_batches"
target_rows_per_batch = 55000000
max_archive_inflight_queries = 2
max_concurrent_downloads = 3
poll_interval_seconds = 5
""".strip()
        + "\n",
        encoding="utf-8",
    )

    project = load_project(project_path)
    assert project.gaia.target_rows_per_batch == 55_000_000
    assert project.gaia.max_archive_inflight_queries == 2
    assert project.gaia.max_concurrent_downloads == 3
    assert project.gaia.poll_interval_seconds == 5
    assert project.gaia.download_plan_csv == tmp_path / "data/catalogs/gaia_batch_plan.csv"


def test_project_gaia_poll_interval_default(tmp_path: Path) -> None:
    project_path = tmp_path / "project.toml"
    project_path.write_text(
        """
format_version = 1

[gaia]
output_dir = "data/processed/gaia"
download_counts_csv = "data/catalogs/gaia_l3_counts.csv"
download_plan_csv = "data/catalogs/gaia_batch_plan.csv"
download_manifest_yaml = "data/catalogs/gaia_download_manifest.yaml"
download_votable_dir = "data/catalogs/gaia_batches"
target_rows_per_batch = 55000000
max_archive_inflight_queries = 2
max_concurrent_downloads = 3
""".strip()
        + "\n",
        encoding="utf-8",
    )

    project = load_project(project_path)
    assert project.gaia.poll_interval_seconds == pytest.approx(10.0)
