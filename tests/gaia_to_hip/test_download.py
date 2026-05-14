"""Tests for Gaia↔HIP cross-match downloading."""

from pathlib import Path

from astropy.table import Table

import foundinspace.pipeline.gaia_to_hip.download


def test_fetch_uses_async_job_and_writes_ecsv(tmp_path: Path, monkeypatch):
    output = tmp_path / "gaia_hipparcos2_best_neighbour.ecsv"
    captured: dict[str, str] = {}

    class _FakeJob:
        def get_results(self):
            return Table(
                rows=[
                    [1, 11, 0.01, 1],
                    [2, 22, 0.02, 1],
                ],
                names=(
                    "source_id",
                    "original_ext_source_id",
                    "angular_distance",
                    "number_of_neighbours",
                ),
            )

    def _fake_launch_job_async(query: str):
        captured["query"] = query
        return _FakeJob()

    monkeypatch.setattr(
        foundinspace.pipeline.gaia_to_hip.download,
        "_launch_gaia_job_async",
        _fake_launch_job_async,
    )

    out = foundinspace.pipeline.gaia_to_hip.download.fetch_hipparcos2_best_neighbour_to_ecsv(
        output,
        overwrite=True,
    )

    assert out == output
    assert output.exists()
    table = Table.read(output, format="ascii.ecsv")
    assert len(table) == 2
    assert "FROM gaiadr3.hipparcos2_best_neighbour" in captured["query"]
