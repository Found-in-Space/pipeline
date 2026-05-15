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


def test_launch_uses_credentials_file_when_available(monkeypatch):
    calls: list[tuple[str, dict]] = []

    class _FakeGaia:
        @staticmethod
        def login(**kwargs):
            calls.append(("login", kwargs))

        @staticmethod
        def launch_job_async(query: str):
            calls.append(("launch", {"query": query}))
            return object()

    monkeypatch.setenv("GAIA_CREDENTIALS_FILE", "/data/gaia.creds")
    monkeypatch.delenv("GAIA_USER", raising=False)
    monkeypatch.delenv("GAIA_PASS", raising=False)

    foundinspace.pipeline.gaia_to_hip.download._launch_gaia_job_async(
        "SELECT 1",
        gaia_client=_FakeGaia,
    )

    assert calls == [
        ("login", {"credentials_file": "/data/gaia.creds"}),
        ("launch", {"query": "SELECT 1"}),
    ]


def test_launch_uses_user_password_when_available(monkeypatch):
    calls: list[tuple[str, dict]] = []

    class _FakeGaia:
        @staticmethod
        def login(**kwargs):
            calls.append(("login", kwargs))

        @staticmethod
        def launch_job_async(query: str):
            calls.append(("launch", {"query": query}))
            return object()

    monkeypatch.delenv("GAIA_CREDENTIALS_FILE", raising=False)
    monkeypatch.setenv("GAIA_USER", "gaia-user")
    monkeypatch.setenv("GAIA_PASS", "gaia-pass")

    foundinspace.pipeline.gaia_to_hip.download._launch_gaia_job_async(
        "SELECT 1",
        gaia_client=_FakeGaia,
    )

    assert calls == [
        ("login", {"user": "gaia-user", "password": "gaia-pass"}),
        ("launch", {"query": "SELECT 1"}),
    ]


def test_launch_keeps_anonymous_when_credentials_absent(monkeypatch):
    calls: list[tuple[str, dict]] = []

    class _FakeGaia:
        @staticmethod
        def login(**kwargs):
            calls.append(("login", kwargs))

        @staticmethod
        def launch_job_async(query: str):
            calls.append(("launch", {"query": query}))
            return object()

    monkeypatch.delenv("GAIA_CREDENTIALS_FILE", raising=False)
    monkeypatch.delenv("GAIA_USER", raising=False)
    monkeypatch.delenv("GAIA_PASS", raising=False)

    foundinspace.pipeline.gaia_to_hip.download._launch_gaia_job_async(
        "SELECT 1",
        gaia_client=_FakeGaia,
    )

    assert calls == [("launch", {"query": "SELECT 1"})]
