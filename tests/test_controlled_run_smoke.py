from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from astropy.table import Table
from click.testing import CliRunner

import foundinspace.pipeline.gaia.cli as gaia_cli_module
from foundinspace.pipeline.cli import cli
from foundinspace.pipeline.constants import OUTPUT_COLS


def _write_ecsv(path: Path, rows: list[list[object]], names: tuple[str, ...]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    Table(rows=rows, names=names).write(path, format="ascii.ecsv", overwrite=True)


def _invoke(runner: CliRunner, *args: str) -> None:
    result = runner.invoke(cli, list(args))
    assert result.exit_code == 0, result.output


def test_named_run_fixture_reaches_merge_and_quality_report(
    tmp_path: Path,
    monkeypatch,
) -> None:
    run_dir = tmp_path / "stars-fixture"
    run_dir.mkdir()
    project_path = run_dir / "pipeline.toml"
    project_path.write_text(
        """
format_version = 1

[run]
id = "stars-fixture"
provenance_dir = "provenance"

[gaia]
input_dir = "raw/gaia"
input_mode = "directory"
output_dir = "staged/gaia"
calculation_engine = "dr3_ap_bj_core"
mag_limit = 9.0

[gaia_download]
mode = "small"
mag_limit = 9.0
state_db = "state/gaia.sqlite"
carry_field_sets = []

[gaia-to-hip]
download_ecsv = "raw/best-neighbour.ecsv"
output_parquet = "staged/gaia-hip.parquet"

[hip]
download_ecsv = "raw/hip.ecsv"
output_parquet = "staged/hip.parquet"

[identifiers]
hip_hd_ecsv = "raw/hip-hd.ecsv"
iv27a_catalog_ecsv = "raw/iv27a.ecsv"
iv27a_proper_names_ecsv = "raw/names.ecsv"
output_parquet = "staged/identifiers.parquet"

[overrides]
output_parquet = "staged/overrides.parquet"
source_paths = []

[merge]
output_dir = "merged"
sidecar_output_dir = "sidecars"
healpix_order = 1
""".lstrip(),
        encoding="utf-8",
    )
    (run_dir / "provenance").mkdir()
    gaia_input = run_dir / "raw/gaia"
    gaia_input.mkdir(parents=True)
    (gaia_input / "fixture.vot").write_text("fixture\n", encoding="utf-8")
    _write_ecsv(
        run_dir / "raw/hip.ecsv",
        [[1, 5, 10.0, 20.0, 10.0, 0.2, 0.0, 0.0, 7.0, 0.6]],
        (
            "HIP",
            "Sn",
            "RArad",
            "DErad",
            "Plx",
            "e_Plx",
            "pmRA",
            "pmDE",
            "Hpmag",
            "B-V",
        ),
    )
    _write_ecsv(
        run_dir / "raw/best-neighbour.ecsv",
        [[100, 1, 0.01, 1]],
        (
            "source_id",
            "original_ext_source_id",
            "angular_distance",
            "number_of_neighbours",
        ),
    )
    _write_ecsv(run_dir / "raw/hip-hd.ecsv", [[1, 10]], ("HIP", "HD"))
    _write_ecsv(
        run_dir / "raw/iv27a.ecsv",
        [[1, 10, "alp", 1, "Cas"]],
        ("HIP", "HD", "Bayer", "Fl", "Cst"),
    )
    _write_ecsv(run_dir / "raw/names.ecsv", [[10, "Fixture"]], ("HD", "Name"))

    def write_gaia_fixture(
        input_path: Path,
        output_path: Path,
        **kwargs,
    ) -> None:
        hip = pd.read_parquet(run_dir / "staged/hip.parquet").iloc[0].copy()
        hip["source"] = "gaia"
        hip["source_id"] = 100
        gaia = pd.DataFrame([hip])
        gaia["ruwe"] = 1.0
        gaia["phot_g_mean_mag"] = 7.0
        output_path.parent.mkdir(parents=True, exist_ok=True)
        gaia[OUTPUT_COLS + ["ruwe", "phot_g_mean_mag"]].to_parquet(
            output_path,
            index=False,
        )

    monkeypatch.setattr(gaia_cli_module, "main", write_gaia_fixture)
    runner = CliRunner()
    project_args = ("--project", str(project_path))
    _invoke(runner, "project", "validate", *project_args)
    _invoke(runner, "hip", "build", *project_args)
    _invoke(runner, "gaia-to-hip", "build", *project_args)
    _invoke(runner, "identifiers", "build", *project_args)
    _invoke(runner, "overrides", "build", *project_args)
    _invoke(runner, "gaia", "build", *project_args)
    _invoke(runner, "merge", "build", "--quality-report", *project_args)

    assert list((run_dir / "merged/healpix").glob("*/*.parquet"))
    assert (run_dir / "merged/merge_report.json").is_file()
    assert (run_dir / "merged/merge_quality_report.json").is_file()
    events = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in (run_dir / "provenance").glob("*.json")
    ]
    assert len(events) == 7
    assert {event["status"] for event in events} == {"succeeded"}
    assert {event["run_id"] for event in events} == {"stars-fixture"}
    merge_event = next(event for event in events if event["command"] == "merge build")
    assert merge_event["parameters"]["quality_report"] is True
