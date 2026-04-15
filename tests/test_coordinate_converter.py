from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from click.testing import CliRunner

from foundinspace.pipeline.cli import cli
from foundinspace.pipeline.common.photometry import (
    TEFF_LOG8_SENTINEL,
    encode_teff_log8,
)
from foundinspace.pipeline.coordinate_converter import (
    convert_coordinate_table,
    parse_dec_deg,
    parse_ra_deg,
    to_viewer_records,
)


def test_parse_sexagesimal_ra_dec_with_unicode_minus():
    assert np.isclose(parse_ra_deg("08 55 10.8317"), 133.79513208333333)
    assert np.isclose(parse_dec_deg("\u221207 14 42.53"), -7.245147222222222)


def test_encode_teff_log8_matches_viewer_sentinel_policy():
    encoded = encode_teff_log8(np.array([5800.0, 250.0, np.nan]))

    assert encoded[0] != TEFF_LOG8_SENTINEL
    assert encoded[1] == TEFF_LOG8_SENTINEL
    assert encoded[2] == TEFF_LOG8_SENTINEL
    assert encode_teff_log8(250.0) == TEFF_LOG8_SENTINEL


def test_convert_wise_0855_style_row_to_project_coordinates():
    df = pd.DataFrame(
        [
            {
                "name": "WISE 0855-0714",
                "ra": "08 55 10.8317",
                "dec": "\u221207 14 42.53",
                "epoch_yr": 2000.0,
                "pmRA*": -8118.9,
                "pmDec": 679.3,
                "parallax_mas": 439.0,
                "distance_pc": 2.277904783,
                "teff_k": 250.0,
            }
        ]
    )

    out = convert_coordinate_table(df)
    row = out.iloc[0]

    np.testing.assert_allclose(
        [row["x_icrs_pc"], row["y_icrs_pc"], row["z_icrs_pc"]],
        [-1.562884, 1.632111, -0.287159],
        rtol=0,
        atol=1e-6,
    )
    np.testing.assert_allclose(
        [row["ra_deg"], row["dec_deg"], row["r_pc"]],
        [133.758758, -7.242127, 2.277905],
        rtol=0,
        atol=1e-6,
    )
    assert row["teff_log8"] == TEFF_LOG8_SENTINEL
    assert bool(row["teff_log8_is_sentinel"])


def test_to_viewer_records_uses_position_pc_and_teff_log8():
    converted = convert_coordinate_table(
        pd.DataFrame(
            [
                {
                    "name": "fixture",
                    "ra_deg": 0.0,
                    "dec_deg": 0.0,
                    "distance_pc": 10.0,
                    "teff_k": 5800.0,
                }
            ]
        )
    )

    record = to_viewer_records(converted)[0]

    assert record["name"] == "fixture"
    assert record["positionPc"] == [10.0, 0.0, 0.0]
    assert record["teffLog8"] == int(converted["teff_log8"].iloc[0])
    assert record["teffK"] == 5800.0


def test_convert_accepts_explicit_hms_and_dms_columns():
    converted = convert_coordinate_table(
        pd.DataFrame(
            [
                {
                    "ra_hms": "00 00 00",
                    "dec_dms": "+90 00 00",
                    "distance_pc": 5.0,
                }
            ]
        )
    )

    row = converted.iloc[0]
    np.testing.assert_allclose(
        [row["x_icrs_pc"], row["y_icrs_pc"], row["z_icrs_pc"]],
        [0.0, 0.0, 5.0],
        atol=1e-12,
    )


def test_coords_convert_cli_reads_csv_and_emits_viewer_json(tmp_path: Path):
    input_path = tmp_path / "coords.csv"
    input_path.write_text(
        ("name,ra_deg,dec_deg,distance_pc,teff_k\nfixture,0,0,10,5800\n"),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        cli,
        ["coords", "convert", "--input", str(input_path), "--format", "viewer-json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload == [
        {
            "positionPc": [10.0, 0.0, 0.0],
            "teffLog8": int(encode_teff_log8(5800.0)),
            "name": "fixture",
            "teffK": 5800.0,
        }
    ]
