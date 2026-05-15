from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from astropy.table import Table

from foundinspace.pipeline.identifiers.pipeline import (
    _bayer_code_to_display,
    _clean_proper_name,
    prepare_identifiers_sidecar,
)
from foundinspace.pipeline.identifiers.schema import IDENTIFIER_OUTPUT_COLS


def _write_ecsv(df: pd.DataFrame, path: Path) -> None:
    table = Table.from_pandas(df, index=False)
    table.write(path, format="ascii.ecsv", overwrite=True)


def _write_crossmatch(path: Path, rows: list[tuple[int, int]]) -> None:
    df = pd.DataFrame(
        rows,
        columns=["gaia_source_id", "hip_source_id"],
    )
    df["mapping_source"] = "test"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pandas(df, preserve_index=False), str(path), compression="zstd"
    )


def test_bayer_code_display_normalizes_greek_and_suffixes():
    # Standard 3-letter abbreviations
    assert _bayer_code_to_display("alp", "Cas") == "alpha Cas"
    assert _bayer_code_to_display("kap01", "Dra") == "kappa1 Dra"
    assert _bayer_code_to_display("A.", "Ori") == "A Ori"
    assert _bayer_code_to_display("", "Ori") is None

    # IV/27A catalog uses "alf" for alpha and "ksi" for xi
    assert _bayer_code_to_display("alf", "Cen") == "alpha Cen"
    assert _bayer_code_to_display("ksi", "Cet") == "xi Cet"

    # Dotted numeric suffixes: "mu.01" → "mu1", "pi.06" → "pi6"
    assert _bayer_code_to_display("mu.01", "Boo") == "mu1 Boo"
    assert _bayer_code_to_display("nu.02", "CMa") == "nu2 CMa"
    assert _bayer_code_to_display("pi.06", "Ori") == "pi6 Ori"

    # Trailing-dot singles are unchanged: "mu." → "mu Cen"
    assert _bayer_code_to_display("mu.", "Cen") == "mu Cen"


def test_clean_proper_name_strips_semicolons_and_cross_refs():
    s = pd.Series(
        [
            "Prima Giedi; Algiedi Prima; Algedi(with HD192947)",
            "Ruchba(see HD195774)",
            "Deneb Dulfim; Deneb el Delphinus",
            "Misam(also HD19476,158899)",
            "Vega",
            None,
            "",
        ]
    )
    result = _clean_proper_name(s)
    assert result.iloc[0] == "Prima Giedi"
    assert result.iloc[1] == "Ruchba"
    assert result.iloc[2] == "Deneb Dulfim"
    assert result.iloc[3] == "Misam"
    assert result.iloc[4] == "Vega"
    assert pd.isna(result.iloc[5])
    assert pd.isna(result.iloc[6])


def test_prepare_identifiers_sidecar_writes_compound_key_and_gaia_ids(tmp_path: Path):
    hip_hd_path = tmp_path / "hip_hd.ecsv"
    catalog_path = tmp_path / "iv27a_catalog.ecsv"
    proper_path = tmp_path / "iv27a_proper_names.ecsv"
    cross_path = tmp_path / "gaia_hip_map.parquet"
    output_path = tmp_path / "identifiers_map.parquet"

    _write_ecsv(
        pd.DataFrame(
            {
                "HIP": [1, 2],
                "HD": [10, 20],
            }
        ),
        hip_hd_path,
    )
    _write_ecsv(
        pd.DataFrame(
            {
                "HIP": [1, 3, 3, 4],
                "HD": [10, 30, 31, 40],
                "Bayer": ["", "alp", "bet", ""],
                "Fl": [pd.NA, 1, 2, 61],
                "Cst": ["", "Cas", "Cas", "Cyg"],
            }
        ),
        catalog_path,
    )
    _write_ecsv(
        pd.DataFrame(
            {
                "HD": [10, 10, 30, 40],
                "Name": ["Sirius", "Ignored Duplicate", "Schedar", "Deneb"],
            }
        ),
        proper_path,
    )
    _write_crossmatch(
        cross_path,
        [
            (100, 1),
            (300, 3),
            (400, 4),
        ],
    )

    out = prepare_identifiers_sidecar(
        hip_hd_path,
        catalog_path,
        proper_path,
        output_path,
        crossmatch_parquet=cross_path,
        overrides_data_dir=None,
        overwrite=False,
    )
    assert out == output_path
    sidecar = pd.read_parquet(output_path)

    assert list(sidecar.columns) == IDENTIFIER_OUTPUT_COLS
    assert sidecar.loc[sidecar["source_id"] == "1", "proper_name"].iloc[0] == "Sirius"
    assert (
        int(sidecar.loc[sidecar["source_id"] == "1", "gaia_source_id"].iloc[0]) == 100
    )
    assert sidecar.loc[sidecar["source_id"] == "3", "hd"].iloc[0] == 30
    assert sidecar.loc[sidecar["source_id"] == "3", "bayer"].iloc[0] == "alpha Cas"
    assert sidecar.loc[sidecar["source_id"] == "3", "proper_name"].iloc[0] == "Schedar"
    assert (
        int(sidecar.loc[sidecar["source_id"] == "3", "gaia_source_id"].iloc[0]) == 300
    )
    assert sidecar.loc[sidecar["source_id"] == "4", "bayer"].isna().iloc[0]
    assert sidecar.loc[sidecar["source_id"] == "4", "proper_name"].iloc[0] == "Deneb"
    assert (
        int(sidecar.loc[sidecar["source_id"] == "4", "gaia_source_id"].iloc[0]) == 400
    )


def test_prepare_identifiers_merges_override_yaml_identifiers(tmp_path: Path):
    hip_hd_path = tmp_path / "hip_hd.ecsv"
    catalog_path = tmp_path / "iv27a_catalog.ecsv"
    proper_path = tmp_path / "iv27a_proper_names.ecsv"
    output_path = tmp_path / "identifiers_map.parquet"
    ov_dir = tmp_path / "overrides_extra"
    ov_dir.mkdir()

    _write_ecsv(pd.DataFrame({"HIP": [1], "HD": [10]}), hip_hd_path)
    _write_ecsv(
        pd.DataFrame(
            {"HIP": [1], "HD": [10], "Bayer": ["alp"], "Fl": [1], "Cst": ["Cas"]}
        ),
        catalog_path,
    )
    _write_ecsv(pd.DataFrame({"HD": [10], "Name": ["Sirius"]}), proper_path)

    (ov_dir / "extra.yaml").write_text(
        "stars:\n"
        "  - source: manual\n"
        "    source_id: sun\n"
        "    identifiers:\n"
        "      proper_name: Sun\n",
        encoding="utf-8",
    )

    out = prepare_identifiers_sidecar(
        hip_hd_path,
        catalog_path,
        proper_path,
        output_path,
        crossmatch_parquet=None,
        overrides_data_dir=ov_dir,
        overwrite=False,
    )
    assert out == output_path
    sidecar = pd.read_parquet(output_path)
    sun = sidecar.loc[(sidecar["source"] == "manual") & (sidecar["source_id"] == "sun")]
    assert len(sun) == 1
    assert sun["proper_name"].iloc[0] == "Sun"
    assert len(sidecar[sidecar["source"] == "hip"]) >= 1
