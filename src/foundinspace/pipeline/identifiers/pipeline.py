"""Prepare a wide star-identifier sidecar keyed by (source, source_id)."""

from __future__ import annotations

import re
from contextlib import suppress
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from astropy.table import Table

# Parquet output: compound key + cross-catalog and display identifiers.
IDENTIFIER_OUTPUT_COLS = [
    "source",
    "source_id",
    "gaia_source_id",
    "hip_id",
    "hd",
    "bayer",
    "flamsteed",
    "constellation",
    "proper_name",
]

# Internal Vizier merge uses these working column names before reshaping.
_VIZIER_WORK_COLS = ["hip_source_id", "hd", "bayer", "fl", "cst", "proper_name"]

_BAYER_GREEK_MAP = {
    "alf": "alpha",
    "alp": "alpha",
    "bet": "beta",
    "gam": "gamma",
    "del": "delta",
    "eps": "epsilon",
    "zet": "zeta",
    "eta": "eta",
    "the": "theta",
    "iot": "iota",
    "kap": "kappa",
    "ksi": "xi",
    "lam": "lambda",
    "mu": "mu",
    "nu": "nu",
    "xi": "xi",
    "omi": "omicron",
    "pi": "pi",
    "rho": "rho",
    "sig": "sigma",
    "tau": "tau",
    "ups": "upsilon",
    "phi": "phi",
    "chi": "chi",
    "psi": "psi",
    "ome": "omega",
}


def _read_ecsv(input_path: Path) -> pd.DataFrame:
    table = Table.read(input_path, format="ascii.ecsv")
    return table.to_pandas()


def _coerce_positive_int(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    values = numeric.to_numpy(dtype=float, na_value=np.nan, copy=False)
    finite = np.isfinite(values)
    integral = np.equal(np.floor(values), values)
    positive = values > 0
    valid = finite & integral & positive
    out = pd.Series(pd.NA, index=series.index, dtype="Int64")
    if np.any(valid):
        out.loc[valid] = values[valid].astype("int64")
    return out


def _clean_text(series: pd.Series) -> pd.Series:
    out = series.astype("string").str.strip()
    out = out.mask(out == "", pd.NA)
    return out


def _clean_proper_name(series: pd.Series) -> pd.Series:
    """Take the first name from semicolon-separated lists and strip parenthetical cross-refs."""
    out = series.astype("string").str.strip()
    out = out.str.split(";").str[0]
    out = out.str.replace(r"\s*\(.*$", "", regex=True)
    out = out.str.strip()
    out = out.mask(out == "", pd.NA)
    return out


def _bayer_code_to_display(bayer_code: str | None, constellation: str | None) -> str | None:
    if bayer_code is None or pd.isna(bayer_code):
        return None
    base = str(bayer_code).strip().rstrip(".")
    if not base:
        return None

    # Normalise dotted numeric suffixes: "mu.01" → "mu01", "pi.06" → "pi06"
    dotted = re.match(r"^([A-Za-z]+)\.(\d+)$", base)
    if dotted is not None:
        base = dotted.group(1) + dotted.group(2)

    match = re.match(r"^([A-Za-z]+)(\d*)$", base)
    if match is None:
        bayer_root = base
    else:
        letters, suffix = match.groups()
        greek = _BAYER_GREEK_MAP.get(letters.lower())
        if greek is None:
            bayer_root = letters
        elif suffix:
            bayer_root = f"{greek}{int(suffix)}"
        else:
            bayer_root = greek

    if constellation is None or pd.isna(constellation):
        cst = None
    else:
        cst = str(constellation).strip()
    if cst:
        return f"{bayer_root} {cst}"
    return bayer_root


def _empty_identifiers_out() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "source": pd.Series(dtype="string"),
            "source_id": pd.Series(dtype="string"),
            "gaia_source_id": pd.Series(dtype="Int64"),
            "hip_id": pd.Series(dtype="Int64"),
            "hd": pd.Series(dtype="Int64"),
            "bayer": pd.Series(dtype="string"),
            "flamsteed": pd.Series(dtype="Int64"),
            "constellation": pd.Series(dtype="string"),
            "proper_name": pd.Series(dtype="string"),
        }
    )


def _prepare_vizier_identifier_rows(
    hip_hd_df: pd.DataFrame,
    iv27a_catalog_df: pd.DataFrame,
    iv27a_proper_names_df: pd.DataFrame,
) -> pd.DataFrame:
    hip_hd = hip_hd_df.copy()
    hip_hd.columns = [str(c).strip() for c in hip_hd.columns]
    hip_hd["hip_source_id"] = _coerce_positive_int(hip_hd.get("HIP", pd.Series(pd.NA)))
    hip_hd["hd"] = _coerce_positive_int(hip_hd.get("HD", pd.Series(pd.NA)))
    hip_to_hd = (
        hip_hd.loc[hip_hd["hip_source_id"].notna() & hip_hd["hd"].notna(), ["hip_source_id", "hd"]]
        .drop_duplicates(subset=["hip_source_id"], keep="first")
        .reset_index(drop=True)
    )

    names = iv27a_proper_names_df.copy()
    names.columns = [str(c).strip() for c in names.columns]
    names["hd"] = _coerce_positive_int(names.get("HD", pd.Series(pd.NA)))
    names["proper_name"] = _clean_proper_name(names.get("Name", pd.Series(pd.NA)))
    hd_to_proper = (
        names.loc[names["hd"].notna() & names["proper_name"].notna(), ["hd", "proper_name"]]
        .drop_duplicates(subset=["hd"], keep="first")
        .reset_index(drop=True)
    )

    catalog = iv27a_catalog_df.copy()
    catalog.columns = [str(c).strip() for c in catalog.columns]
    catalog["hip_source_id"] = _coerce_positive_int(catalog.get("HIP", pd.Series(pd.NA)))
    catalog["hd"] = _coerce_positive_int(catalog.get("HD", pd.Series(pd.NA)))
    catalog["fl"] = _coerce_positive_int(catalog.get("Fl", pd.Series(pd.NA)))
    catalog["cst"] = _clean_text(catalog.get("Cst", pd.Series(pd.NA)))
    catalog["bayer_raw"] = _clean_text(catalog.get("Bayer", pd.Series(pd.NA)))
    catalog["bayer"] = [
        _bayer_code_to_display(bayer, cst)
        for bayer, cst in zip(catalog["bayer_raw"], catalog["cst"], strict=True)
    ]
    catalog["bayer"] = pd.Series(catalog["bayer"], index=catalog.index, dtype="string")
    catalog.loc[catalog["bayer"] == "", "bayer"] = pd.NA

    catalog_hip = catalog.loc[
        catalog["hip_source_id"].notna(),
        ["hip_source_id", "hd", "bayer", "fl", "cst"],
    ].drop_duplicates(subset=["hip_source_id"], keep="first")

    merged = catalog_hip.merge(
        hip_to_hd.rename(columns={"hd": "hd_from_hip_main"}),
        on="hip_source_id",
        how="outer",
    )
    merged["hd"] = merged["hd_from_hip_main"].where(
        merged["hd_from_hip_main"].notna(),
        merged["hd"],
    )
    merged = merged.drop(columns=["hd_from_hip_main"])

    merged = merged.merge(hd_to_proper, on="hd", how="left")
    merged = merged.loc[
        merged["bayer"].notna() | merged["proper_name"].notna(),
        _VIZIER_WORK_COLS,
    ].copy()

    if merged.empty:
        return _empty_identifiers_out()

    merged["hip_source_id"] = merged["hip_source_id"].astype("uint64")
    merged["hd"] = merged["hd"].astype("Int64")
    merged["fl"] = merged["fl"].astype("Int64")
    merged["bayer"] = merged["bayer"].astype("string")
    merged["cst"] = merged["cst"].astype("string")
    merged["proper_name"] = merged["proper_name"].astype("string")

    merged = merged.rename(columns={"fl": "flamsteed", "cst": "constellation"})
    merged["source"] = pd.Series("hip", index=merged.index, dtype="string")
    merged["source_id"] = merged["hip_source_id"].astype("string")
    merged["hip_id"] = merged["hip_source_id"].astype("Int64")
    merged = merged.drop(columns=["hip_source_id"])
    merged["gaia_source_id"] = pd.Series(pd.NA, index=merged.index, dtype="Int64")
    return merged[IDENTIFIER_OUTPUT_COLS]


def _load_hip_to_gaia(crossmatch_parquet: Path) -> dict[int, int]:
    df = pq.read_table(crossmatch_parquet).to_pandas()
    if "gaia_source_id" not in df.columns or "hip_source_id" not in df.columns:
        return {}
    out: dict[int, int] = {}
    for _, row in df[["gaia_source_id", "hip_source_id"]].iterrows():
        try:
            g = int(row["gaia_source_id"])
            h = int(row["hip_source_id"])
        except (TypeError, ValueError):
            continue
        out[h] = g
    return out


def _override_identifier_rows(data_dir: Path | None) -> pd.DataFrame:
    from foundinspace.pipeline.overrides.loader import load_parsed_override_documents

    rows: list[dict[str, Any]] = []
    for doc in load_parsed_override_documents(data_dir=data_dir):
        stars = doc.get("stars")
        if not isinstance(stars, list):
            continue
        for star in stars:
            if not isinstance(star, dict):
                continue
            ident = star.get("identifiers")
            if not ident or not isinstance(ident, dict):
                continue
            src = str(star.get("source", "")).strip().lower()
            sid_raw = star.get("source_id")
            if sid_raw is None:
                continue
            sid = str(sid_raw).strip()
            row: dict[str, Any] = {
                "source": src,
                "source_id": sid,
                "gaia_source_id": pd.NA,
                "hip_id": pd.NA,
                "hd": pd.NA,
                "bayer": pd.NA,
                "flamsteed": pd.NA,
                "constellation": pd.NA,
                "proper_name": pd.NA,
            }
            int_keys = ("gaia_source_id", "hip_id", "hd", "flamsteed")
            str_keys = ("bayer", "constellation", "proper_name")
            for k in int_keys:
                if k in ident and ident[k] is not None and ident[k] != "":
                    with suppress(TypeError, ValueError):
                        row[k] = int(ident[k])
            for k in str_keys:
                if k in ident and ident[k] is not None and str(ident[k]).strip() != "":
                    row[k] = str(ident[k]).strip()
            rows.append(row)

    if not rows:
        return _empty_identifiers_out()

    df = pd.DataFrame(rows)
    df["source"] = df["source"].astype("string")
    df["source_id"] = df["source_id"].astype("string")
    for c in ("gaia_source_id", "hip_id", "hd", "flamsteed"):
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    for c in ("bayer", "constellation", "proper_name"):
        df[c] = df[c].astype("string")
    return df[IDENTIFIER_OUTPUT_COLS]


def prepare_identifiers_sidecar(
    hip_hd_path: Path,
    iv27a_catalog_path: Path,
    iv27a_proper_names_path: Path,
    output_path: Path,
    *,
    crossmatch_parquet: Path | None = None,
    overrides_data_dir: Path | None = None,
    overwrite: bool = False,
) -> Path:
    output_path = Path(output_path).expanduser()
    if output_path.exists() and not overwrite:
        raise FileExistsError(str(output_path))

    for path in (hip_hd_path, iv27a_catalog_path, iv27a_proper_names_path):
        if not Path(path).is_file():
            raise FileNotFoundError(
                f"Missing required identifier catalog input: {path}."
                " Run `fis-pipeline identifiers download` first."
            )

    vizier_df = _prepare_vizier_identifier_rows(
        _read_ecsv(Path(hip_hd_path)),
        _read_ecsv(Path(iv27a_catalog_path)),
        _read_ecsv(Path(iv27a_proper_names_path)),
    )

    hip_to_gaia: dict[int, int] = {}
    if crossmatch_parquet is not None:
        cm = Path(crossmatch_parquet).expanduser()
        if cm.is_file():
            hip_to_gaia = _load_hip_to_gaia(cm)

    if not vizier_df.empty and hip_to_gaia:
        hip_ids = pd.to_numeric(vizier_df["hip_id"], errors="coerce")
        vizier_df = vizier_df.copy()
        mapped = hip_ids.map(hip_to_gaia)
        vizier_df["gaia_source_id"] = mapped.astype("Int64")

    override_df = _override_identifier_rows(overrides_data_dir)

    if vizier_df.empty and override_df.empty:
        combined = _empty_identifiers_out()
    elif override_df.empty:
        combined = vizier_df
    elif vizier_df.empty:
        combined = override_df
    else:
        combined = pd.concat([vizier_df, override_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["source", "source_id"], keep="last")

    if not combined.empty:
        combined = combined.sort_values(
            by=["source", "source_id"],
            kind="mergesort",
            ignore_index=True,
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(combined[IDENTIFIER_OUTPUT_COLS], preserve_index=False)
    pq.write_table(table, str(output_path), compression="zstd")
    return output_path
