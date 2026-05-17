from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from foundinspace.pipeline.merge import shards

SIDECAR_ID_COLS = ["source", "source_id", "gaia_source_id"]

_MOTION_COLUMN_MAP = {
    "gaia_ref_epoch": "ref_epoch",
    "gaia_pmra_masyr": "pmra_masyr",
    "gaia_pmdec_masyr": "pmdec_masyr",
    "gaia_pm_masyr": "pm_total_masyr",
    "gaia_pmra_error_masyr": "pmra_error_masyr",
    "gaia_pmdec_error_masyr": "pmdec_error_masyr",
    "gaia_pmra_pmdec_corr": "pmra_pmdec_corr",
    "gaia_parallax_mas": "parallax_mas",
    "gaia_parallax_error_mas": "parallax_error_mas",
    "gaia_bp_rp": "bp_rp",
    "gaia_photometry_quality": "photometry_quality",
    "gaia_distance_use_pc": "distance_use_pc",
    "gaia_r_lo_pc": "r_lo_pc",
    "gaia_r_hi_pc": "r_hi_pc",
    "gaia_radial_velocity_kms": "radial_velocity_kms",
    "gaia_radial_velocity_error_kms": "radial_velocity_error_kms",
    "gaia_rv_method_used": "rv_method_used",
    "gaia_rv_nb_transits": "rv_nb_transits",
    "gaia_rv_visibility_periods_used": "rv_visibility_periods_used",
    "gaia_rv_expected_sig_to_noise": "rv_expected_sig_to_noise",
    "gaia_rv_renormalised_gof": "rv_renormalised_gof",
    "gaia_rv_chisq_pvalue": "rv_chisq_pvalue",
    "gaia_rv_amplitude_robust": "rv_amplitude_robust",
}

_MASS_COLUMN_MAP = {
    "gaia_mass_flame_solar": "mass_flame_solar",
    "gaia_mass_flame_lower_solar": "mass_flame_lower_solar",
    "gaia_mass_flame_upper_solar": "mass_flame_upper_solar",
    "gaia_mass_flame_spec_solar": "mass_flame_spec_solar",
    "gaia_mass_flame_spec_lower_solar": "mass_flame_spec_lower_solar",
    "gaia_mass_flame_spec_upper_solar": "mass_flame_spec_upper_solar",
    "gaia_lum_flame_solar": "lum_flame_solar",
    "gaia_lum_flame_lower_solar": "lum_flame_lower_solar",
    "gaia_lum_flame_upper_solar": "lum_flame_upper_solar",
    "gaia_lum_flame_spec_solar": "lum_flame_spec_solar",
    "gaia_lum_flame_spec_lower_solar": "lum_flame_spec_lower_solar",
    "gaia_lum_flame_spec_upper_solar": "lum_flame_spec_upper_solar",
    "gaia_radius_flame_solar": "radius_flame_solar",
    "gaia_radius_flame_lower_solar": "radius_flame_lower_solar",
    "gaia_radius_flame_upper_solar": "radius_flame_upper_solar",
    "gaia_radius_flame_spec_solar": "radius_flame_spec_solar",
    "gaia_radius_flame_spec_lower_solar": "radius_flame_spec_lower_solar",
    "gaia_radius_flame_spec_upper_solar": "radius_flame_spec_upper_solar",
    "gaia_age_flame_gyr": "age_flame_gyr",
    "gaia_age_flame_lower_gyr": "age_flame_lower_gyr",
    "gaia_age_flame_upper_gyr": "age_flame_upper_gyr",
    "gaia_age_flame_spec_gyr": "age_flame_spec_gyr",
    "gaia_age_flame_spec_lower_gyr": "age_flame_spec_lower_gyr",
    "gaia_age_flame_spec_upper_gyr": "age_flame_spec_upper_gyr",
    "gaia_bc_flame": "bc_flame",
    "gaia_teff_gspphot": "teff_gspphot",
    "gaia_logg_gspphot": "logg_gspphot",
    "gaia_mh_gspphot": "mh_gspphot",
    "gaia_ag_gspphot": "ag_gspphot",
    "gaia_flags_flame": "flags_flame",
    "gaia_evolstage_flame": "evolstage_flame",
    "gaia_flags_flame_spec": "flags_flame_spec",
    "gaia_evolstage_flame_spec": "evolstage_flame_spec",
    "gaia_bc_flame_spec": "bc_flame_spec",
}

_QUALITY_COLUMN_MAP = {
    "gaia_phot_g_mean_mag": "phot_g_mean_mag",
    "gaia_ruwe": "ruwe",
    "gaia_parallax_over_error": "parallax_over_error",
    "gaia_astrometric_params_solved": "astrometric_params_solved",
    "gaia_visibility_periods_used": "visibility_periods_used",
    "gaia_astrometric_excess_noise": "astrometric_excess_noise",
    "gaia_astrometric_excess_noise_sig": "astrometric_excess_noise_sig",
    "gaia_astrometric_sigma5d_max": "astrometric_sigma5d_max",
    "gaia_astrometric_chi2_al": "astrometric_chi2_al",
    "gaia_astrometric_n_good_obs_al": "astrometric_n_good_obs_al",
    "gaia_astrometric_n_bad_obs_al": "astrometric_n_bad_obs_al",
    "gaia_ipd_gof_harmonic_amplitude": "ipd_gof_harmonic_amplitude",
    "gaia_ipd_frac_multi_peak": "ipd_frac_multi_peak",
    "gaia_ipd_frac_odd_win": "ipd_frac_odd_win",
    "gaia_duplicated_source": "duplicated_source",
    "gaia_non_single_star": "non_single_star",
    "gaia_phot_bp_rp_excess_factor": "phot_bp_rp_excess_factor",
    "gaia_phot_variable_flag": "phot_variable_flag",
}


def gaia_enrichment_columns(columns: list[str] | set[str]) -> list[str]:
    return [
        column
        for column in columns
        if column.startswith("gaia_") and column != "gaia_source_id"
    ]


def from_frames(
    gaia_df: pd.DataFrame,
    dense_df: pd.DataFrame,
    *,
    enrichment_cols: list[str],
) -> pd.DataFrame:
    if not enrichment_cols or gaia_df.empty or dense_df.empty:
        return pd.DataFrame()
    gaia = gaia_df.reset_index(drop=True)
    dense = dense_df.reset_index(drop=True)
    out = pd.DataFrame(
        {
            "source": dense["source"].astype("string"),
            "source_id": dense["source_id"].astype("string"),
            "gaia_source_id": pd.to_numeric(gaia["source_id"], errors="raise").astype(
                "uint64"
            ),
            "_shard_ra_deg": pd.to_numeric(dense["ra_deg"], errors="coerce"),
            "_shard_dec_deg": pd.to_numeric(dense["dec_deg"], errors="coerce"),
        }
    )
    for col in enrichment_cols:
        out[col] = gaia[col].reset_index(drop=True) if col in gaia.columns else pd.NA
    return out


def from_records(
    gaia_records: list[dict[str, Any]],
    dense_records: list[dict[str, Any]],
    *,
    enrichment_cols: list[str],
) -> pd.DataFrame:
    if not enrichment_cols or not gaia_records or not dense_records:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    for gaia_rec, dense_rec in zip(gaia_records, dense_records, strict=True):
        row = {
            "source": str(dense_rec["source"]),
            "source_id": str(dense_rec["source_id"]),
            "gaia_source_id": int(gaia_rec["source_id"]),
            "_shard_ra_deg": dense_rec["ra_deg"],
            "_shard_dec_deg": dense_rec["dec_deg"],
        }
        for col in enrichment_cols:
            row[col] = gaia_rec.get(col, pd.NA)
        rows.append(row)
    return pd.DataFrame(rows)


def write_gaia_sidecars(
    sidecar_df: pd.DataFrame,
    *,
    hp: Any,
    sidecar_root: Path,
    phase_tag: str,
    seq_by_key: dict[tuple[str, int], int],
) -> int:
    if sidecar_df.empty:
        return 0
    enrichment_cols = gaia_enrichment_columns(list(sidecar_df.columns))
    if not enrichment_cols:
        return 0

    rows = shards._write_sidecar_shards(
        sidecar_df,
        hp=hp,
        sidecar_root=sidecar_root,
        sidecar_name="gaia_enrichment",
        phase_tag=phase_tag,
        output_cols=SIDECAR_ID_COLS + enrichment_cols,
        seq_by_key=seq_by_key,
    )
    rows += _write_derived_sidecar(
        sidecar_df,
        hp=hp,
        sidecar_root=sidecar_root,
        sidecar_name="motion",
        column_map=_MOTION_COLUMN_MAP,
        phase_tag=phase_tag,
        seq_by_key=seq_by_key,
    )
    rows += _write_derived_sidecar(
        sidecar_df,
        hp=hp,
        sidecar_root=sidecar_root,
        sidecar_name="mass",
        column_map=_MASS_COLUMN_MAP,
        phase_tag=phase_tag,
        seq_by_key=seq_by_key,
    )
    rows += _write_derived_sidecar(
        sidecar_df,
        hp=hp,
        sidecar_root=sidecar_root,
        sidecar_name="quality",
        column_map=_QUALITY_COLUMN_MAP,
        phase_tag=phase_tag,
        seq_by_key=seq_by_key,
    )
    return rows


def _write_derived_sidecar(
    sidecar_df: pd.DataFrame,
    *,
    hp: Any,
    sidecar_root: Path,
    sidecar_name: str,
    column_map: dict[str, str],
    phase_tag: str,
    seq_by_key: dict[tuple[str, int], int],
) -> int:
    available = [
        (src, dst) for src, dst in column_map.items() if src in sidecar_df.columns
    ]
    if not available:
        return 0
    out = sidecar_df[[*SIDECAR_ID_COLS, "_shard_ra_deg", "_shard_dec_deg"]].copy()
    output_cols = list(SIDECAR_ID_COLS)
    for src, dst in available:
        out[dst] = sidecar_df[src]
        output_cols.append(dst)
    return shards._write_sidecar_shards(
        out,
        hp=hp,
        sidecar_root=sidecar_root,
        sidecar_name=sidecar_name,
        phase_tag=phase_tag,
        output_cols=output_cols,
        seq_by_key=seq_by_key,
    )
