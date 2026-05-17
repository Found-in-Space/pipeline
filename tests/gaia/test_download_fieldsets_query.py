from __future__ import annotations

from foundinspace.pipeline.gaia.download.fieldsets import (
    fields_by_sidecar,
    gaia_enrichment_columns,
    load_gaia_field_sets,
)
from foundinspace.pipeline.gaia.download.query import (
    GaiaQuerySpec,
    build_count_query,
    build_download_query,
)


def test_load_gaia_field_sets_defaults_and_enrichment_columns() -> None:
    fields = load_gaia_field_sets(("motion", "mass"))

    names = {field.name for field in fields}
    assert "ref_epoch" in names
    assert "mass_flame_solar" in names
    assert "age_flame_gyr" in names
    assert "bc_flame" in names
    assert "mass_flame_spec_solar" not in names
    assert "age_flame_spec_gyr" not in names
    assert "gaia_ref_epoch" in gaia_enrichment_columns(fields)
    assert "gaia_age_flame_gyr" in gaia_enrichment_columns(fields)

    grouped = fields_by_sidecar(fields)
    assert "mass" in grouped
    assert "motion" in grouped


def test_download_query_joins_aps_only_when_field_set_needs_it() -> None:
    base_fields = load_gaia_field_sets(("motion", "mass"))
    spec = GaiaQuerySpec(mode="small", mag_limit=9.0, carry_fields=base_fields)

    query = build_download_query(spec, hp3_values=(7, 3))

    assert "g.ref_epoch AS ref_epoch" in query
    assert "ap.mass_flame AS mass_flame_solar" in query
    assert "ap.age_flame AS age_flame_gyr" in query
    assert "ap.bc_flame AS bc_flame" in query
    assert "astrophysical_parameters_supp" not in query
    assert "g.phot_g_mean_mag <= 9" in query
    assert "IN (3, 7)" in query

    spec_with_supp = GaiaQuerySpec(
        mode="full",
        mag_limit=None,
        carry_fields=load_gaia_field_sets(("mass_spec",)),
    )
    query_with_supp = build_download_query(spec_with_supp, hp3_values=(1,))
    assert "astrophysical_parameters_supp AS aps" in query_with_supp
    assert "aps.mass_flame_spec AS mass_flame_spec_solar" in query_with_supp
    assert "aps.age_flame_spec AS age_flame_spec_gyr" in query_with_supp


def test_download_query_can_omit_healpix_filter_for_browser_export() -> None:
    fields = load_gaia_field_sets(("motion", "mass"))
    spec = GaiaQuerySpec(mode="small", mag_limit=9.0, carry_fields=fields)

    query = build_download_query(spec)

    assert "g.phot_g_mean_mag <= 9" in query
    assert "(g.source_id / 9007199254740992) IN" not in query
    assert "g.ref_epoch AS ref_epoch" in query


def test_quality_field_set_carries_gaia_qc_diagnostics() -> None:
    fields = load_gaia_field_sets(("quality",))

    names = {field.name for field in fields}
    assert "ruwe" in names
    assert "astrometric_params_solved" in names
    assert "ipd_gof_harmonic_amplitude" in names
    assert "phot_variable_flag" in names
    assert "quality" in fields_by_sidecar(fields)
    assert "gaia_astrometric_params_solved" in gaia_enrichment_columns(fields)

    spec = GaiaQuerySpec(mode="full", mag_limit=None, carry_fields=fields)
    query = build_download_query(spec, hp3_values=(1,))

    assert "g.astrometric_params_solved AS astrometric_params_solved" in query
    assert "g.ipd_gof_harmonic_amplitude AS ipd_gof_harmonic_amplitude" in query
    assert "g.phot_variable_flag AS phot_variable_flag" in query
    assert "astrophysical_parameters_supp" not in query


def test_count_query_uses_same_source_filter_without_enrichment_joins() -> None:
    spec = GaiaQuerySpec(
        mode="small",
        mag_limit=9.0,
        carry_fields=load_gaia_field_sets(("mass_spec",)),
    )

    query = build_count_query(spec)

    assert "COUNT(*) AS n" in query
    assert "g.phot_g_mean_mag <= 9" in query
    assert "astrophysical_parameters_supp" not in query
    assert "GROUP BY 1" in query
