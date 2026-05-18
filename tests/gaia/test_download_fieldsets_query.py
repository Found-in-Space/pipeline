from __future__ import annotations

import pytest

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


def test_download_query_core_astrophysical_fields_match_active_pipeline() -> None:
    spec = GaiaQuerySpec(mode="full", mag_limit=None, carry_fields=())

    query = build_download_query(spec, hp3_values=(1,))

    for field_name in (
        "mg_gspphot",
        "mg_gspphot_lower",
        "mg_gspphot_upper",
        "ag_gspphot",
        "teff_esphs",
        "teff_gspspec",
        "teff_espucd",
        "teff_gspphot",
    ):
        assert f"ap.{field_name} AS {field_name}" in query

    for field_name in (
        "teff_gspphot_lower",
        "teff_gspphot_upper",
        "logg_esphs",
        "logg_gspspec",
        "logg_gspphot",
        "logg_gspphot_lower",
        "logg_gspphot_upper",
    ):
        assert field_name not in query


def test_source_where_adql_is_added_to_count_and_download_queries() -> None:
    source_filter = """
    g.phot_g_mean_mag <= 15
    OR d.r_med_photogeo <= 100
    OR d.r_med_geo <= 100
    """
    spec = GaiaQuerySpec(
        mode="small",
        mag_limit=None,
        carry_fields=(),
        source_where_adql=source_filter,
    )

    count_query = build_count_query(spec)
    download_query = build_download_query(spec, hp3_values=(7,))

    expected = (
        "(g.phot_g_mean_mag <= 15\n"
        "    OR d.r_med_photogeo <= 100\n"
        "    OR d.r_med_geo <= 100)"
    )
    assert expected in count_query
    assert expected in download_query
    assert "(g.source_id / 9007199254740992) IN (7)" in download_query


def test_source_where_adql_rejects_semicolons() -> None:
    spec = GaiaQuerySpec(
        mode="small",
        mag_limit=None,
        carry_fields=(),
        source_where_adql="g.phot_g_mean_mag <= 15; DROP TABLE x",
    )

    with pytest.raises(ValueError, match="source_where_adql"):
        build_count_query(spec)


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
