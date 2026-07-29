from pathlib import Path

import pandas as pd
import pytest
from astropy.table import Table

from foundinspace.pipeline.overrides.distance_research import (
    DEFAULT_TRACKER_PATH,
    build_evidence_summary,
    build_gaia_details_query,
    build_gaia_hip_xmatch_query,
    build_simbad_aliases,
    build_starhorse_query,
    load_distance_tracker,
    query_gaia,
    query_simbad,
    query_starhorse,
    run_distance_research,
)


def _target_frame() -> pd.DataFrame:
    return pd.DataFrame.from_records(
        [
            {
                "status": "unresolved",
                "official_source": "hip",
                "official_source_id": "42",
                "gaia_source_id": "123456789",
                "hip_source_id": "42",
                "proper_name": "Example",
                "simbad_main_id": "HD 42",
                "current_distance_source": "HIP_EDSD",
                "reason_for_external_search": (
                    "hipparcos_distance_is_generic_edsd_fallback"
                ),
                "next_action": "gaia_cone_search_then_literature_distance",
                "tracker_key": "hip:42",
            }
        ]
    )


def test_packaged_tracker_has_a_cited_distance_for_all_81_stars():
    tracker = load_distance_tracker()

    assert DEFAULT_TRACKER_PATH.is_file()
    assert len(tracker) == 81
    assert tracker["tracker_key"].nunique() == 81
    assert tracker["status"].value_counts().to_dict() == {
        "resolved": 48,
        "provisional": 33,
    }
    assert tracker["simbad_main_id"].str.strip().ne("").all()
    assert tracker["gaia_source_id"].str.strip().ne("").sum() == 80
    for column in (
        "resolved_distance_pc",
        "resolved_distance_error_pc",
        "resolved_reference",
        "resolution_notes",
    ):
        assert tracker[column].str.strip().ne("").all()
    assert pd.to_numeric(tracker["resolved_distance_pc"]).gt(0).all()
    assert pd.to_numeric(tracker["resolved_distance_error_pc"]).gt(0).all()
    assert tracker["official_source"].value_counts().to_dict() == {
        "hip": 53,
        "gaia": 28,
    }
    assert tracker["current_distance_source"].value_counts().to_dict() == {
        "HIP_EDSD": 40,
        "PHOTOGEO_WEAK": 19,
        "GEO_WEAK": 16,
        "PRIOR": 6,
    }
    reasons = tracker["reason_for_external_search"].str.split(";").explode()
    assert reasons.value_counts().to_dict() == {
        "prior_authority_review_likely_bad": 40,
        "hipparcos_distance_is_generic_edsd_fallback": 40,
        "prior_authority_review_unresolved": 17,
        "gaia_distance_is_fixed_magnitude_prior": 6,
        "new_luminous_weak_astrometry": 1,
    }


def test_load_tracker_rejects_duplicate_canonical_identity(tmp_path: Path):
    tracker = pd.read_csv(DEFAULT_TRACKER_PATH, dtype=str, keep_default_na=False)
    duplicate = pd.concat([tracker.head(1), tracker.head(1)], ignore_index=True)
    path = tmp_path / "tracker.csv"
    duplicate.to_csv(path, index=False)

    with pytest.raises(ValueError, match="duplicate target"):
        load_distance_tracker(path)


def test_load_tracker_rejects_researched_target_without_citation(tmp_path: Path):
    tracker = pd.read_csv(DEFAULT_TRACKER_PATH, dtype=str, keep_default_na=False)
    tracker.loc[0, "resolved_reference"] = ""
    path = tmp_path / "tracker.csv"
    tracker.to_csv(path, index=False)

    with pytest.raises(ValueError, match="blank resolved_reference"):
        load_distance_tracker(path)


def test_simbad_aliases_retain_all_catalog_identities():
    aliases = build_simbad_aliases(_target_frame())

    assert aliases.to_dict(orient="records") == [
        {
            "tracker_key": "hip:42",
            "alias_kind": "gaia_dr3",
            "query_name": "Gaia DR3 123456789",
        },
        {
            "tracker_key": "hip:42",
            "alias_kind": "gaia_edr3",
            "query_name": "Gaia EDR3 123456789",
        },
        {
            "tracker_key": "hip:42",
            "alias_kind": "hip",
            "query_name": "HIP 42",
        },
        {
            "tracker_key": "hip:42",
            "alias_kind": "simbad_main_id",
            "query_name": "HD 42",
        },
        {
            "tracker_key": "hip:42",
            "alias_kind": "proper_name",
            "query_name": "Example",
        },
    ]


def test_catalog_queries_use_only_exact_numeric_ids():
    assert "original_ext_source_id IN (42, 100)" in build_gaia_hip_xmatch_query(
        ["100", "42"]
    )
    assert "g.source_id IN (42, 100)" in build_gaia_details_query(["100", "42"])
    starhorse_query = build_starhorse_query(["123456789"])
    assert '"I/354/starhorse2021"' in starhorse_query
    assert 'sh."Source" IN (123456789)' in starhorse_query

    with pytest.raises(ValueError, match="positive integer"):
        build_gaia_details_query(["1); DROP TABLE x"])


def test_query_simbad_preserves_alias_and_bibliography():
    class FakeSimbad:
        fields: tuple[str, ...] = ()

        def add_votable_fields(self, *fields):
            self.fields = fields

        def query_objects(self, names, *, async_job):
            assert async_job is True
            return Table(
                {
                    "user_specified_id": [f"{name}   " for name in names],
                    "main_id": ["HD 42"] * len(names),
                    "plx_value": [2.0] * len(names),
                    "plx_err": [0.2] * len(names),
                    "plx_bibcode": ["2024A&A...000A...1X"] * len(names),
                }
            )

    client = FakeSimbad()
    evidence = query_simbad(_target_frame(), client=client)

    assert client.fields == ("parallax", "sp", "otype", "distance", "ids")
    assert len(evidence) == 5
    assert set(evidence["tracker_key"]) == {"hip:42"}
    assert set(evidence["match_status"]) == {"found"}
    assert set(evidence["plx_snr"]) == {10.0}
    assert set(evidence["plx_bibcode"]) == {"2024A&A...000A...1X"}


def test_query_gaia_uses_best_neighbour_and_bailer_jones():
    class FakeJob:
        def __init__(self, result):
            self.result = result

        def get_results(self):
            return self.result

    class FakeGaia:
        queries: list[str] = []

        @classmethod
        def launch_job_async(cls, query):
            cls.queries.append(query)
            if "hipparcos2_best_neighbour" in query:
                return FakeJob(
                    Table(
                        {
                            "source_id": [123456789],
                            "original_ext_source_id": [42],
                            "angular_distance": [0.1],
                            "number_of_neighbours": [1],
                        }
                    )
                )
            return FakeJob(
                Table(
                    {
                        "source_id": [123456789],
                        "ra": [10.0],
                        "dec": [-20.0],
                        "parallax": [2.0],
                        "parallax_error": [0.2],
                        "ruwe": [1.1],
                        "r_med_geo": [505.0],
                        "r_lo_geo": [450.0],
                        "r_hi_geo": [570.0],
                        "r_med_photogeo": [500.0],
                        "r_lo_photogeo": [460.0],
                        "r_hi_photogeo": [550.0],
                    }
                )
            )

    evidence = query_gaia(_target_frame(), client=FakeGaia)

    assert len(FakeGaia.queries) == 2
    assert len(evidence) == 1
    row = evidence.iloc[0]
    assert row["tracker_key"] == "hip:42"
    assert row["source_id"] == "123456789"
    assert row["match_kind"] == "tracker_gaia_id"
    assert row["r_med_photogeo"] == 500.0
    assert row["parallax_snr"] == 10.0


def test_query_gaia_uses_simbad_cross_identifier_when_official_map_misses():
    target = _target_frame().copy()
    target["gaia_source_id"] = ""
    simbad = pd.DataFrame(
        {
            "tracker_key": ["hip:42"],
            "match_status": ["found"],
            "ids": ["HIP 42|Gaia DR3 987654321|HD 42"],
        }
    )

    class FakeJob:
        def __init__(self, result):
            self.result = result

        def get_results(self):
            return self.result

    class FakeGaia:
        @staticmethod
        def launch_job_async(query):
            if "hipparcos2_best_neighbour" in query:
                return FakeJob(
                    Table(
                        names=(
                            "source_id",
                            "original_ext_source_id",
                            "angular_distance",
                            "number_of_neighbours",
                        )
                    )
                )
            assert "987654321" in query
            return FakeJob(
                Table(
                    {
                        "source_id": [987654321],
                        "ra": [10.0],
                        "dec": [-20.0],
                        "parallax": [2.0],
                        "parallax_error": [0.2],
                    }
                )
            )

    evidence = query_gaia(
        target,
        simbad_evidence=simbad,
        client=FakeGaia,
    )

    assert evidence.loc[0, "source_id"] == "987654321"
    assert evidence.loc[0, "match_kind"] == "simbad_gaia_dr3_id"


def test_query_starhorse_converts_kpc_percentiles_to_pc():
    class FakeJob:
        def get_results(self):
            return Table(
                {
                    "tracker_key": ["hip:42"],
                    "requested_gaia_source_id": [123456789],
                    "Source": [123456789],
                    "dist16": [0.45],
                    "dist50": [0.5],
                    "dist84": [0.56],
                    "fidelity": [0.9],
                }
            )

    class FakeTap:
        def launch_job(self, query):
            assert '"I/354/starhorse2021"' in query
            assert 'sh."Source" IN (123456789)' in query
            return FakeJob()

    evidence = query_starhorse(_target_frame(), pd.DataFrame(), client=FakeTap())

    row = evidence.iloc[0]
    assert row["match_status"] == "found"
    assert row["dist16_pc"] == 450.0
    assert row["dist50_pc"] == 500.0
    assert row["dist84_pc"] == 560.0


def test_summary_never_allows_automatic_override_promotion():
    targets = _target_frame()
    simbad = pd.DataFrame(
        {
            "tracker_key": ["hip:42"],
            "match_status": ["found"],
            "main_id": ["HD 42"],
            "plx_value": [2.0],
            "plx_err": [0.2],
            "plx_snr": [10.0],
            "plx_bibcode": ["2024A&A...000A...1X"],
        }
    )
    starhorse = pd.DataFrame(
        {
            "tracker_key": ["hip:42"],
            "match_status": ["found"],
            "source": ["123456789"],
            "dist50_pc": [500.0],
        }
    )

    summary = build_evidence_summary(
        targets, simbad=simbad, gaia=pd.DataFrame(), starhorse=starhorse
    )

    assert len(summary) == 1
    assert summary.loc[0, "evidence_status"] == "simbad_and_starhorse"
    assert bool(summary.loc[0, "automatic_promotion_allowed"]) is False


def test_runner_can_validate_and_manifest_without_network(tmp_path: Path):
    manifest = run_distance_research(
        output_dir=tmp_path,
        services=(),
        limit=2,
    )

    assert manifest["target_count"] == 2
    assert manifest["error_count"] == 0
    assert manifest["automatic_override_promotion"] is False
    summary = pd.read_csv(tmp_path / "distance-evidence-summary.csv")
    assert len(summary) == 2
    assert set(summary["evidence_status"]) == {"no_catalog_distance"}
