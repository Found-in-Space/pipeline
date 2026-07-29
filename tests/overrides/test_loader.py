from pathlib import Path

import numpy as np

from foundinspace.pipeline.overrides.loader import (
    icrs_spherical_to_cartesian_pc,
    iter_override_source_files,
    load_normalized_override_stars,
    load_override_source_texts,
)


def test_iter_override_source_files_returns_sorted_yaml_only(tmp_path: Path):
    (tmp_path / "z.yaml").write_text("stars: []\n", encoding="utf-8")
    (tmp_path / "a.yml").write_text("stars: []\n", encoding="utf-8")
    (tmp_path / "ignore.txt").write_text("not yaml\n", encoding="utf-8")

    files = iter_override_source_files(tmp_path)
    names = [p.name for p in files]
    assert names == ["a.yml", "z.yaml"]


def test_iter_override_source_files_uses_explicit_paths_in_given_order(
    tmp_path: Path,
):
    first = tmp_path / "first.yaml"
    second = tmp_path / "second.yaml"
    first.write_text("stars: []\n", encoding="utf-8")
    second.write_text("stars: []\n", encoding="utf-8")

    files = iter_override_source_files(source_paths=(second, first))

    assert files == [second, first]


def test_load_override_source_texts_reads_packaged_yaml():
    sources = load_override_source_texts()
    assert set(sources) == {"sun.yaml"}
    assert "manual.sun.add.v1" in sources["sun.yaml"]


def test_load_normalized_override_stars_fills_cartesian_for_spherical_fixture(
    tmp_path: Path,
):
    (tmp_path / "fixture.yaml").write_text(
        (
            "stars:\n"
            "  - override_id: fixture.star.replace\n"
            "    action: replace\n"
            "    source: manual\n"
            "    source_id: fixture-star\n"
            "    override_reason: fixture\n"
            "    override_policy_version: fixture\n"
            "    ra_deg: 45.0\n"
            "    dec_deg: 30.0\n"
            "    r_pc: 10.0\n"
        ),
        encoding="utf-8",
    )
    stars = load_normalized_override_stars(tmp_path)
    b = next(s for s in stars if s["override_id"] == "fixture.star.replace")
    ex, ey, ez = icrs_spherical_to_cartesian_pc(
        float(b["ra_deg"]),
        float(b["dec_deg"]),
        float(b["r_pc"]),
    )
    assert np.allclose([b["x_icrs_pc"], b["y_icrs_pc"], b["z_icrs_pc"]], [ex, ey, ez])


def test_sun_is_sole_packaged_override_with_checked_provenance():
    stars = load_normalized_override_stars()
    assert len(stars) == 1
    sun = stars[0]
    assert sun["override_id"] == "manual.sun.add.v1"
    assert sun["action"] == "add"
    assert sun["source"] == "manual"
    assert sun["source_id"] == "sun"
    assert sun["x_icrs_pc"] == 0.0
    assert sun["y_icrs_pc"] == 0.0
    assert sun["z_icrs_pc"] == 0.0
    assert sun["ra_deg"] == 0.0
    assert sun["dec_deg"] == 0.0
    assert sun["r_pc"] == 0.0
    assert sun["mag_abs"] == 4.81
    assert sun["mag_band"] == "Johnson_V"
    assert sun["mag_system"] == "vegamag"
    assert sun["teff"] == 5772

    provenance = sun["provenance"]
    assert provenance["astrometry"]["value_kind"] == "synthetic_dataset_convention"
    assert (
        provenance["absolute_magnitude"]["source"]["doi"] == "10.3847/1538-4365/aabfdf"
    )
    assert (
        provenance["effective_temperature"]["value_kind"]
        == "nominal_exact_conversion_constant"
    )
    assert (
        provenance["effective_temperature"]["source"]["doi"]
        == "10.3847/0004-6256/152/2/41"
    )
