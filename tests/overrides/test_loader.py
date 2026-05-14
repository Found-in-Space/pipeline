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


def test_load_override_source_texts_reads_packaged_yaml():
    sources = load_override_source_texts()
    assert "sun.yaml" in sources
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


def test_load_normalized_override_stars_fills_cartesian_for_alpha_cen():
    stars = load_normalized_override_stars()
    by_id = {s["override_id"]: s for s in stars}
    a = by_id["manual.alpha_cen_a.replace.v1"]
    assert "x_icrs_pc" in a and "y_icrs_pc" in a and "z_icrs_pc" in a
    ex, ey, ez = icrs_spherical_to_cartesian_pc(
        float(a["ra_deg"]),
        float(a["dec_deg"]),
        float(a["r_pc"]),
    )
    assert np.allclose([a["x_icrs_pc"], a["y_icrs_pc"], a["z_icrs_pc"]], [ex, ey, ez])


def test_sun_override_keeps_explicit_cartesian():
    stars = load_normalized_override_stars()
    sun = next(s for s in stars if s["override_id"] == "manual.sun.add.v1")
    assert sun["x_icrs_pc"] == 0.0
    assert sun["y_icrs_pc"] == 0.0
    assert sun["z_icrs_pc"] == 0.0
