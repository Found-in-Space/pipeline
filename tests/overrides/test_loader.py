from pathlib import Path

import numpy as np
import pytest

from foundinspace.pipeline.overrides.loader import (
    icrs_spherical_to_cartesian_pc,
    iter_override_source_files,
    load_normalized_override_stars,
    load_override_source_texts,
)


def test_iter_override_source_files_returns_explicit_include_order(tmp_path: Path):
    (tmp_path / "z.yaml").write_text("stars: []\n", encoding="utf-8")
    (tmp_path / "a.yml").write_text("stars: []\n", encoding="utf-8")

    files = iter_override_source_files([tmp_path / "z.yaml", tmp_path / "a.yml"])
    names = [p.name for p in files]
    assert names == ["z.yaml", "a.yml"]


def test_iter_override_source_files_rejects_non_yaml(tmp_path: Path):
    path = tmp_path / "ignore.txt"
    path.write_text("not yaml\n", encoding="utf-8")

    with pytest.raises(ValueError, match="YAML"):
        iter_override_source_files([path])


def test_iter_override_source_files_rejects_missing_file(tmp_path: Path):
    with pytest.raises(FileNotFoundError, match="Missing override include file"):
        iter_override_source_files([tmp_path / "missing.yaml"])


def test_iter_override_source_files_rejects_unknown_builtin():
    with pytest.raises(ValueError, match="Unknown builtin"):
        iter_override_source_files(["builtin:alpha_cen.yaml"])


def test_load_override_source_texts_reads_builtin_sun_yaml():
    sources = load_override_source_texts(["builtin:sun.yaml"])
    assert list(sources) == ["sun.yaml"]
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
    stars = load_normalized_override_stars([tmp_path / "fixture.yaml"])
    b = next(s for s in stars if s["override_id"] == "fixture.star.replace")
    ex, ey, ez = icrs_spherical_to_cartesian_pc(
        float(b["ra_deg"]),
        float(b["dec_deg"]),
        float(b["r_pc"]),
    )
    assert np.allclose([b["x_icrs_pc"], b["y_icrs_pc"], b["z_icrs_pc"]], [ex, ey, ez])


def test_sun_override_keeps_explicit_cartesian():
    stars = load_normalized_override_stars(["builtin:sun.yaml"])
    sun = next(s for s in stars if s["override_id"] == "manual.sun.add.v1")
    assert sun["x_icrs_pc"] == 0.0
    assert sun["y_icrs_pc"] == 0.0
    assert sun["z_icrs_pc"] == 0.0
