import numpy as np

from foundinspace.pipeline.common.astrometry import (
    edsd_mode_distance_pc,
    finite_positive_mask,
    fractional_distance_interval_width,
    fractional_parallax_error,
    parallax_distance_pc,
    plausible_distance_mask,
    valid_parallax_mask,
)


def test_edsd_mode_distance_is_finite_for_weak_and_nonpositive_parallaxes():
    distances = edsd_mode_distance_pc(
        np.array([0.01, 0.0, -1.0, np.nan]),
        np.array([0.62, 1.0, 0.5, np.nan]),
        length_scale_pc=1350.0,
    )

    assert np.isfinite(distances).all()
    assert (distances > 0.0).all()
    assert distances[0] < 10_000.0
    assert distances[3] == 2700.0


def test_edsd_mode_approaches_prior_mode_for_uninformative_measurement():
    distance = edsd_mode_distance_pc(
        np.array([0.0]),
        np.array([1e9]),
        length_scale_pc=1350.0,
    )[0]

    np.testing.assert_allclose(distance, 2700.0, rtol=1e-6)


def test_parallax_distance_and_error_require_finite_positive_values():
    parallax = np.array([10.0, 0.0, -1.0, np.nan, np.inf])
    error = np.array([1.0, 1.0, 1.0, 1.0, 1.0])

    np.testing.assert_array_equal(
        valid_parallax_mask(parallax, error),
        np.array([True, False, False, False, False]),
    )
    np.testing.assert_allclose(
        parallax_distance_pc(parallax),
        np.array([100.0, np.nan, np.nan, np.nan, np.nan]),
        equal_nan=True,
    )
    np.testing.assert_allclose(
        fractional_parallax_error(parallax, error),
        np.array([0.1, np.nan, np.nan, np.nan, np.nan]),
        equal_nan=True,
    )


def test_distance_masks_and_interval_width():
    distance = np.array([100.0, 0.0, -1.0, np.nan, 250_000.0])

    np.testing.assert_array_equal(
        finite_positive_mask(distance),
        np.array([True, False, False, False, True]),
    )
    np.testing.assert_array_equal(
        plausible_distance_mask(distance),
        np.array([True, False, False, False, False]),
    )
    np.testing.assert_allclose(
        fractional_distance_interval_width(
            np.array([100.0]),
            np.array([90.0]),
            np.array([120.0]),
        ),
        np.array([0.15]),
    )


def test_distance_interval_width_rejects_malformed_quantiles():
    np.testing.assert_allclose(
        fractional_distance_interval_width(
            np.array([100.0, 100.0, 100.0, 100.0]),
            np.array([90.0, 110.0, 90.0, -1.0]),
            np.array([120.0, 120.0, 80.0, 120.0]),
        ),
        np.array([0.15, np.nan, np.nan, np.nan]),
        equal_nan=True,
    )
