import numpy as np

from foundinspace.pipeline.common.astrometry import (
    finite_positive_mask,
    fractional_distance_interval_width,
    fractional_parallax_error,
    parallax_distance_pc,
    plausible_distance_mask,
    valid_parallax_mask,
)


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
