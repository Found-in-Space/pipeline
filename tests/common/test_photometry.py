import numpy as np

from foundinspace.pipeline.common.photometry import (
    absolute_magnitude_from_apparent,
    apparent_magnitude_from_absolute,
    distance_modulus,
)


def test_distance_modulus_handles_valid_and_invalid_distances():
    np.testing.assert_allclose(
        distance_modulus(np.array([10.0, 100.0, 0.0, -1.0, np.nan])),
        np.array([0.0, 5.0, np.nan, np.nan, np.nan]),
        equal_nan=True,
    )
    assert distance_modulus(100.0) == 5.0


def test_apparent_and_absolute_magnitude_round_trip():
    mag_abs = np.array([4.0, -1.0])
    distance = np.array([100.0, 10.0])
    apparent = apparent_magnitude_from_absolute(mag_abs, distance)

    np.testing.assert_allclose(apparent, np.array([9.0, -1.0]))
    np.testing.assert_allclose(
        absolute_magnitude_from_apparent(apparent, distance),
        mag_abs,
    )
    assert absolute_magnitude_from_apparent(10.0, 100.0, 0.5) == 4.5
