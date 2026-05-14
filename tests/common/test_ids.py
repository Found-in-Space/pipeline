import pandas as pd

from foundinspace.pipeline.common.ids import (
    coerce_positive_int_series,
    coerce_positive_integer_values,
    normalize_compound_key,
    normalize_source,
    serialize_source_id,
)


def test_source_and_source_id_normalization():
    assert normalize_source(" HIP ") == "hip"
    assert normalize_compound_key("gaia", " 123 ") == ("gaia", 123)
    assert normalize_compound_key("manual", " sun ") == ("manual", "sun")
    assert serialize_source_id(42) == "42"
    assert serialize_source_id(True) == "True"
    assert pd.isna(serialize_source_id(None))


def test_positive_integer_coercion():
    series = pd.Series(["1", "2.0", "0", "-3", "bad", 4.2])
    numeric, valid = coerce_positive_integer_values(series)

    assert numeric.tolist()[:2] == [1.0, 2.0]
    assert valid.tolist() == [True, True, False, False, False, False]

    out = coerce_positive_int_series(series)
    assert out.iloc[0] == 1
    assert out.iloc[1] == 2
    assert out.isna().tolist() == [False, False, True, True, True, True]
