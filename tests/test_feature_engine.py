from __future__ import annotations

import pandas as pd
import pytest

from alpha_x.features.catalog import get_feature_catalog
from alpha_x.features.engine import run_feature_engine


def _build_dataset(rows: int = 160) -> pd.DataFrame:
    timestamps = [index * 3_600_000 for index in range(rows)]
    close = []
    for index in range(rows):
        trend = 100.0 + (index * 0.5)
        wave = ((index % 10) - 5) * 0.2
        close.append(trend + wave)

    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "datetime": pd.to_datetime(timestamps, unit="ms", utc=True),
            "open": [value - 0.3 for value in close],
            "high": [value + 1.0 for value in close],
            "low": [value - 1.0 for value in close],
            "close": close,
            "volume": [50.0 + (index % 7) for index in range(rows)],
        }
    )


def test_feature_engine_builds_expected_columns_and_valid_rows() -> None:
    result = run_feature_engine(_build_dataset())

    assert result.summary["feature_count"] == len(get_feature_catalog())
    assert "ret_24" in result.feature_frame.columns
    assert "atr_pct_24" in result.feature_frame.columns
    assert "breakout_pressure_24" in result.feature_frame.columns
    assert "valid_feature_row" in result.feature_frame.columns
    assert result.summary["valid_rows"] > 0
    assert result.summary["warmup_rows"] == len(result.feature_frame) - result.summary["valid_rows"]


def test_feature_engine_calculates_key_feature_values() -> None:
    result = run_feature_engine(_build_dataset())
    frame = result.feature_frame
    row = frame.loc[95]

    expected_ret_1 = (frame.loc[95, "close"] / frame.loc[94, "close"]) - 1.0
    sma_24 = frame.loc[72:95, "close"].mean()
    expected_dist_sma_24 = (frame.loc[95, "close"] / sma_24) - 1.0

    assert row["ret_1"] == pytest.approx(expected_ret_1)
    assert row["dist_sma_24"] == pytest.approx(expected_dist_sma_24)
    assert 0.0 <= row["close_in_range_24"] <= 1.0


def test_feature_engine_has_basic_no_leakage_property() -> None:
    base = _build_dataset()
    mutated = base.copy()
    mutated.loc[120:, "close"] = mutated.loc[120:, "close"] * 10.0
    mutated.loc[120:, "high"] = mutated.loc[120:, "close"] + 1.0
    mutated.loc[120:, "low"] = mutated.loc[120:, "close"] - 1.0
    mutated.loc[120:, "open"] = mutated.loc[120:, "close"] - 0.3

    base_result = run_feature_engine(base).feature_frame
    mutated_result = run_feature_engine(mutated).feature_frame

    feature_columns = [definition.feature_id for definition in get_feature_catalog()]
    pd.testing.assert_series_equal(
        base_result.loc[110, feature_columns],
        mutated_result.loc[110, feature_columns],
        check_names=False,
    )
