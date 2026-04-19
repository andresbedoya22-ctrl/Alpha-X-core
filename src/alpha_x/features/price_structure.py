from __future__ import annotations

import pandas as pd

from alpha_x.features.base import FeatureDefinition


def _rolling_high(frame: pd.DataFrame, window: int) -> pd.Series:
    return frame["high"].rolling(window=window, min_periods=window).max()


def _rolling_low(frame: pd.DataFrame, window: int) -> pd.Series:
    return frame["low"].rolling(window=window, min_periods=window).min()


def _range_denom(frame: pd.DataFrame, window: int) -> pd.Series:
    return _rolling_high(frame, window) - _rolling_low(frame, window)


def get_price_structure_features() -> list[FeatureDefinition]:
    return [
        FeatureDefinition(
            feature_id="close_in_range_24",
            family="price_structure",
            description="Close position within the 24-bar rolling range from 0 to 1.",
            parameters={"window": 24},
            warmup_bars=24,
            builder=lambda frame: (frame["close"] - _rolling_low(frame, 24))
            / _range_denom(frame, 24),
        ),
        FeatureDefinition(
            feature_id="dist_rolling_high_24",
            family="price_structure",
            description="Distance from close to 24-bar rolling high, scaled by close.",
            parameters={"window": 24},
            warmup_bars=24,
            builder=lambda frame: (frame["close"] - _rolling_high(frame, 24)) / frame["close"],
        ),
        FeatureDefinition(
            feature_id="dist_rolling_low_24",
            family="price_structure",
            description="Distance from close to 24-bar rolling low, scaled by close.",
            parameters={"window": 24},
            warmup_bars=24,
            builder=lambda frame: (frame["close"] - _rolling_low(frame, 24)) / frame["close"],
        ),
        FeatureDefinition(
            feature_id="breakout_pressure_24",
            family="price_structure",
            description="Normalized pressure toward the top of the 24-bar range, centered at zero.",
            parameters={"window": 24},
            warmup_bars=24,
            builder=lambda frame: (
                2.0
                * ((frame["close"] - _rolling_low(frame, 24)) / _range_denom(frame, 24))
            )
            - 1.0,
        ),
    ]
