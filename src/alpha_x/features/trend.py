from __future__ import annotations

import pandas as pd

from alpha_x.features.base import FeatureDefinition


def _sma(close: pd.Series, window: int) -> pd.Series:
    return close.rolling(window=window, min_periods=window).mean()


def _distance_to_sma(close: pd.Series, window: int) -> pd.Series:
    sma = _sma(close, window)
    return (close / sma) - 1.0


def _sma_slope(close: pd.Series, window: int, lookback: int) -> pd.Series:
    sma = _sma(close, window)
    return sma.diff(periods=lookback) / lookback


def get_trend_features() -> list[FeatureDefinition]:
    return [
        FeatureDefinition(
            feature_id="dist_sma_8",
            family="trend",
            description="Relative distance from close to 8-bar SMA.",
            parameters={"window": 8},
            warmup_bars=8,
            builder=lambda frame: _distance_to_sma(frame["close"], 8),
        ),
        FeatureDefinition(
            feature_id="dist_sma_24",
            family="trend",
            description="Relative distance from close to 24-bar SMA.",
            parameters={"window": 24},
            warmup_bars=24,
            builder=lambda frame: _distance_to_sma(frame["close"], 24),
        ),
        FeatureDefinition(
            feature_id="dist_sma_72",
            family="trend",
            description="Relative distance from close to 72-bar SMA.",
            parameters={"window": 72},
            warmup_bars=72,
            builder=lambda frame: _distance_to_sma(frame["close"], 72),
        ),
        FeatureDefinition(
            feature_id="sma_24_slope_4",
            family="trend",
            description="Average per-bar slope of 24-bar SMA over the last 4 bars.",
            parameters={"window": 24, "lookback": 4},
            warmup_bars=27,
            builder=lambda frame: _sma_slope(frame["close"], 24, 4),
        ),
        FeatureDefinition(
            feature_id="sma_8_over_24",
            family="trend",
            description="Relative spread between 8-bar SMA and 24-bar SMA.",
            parameters={"fast_window": 8, "slow_window": 24},
            warmup_bars=24,
            builder=lambda frame: (_sma(frame["close"], 8) / _sma(frame["close"], 24)) - 1.0,
        ),
    ]
