from __future__ import annotations

import pandas as pd

from alpha_x.features.base import FeatureDefinition


def _rolling_range(frame: pd.DataFrame, window: int) -> pd.Series:
    rolling_high = frame["high"].rolling(window=window, min_periods=window).max()
    rolling_low = frame["low"].rolling(window=window, min_periods=window).min()
    return (rolling_high - rolling_low) / frame["close"]


def _range_rank(series: pd.Series, rank_window: int) -> pd.Series:
    def percentile_rank(window_values: pd.Series) -> float:
        return float(window_values.rank(pct=True).iloc[-1])

    return series.rolling(window=rank_window, min_periods=rank_window).apply(
        percentile_rank,
        raw=False,
    )


def get_compression_features() -> list[FeatureDefinition]:
    return [
        FeatureDefinition(
            feature_id="range_pct_24",
            family="compression",
            description="Relative rolling price range over 24 bars.",
            parameters={"window": 24},
            warmup_bars=24,
            builder=lambda frame: _rolling_range(frame, 24),
        ),
        FeatureDefinition(
            feature_id="range_pct_72",
            family="compression",
            description="Relative rolling price range over 72 bars.",
            parameters={"window": 72},
            warmup_bars=72,
            builder=lambda frame: _rolling_range(frame, 72),
        ),
        FeatureDefinition(
            feature_id="range_pct_24_rank_72",
            family="compression",
            description=(
                "Percentile rank of the latest 24-bar range within the last "
                "72 observed 24-bar ranges."
            ),
            parameters={"range_window": 24, "rank_window": 72},
            warmup_bars=95,
            builder=lambda frame: _range_rank(_rolling_range(frame, 24), 72),
        ),
        FeatureDefinition(
            feature_id="range_expansion_24_vs_prev24",
            family="compression",
            description=(
                "Current 24-bar relative range divided by the previous "
                "24-bar relative range minus one."
            ),
            parameters={"window": 24},
            warmup_bars=47,
            builder=lambda frame: (_rolling_range(frame, 24) / _rolling_range(frame, 24).shift(24))
            - 1.0,
        ),
    ]
