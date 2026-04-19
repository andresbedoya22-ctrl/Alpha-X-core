from __future__ import annotations

import pandas as pd

from alpha_x.features.base import FeatureDefinition


def _true_range(frame: pd.DataFrame) -> pd.Series:
    prev_close = frame["close"].shift(1)
    ranges = pd.concat(
        [
            frame["high"] - frame["low"],
            (frame["high"] - prev_close).abs(),
            (frame["low"] - prev_close).abs(),
        ],
        axis=1,
    )
    return ranges.max(axis=1)


def get_volatility_features() -> list[FeatureDefinition]:
    return [
        FeatureDefinition(
            feature_id="ret_std_24",
            family="volatility",
            description="Rolling standard deviation of 1-bar returns over 24 bars.",
            parameters={"window": 24},
            warmup_bars=24,
            builder=lambda frame: frame["close"].pct_change().rolling(24, min_periods=24).std(),
        ),
        FeatureDefinition(
            feature_id="ret_std_72",
            family="volatility",
            description="Rolling standard deviation of 1-bar returns over 72 bars.",
            parameters={"window": 72},
            warmup_bars=72,
            builder=lambda frame: frame["close"].pct_change().rolling(72, min_periods=72).std(),
        ),
        FeatureDefinition(
            feature_id="atr_24",
            family="volatility",
            description="Average true range over 24 bars in price units.",
            parameters={"window": 24},
            warmup_bars=24,
            builder=lambda frame: _true_range(frame).rolling(24, min_periods=24).mean(),
        ),
        FeatureDefinition(
            feature_id="atr_pct_24",
            family="volatility",
            description="ATR(24) scaled by close.",
            parameters={"window": 24},
            warmup_bars=24,
            builder=lambda frame: _true_range(frame).rolling(24, min_periods=24).mean()
            / frame["close"],
        ),
        FeatureDefinition(
            feature_id="hl_range_pct",
            family="volatility",
            description="Current high-low range divided by close.",
            parameters={},
            warmup_bars=0,
            builder=lambda frame: (frame["high"] - frame["low"]) / frame["close"],
        ),
        FeatureDefinition(
            feature_id="realized_vol_24",
            family="volatility",
            description=(
                "Simple realized volatility proxy: square root of rolling sum "
                "of squared 1-bar returns over 24 bars."
            ),
            parameters={"window": 24},
            warmup_bars=24,
            builder=lambda frame: (
                frame["close"].pct_change().pow(2).rolling(24, min_periods=24).sum()
            )
            ** 0.5,
        ),
    ]
