from __future__ import annotations

import pandas as pd

from alpha_x.features.base import FeatureDefinition


def _pct_return(close: pd.Series, window: int) -> pd.Series:
    return close.pct_change(periods=window)


def get_return_features() -> list[FeatureDefinition]:
    return [
        FeatureDefinition(
            feature_id="ret_1",
            family="returns",
            description="Close-to-close return over 1 bar.",
            parameters={"window": 1},
            warmup_bars=1,
            builder=lambda frame: _pct_return(frame["close"], 1),
        ),
        FeatureDefinition(
            feature_id="ret_4",
            family="returns",
            description="Close-to-close return over 4 bars.",
            parameters={"window": 4},
            warmup_bars=4,
            builder=lambda frame: _pct_return(frame["close"], 4),
        ),
        FeatureDefinition(
            feature_id="ret_24",
            family="returns",
            description="Close-to-close return over 24 bars.",
            parameters={"window": 24},
            warmup_bars=24,
            builder=lambda frame: _pct_return(frame["close"], 24),
        ),
        FeatureDefinition(
            feature_id="ret_72",
            family="returns",
            description="Close-to-close return over 72 bars.",
            parameters={"window": 72},
            warmup_bars=72,
            builder=lambda frame: _pct_return(frame["close"], 72),
        ),
        FeatureDefinition(
            feature_id="momentum_ratio_24_72",
            family="returns",
            description=(
                "Relative momentum: 24-bar return divided by "
                "72-bar absolute return plus epsilon."
            ),
            parameters={"fast_window": 24, "slow_window": 72, "epsilon": 1e-12},
            warmup_bars=72,
            builder=lambda frame: _pct_return(frame["close"], 24)
            / (_pct_return(frame["close"], 72).abs() + 1e-12),
        ),
    ]
