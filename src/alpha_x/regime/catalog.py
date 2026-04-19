from __future__ import annotations

from alpha_x.regime.base import RegimeRuleSet


def get_default_regime_rule_set() -> RegimeRuleSet:
    return RegimeRuleSet(
        rule_set_id="simple_trend_volatility_v1",
        name="Simple Trend/Volatility Regime v1",
        description=(
            "Six explicit regimes built from long-horizon trend and relative volatility. "
            "Compression/expansion is exported as an auxiliary context column."
        ),
        parameters={
            "trend_distance_feature": "dist_sma_72",
            "trend_slope_feature": "sma_24_slope_4",
            "trend_distance_threshold": 0.01,
            "volatility_feature": "atr_pct_24",
            "volatility_baseline_window": 168,
            "compression_feature": "range_pct_24_rank_72",
            "compression_low_threshold": 0.25,
            "compression_high_threshold": 0.75,
        },
        regime_descriptions={
            "trend_up_low_vol": "Positive trend and below-baseline volatility.",
            "trend_up_high_vol": "Positive trend and above-baseline volatility.",
            "trend_down_low_vol": "Negative trend and below-baseline volatility.",
            "trend_down_high_vol": "Negative trend and above-baseline volatility.",
            "range_low_vol": "No confirmed trend and below-baseline volatility.",
            "range_high_vol": "No confirmed trend and above-baseline volatility.",
        },
    )
