from __future__ import annotations

from alpha_x.strategies.base import StrategyDefinition
from alpha_x.strategies.breakout import build_breakout_signal
from alpha_x.strategies.mean_reversion import build_mean_reversion_signal
from alpha_x.strategies.momentum import build_momentum_signal
from alpha_x.strategies.trend import build_trend_signal
from alpha_x.strategies.volatility import build_volatility_filter_signal


def get_strategy_catalog() -> list[StrategyDefinition]:
    return [
        StrategyDefinition(
            strategy_id="trend_sma200",
            name="Hypothesis 1 - Trend Following (Close > SMA 200)",
            family="trend_following",
            description="Long when close is above the 200-bar simple moving average.",
            parameters={"slow_window": 200},
            builder=build_trend_signal,
        ),
        StrategyDefinition(
            strategy_id="momentum_24h_threshold",
            name="Hypothesis 2 - Momentum (24h return > 1.5%)",
            family="momentum",
            description="Long when the 24-bar cumulative return is greater than 1.5%.",
            parameters={"lookback_bars": 24, "threshold": 0.015},
            builder=build_momentum_signal,
        ),
        StrategyDefinition(
            strategy_id="breakout_48h_high",
            name="Hypothesis 3 - Breakout (Close > prior 48h high)",
            family="breakout",
            description="Long when close exceeds the rolling 48-bar high shifted by one bar.",
            parameters={"lookback_bars": 48},
            builder=build_breakout_signal,
        ),
        StrategyDefinition(
            strategy_id="mean_reversion_zscore",
            name="Hypothesis 4 - Mean Reversion (z-score < -1.5)",
            family="mean_reversion",
            description="Long when the 24-bar close z-score is below -1.5.",
            parameters={"window": 24, "zscore_threshold": -1.5},
            builder=build_mean_reversion_signal,
        ),
        StrategyDefinition(
            strategy_id="trend_volatility_filter",
            name="Hypothesis 5 - Volatility Filter (Trend + vol band)",
            family="volatility_filter",
            description=(
                "Long only when the trend signal is active and realized volatility "
                "stays within bounds."
            ),
            parameters={
                "base_slow_window": 200,
                "volatility_window": 24,
                "min_volatility": 0.008,
                "max_volatility": 0.05,
            },
            builder=build_volatility_filter_signal,
        ),
    ]
