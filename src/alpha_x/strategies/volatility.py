from __future__ import annotations

import pandas as pd

from alpha_x.strategies.trend import build_trend_signal


def build_volatility_filter_signal(
    frame: pd.DataFrame,
    *,
    base_slow_window: int = 200,
    volatility_window: int = 24,
    min_volatility: float = 0.008,
    max_volatility: float = 0.05,
) -> pd.DataFrame:
    if volatility_window <= 1:
        raise ValueError("volatility_window must be greater than 1.")
    if min_volatility < 0 or max_volatility <= 0:
        raise ValueError("Volatility bounds must be positive.")
    if min_volatility >= max_volatility:
        raise ValueError("min_volatility must be smaller than max_volatility.")

    prepared = build_trend_signal(frame, slow_window=base_slow_window)
    prepared["bar_return"] = prepared["close"].pct_change()
    prepared["realized_volatility"] = prepared["bar_return"].rolling(
        window=volatility_window,
        min_periods=volatility_window,
    ).std(ddof=0)
    prepared["signal"] = (
        prepared["signal"].eq(1)
        & prepared["realized_volatility"].between(min_volatility, max_volatility, inclusive="both")
    ).astype("int64")
    return prepared
