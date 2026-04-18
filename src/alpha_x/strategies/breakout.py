from __future__ import annotations

import pandas as pd


def build_breakout_signal(
    frame: pd.DataFrame,
    *,
    lookback_bars: int = 48,
) -> pd.DataFrame:
    if lookback_bars <= 1:
        raise ValueError("lookback_bars must be greater than 1.")

    prepared = frame.loc[:, ["timestamp", "datetime", "close"]].copy()
    prepared["rolling_high_prev"] = (
        prepared["close"].rolling(window=lookback_bars, min_periods=lookback_bars).max().shift(1)
    )
    prepared["signal"] = (
        (prepared["close"] > prepared["rolling_high_prev"]) & prepared["rolling_high_prev"].notna()
    ).astype("int64")
    return prepared
