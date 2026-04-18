from __future__ import annotations

import pandas as pd


def build_trend_signal(
    frame: pd.DataFrame,
    *,
    slow_window: int = 200,
) -> pd.DataFrame:
    if slow_window <= 0:
        raise ValueError("slow_window must be positive.")

    prepared = frame.loc[:, ["timestamp", "datetime", "close"]].copy()
    prepared["sma_slow"] = prepared["close"].rolling(
        window=slow_window,
        min_periods=slow_window,
    ).mean()
    prepared["signal"] = (
        (prepared["close"] > prepared["sma_slow"]) & prepared["sma_slow"].notna()
    ).astype("int64")
    return prepared
