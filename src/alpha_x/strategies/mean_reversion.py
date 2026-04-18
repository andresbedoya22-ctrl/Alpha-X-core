from __future__ import annotations

import pandas as pd


def build_mean_reversion_signal(
    frame: pd.DataFrame,
    *,
    window: int = 24,
    zscore_threshold: float = -1.5,
) -> pd.DataFrame:
    if window <= 1:
        raise ValueError("window must be greater than 1.")
    if zscore_threshold >= 0:
        raise ValueError("zscore_threshold must be negative for a long mean reversion rule.")

    prepared = frame.loc[:, ["timestamp", "datetime", "close"]].copy()
    rolling = prepared["close"].rolling(window=window, min_periods=window)
    prepared["rolling_mean"] = rolling.mean()
    prepared["rolling_std"] = rolling.std(ddof=0)
    prepared["zscore"] = (
        (prepared["close"] - prepared["rolling_mean"]) / prepared["rolling_std"]
    )
    prepared["signal"] = (
        (prepared["zscore"] < zscore_threshold)
        & prepared["rolling_std"].gt(0.0)
        & prepared["zscore"].notna()
    ).astype("int64")
    return prepared
