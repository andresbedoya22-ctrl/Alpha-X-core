from __future__ import annotations

import pandas as pd


def build_momentum_signal(
    frame: pd.DataFrame,
    *,
    lookback_bars: int = 24,
    threshold: float = 0.015,
) -> pd.DataFrame:
    if lookback_bars <= 0:
        raise ValueError("lookback_bars must be positive.")

    prepared = frame.loc[:, ["timestamp", "datetime", "close"]].copy()
    prepared["momentum_return"] = prepared["close"].pct_change(periods=lookback_bars)
    prepared["signal"] = (
        (prepared["momentum_return"] > threshold) & prepared["momentum_return"].notna()
    ).astype("int64")
    return prepared
