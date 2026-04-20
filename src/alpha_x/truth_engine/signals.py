from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class SignalConfig:
    short_window: int = 30
    medium_window: int = 90
    long_window: int = 180
    sma_window: int = 50
    slope_lookback: int = 20
    breakout_window: int = 90
    volume_window: int = 30
    volatility_window: int = 30
    volatility_baseline_window: int = 180


REQUIRED_COLUMNS = ("timestamp", "datetime", "close", "volume")


def build_signal_frame(
    market: str,
    frame: pd.DataFrame,
    config: SignalConfig | None = None,
) -> pd.DataFrame:
    cfg = config or SignalConfig()
    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"Signal frame missing required columns: {missing}")

    prepared = frame.loc[:, list(REQUIRED_COLUMNS)].copy()
    prepared["market"] = market
    close = pd.to_numeric(prepared["close"], errors="coerce")
    volume = pd.to_numeric(prepared["volume"], errors="coerce")
    returns = close.pct_change()
    sma = close.rolling(cfg.sma_window, min_periods=cfg.sma_window).mean()
    rolling_high = close.rolling(cfg.breakout_window, min_periods=cfg.breakout_window).max()
    turnover = close * volume
    realized_vol = returns.rolling(
        cfg.volatility_window, min_periods=cfg.volatility_window
    ).std() * np.sqrt(365.25)
    baseline_vol = realized_vol.rolling(
        cfg.volatility_baseline_window,
        min_periods=max(cfg.volatility_window * 2, 60),
    ).median()

    prepared["ret_30d"] = close.pct_change(cfg.short_window)
    prepared["ret_90d"] = close.pct_change(cfg.medium_window)
    prepared["ret_180d"] = close.pct_change(cfg.long_window)
    prepared["absolute_filter_30d"] = (prepared["ret_30d"] > 0.0).astype("int64")
    prepared["distance_to_sma"] = (close / sma) - 1.0
    prepared["sma_slope"] = sma.pct_change(cfg.slope_lookback)
    prepared["relative_breakout"] = (close / rolling_high.shift(1)) - 1.0
    prepared["relative_volume"] = (
        turnover / turnover.rolling(cfg.volume_window, min_periods=cfg.volume_window).mean()
    )
    prepared["realized_vol_30d"] = realized_vol
    prepared["volatility_penalty"] = (
        ((realized_vol / baseline_vol) - 1.0).clip(lower=0.0).fillna(0.0)
    )
    prepared["daily_return"] = returns
    return prepared


def build_signal_panel(
    market_frames: dict[str, pd.DataFrame],
    config: SignalConfig | None = None,
) -> pd.DataFrame:
    frames = [build_signal_frame(market, frame, config) for market, frame in market_frames.items()]
    if not frames:
        return pd.DataFrame()
    return (
        pd.concat(frames, ignore_index=True)
        .sort_values(["timestamp", "market"])
        .reset_index(drop=True)
    )
