from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class RegimeConfig:
    sma_window: int = 200
    volatility_window: int = 30
    percentile_window: int = 252
    low_cutoff: float = 0.33
    high_cutoff: float = 0.67


RISK_MULTIPLIERS = {
    "bull_low_vol": 1.00,
    "bull_normal_vol": 0.85,
    "bull_high_vol": 0.60,
    "bear_low_vol": 0.35,
    "bear_normal_vol": 0.15,
    "bear_high_vol": 0.00,
}


def build_regime_frame(
    btc_frame: pd.DataFrame,
    config: RegimeConfig | None = None,
) -> pd.DataFrame:
    cfg = config or RegimeConfig()
    prepared = btc_frame.loc[:, ["timestamp", "datetime", "close"]].copy()
    close = pd.to_numeric(prepared["close"], errors="coerce")
    sma200 = close.rolling(cfg.sma_window, min_periods=cfg.sma_window).mean()
    realized_vol = close.pct_change().rolling(
        cfg.volatility_window,
        min_periods=cfg.volatility_window,
    ).std() * (365.25**0.5)

    prepared["trend_regime"] = (close > sma200).map({True: "bull", False: "bear"})
    prepared["volatility_percentile"] = realized_vol.rolling(
        cfg.percentile_window,
        min_periods=max(cfg.volatility_window * 2, 60),
    ).apply(lambda values: pd.Series(values).rank(pct=True).iloc[-1], raw=False)
    prepared["vol_regime"] = prepared["volatility_percentile"].map(
        lambda value: _classify_volatility(value, cfg.low_cutoff, cfg.high_cutoff)
    )
    prepared["operating_regime"] = prepared["trend_regime"] + "_" + prepared["vol_regime"]
    prepared["risk_multiplier"] = prepared["operating_regime"].map(RISK_MULTIPLIERS).fillna(0.0)
    prepared["btc_realized_vol_30d"] = realized_vol
    prepared["btc_sma200"] = sma200
    return prepared


def _classify_volatility(value: float | None, low_cutoff: float, high_cutoff: float) -> str:
    if value is None or pd.isna(value):
        return "normal_vol"
    if value <= low_cutoff:
        return "low_vol"
    if value >= high_cutoff:
        return "high_vol"
    return "normal_vol"
