from __future__ import annotations

import pandas as pd

FAMILY_B_REGIME_SCORE_MULTIPLIER = {
    "bull_low_vol": 1.00,
    "bull_normal_vol": 0.90,
    "bull_high_vol": 0.70,
    "bear_low_vol": 0.35,
    "bear_normal_vol": 0.15,
    "bear_high_vol": 0.00,
}


def build_family_scores(
    signal_panel: pd.DataFrame,
    regime_frame: pd.DataFrame,
) -> pd.DataFrame:
    merged = signal_panel.merge(
        regime_frame.loc[:, ["timestamp", "operating_regime", "risk_multiplier"]],
        on="timestamp",
        how="left",
    )
    prepared = merged.copy()
    for column in (
        "ret_30d",
        "ret_90d",
        "ret_180d",
        "distance_to_sma",
        "sma_slope",
        "relative_breakout",
        "relative_volume",
        "volatility_penalty",
    ):
        prepared[f"rank_{column}"] = prepared.groupby("timestamp")[column].rank(pct=True)

    momentum_score = prepared[["rank_ret_30d", "rank_ret_90d", "rank_ret_180d"]].mean(axis=1)
    prepared["family_a_score"] = momentum_score.where(
        prepared["absolute_filter_30d"] == 1, 0.0
    ).fillna(0.0)
    prepared["family_b_score"] = prepared["family_a_score"] * prepared["operating_regime"].map(
        FAMILY_B_REGIME_SCORE_MULTIPLIER
    ).fillna(0.0)
    prepared["family_c_score"] = (
        0.22 * prepared["rank_ret_30d"].fillna(0.0)
        + 0.18 * prepared["rank_ret_90d"].fillna(0.0)
        + 0.15 * prepared["rank_distance_to_sma"].fillna(0.0)
        + 0.15 * prepared["rank_sma_slope"].fillna(0.0)
        + 0.15 * prepared["rank_relative_breakout"].fillna(0.0)
        + 0.10 * prepared["rank_relative_volume"].fillna(0.0)
        + 0.05 * (1.0 - prepared["rank_volatility_penalty"].fillna(1.0))
    )
    prepared["family_c_score"] = prepared["family_c_score"].where(
        prepared["absolute_filter_30d"] == 1,
        0.0,
    )
    return prepared
