from __future__ import annotations

import pandas as pd

from alpha_x.regime.base import RegimeDetectionResult, RegimeRuleSet

REQUIRED_FEATURE_COLUMNS = (
    "timestamp",
    "datetime",
    "close",
    "dist_sma_72",
    "sma_24_slope_4",
    "atr_pct_24",
    "range_pct_24_rank_72",
)


def detect_regimes(frame: pd.DataFrame, rule_set: RegimeRuleSet) -> RegimeDetectionResult:
    missing_columns = sorted(set(REQUIRED_FEATURE_COLUMNS) - set(frame.columns))
    if missing_columns:
        raise ValueError(f"Missing required regime columns: {missing_columns}")

    parameters = rule_set.parameters
    prepared = frame.copy()
    prepared["volatility_baseline"] = prepared["atr_pct_24"].rolling(
        window=int(parameters["volatility_baseline_window"]),
        min_periods=int(parameters["volatility_baseline_window"]),
    ).median()
    prepared["trend_state"] = _build_trend_state(prepared, parameters)
    prepared["volatility_state"] = _build_volatility_state(prepared)
    prepared["compression_state"] = _build_compression_state(prepared, parameters)
    prepared["regime_is_valid"] = (
        prepared["trend_state"].notna()
        & prepared["volatility_state"].notna()
        & prepared["compression_state"].notna()
    )
    prepared["regime_discard_reason"] = "valid"
    prepared.loc[~prepared["valid_feature_row"], "regime_discard_reason"] = "feature_warmup"
    prepared.loc[
        prepared["valid_feature_row"] & prepared["volatility_baseline"].isna(),
        "regime_discard_reason",
    ] = "volatility_baseline_warmup"
    prepared["regime"] = pd.Series(pd.NA, index=prepared.index, dtype="string")
    prepared.loc[prepared["regime_is_valid"], "regime"] = (
        prepared.loc[prepared["regime_is_valid"], "trend_state"]
        + "_"
        + prepared.loc[prepared["regime_is_valid"], "volatility_state"]
    ).astype("string")

    assigned_rows = int(prepared["regime_is_valid"].sum())
    discarded_rows = int((~prepared["regime_is_valid"]).sum())
    total_rows = len(prepared)
    return RegimeDetectionResult(
        frame=prepared,
        assigned_rows=assigned_rows,
        discarded_rows=discarded_rows,
        discard_pct=0.0 if total_rows == 0 else discarded_rows / total_rows,
        regime_names=list(rule_set.regime_descriptions.keys()),
        rules_used={
            "rule_set_id": rule_set.rule_set_id,
            "name": rule_set.name,
            "description": rule_set.description,
            "parameters": rule_set.parameters,
            "regime_descriptions": rule_set.regime_descriptions,
        },
    )


def _build_trend_state(frame: pd.DataFrame, parameters: dict[str, object]) -> pd.Series:
    distance_threshold = float(parameters["trend_distance_threshold"])
    up_mask = (frame["dist_sma_72"] >= distance_threshold) & (frame["sma_24_slope_4"] > 0.0)
    down_mask = (frame["dist_sma_72"] <= -distance_threshold) & (frame["sma_24_slope_4"] < 0.0)
    trend_state = pd.Series("range", index=frame.index, dtype="string")
    trend_state.loc[up_mask] = "trend_up"
    trend_state.loc[down_mask] = "trend_down"
    trend_state.loc[~frame["valid_feature_row"]] = pd.NA
    return trend_state


def _build_volatility_state(frame: pd.DataFrame) -> pd.Series:
    volatility_state = pd.Series(pd.NA, index=frame.index, dtype="string")
    valid_mask = frame["valid_feature_row"] & frame["volatility_baseline"].notna()
    volatility_state.loc[valid_mask] = "low_vol"
    volatility_state.loc[valid_mask & frame["atr_pct_24"].gt(frame["volatility_baseline"])] = (
        "high_vol"
    )
    return volatility_state


def _build_compression_state(frame: pd.DataFrame, parameters: dict[str, object]) -> pd.Series:
    low_threshold = float(parameters["compression_low_threshold"])
    high_threshold = float(parameters["compression_high_threshold"])
    compression_state = pd.Series("normal", index=frame.index, dtype="string")
    compression_state.loc[frame["range_pct_24_rank_72"] <= low_threshold] = "compressed"
    compression_state.loc[frame["range_pct_24_rank_72"] >= high_threshold] = "expanded"
    compression_state.loc[~frame["valid_feature_row"]] = pd.NA
    return compression_state
