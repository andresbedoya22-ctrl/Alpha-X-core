from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from alpha_x.backtest.engine import run_long_flat_backtest
from alpha_x.backtest.metrics import PerformanceRow, calculate_backtest_metrics


@dataclass(frozen=True)
class PolicyVariant:
    policy_id: str
    name: str
    threshold: float
    allowed_regime: str | None


def get_policy_variants() -> list[PolicyVariant]:
    return [
        PolicyVariant(
            policy_id="policy_a_p065",
            name="Variant A - p > 0.65",
            threshold=0.65,
            allowed_regime=None,
        ),
        PolicyVariant(
            policy_id="policy_b_p070",
            name="Variant B - p > 0.70",
            threshold=0.70,
            allowed_regime=None,
        ),
        PolicyVariant(
            policy_id="policy_c_regime_p060",
            name="Variant C - trend_up_high_vol and p > 0.60",
            threshold=0.60,
            allowed_regime="trend_up_high_vol",
        ),
        PolicyVariant(
            policy_id="policy_d_regime_p065",
            name="Variant D - trend_up_high_vol and p > 0.65",
            threshold=0.65,
            allowed_regime="trend_up_high_vol",
        ),
    ]


def build_policy_signal_frame(
    test_predictions: pd.DataFrame,
    *,
    variant: PolicyVariant,
) -> pd.DataFrame:
    required_columns = {"timestamp", "datetime", "close", "predicted_proba", "regime"}
    missing_columns = sorted(required_columns - set(test_predictions.columns))
    if missing_columns:
        raise ValueError(f"Missing required policy columns: {missing_columns}")

    frame = test_predictions.loc[
        :,
        ["timestamp", "datetime", "close", "predicted_proba", "regime"],
    ].copy()
    regime_mask = pd.Series(True, index=frame.index)
    if variant.allowed_regime is not None:
        regime_mask = frame["regime"].eq(variant.allowed_regime)

    frame["policy_id"] = variant.policy_id
    frame["policy_name"] = variant.name
    frame["policy_threshold"] = variant.threshold
    frame["policy_allowed_regime"] = variant.allowed_regime
    frame["regime_allowed"] = regime_mask.astype("bool")
    frame["signal"] = (
        frame["predicted_proba"].gt(variant.threshold) & frame["regime_allowed"]
    ).astype("int64")
    return frame


def run_policy_backtest(
    signal_frame: pd.DataFrame,
    *,
    initial_capital: float,
    fee_rate: float,
    slippage_rate: float,
) -> tuple[PerformanceRow, pd.DataFrame]:
    result = run_long_flat_backtest(
        signal_frame,
        initial_capital=initial_capital,
        fee_rate=fee_rate,
        slippage_rate=slippage_rate,
        name=str(signal_frame["policy_name"].iloc[0]),
    )
    return calculate_backtest_metrics(result), result.equity_curve


def build_policy_summary(signal_frame: pd.DataFrame, metrics: PerformanceRow) -> dict[str, object]:
    signal_on_rate = float(signal_frame["signal"].mean())
    regime_pass_rate = float(signal_frame["regime_allowed"].mean())
    active = signal_frame.loc[signal_frame["signal"].eq(1)].copy()
    active_regimes = (
        active.groupby("regime").size().rename("rows").reset_index().to_dict(orient="records")
        if not active.empty
        else []
    )
    return {
        "policy_id": str(signal_frame["policy_id"].iloc[0]),
        "policy_name": str(signal_frame["policy_name"].iloc[0]),
        "threshold": float(signal_frame["policy_threshold"].iloc[0]),
        "allowed_regime": signal_frame["policy_allowed_regime"].iloc[0],
        "rows": len(signal_frame),
        "signal_on_rate": signal_on_rate,
        "activation_rate": signal_on_rate,
        "regime_pass_rate": regime_pass_rate,
        "active_rows": int(signal_frame["signal"].sum()),
        "active_regime_distribution": active_regimes,
        "total_return": metrics.total_return,
        "max_drawdown": metrics.max_drawdown,
        "trades": metrics.trades,
        "exposure": metrics.exposure,
        "final_equity": metrics.final_equity,
        "profit_factor": metrics.profit_factor,
    }
