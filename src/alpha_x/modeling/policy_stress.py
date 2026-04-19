from __future__ import annotations

import pandas as pd

from alpha_x.backtest.metrics import PerformanceRow
from alpha_x.modeling.policy import PolicyVariant, build_policy_signal_frame, run_policy_backtest


def get_policy_stress_variants() -> list[PolicyVariant]:
    return [
        PolicyVariant(
            policy_id="stress_regime_p060",
            name="Stress A - trend_up_high_vol and p > 0.60",
            threshold=0.60,
            allowed_regime="trend_up_high_vol",
        ),
        PolicyVariant(
            policy_id="stress_regime_p065",
            name="Stress B - trend_up_high_vol and p > 0.65",
            threshold=0.65,
            allowed_regime="trend_up_high_vol",
        ),
        PolicyVariant(
            policy_id="stress_regime_p070",
            name="Stress C - trend_up_high_vol and p > 0.70",
            threshold=0.70,
            allowed_regime="trend_up_high_vol",
        ),
    ]


def build_policy_stress_summary(
    signal_frame: pd.DataFrame,
    metrics: PerformanceRow,
) -> dict[str, object]:
    active_rows = int(signal_frame["signal"].sum())
    rows = len(signal_frame)
    return {
        "policy_id": str(signal_frame["policy_id"].iloc[0]),
        "policy_name": str(signal_frame["policy_name"].iloc[0]),
        "threshold": float(signal_frame["policy_threshold"].iloc[0]),
        "allowed_regime": signal_frame["policy_allowed_regime"].iloc[0],
        "rows": rows,
        "activation_rate": 0.0 if rows == 0 else active_rows / rows,
        "active_rows": active_rows,
        "trades": metrics.trades,
        "exposure": metrics.exposure,
        "total_return": metrics.total_return,
        "max_drawdown": metrics.max_drawdown,
        "final_equity": metrics.final_equity,
        "profit_factor": metrics.profit_factor,
        "return_per_trade": (
            None
            if not metrics.trades or metrics.trades <= 0
            else metrics.total_return / metrics.trades
        ),
    }


def split_test_frame_into_subperiods(
    frame: pd.DataFrame,
    *,
    parts: int = 3,
) -> list[tuple[str, pd.DataFrame]]:
    if parts <= 0:
        raise ValueError("parts must be positive.")
    if frame.empty:
        raise ValueError("Cannot split an empty frame into subperiods.")

    boundaries = [int(len(frame) * index / parts) for index in range(parts + 1)]
    subperiods: list[tuple[str, pd.DataFrame]] = []
    for index in range(parts):
        start = boundaries[index]
        end = boundaries[index + 1]
        subframe = frame.iloc[start:end].copy().reset_index(drop=True)
        if subframe.empty:
            continue
        subperiods.append((f"subperiod_{index + 1}", subframe))
    return subperiods


def build_subperiod_stress_table(
    test_predictions: pd.DataFrame,
    *,
    variant: PolicyVariant,
    initial_capital: float,
    fee_rate: float,
    slippage_rate: float,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for subperiod_id, subframe in split_test_frame_into_subperiods(test_predictions, parts=3):
        signal_frame = build_policy_signal_frame(subframe, variant=variant)
        metrics, _equity_curve = run_policy_backtest(
            signal_frame,
            initial_capital=initial_capital,
            fee_rate=fee_rate,
            slippage_rate=slippage_rate,
        )
        rows.append(
            {
                "policy_id": variant.policy_id,
                "subperiod_id": subperiod_id,
                "start_timestamp": int(subframe["timestamp"].iloc[0]),
                "end_timestamp": int(subframe["timestamp"].iloc[-1]),
                "rows": len(subframe),
                "active_rows": int(signal_frame["signal"].sum()),
                "activation_rate": float(signal_frame["signal"].mean()),
                "trades": metrics.trades,
                "exposure": metrics.exposure,
                "total_return": metrics.total_return,
                "max_drawdown": metrics.max_drawdown,
                "final_equity": metrics.final_equity,
            }
        )
    return pd.DataFrame(rows)


def run_policy_stress_variants(
    test_predictions: pd.DataFrame,
    *,
    initial_capital: float,
    fee_rate: float,
    slippage_rate: float,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    signal_frames: list[pd.DataFrame] = []
    summary_rows: list[dict[str, object]] = []
    subperiod_frames: list[pd.DataFrame] = []

    for variant in get_policy_stress_variants():
        signal_frame = build_policy_signal_frame(test_predictions, variant=variant)
        metrics, _equity_curve = run_policy_backtest(
            signal_frame,
            initial_capital=initial_capital,
            fee_rate=fee_rate,
            slippage_rate=slippage_rate,
        )
        signal_frames.append(signal_frame)
        summary_rows.append(build_policy_stress_summary(signal_frame, metrics))
        subperiod_frames.append(
            build_subperiod_stress_table(
                test_predictions,
                variant=variant,
                initial_capital=initial_capital,
                fee_rate=fee_rate,
                slippage_rate=slippage_rate,
            )
        )

    return (
        pd.concat(signal_frames, ignore_index=True),
        pd.DataFrame(summary_rows),
        pd.concat(subperiod_frames, ignore_index=True),
    )


def build_stress_conclusion(
    stress_summary_frame: pd.DataFrame,
    comparison_frame: pd.DataFrame,
) -> str:
    baseline = stress_summary_frame.loc[
        stress_summary_frame["policy_id"].eq("stress_regime_p065")
    ].iloc[0]
    local_range = (
        stress_summary_frame["total_return"].max() - stress_summary_frame["total_return"].min()
    )
    best_variant = stress_summary_frame.sort_values("total_return", ascending=False).iloc[0]
    hypothesis_5 = comparison_frame.loc[
        comparison_frame["name"].eq("Hypothesis 5 - Volatility Filter (Trend + vol band)")
    ].iloc[0]

    if float(best_variant["total_return"]) <= float(hypothesis_5["total_return"]):
        return (
            "La senal condicional no soporta un stress test minimo: incluso con estres local "
            "no supera de forma material a Hypothesis 5."
        )
    if float(local_range) > 0.05 or float(baseline["activation_rate"]) < 0.005:
        return (
            "La senal condicional sigue siendo demasiado fragil: pequenas variaciones de umbral "
            "mueven mucho el resultado y la activacion sigue siendo muy baja."
        )
    return (
        "La senal condicional soporta un stress test minimo: el rendimiento cambia poco "
        "en la vecindad local y sigue superando a los baselines operativos relevantes."
    )
