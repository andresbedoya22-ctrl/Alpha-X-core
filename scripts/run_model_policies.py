from __future__ import annotations

import argparse
from dataclasses import asdict
from pathlib import Path

import pandas as pd

from alpha_x.backtest.metrics import benchmark_result_to_performance_row
from alpha_x.benchmarks.buy_and_hold import run_buy_and_hold
from alpha_x.config.settings import get_settings
from alpha_x.data.ohlcv_storage import build_ohlcv_csv_path
from alpha_x.features.engine import build_dataset_context, load_feature_dataset
from alpha_x.modeling.dataset import TARGET_COLUMN, build_supervised_dataset
from alpha_x.modeling.evaluation import (
    build_temporal_model_splits,
    build_test_baseline_comparison,
    fit_and_evaluate_models,
    refit_best_model_for_test_signal,
)
from alpha_x.modeling.policy import (
    build_policy_signal_frame,
    build_policy_summary,
    get_policy_variants,
    run_policy_backtest,
)
from alpha_x.modeling.policy_reporting import export_policy_report
from alpha_x.reporting.io import build_run_id
from alpha_x.utils.logging_utils import configure_logging


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    settings = get_settings()
    parser = argparse.ArgumentParser(
        description="Run V3/F3.4 decision policies on supervised model scores."
    )
    parser.add_argument("--market", default=settings.bitvavo_market, help="Market symbol.")
    parser.add_argument(
        "--timeframe",
        default=settings.ohlcv_default_interval,
        help="OHLCV timeframe.",
    )
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--no-export", action="store_true")
    return parser.parse_args(argv)


def build_conclusion(
    policy_summary_frame: pd.DataFrame,
    backtest_comparison_frame: pd.DataFrame,
) -> str:
    best_policy = policy_summary_frame.sort_values("total_return", ascending=False).iloc[0]
    hypothesis_5 = backtest_comparison_frame.loc[
        backtest_comparison_frame["name"].eq("Hypothesis 5 - Volatility Filter (Trend + vol band)")
    ].iloc[0]

    if float(best_policy["total_return"]) > float(hypothesis_5["total_return"]):
        if float(best_policy["activation_rate"]) < 0.02:
            return (
                "Hay evidencia minima pero fragil: una policy supera a Hypothesis 5, "
                "aunque con activacion muy baja y riesgo de estar mejorando "
                "sobre todo por no operar."
            )
        return (
            "Hay evidencia minima de edge condicional: al menos una policy supera a Hypothesis 5 "
            "sin apagar casi por completo la operativa."
        )
    return (
        "No hay evidencia minima de edge condicional suficientemente util: "
        "las policies no superan de forma material a Hypothesis 5 en el periodo test."
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = get_settings()
    created_at = pd.Timestamp.now(tz="UTC").floor("s")
    run_id = args.run_id or build_run_id(created_at)
    logger = configure_logging(
        settings.log_dir,
        settings.log_level,
        logger_name="alpha_x_model_policies",
    )

    dataset_path = build_ohlcv_csv_path(
        raw_data_dir=settings.raw_data_dir,
        exchange="bitvavo",
        market=args.market,
        timeframe=args.timeframe,
    )
    dataset = load_feature_dataset(Path(dataset_path), args.timeframe)
    _full_frame, supervised_frame, feature_columns, categorical_columns, dataset_summary = (
        build_supervised_dataset(dataset, timeframe=args.timeframe)
    )
    _evaluation_rows, _fitted_models, best_model_id = fit_and_evaluate_models(
        supervised_frame,
        feature_columns=feature_columns,
        categorical_columns=categorical_columns,
        target_column=TARGET_COLUMN,
    )
    _pipeline, test_predictions = refit_best_model_for_test_signal(
        supervised_frame,
        feature_columns=feature_columns,
        categorical_columns=categorical_columns,
        target_column=TARGET_COLUMN,
        best_model_id=best_model_id,
    )

    policy_signal_frames: list[pd.DataFrame] = []
    policy_summaries: list[dict[str, object]] = []
    comparison_rows: list[dict[str, object]] = []

    for variant in get_policy_variants():
        signal_frame = build_policy_signal_frame(test_predictions, variant=variant)
        metrics, _equity_curve = run_policy_backtest(
            signal_frame,
            initial_capital=settings.benchmark_initial_capital,
            fee_rate=settings.benchmark_fee_rate,
            slippage_rate=0.0005,
        )
        policy_signal_frames.append(signal_frame)
        policy_summary = build_policy_summary(signal_frame, metrics)
        policy_summaries.append(policy_summary)
        comparison_rows.append(
            {
                "name": variant.name,
                "source_id": variant.policy_id,
                "total_return": metrics.total_return,
                "annualized_return": metrics.annualized_return,
                "max_drawdown": metrics.max_drawdown,
                "profit_factor": metrics.profit_factor,
                "trades": metrics.trades,
                "exposure": metrics.exposure,
                "final_equity": metrics.final_equity,
                "activation_rate": policy_summary["activation_rate"],
                "regime_pass_rate": policy_summary["regime_pass_rate"],
            }
        )

    split_frames = build_temporal_model_splits(supervised_frame)
    test_start_timestamp = int(split_frames["test"]["timestamp"].iloc[0])
    test_dataset_frame = dataset.frame.loc[
        dataset.frame["timestamp"] >= test_start_timestamp
    ].copy()
    baseline_rows = build_test_baseline_comparison(
        test_dataset_frame,
        fee_rate=settings.benchmark_fee_rate,
        slippage_rate=0.0005,
        initial_capital=settings.benchmark_initial_capital,
        sma_fast=settings.benchmark_sma_fast,
        sma_slow=settings.benchmark_sma_slow,
    )
    buy_and_hold = run_buy_and_hold(
        test_dataset_frame,
        fee_rate=settings.benchmark_fee_rate,
        initial_capital=settings.benchmark_initial_capital,
    )
    buy_and_hold_row = asdict(
        benchmark_result_to_performance_row(buy_and_hold, source_id="buy_and_hold")
    )
    buy_and_hold_row["activation_rate"] = 1.0
    buy_and_hold_row["regime_pass_rate"] = 1.0

    backtest_comparison_frame = pd.DataFrame(
        comparison_rows
        + [
            {**row, "activation_rate": None, "regime_pass_rate": None} for row in baseline_rows
        ]
        + [buy_and_hold_row]
    )
    policy_summary_frame = pd.DataFrame(policy_summaries)
    policy_signals_frame = pd.concat(policy_signal_frames, ignore_index=True)

    conclusion = build_conclusion(policy_summary_frame, backtest_comparison_frame)
    dataset_context = build_dataset_context(dataset)
    summary_payload = {
        "base_model_used": best_model_id,
        "training_window": "train + validation, scored on test only.",
        "test_start_timestamp": int(test_predictions["timestamp"].iloc[0]),
        "test_end_timestamp": int(test_predictions["timestamp"].iloc[-1]),
        "scored_rows": len(test_predictions),
        "dataset_summary": dataset_summary,
        "policy_variants": [asdict(variant) for variant in get_policy_variants()],
        "conclusion": conclusion,
    }
    parameters = {
        "timeframe": args.timeframe,
        "fee_rate": settings.benchmark_fee_rate,
        "slippage_rate": 0.0005,
        "initial_capital": settings.benchmark_initial_capital,
        "best_model_id": best_model_id,
    }

    report_dir: Path | None = None
    if not args.no_export:
        report_dir = export_policy_report(
            reports_dir=settings.reports_dir,
            run_id=run_id,
            created_at=created_at,
            dataset_context=dataset_context,
            parameters=parameters,
            summary_payload=summary_payload,
            scored_test_frame=test_predictions,
            policy_signals_frame=policy_signals_frame,
            policy_summary_frame=policy_summary_frame,
            backtest_comparison_frame=backtest_comparison_frame,
        )

    logger.info(
        "run_id=%s base_model=%s scored_rows=%s policies=%s",
        run_id,
        best_model_id,
        len(test_predictions),
        len(policy_summaries),
    )

    print(f"Modelo base usado: {best_model_id}")
    print(
        f"Periodo test usado: {test_predictions['timestamp'].iloc[0]} -> "
        f"{test_predictions['timestamp'].iloc[-1]}"
    )
    print(f"Filas scoreadas: {len(test_predictions)}")
    print()
    print("Activation rate por variante:")
    print(
        policy_summary_frame.loc[
            :,
            [
                "policy_id",
                "threshold",
                "allowed_regime",
                "activation_rate",
                "regime_pass_rate",
                "trades",
                "exposure",
                "total_return",
                "max_drawdown",
                "final_equity",
            ],
        ].to_string(index=False)
    )
    print()
    print("Comparacion de backtest:")
    print(backtest_comparison_frame.to_string(index=False))
    print()
    print("Conclusion:")
    print(conclusion)
    if report_dir is not None:
        print()
        print(f"Report path: {report_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
