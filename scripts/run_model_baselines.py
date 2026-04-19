from __future__ import annotations

import argparse
from dataclasses import asdict
from pathlib import Path

import pandas as pd

from alpha_x.config.settings import get_settings
from alpha_x.data.ohlcv_storage import build_ohlcv_csv_path
from alpha_x.features.engine import (
    build_dataset_context,
    load_feature_dataset,
)
from alpha_x.modeling.dataset import TARGET_COLUMN, build_supervised_dataset
from alpha_x.modeling.evaluation import (
    build_best_model_regime_metrics,
    build_temporal_model_splits,
    build_test_baseline_comparison,
    build_test_signal_backtest,
    fit_and_evaluate_models,
    refit_best_model_for_test_signal,
)
from alpha_x.modeling.reporting import export_modeling_report
from alpha_x.reporting.io import build_run_id
from alpha_x.utils.logging_utils import configure_logging


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    settings = get_settings()
    parser = argparse.ArgumentParser(
        description="Run V3/F3.3 supervised model baselines on the persisted BTC-EUR dataset."
    )
    parser.add_argument("--market", default=settings.bitvavo_market, help="Market symbol.")
    parser.add_argument(
        "--timeframe",
        default=settings.ohlcv_default_interval,
        help="OHLCV timeframe.",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.55,
        help="Probability threshold for the operational long/flat signal.",
    )
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--no-export", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = get_settings()
    created_at = pd.Timestamp.now(tz="UTC").floor("s")
    run_id = args.run_id or build_run_id(created_at)
    logger = configure_logging(
        settings.log_dir,
        settings.log_level,
        logger_name="alpha_x_modeling",
    )

    dataset_path = build_ohlcv_csv_path(
        raw_data_dir=settings.raw_data_dir,
        exchange="bitvavo",
        market=args.market,
        timeframe=args.timeframe,
    )
    dataset = load_feature_dataset(Path(dataset_path), args.timeframe)
    full_frame, supervised_frame, feature_columns, categorical_columns, dataset_summary = (
        build_supervised_dataset(dataset, timeframe=args.timeframe)
    )
    evaluation_rows, _fitted_models, best_model_id = fit_and_evaluate_models(
        supervised_frame,
        feature_columns=feature_columns,
        categorical_columns=categorical_columns,
        target_column=TARGET_COLUMN,
    )
    evaluation_frame = pd.DataFrame([asdict(row) for row in evaluation_rows])
    validation_scores = evaluation_frame.loc[evaluation_frame["segment"] == "validation"].copy()
    best_validation_row = (
        validation_scores.sort_values(["balanced_accuracy", "macro_f1"], ascending=False).iloc[0]
    )

    _refit_pipeline, test_predictions = refit_best_model_for_test_signal(
        supervised_frame,
        feature_columns=feature_columns,
        categorical_columns=categorical_columns,
        target_column=TARGET_COLUMN,
        best_model_id=best_model_id,
    )
    best_model_backtest, _equity_curve = build_test_signal_backtest(
        dataset.frame,
        test_predictions,
        threshold=args.threshold,
        fee_rate=settings.benchmark_fee_rate,
        slippage_rate=0.0005,
        initial_capital=settings.benchmark_initial_capital,
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
    backtest_comparison = pd.DataFrame([asdict(best_model_backtest), *baseline_rows])
    regime_metrics = build_best_model_regime_metrics(
        test_predictions,
        target_column=TARGET_COLUMN,
        threshold=args.threshold,
    )

    dataset_context = build_dataset_context(dataset)
    summary_payload = {
        "dataset_summary": dataset_summary,
        "best_model_id": best_model_id,
        "selection_rule": "Best validation balanced_accuracy, tie-break on macro_f1.",
        "best_validation_metrics": best_validation_row.to_dict(),
        "operational_threshold": args.threshold,
        "backtest_result": asdict(best_model_backtest),
    }
    parameters = {
        "timeframe": args.timeframe,
        "target_definition": dataset_summary["target_definition"],
        "threshold": args.threshold,
        "fee_rate": settings.benchmark_fee_rate,
        "slippage_rate": 0.0005,
        "initial_capital": settings.benchmark_initial_capital,
    }

    report_dir: Path | None = None
    if not args.no_export:
        report_dir = export_modeling_report(
            reports_dir=settings.reports_dir,
            run_id=run_id,
            created_at=created_at,
            dataset_context=dataset_context,
            parameters=parameters,
            summary_payload=summary_payload,
            supervised_frame=full_frame,
            evaluation_frame=evaluation_frame,
            regime_metrics=regime_metrics,
            backtest_comparison=backtest_comparison,
        )

    logger.info(
        "run_id=%s supervised_rows=%s best_model=%s threshold=%.2f",
        run_id,
        dataset_summary["supervised_rows"],
        best_model_id,
        args.threshold,
    )

    print(
        f"Dataset: {dataset_context['path']} | market={dataset_context['market']} | "
        f"timeframe={dataset_context['timeframe']} | rows={dataset_context['rows']}"
    )
    print(
        f"Filas supervisadas: {dataset_summary['supervised_rows']} | "
        "descartadas: "
        f"{dataset_summary['discarded_rows']} ({dataset_summary['discard_pct'] * 100:.2f}%)"
    )
    print(f"Target elegido: {dataset_summary['target_definition']}")
    print(f"Distribucion target: {dataset_summary['target_distribution']}")
    print(f"Descartes: {dataset_summary['discard_counts']}")
    print()
    print("Metricas por modelo:")
    print(evaluation_frame.to_string(index=False))
    print()
    print(
        f"Mejor modelo base: {best_model_id} | "
        f"validation_balanced_accuracy={best_validation_row['balanced_accuracy']:.4f} | "
        f"validation_macro_f1={best_validation_row['macro_f1']:.4f}"
    )
    print()
    print("Backtest comparativo en test:")
    print(backtest_comparison.to_string(index=False))
    print()
    print("Metricas del mejor modelo por regimen en test:")
    print(regime_metrics.to_string(index=False))
    if report_dir is not None:
        print()
        print(f"Report path: {report_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
