from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from alpha_x.backtest.data_loader import load_backtest_dataset
from alpha_x.backtest.engine import run_long_flat_backtest
from alpha_x.backtest.metrics import benchmark_result_to_performance_row, calculate_backtest_metrics
from alpha_x.benchmarks.sma_baseline import run_sma_baseline
from alpha_x.config.settings import get_settings
from alpha_x.data.ohlcv_storage import build_ohlcv_csv_path
from alpha_x.labeling.catalog import get_labeling_catalog
from alpha_x.reporting.io import (
    build_run_id,
    create_report_directory,
    list_report_files,
    write_json_file,
    write_table_csv,
)
from alpha_x.reporting.run_manifest import build_run_manifest
from alpha_x.reporting.serializers import serialize_value
from alpha_x.strategies.trend import build_trend_signal
from alpha_x.strategies.volatility import build_volatility_filter_signal
from alpha_x.utils.logging_utils import configure_logging
from alpha_x.validation.base import ValidationCandidate, ValidationResultRow
from alpha_x.validation.reporting import (
    build_oos_aggregate,
    build_oos_table,
    build_validation_table,
    validation_rows_to_frame,
)
from alpha_x.validation.sensitivity import (
    get_parameter_sensitivity_grid,
    get_validation_candidates,
)
from alpha_x.validation.splits import (
    build_temporal_splits,
    slice_frame_for_split,
    summarize_segment_gaps,
)
from alpha_x.validation.walk_forward import (
    build_expanding_walk_forward_windows,
    slice_test_frame,
    slice_train_frame,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    settings = get_settings()
    parser = argparse.ArgumentParser(
        description="Run V2/F2.3 temporal validation, walk-forward and sensitivity checks."
    )
    parser.add_argument("--market", default=settings.bitvavo_market, help="Market symbol.")
    parser.add_argument(
        "--timeframe",
        default=settings.ohlcv_default_interval,
        help="OHLCV timeframe.",
    )
    fee_group = parser.add_mutually_exclusive_group()
    fee_group.add_argument(
        "--fee-bps",
        type=float,
        default=25.0,
        help="Trading fee in basis points for validation. Default is intentionally aggressive.",
    )
    fee_group.add_argument(
        "--fee",
        type=float,
        default=None,
        help="Trading fee as decimal rate.",
    )
    slippage_group = parser.add_mutually_exclusive_group()
    slippage_group.add_argument(
        "--slippage-bps",
        type=float,
        default=10.0,
        help="Execution slippage in basis points for validation.",
    )
    slippage_group.add_argument(
        "--slippage",
        type=float,
        default=None,
        help="Execution slippage as decimal rate.",
    )
    parser.add_argument(
        "--initial-capital",
        type=float,
        default=settings.benchmark_initial_capital,
        help="Initial capital for each segment backtest.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional run identifier. If omitted, a UTC timestamp-based run_id is used.",
    )
    parser.add_argument(
        "--no-export",
        action="store_true",
        help="Skip report export under reports/validation/<run_id>/.",
    )
    return parser.parse_args(argv)


def resolve_fee_rate(args: argparse.Namespace) -> float:
    if args.fee is not None:
        if args.fee < 0:
            raise ValueError("--fee must be non-negative.")
        return args.fee
    if args.fee_bps < 0:
        raise ValueError("--fee-bps must be non-negative.")
    return args.fee_bps / 10_000.0


def resolve_slippage_rate(args: argparse.Namespace) -> float:
    if args.slippage is not None:
        if args.slippage < 0:
            raise ValueError("--slippage must be non-negative.")
        return args.slippage
    if args.slippage_bps < 0:
        raise ValueError("--slippage-bps must be non-negative.")
    return args.slippage_bps / 10_000.0


def build_dataset_context(dataset: object) -> dict[str, object]:
    dataset_info = dataset.dataset_info
    gap_summary = dataset.gap_summary
    return {
        "path": dataset_info.path,
        "market": dataset_info.market,
        "timeframe": dataset_info.timeframe,
        "rows": dataset_info.row_count,
        "start_timestamp": dataset_info.start_timestamp,
        "end_timestamp": dataset_info.end_timestamp,
        "gap_count": gap_summary.gap_count,
        "total_missing_intervals": gap_summary.total_missing_intervals,
        "largest_gap": gap_summary.largest_gap,
    }


def parameter_label(parameters: dict[str, float | int]) -> str:
    return ",".join(f"{key}={value}" for key, value in parameters.items())


def run_candidate_on_frame(
    candidate: ValidationCandidate,
    frame: pd.DataFrame,
    *,
    parameters: dict[str, float | int],
    initial_capital: float,
    fee_rate: float,
    slippage_rate: float,
) -> object:
    if candidate.candidate_id == "benchmark_sma_crossover":
        return run_sma_baseline(
            frame,
            fee_rate=fee_rate,
            initial_capital=initial_capital,
            fast_window=int(parameters["fast_window"]),
            slow_window=int(parameters["slow_window"]),
        )
    if candidate.candidate_id == "trend_sma200":
        signal_frame = build_trend_signal(frame, slow_window=int(parameters["slow_window"]))
        return run_long_flat_backtest(
            signal_frame,
            initial_capital=initial_capital,
            fee_rate=fee_rate,
            slippage_rate=slippage_rate,
            name=candidate.name,
        )
    if candidate.candidate_id == "trend_volatility_filter":
        signal_frame = build_volatility_filter_signal(
            frame,
            base_slow_window=int(parameters["base_slow_window"]),
            volatility_window=int(parameters["volatility_window"]),
            min_volatility=float(parameters["min_volatility"]),
            max_volatility=float(parameters["max_volatility"]),
        )
        return run_long_flat_backtest(
            signal_frame,
            initial_capital=initial_capital,
            fee_rate=fee_rate,
            slippage_rate=slippage_rate,
            name=candidate.name,
        )
    raise ValueError(f"Unsupported candidate: {candidate.candidate_id}")


def performance_to_validation_row(
    candidate: ValidationCandidate,
    performance: object,
    *,
    mode: str,
    segment: str,
    split_id: str,
    parameter_set: str,
    rows: int,
    start_timestamp: int,
    end_timestamp: int,
    gap_count: int,
    total_missing_intervals: int,
) -> ValidationResultRow:
    if candidate.source_type == "benchmark":
        metrics = benchmark_result_to_performance_row(performance)
    else:
        metrics = calculate_backtest_metrics(performance)
    return ValidationResultRow(
        candidate_id=candidate.candidate_id,
        candidate_name=candidate.name,
        family=candidate.family,
        source_type=candidate.source_type,
        mode=mode,
        segment=segment,
        split_id=split_id,
        parameter_set=parameter_set,
        total_return=metrics.total_return,
        annualized_return=metrics.annualized_return,
        max_drawdown=metrics.max_drawdown,
        profit_factor=metrics.profit_factor,
        trades=metrics.trades,
        exposure=metrics.exposure,
        final_equity=metrics.final_equity,
        rows=rows,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        gap_count=gap_count,
        total_missing_intervals=total_missing_intervals,
    )


def run_temporal_split_validation(
    frame: pd.DataFrame,
    *,
    timeframe: str,
    initial_capital: float,
    fee_rate: float,
    slippage_rate: float,
) -> list[ValidationResultRow]:
    rows: list[ValidationResultRow] = []
    for split in build_temporal_splits(frame):
        split_frame = slice_frame_for_split(frame, split)
        gap_count, total_missing_intervals = summarize_segment_gaps(
            split_frame,
            timeframe=timeframe,
        )
        for candidate in get_validation_candidates():
            result = run_candidate_on_frame(
                candidate,
                split_frame,
                parameters=candidate.parameters,
                initial_capital=initial_capital,
                fee_rate=fee_rate,
                slippage_rate=slippage_rate,
            )
            rows.append(
                performance_to_validation_row(
                    candidate,
                    result,
                    mode="temporal_split",
                    segment=split.segment,
                    split_id=split.split_id,
                    parameter_set=parameter_label(candidate.parameters),
                    rows=len(split_frame),
                    start_timestamp=int(split_frame["timestamp"].iloc[0]),
                    end_timestamp=int(split_frame["timestamp"].iloc[-1]),
                    gap_count=gap_count,
                    total_missing_intervals=total_missing_intervals,
                )
            )
    return rows


def run_walk_forward_validation(
    frame: pd.DataFrame,
    *,
    timeframe: str,
    initial_capital: float,
    fee_rate: float,
    slippage_rate: float,
) -> list[ValidationResultRow]:
    rows: list[ValidationResultRow] = []
    train_size = int(len(frame) * 0.5)
    test_size = int(len(frame) * 0.1)
    windows = build_expanding_walk_forward_windows(
        frame,
        train_size=train_size,
        test_size=test_size,
    )
    for window in windows:
        train_frame = slice_train_frame(frame, window)
        test_frame = slice_test_frame(frame, window)
        train_gaps, train_missing = summarize_segment_gaps(train_frame, timeframe=timeframe)
        test_gaps, test_missing = summarize_segment_gaps(test_frame, timeframe=timeframe)
        for candidate in get_validation_candidates():
            train_result = run_candidate_on_frame(
                candidate,
                train_frame,
                parameters=candidate.parameters,
                initial_capital=initial_capital,
                fee_rate=fee_rate,
                slippage_rate=slippage_rate,
            )
            rows.append(
                performance_to_validation_row(
                    candidate,
                    train_result,
                    mode="walk_forward",
                    segment="walk_forward_train",
                    split_id=window.window_id,
                    parameter_set=parameter_label(candidate.parameters),
                    rows=len(train_frame),
                    start_timestamp=int(train_frame["timestamp"].iloc[0]),
                    end_timestamp=int(train_frame["timestamp"].iloc[-1]),
                    gap_count=train_gaps,
                    total_missing_intervals=train_missing,
                )
            )
            test_result = run_candidate_on_frame(
                candidate,
                test_frame,
                parameters=candidate.parameters,
                initial_capital=initial_capital,
                fee_rate=fee_rate,
                slippage_rate=slippage_rate,
            )
            rows.append(
                performance_to_validation_row(
                    candidate,
                    test_result,
                    mode="walk_forward",
                    segment="walk_forward_test",
                    split_id=window.window_id,
                    parameter_set=parameter_label(candidate.parameters),
                    rows=len(test_frame),
                    start_timestamp=int(test_frame["timestamp"].iloc[0]),
                    end_timestamp=int(test_frame["timestamp"].iloc[-1]),
                    gap_count=test_gaps,
                    total_missing_intervals=test_missing,
                )
            )
    return rows


def run_sensitivity_checks(
    frame: pd.DataFrame,
    *,
    timeframe: str,
    initial_capital: float,
    fee_rate: float,
    slippage_rate: float,
) -> list[ValidationResultRow]:
    rows: list[ValidationResultRow] = []
    test_split = next(split for split in build_temporal_splits(frame) if split.segment == "test")
    test_frame = slice_frame_for_split(frame, test_split)
    gap_count, total_missing_intervals = summarize_segment_gaps(test_frame, timeframe=timeframe)
    for candidate in get_validation_candidates():
        for parameter_set in get_parameter_sensitivity_grid(candidate):
            result = run_candidate_on_frame(
                candidate,
                test_frame,
                parameters=parameter_set,
                initial_capital=initial_capital,
                fee_rate=fee_rate,
                slippage_rate=slippage_rate,
            )
            rows.append(
                performance_to_validation_row(
                    candidate,
                    result,
                    mode="sensitivity",
                    segment="test",
                    split_id="sensitivity_test",
                    parameter_set=parameter_label(parameter_set),
                    rows=len(test_frame),
                    start_timestamp=int(test_frame["timestamp"].iloc[0]),
                    end_timestamp=int(test_frame["timestamp"].iloc[-1]),
                    gap_count=gap_count,
                    total_missing_intervals=total_missing_intervals,
                )
            )
    return rows


def export_validation_report(
    *,
    run_id: str,
    created_at: pd.Timestamp,
    dataset: object,
    parameters: dict[str, object],
    validation_rows: list[ValidationResultRow],
    oos_aggregate: pd.DataFrame,
) -> Path:
    settings = get_settings()
    dataset_context = build_dataset_context(dataset)
    report_dir = create_report_directory(settings.reports_dir, "validation", run_id)
    write_json_file(
        report_dir / "summary.json",
        {
            "run_id": run_id,
            "report_type": "validation",
            "created_at": serialize_value(created_at),
            "dataset": serialize_value(dataset_context),
            "parameters": serialize_value(parameters),
            "oos_summary": serialize_value(oos_aggregate.to_dict(orient="records")),
        },
    )
    write_table_csv(report_dir / "validation_rows.csv", validation_rows_to_frame(validation_rows))
    write_table_csv(report_dir / "oos_aggregate.csv", oos_aggregate)
    artifacts = list_report_files(report_dir)
    manifest = build_run_manifest(
        run_id=run_id,
        report_type="validation",
        created_at=created_at,
        dataset=dataset_context,
        parameters=parameters,
        summary_rows=[],
        artifacts=artifacts + ["manifest.json"],
    )
    write_json_file(report_dir / "manifest.json", serialize_value(manifest))
    return report_dir


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = get_settings()
    fee_rate = resolve_fee_rate(args)
    slippage_rate = resolve_slippage_rate(args)
    created_at = pd.Timestamp.now(tz="UTC").floor("s")
    run_id = args.run_id or build_run_id(created_at)
    logger = configure_logging(
        settings.log_dir,
        settings.log_level,
        logger_name="alpha_x_validation",
    )

    dataset_path = build_ohlcv_csv_path(
        raw_data_dir=settings.raw_data_dir,
        exchange="bitvavo",
        market=args.market,
        timeframe=args.timeframe,
    )
    dataset = load_backtest_dataset(Path(dataset_path), args.timeframe)
    split_rows = run_temporal_split_validation(
        dataset.frame,
        timeframe=args.timeframe,
        initial_capital=args.initial_capital,
        fee_rate=fee_rate,
        slippage_rate=slippage_rate,
    )
    walk_forward_rows = run_walk_forward_validation(
        dataset.frame,
        timeframe=args.timeframe,
        initial_capital=args.initial_capital,
        fee_rate=fee_rate,
        slippage_rate=slippage_rate,
    )
    sensitivity_rows = run_sensitivity_checks(
        dataset.frame,
        timeframe=args.timeframe,
        initial_capital=args.initial_capital,
        fee_rate=fee_rate,
        slippage_rate=slippage_rate,
    )
    validation_rows = split_rows + walk_forward_rows + sensitivity_rows
    oos_aggregate = build_oos_aggregate(validation_rows)
    triple_barrier = next(
        item for item in get_labeling_catalog() if item.labeling_id == "triple_barrier_24h"
    )

    parameters = {
        "fee_rate": fee_rate,
        "fee_bps": fee_rate * 10_000.0,
        "slippage_rate": slippage_rate,
        "slippage_bps": slippage_rate * 10_000.0,
        "initial_capital": args.initial_capital,
        "temporal_split": {"train_ratio": 0.6, "validation_ratio": 0.2, "test_ratio": 0.2},
        "walk_forward": {
            "scheme": "expanding",
            "train_size_rows": int(len(dataset.frame) * 0.5),
            "test_size_rows": int(len(dataset.frame) * 0.1),
            "step_size_rows": int(len(dataset.frame) * 0.1),
        },
        "sensitivity_scope": "Small local neighborhoods around default parameters.",
        "labeling_reference": {
            "name": triple_barrier.name,
            "parameters": triple_barrier.parameters,
            "note": (
                "Triple barrier is not used to score backtests in F2.3. "
                "It is the preferred operable-event reference for later supervised phases."
            ),
        },
    }

    report_dir: Path | None = None
    if not args.no_export:
        report_dir = export_validation_report(
            run_id=run_id,
            created_at=created_at,
            dataset=dataset,
            parameters=parameters,
            validation_rows=validation_rows,
            oos_aggregate=oos_aggregate,
        )

    dataset_context = build_dataset_context(dataset)
    logger.info("Loaded validation dataset from %s", dataset_path)
    logger.info(
        "run_id=%s rows=%s gaps=%s validation_rows=%s",
        run_id,
        dataset_context["rows"],
        dataset_context["gap_count"],
        len(validation_rows),
    )

    print(
        f"Dataset: {dataset_context['path']} | market={dataset_context['market']} | "
        f"timeframe={dataset_context['timeframe']} | rows={dataset_context['rows']}"
    )
    print(
        f"Gaps residuales: {dataset_context['gap_count']} | "
        f"missing_intervals={dataset_context['total_missing_intervals']} | "
        f"largest_gap={dataset_context['largest_gap']}"
    )
    print(
        f"Fees: {fee_rate:.6f} ({fee_rate * 10_000.0:.2f} bps) | "
        f"Slippage: {slippage_rate:.6f} ({slippage_rate * 10_000.0:.2f} bps)"
    )
    print(f"Triple barrier reference for later phases: {triple_barrier.parameters}")
    if report_dir is not None:
        print(f"Report path: {report_dir}")
    print()
    print("Temporal Split and Walk-Forward Rows")
    print(build_validation_table(validation_rows_to_frame(split_rows + walk_forward_rows)))
    print()
    print("OOS Aggregate")
    print(build_oos_table(oos_aggregate))
    print()
    print("Sensitivity on Test Split")
    print(build_validation_table(validation_rows_to_frame(sensitivity_rows)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
