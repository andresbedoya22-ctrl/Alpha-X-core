from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from alpha_x.backtest.data_loader import load_backtest_dataset
from alpha_x.backtest.engine import run_long_flat_backtest
from alpha_x.backtest.metrics import (
    PerformanceRow,
    benchmark_result_to_performance_row,
    calculate_backtest_metrics,
)
from alpha_x.backtest.reporting import (
    build_dataset_summary,
    build_gap_status,
    build_performance_table,
)
from alpha_x.benchmarks.buy_and_hold import run_buy_and_hold
from alpha_x.benchmarks.dca import run_monthly_dca
from alpha_x.benchmarks.sma_baseline import run_sma_baseline
from alpha_x.config.settings import get_settings
from alpha_x.data.ohlcv_storage import build_ohlcv_csv_path
from alpha_x.reporting.io import (
    build_run_id,
    create_report_directory,
    list_report_files,
    write_json_file,
    write_table_csv,
)
from alpha_x.reporting.run_manifest import build_run_manifest
from alpha_x.reporting.serializers import (
    build_equity_curves_frame,
    build_summary_payload,
    performance_rows_to_frame,
    serialize_value,
)
from alpha_x.strategies.catalog import get_strategy_catalog
from alpha_x.utils.logging_utils import configure_logging


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    settings = get_settings()
    parser = argparse.ArgumentParser(
        description="Run V2/F2.1 hypothesis strategies on the honest long/flat backtester."
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
        default=None,
        help=(
            "Trading fee in basis points. Preferred option. "
            f"Default effective value: {settings.benchmark_fee_rate * 10_000:.2f} bps"
        ),
    )
    fee_group.add_argument(
        "--fee",
        type=float,
        default=None,
        help="Trading fee as decimal rate. Example: 0.001 means 10 bps.",
    )
    slippage_group = parser.add_mutually_exclusive_group()
    slippage_group.add_argument(
        "--slippage-bps",
        type=float,
        default=5.0,
        help="Execution slippage in basis points for hypothesis backtests.",
    )
    slippage_group.add_argument(
        "--slippage",
        type=float,
        default=None,
        help="Execution slippage as decimal rate. Example: 0.0005 means 5 bps.",
    )
    parser.add_argument(
        "--initial-capital",
        type=float,
        default=settings.benchmark_initial_capital,
        help="Initial capital for each strategy backtest.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional run identifier. If omitted, a UTC timestamp-based run_id is used.",
    )
    parser.add_argument(
        "--no-export",
        action="store_true",
        help="Skip report export under reports/hypotheses/<run_id>/.",
    )
    return parser.parse_args(argv)


def resolve_fee_rate(args: argparse.Namespace, default_fee_rate: float) -> float:
    if args.fee_bps is not None:
        if args.fee_bps < 0:
            raise ValueError("--fee-bps must be non-negative.")
        return args.fee_bps / 10_000.0
    if args.fee is not None:
        if args.fee < 0:
            raise ValueError("--fee must be non-negative.")
        return args.fee
    return default_fee_rate


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


def run_hypotheses(
    *,
    dataset_frame: pd.DataFrame,
    initial_capital: float,
    fee_rate: float,
    slippage_rate: float,
) -> tuple[list[PerformanceRow], list[tuple[str, pd.DataFrame]], list[tuple[str, pd.DataFrame]]]:
    performance_rows: list[PerformanceRow] = []
    equity_curves: list[tuple[str, pd.DataFrame]] = []
    signal_frames: list[tuple[str, pd.DataFrame]] = []

    for strategy in get_strategy_catalog():
        signal_frame = strategy.build_signal(dataset_frame)
        backtest = run_long_flat_backtest(
            signal_frame,
            initial_capital=initial_capital,
            fee_rate=fee_rate,
            slippage_rate=slippage_rate,
            name=strategy.name,
        )
        row = calculate_backtest_metrics(backtest)
        performance_rows.append(row)
        equity_curves.append((strategy.name, backtest.equity_curve))

        signal_export = signal_frame.copy()
        signal_export.insert(0, "strategy_id", strategy.strategy_id)
        signal_export.insert(1, "strategy_name", strategy.name)
        signal_frames.append((strategy.name, signal_export))

    return performance_rows, equity_curves, signal_frames


def export_hypothesis_report(
    *,
    run_id: str,
    created_at: pd.Timestamp,
    dataset: object,
    parameters: dict[str, object],
    summary_rows: list[PerformanceRow],
    equity_curves: list[tuple[str, pd.DataFrame]],
    signal_frames: list[tuple[str, pd.DataFrame]],
) -> Path:
    settings = get_settings()
    dataset_context = build_dataset_context(dataset)
    report_dir = create_report_directory(settings.reports_dir, "hypotheses", run_id)
    summary_payload = build_summary_payload(
        run_id=run_id,
        report_type="hypotheses",
        created_at=created_at,
        dataset=dataset_context,
        parameters=parameters,
        summary_rows=summary_rows,
    )
    write_json_file(report_dir / "summary.json", summary_payload)
    write_table_csv(report_dir / "summary.csv", performance_rows_to_frame(summary_rows))
    write_table_csv(report_dir / "equity_curves.csv", build_equity_curves_frame(equity_curves))
    write_table_csv(
        report_dir / "signals.csv",
        pd.concat([frame for _, frame in signal_frames], ignore_index=True),
    )

    artifacts = list_report_files(report_dir)
    manifest = build_run_manifest(
        run_id=run_id,
        report_type="hypotheses",
        created_at=created_at,
        dataset=dataset_context,
        parameters=parameters,
        summary_rows=summary_rows,
        artifacts=artifacts + ["manifest.json"],
    )
    write_json_file(report_dir / "manifest.json", serialize_value(manifest))
    return report_dir


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = get_settings()
    fee_rate = resolve_fee_rate(args, settings.benchmark_fee_rate)
    slippage_rate = resolve_slippage_rate(args)
    created_at = pd.Timestamp.now(tz="UTC").floor("s")
    run_id = args.run_id or build_run_id(created_at)
    logger = configure_logging(
        settings.log_dir,
        settings.log_level,
        logger_name="alpha_x_hypotheses",
    )

    dataset_path = build_ohlcv_csv_path(
        raw_data_dir=settings.raw_data_dir,
        exchange="bitvavo",
        market=args.market,
        timeframe=args.timeframe,
    )
    dataset = load_backtest_dataset(Path(dataset_path), args.timeframe)

    strategy_rows, equity_curves, signal_frames = run_hypotheses(
        dataset_frame=dataset.frame,
        initial_capital=args.initial_capital,
        fee_rate=fee_rate,
        slippage_rate=slippage_rate,
    )

    buy_and_hold = run_buy_and_hold(
        dataset.frame,
        fee_rate=fee_rate,
        initial_capital=args.initial_capital,
    )
    dca = run_monthly_dca(
        dataset.frame,
        fee_rate=fee_rate,
        contribution=settings.benchmark_dca_amount,
    )
    sma_benchmark = run_sma_baseline(
        dataset.frame,
        fee_rate=fee_rate,
        initial_capital=args.initial_capital,
        fast_window=settings.benchmark_sma_fast,
        slow_window=settings.benchmark_sma_slow,
    )

    benchmark_rows = [
        benchmark_result_to_performance_row(buy_and_hold),
        benchmark_result_to_performance_row(dca),
        benchmark_result_to_performance_row(sma_benchmark),
    ]
    performance_rows = strategy_rows + benchmark_rows

    parameters = {
        "fee_rate": fee_rate,
        "fee_bps": fee_rate * 10_000.0,
        "slippage_rate": slippage_rate,
        "slippage_bps": slippage_rate * 10_000.0,
        "initial_capital": args.initial_capital,
        "benchmark_dca_amount": settings.benchmark_dca_amount,
        "benchmark_sma_fast": settings.benchmark_sma_fast,
        "benchmark_sma_slow": settings.benchmark_sma_slow,
        "execution_rule": "Signal observed on close[t], executed on close[t+1].",
    }

    report_dir: Path | None = None
    if not args.no_export:
        report_dir = export_hypothesis_report(
            run_id=run_id,
            created_at=created_at,
            dataset=dataset,
            parameters=parameters,
            summary_rows=performance_rows,
            equity_curves=equality_curves_with_benchmarks(
                equity_curves,
                buy_and_hold,
                dca,
                sma_benchmark,
            ),
            signal_frames=signal_frames,
        )

    logger.info("Loaded hypothesis dataset from %s", dataset_path)
    logger.info(
        "run_id=%s hypotheses=%s benchmarks=%s gaps=%s",
        run_id,
        len(strategy_rows),
        len(benchmark_rows),
        dataset.gap_summary.gap_count,
    )

    print(build_dataset_summary(dataset))
    print(build_gap_status(dataset))
    print(
        f"Fees: {fee_rate:.6f} ({fee_rate * 10_000.0:.2f} bps) | "
        f"Slippage: {slippage_rate:.6f} ({slippage_rate * 10_000.0:.2f} bps)"
    )
    print("Execution rule: Signal observed on close[t], executed on close[t+1].")
    if report_dir is not None:
        print(f"Report path: {report_dir}")
    print()
    print(build_performance_table(performance_rows))
    return 0


def equality_curves_with_benchmarks(
    strategy_curves: list[tuple[str, pd.DataFrame]],
    buy_and_hold: object,
    dca: object,
    sma_benchmark: object,
) -> list[tuple[str, pd.DataFrame]]:
    return strategy_curves + [
        (buy_and_hold.name, buy_and_hold.equity_curve),
        (dca.name, dca.equity_curve),
        (sma_benchmark.name, sma_benchmark.equity_curve),
    ]


if __name__ == "__main__":
    raise SystemExit(main())
