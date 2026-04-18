from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from alpha_x.backtest.data_loader import load_backtest_dataset
from alpha_x.backtest.metrics import benchmark_result_to_performance_row
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
from alpha_x.reporting.serializers import (
    build_equity_curves_frame,
    build_summary_payload,
    performance_rows_to_frame,
)
from alpha_x.utils.logging_utils import configure_logging


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    settings = get_settings()
    parser = argparse.ArgumentParser(
        description="Export the F1.3 benchmark results to reproducible report files."
    )
    parser.add_argument(
        "--market",
        default=settings.bitvavo_market,
        help="Market symbol, e.g. BTC-EUR",
    )
    parser.add_argument(
        "--timeframe",
        default=settings.ohlcv_default_interval,
        help="OHLCV timeframe",
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
        help="Trading fee as decimal rate. Example: 0.0025 means 25 bps.",
    )
    parser.add_argument(
        "--initial-capital",
        type=float,
        default=settings.benchmark_initial_capital,
        help="Initial capital for Buy & Hold and SMA baseline",
    )
    parser.add_argument(
        "--dca-amount",
        type=float,
        default=settings.benchmark_dca_amount,
        help="Fixed monthly DCA contribution amount",
    )
    parser.add_argument(
        "--sma-fast",
        type=int,
        default=settings.benchmark_sma_fast,
        help="Fast SMA window",
    )
    parser.add_argument(
        "--sma-slow",
        type=int,
        default=settings.benchmark_sma_slow,
        help="Slow SMA window",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional run identifier. If omitted, a UTC timestamp-based run_id is used.",
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


def main() -> int:
    args = parse_args()
    settings = get_settings()
    fee_rate = resolve_fee_rate(args, settings.benchmark_fee_rate)
    created_at = pd.Timestamp.now(tz="UTC").floor("s")
    run_id = args.run_id or build_run_id(created_at)
    logger = configure_logging(
        settings.log_dir,
        settings.log_level,
        logger_name="alpha_x_export_benchmarks",
    )

    dataset_path = build_ohlcv_csv_path(
        raw_data_dir=settings.raw_data_dir,
        exchange="bitvavo",
        market=args.market,
        timeframe=args.timeframe,
    )
    dataset = load_backtest_dataset(Path(dataset_path), args.timeframe)

    buy_and_hold = run_buy_and_hold(
        dataset.frame,
        fee_rate=fee_rate,
        initial_capital=args.initial_capital,
    )
    dca = run_monthly_dca(
        dataset.frame,
        fee_rate=fee_rate,
        contribution=args.dca_amount,
    )
    sma = run_sma_baseline(
        dataset.frame,
        fee_rate=fee_rate,
        initial_capital=args.initial_capital,
        fast_window=args.sma_fast,
        slow_window=args.sma_slow,
    )

    summary_rows = [
        benchmark_result_to_performance_row(buy_and_hold),
        benchmark_result_to_performance_row(dca),
        benchmark_result_to_performance_row(sma),
    ]

    summary_frame = performance_rows_to_frame(summary_rows)
    equity_curves = build_equity_curves_frame(
        [
            (buy_and_hold.name, buy_and_hold.equity_curve),
            (dca.name, dca.equity_curve),
            (sma.name, sma.equity_curve),
        ]
    )
    dataset_context = build_dataset_context(dataset)
    parameters = {
        "fee_rate": fee_rate,
        "fee_bps": fee_rate * 10_000.0,
        "initial_capital": args.initial_capital,
        "dca_amount": args.dca_amount,
        "sma_fast": args.sma_fast,
        "sma_slow": args.sma_slow,
    }
    summary_payload = build_summary_payload(
        run_id=run_id,
        report_type="benchmarks",
        created_at=created_at,
        dataset=dataset_context,
        parameters=parameters,
        summary_rows=summary_rows,
    )

    report_dir = create_report_directory(settings.reports_dir, "benchmarks", run_id)
    write_json_file(report_dir / "summary.json", summary_payload)
    write_table_csv(report_dir / "summary.csv", summary_frame)
    write_table_csv(report_dir / "equity_curves.csv", equity_curves)
    created_files = list_report_files(report_dir)

    logger.info("Benchmark report exported to %s", report_dir)
    logger.info(
        "Run id=%s rows=%s gaps=%s",
        run_id,
        dataset_context["rows"],
        dataset_context["gap_count"],
    )

    print(f"Report path: {report_dir}")
    print("Files created:")
    for file_name in created_files:
        print(f"- {file_name}")
    print(
        "Run summary: "
        f"run_id={run_id} | rows={dataset_context['rows']} | "
        f"gaps={dataset_context['gap_count']} | fee_bps={parameters['fee_bps']:.2f}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
