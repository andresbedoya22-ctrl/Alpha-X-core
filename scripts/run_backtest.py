from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from alpha_x.backtest.data_loader import load_backtest_dataset
from alpha_x.backtest.engine import run_long_flat_backtest
from alpha_x.backtest.metrics import (
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
from alpha_x.utils.logging_utils import configure_logging


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    settings = get_settings()
    parser = argparse.ArgumentParser(
        description="Run the F1.4 honest long/flat backtest on the persisted OHLCV CSV."
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
    slippage_group = parser.add_mutually_exclusive_group()
    slippage_group.add_argument(
        "--slippage-bps",
        type=float,
        default=5.0,
        help="Execution slippage in basis points for the F1.4 strategy.",
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
        help="Initial capital for the backtest.",
    )
    parser.add_argument(
        "--sma-fast",
        type=int,
        default=settings.benchmark_sma_fast,
        help="Fast SMA window for the sample long/flat signal.",
    )
    parser.add_argument(
        "--sma-slow",
        type=int,
        default=settings.benchmark_sma_slow,
        help="Slow SMA window for the sample long/flat signal.",
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


def build_sample_signal(frame: pd.DataFrame, fast_window: int, slow_window: int) -> pd.DataFrame:
    if fast_window <= 0 or slow_window <= 0:
        raise ValueError("SMA windows must be positive.")
    if fast_window >= slow_window:
        raise ValueError("SMA fast window must be smaller than slow window.")

    prepared = frame.loc[:, ["timestamp", "datetime", "close"]].copy()
    prepared["sma_fast"] = prepared["close"].rolling(
        window=fast_window,
        min_periods=fast_window,
    ).mean()
    prepared["sma_slow"] = prepared["close"].rolling(
        window=slow_window,
        min_periods=slow_window,
    ).mean()
    prepared["signal"] = (
        (prepared["sma_fast"] > prepared["sma_slow"])
        & prepared["sma_fast"].notna()
        & prepared["sma_slow"].notna()
    ).astype("int64")
    return prepared


def main() -> int:
    args = parse_args()
    settings = get_settings()
    fee_rate = resolve_fee_rate(args, settings.benchmark_fee_rate)
    slippage_rate = resolve_slippage_rate(args)
    logger = configure_logging(
        settings.log_dir,
        settings.log_level,
        logger_name="alpha_x_backtest",
    )

    dataset_path = build_ohlcv_csv_path(
        raw_data_dir=settings.raw_data_dir,
        exchange="bitvavo",
        market=args.market,
        timeframe=args.timeframe,
    )
    dataset = load_backtest_dataset(Path(dataset_path), args.timeframe)
    signal_frame = build_sample_signal(dataset.frame, args.sma_fast, args.sma_slow)

    backtest = run_long_flat_backtest(
        signal_frame,
        initial_capital=args.initial_capital,
        fee_rate=fee_rate,
        slippage_rate=slippage_rate,
        name=f"F1.4 - SMA Long/Flat ({args.sma_fast}/{args.sma_slow})",
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
        fast_window=args.sma_fast,
        slow_window=args.sma_slow,
    )

    performance_rows = [
        calculate_backtest_metrics(backtest),
        benchmark_result_to_performance_row(buy_and_hold),
        benchmark_result_to_performance_row(dca),
        benchmark_result_to_performance_row(sma_benchmark),
    ]

    logger.info("Loaded backtest dataset from %s", dataset_path)
    logger.info(
        "Residual gaps=%s missing_intervals=%s",
        dataset.gap_summary.gap_count,
        dataset.gap_summary.total_missing_intervals,
    )
    logger.info("Fee rate=%.6f | Slippage rate=%.6f", fee_rate, slippage_rate)

    print(build_dataset_summary(dataset))
    print(build_gap_status(dataset))
    print(
        f"Fees: {fee_rate:.6f} ({fee_rate * 10_000.0:.2f} bps) | "
        f"Slippage: {slippage_rate:.6f} ({slippage_rate * 10_000.0:.2f} bps)"
    )
    print("Execution rule: Signal observed on close[t], executed on close[t+1].")
    print()
    print(build_performance_table(performance_rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
