from __future__ import annotations

import argparse
from pathlib import Path

from alpha_x.benchmarks.buy_and_hold import run_buy_and_hold
from alpha_x.benchmarks.data_loader import load_benchmark_dataset
from alpha_x.benchmarks.dca import run_monthly_dca
from alpha_x.benchmarks.metrics import calculate_benchmark_metrics
from alpha_x.benchmarks.reporting import build_comparative_table, build_summary
from alpha_x.benchmarks.sma_baseline import run_sma_baseline
from alpha_x.config.settings import get_settings
from alpha_x.data.ohlcv_storage import build_ohlcv_csv_path
from alpha_x.utils.logging_utils import configure_logging


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    settings = get_settings()
    parser = argparse.ArgumentParser(
        description="Run F1.3 benchmark engine on persisted OHLCV CSV."
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
            "Trading fee in basis points. "
            "Preferred option. Default effective value: "
            f"{settings.benchmark_fee_rate * 10_000:.2f} bps"
        ),
    )
    fee_group.add_argument(
        "--fee",
        type=float,
        default=None,
        help=(
            "Trading fee as decimal rate for backward compatibility. "
            "Example: 0.0025 means 25 bps."
        ),
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


def main() -> int:
    args = parse_args()
    settings = get_settings()
    fee_rate = resolve_fee_rate(args, settings.benchmark_fee_rate)
    fee_bps = fee_rate * 10_000.0
    logger = configure_logging(
        settings.log_dir,
        settings.log_level,
        logger_name="alpha_x_benchmarks",
    )

    dataset_path = build_ohlcv_csv_path(
        raw_data_dir=settings.raw_data_dir,
        exchange="bitvavo",
        market=args.market,
        timeframe=args.timeframe,
    )
    frame, dataset_info = load_benchmark_dataset(Path(dataset_path), args.timeframe)

    logger.info("Loaded benchmark dataset from %s", dataset_path)
    logger.info(
        "Rows=%s timeframe=%s market=%s",
        dataset_info.row_count,
        args.timeframe,
        args.market,
    )
    logger.info("Effective fee rate=%.6f (%.2f bps)", fee_rate, fee_bps)

    buy_and_hold = run_buy_and_hold(
        frame,
        fee_rate=fee_rate,
        initial_capital=args.initial_capital,
    )
    dca = run_monthly_dca(frame, fee_rate=fee_rate, contribution=args.dca_amount)
    sma = run_sma_baseline(
        frame,
        fee_rate=fee_rate,
        initial_capital=args.initial_capital,
        fast_window=args.sma_fast,
        slow_window=args.sma_slow,
    )

    metrics_list = [
        calculate_benchmark_metrics(buy_and_hold),
        calculate_benchmark_metrics(dca),
        calculate_benchmark_metrics(sma),
    ]

    print(build_summary(dataset_info, metrics_list))
    print()
    print(f"Fee effective: {fee_rate:.6f} ({fee_bps:.2f} bps)")
    print()
    print(build_comparative_table(metrics_list))
    print()
    print(
        "Nota: la metrica annualized aparece como N/A cuando el historico es insuficiente "
        "o cuando la estrategia tiene aportaciones externas (DCA)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
