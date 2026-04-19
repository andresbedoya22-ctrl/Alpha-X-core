from __future__ import annotations

import argparse

import pandas as pd

from alpha_x.config.settings import get_settings
from alpha_x.multi_asset.config import OFFICIAL_INTERVAL, OFFICIAL_MARKETS
from alpha_x.multi_asset_experiments.common_window import load_common_enriched_window
from alpha_x.multi_asset_experiments.comparison import run_multi_asset_comparison
from alpha_x.multi_asset_experiments.reporting import (
    export_multi_asset_comparison_report,
)
from alpha_x.reporting.io import build_run_id
from alpha_x.utils.logging_utils import configure_logging


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a controlled multi-asset comparison on the common enriched window."
    )
    parser.add_argument("--markets", nargs="+", default=None, metavar="MARKET")
    parser.add_argument("--timeframe", default=OFFICIAL_INTERVAL)
    parser.add_argument("--audit-run-id", default=None)
    parser.add_argument("--threshold", type=float, default=0.55)
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
        logger_name="alpha_x_multi_asset_comparison",
    )

    common_window = load_common_enriched_window(
        settings.reports_dir,
        audit_run_id=args.audit_run_id,
    )
    markets = args.markets or OFFICIAL_MARKETS
    result = run_multi_asset_comparison(
        raw_data_dir=settings.raw_data_dir,
        external_data_dir=settings.external_data_dir,
        common_window=common_window,
        markets=markets,
        timeframe=args.timeframe,
        threshold=args.threshold,
        fee_rate=settings.benchmark_fee_rate,
        slippage_rate=0.0005,
        initial_capital=settings.benchmark_initial_capital,
        sma_fast=settings.benchmark_sma_fast,
        sma_slow=settings.benchmark_sma_slow,
    )

    report_dir = None
    if not args.no_export:
        report_dir = export_multi_asset_comparison_report(
            reports_dir=settings.reports_dir,
            run_id=run_id,
            created_at=created_at,
            parameters={
                "markets": markets,
                "timeframe": args.timeframe,
                "audit_run_id": common_window.audit_run_id,
                "policy_threshold": args.threshold,
                "fee_rate": settings.benchmark_fee_rate,
                "slippage_rate": 0.0005,
                "initial_capital": settings.benchmark_initial_capital,
            },
            result=result,
        )

    logger.info(
        "run_id=%s markets=%s common_window=%s->%s",
        run_id,
        markets,
        common_window.start,
        common_window.end,
    )

    print(
        f"Ventana comun enriquecida: {common_window.start.isoformat()} -> "
        f"{common_window.end.isoformat()} (~{common_window.row_count_estimate} barras)"
    )
    print(f"Mercados comparados: {', '.join(markets)}")
    print(f"Policy comun: long si predicted_proba > {args.threshold:.2f}, si no flat")
    print()
    print("Resumen comparativo por activo:")
    print(result.comparison_frame.to_string(index=False))
    print()
    print("Promisingness vs BTC:")
    print(result.promisingness_frame.to_string(index=False))
    print()
    print(f"Conclusion: {result.conclusion}")
    if report_dir is not None:
        print()
        print(f"Report path: {report_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
