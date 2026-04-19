from __future__ import annotations

import argparse
from pathlib import Path

from alpha_x.config.settings import get_settings
from alpha_x.external_data.etf_flows import BitboEtfFlowSource
from alpha_x.external_data.funding import BybitFundingSource
from alpha_x.external_data.reporting import (
    CoverageReport,
    compute_external_coverage,
    export_coverage_report,
)
from alpha_x.multi_asset.config import OFFICIAL_MARKETS
from alpha_x.multi_asset.dataset import load_multi_asset_ohlcv
from alpha_x.multi_asset.markets import MARKET_REGISTRY
from alpha_x.reporting.io import build_run_id
from alpha_x.utils.logging_utils import configure_logging

GLOBAL_BTC_ETF_KEY = "BTC_SPOT_ETF"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Audit multi-asset OHLCV and external context coverage."
    )
    parser.add_argument("--output-dir", default=None, help="Optional output directory override.")
    parser.add_argument(
        "--markets", nargs="+", default=None, metavar="MARKET", help="Markets to audit."
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = get_settings()
    logger = configure_logging(settings.log_dir, settings.log_level, logger_name="alpha_x_audit")
    run_id = build_run_id()

    markets = args.markets or OFFICIAL_MARKETS
    dataset = load_multi_asset_ohlcv(settings.raw_data_dir, markets=markets)

    funding_source = BybitFundingSource(storage_dir=settings.external_data_dir)
    etf_source = BitboEtfFlowSource(storage_dir=settings.external_data_dir)

    funding_frames = {}
    for market in markets:
        market_info = MARKET_REGISTRY[market]
        funding_frames[market_info.base_asset] = funding_source.load(market_info.funding_symbol)

    etf_flow_frames = {}
    for market in markets:
        market_info = MARKET_REGISTRY[market]
        etf_key = market_info.etf_ticker or GLOBAL_BTC_ETF_KEY
        if etf_key not in etf_flow_frames:
            etf_flow_frames[etf_key] = etf_source.load(etf_key)
    if GLOBAL_BTC_ETF_KEY not in etf_flow_frames:
        etf_flow_frames[GLOBAL_BTC_ETF_KEY] = etf_source.load(GLOBAL_BTC_ETF_KEY)

    report = compute_external_coverage(
        dataset=dataset,
        funding_frames=funding_frames,
        etf_flow_frames=etf_flow_frames,
        global_etf_key=GLOBAL_BTC_ETF_KEY,
        run_id=run_id,
    )

    output_dir = (
        Path(args.output_dir)
        if args.output_dir
        else settings.reports_dir / "multi_asset_data" / run_id
    )
    paths = export_coverage_report(report, output_dir)
    logger.info("Audit report exported to %s", output_dir.resolve())
    _print_audit(report)
    logger.info("Summary JSON: %s", paths["summary"])
    return 0


def _print_audit(report: CoverageReport) -> None:
    line = "=" * 72
    print()
    print(line)
    print("ALPHA-X CORE - Multi-Asset Data Audit")
    print(f"Run ID: {report.run_id}")
    print(f"Markets audited: {', '.join(report.markets_audited)}")
    print(line)
    print()
    print("OHLCV coverage")
    for row in report.asset_rows:
        print(
            f"- {row.market}: rows={row.ohlcv_rows}, range={row.ohlcv_start} -> {row.ohlcv_end}, "
            f"gaps={row.ohlcv_gaps}, missing_intervals={row.ohlcv_missing_intervals}"
        )
    print()
    print("Funding coverage")
    for row in report.asset_rows:
        print(
            f"- {row.market}: funding_rows={row.funding_rows}, "
            f"range={row.funding_start} -> {row.funding_end}, "
            f"coverage={row.funding_coverage_pct:.2f}%"
        )
    print()
    print("ETF flow coverage")
    for row in report.asset_rows:
        print(
            f"- {row.market}: etf_rows={row.etf_flow_rows}, "
            f"range={row.etf_flow_start} -> {row.etf_flow_end}, "
            f"coverage={row.etf_flow_coverage_pct:.2f}%"
        )
    print()
    print("Valid enriched rows")
    for row in report.asset_rows:
        print(
            f"- {row.market}: full_context_rows={row.rows_with_full_context}, "
            f"full_context_pct={row.full_context_pct:.2f}%"
        )
    print()
    print("Common windows")
    print(
        f"- OHLCV: {report.common_window_ohlcv_start} -> {report.common_window_ohlcv_end} "
        f"(~{report.common_window_ohlcv_rows_approx} rows)"
    )
    print(
        f"- OHLCV + funding: {report.common_window_ohlcv_funding_start} -> "
        f"{report.common_window_ohlcv_funding_end}"
    )
    print(
        f"- OHLCV + funding + ETF flows: {report.common_window_ohlcv_funding_etf_start} -> "
        f"{report.common_window_ohlcv_funding_etf_end}"
    )
    print()
    print("Comparability")
    print("- Full individual windows: NO")
    print(f"- Common OHLCV window: {'YES' if report.comparable_in_common_ohlcv else 'NO'}")
    print(
        "- Common OHLCV + funding window: "
        f"{'YES' if report.comparable_in_common_ohlcv_funding else 'NO'}"
    )
    print(
        f"- Common OHLCV + funding + ETF window: "
        f"{'YES' if report.comparable_in_common_ohlcv_funding_etf else 'NO'}"
    )
    print()
    print("Known limitations")
    for limitation in report.known_limitations:
        print(f"- {limitation}")
    print()


if __name__ == "__main__":
    raise SystemExit(main())
