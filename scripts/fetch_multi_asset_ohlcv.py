"""fetch_multi_asset_ohlcv.py — Download OHLCV for all official markets.

Usage examples:
    # Backfill all 4 markets to 30 000 rows each:
    python scripts/fetch_multi_asset_ohlcv.py --backfill --target-rows 30000

    # Only validate existing CSVs (no network calls):
    python scripts/fetch_multi_asset_ohlcv.py --validate-only

    # Single market:
    python scripts/fetch_multi_asset_ohlcv.py --markets ETH-EUR --backfill --target-rows 30000
"""

from __future__ import annotations

import argparse
import sys

from alpha_x.config.settings import get_settings
from alpha_x.data.bitvavo_client import BitvavoClient
from alpha_x.data.ohlcv_pipeline import (
    backfill_and_store_ohlcv,
    fetch_and_store_ohlcv,
    format_gap_report,
    validate_existing_ohlcv,
)
from alpha_x.multi_asset.config import OFFICIAL_INTERVAL, OFFICIAL_MARKETS, TARGET_ROWS_DEFAULT
from alpha_x.utils.logging_utils import configure_logging


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch or validate OHLCV data for all official multi-asset markets."
    )
    parser.add_argument(
        "--markets",
        nargs="+",
        default=None,
        metavar="MARKET",
        help="Space-separated list of markets. Defaults to all official markets.",
    )
    parser.add_argument(
        "--interval", default=OFFICIAL_INTERVAL, help="OHLCV timeframe (default: 1h)."
    )
    parser.add_argument("--limit", type=int, default=1000, help="Candles per Bitvavo request.")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Download historical data for each market until target-rows is reached.",
    )
    parser.add_argument(
        "--target-rows",
        type=int,
        default=TARGET_ROWS_DEFAULT,
        help=f"Target row count for backfill mode (default: {TARGET_ROWS_DEFAULT}).",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate existing CSVs without fetching from Bitvavo.",
    )
    parser.add_argument(
        "--report-gaps",
        action="store_true",
        help="Print gap report for each market.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    settings = get_settings()
    logger = configure_logging(
        settings.log_dir, settings.log_level, logger_name="alpha_x_multi_ohlcv"
    )

    markets = args.markets or OFFICIAL_MARKETS
    interval = args.interval
    limit = args.limit

    mode_flags = [args.backfill, args.validate_only, args.report_gaps]
    if sum(bool(f) for f in mode_flags) > 1:
        logger.error("--backfill, --validate-only and --report-gaps are mutually exclusive.")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("Multi-asset OHLCV fetch — markets: %s", markets)
    logger.info("Interval: %s | Target rows: %s", interval, args.target_rows)
    logger.info("=" * 60)

    client = BitvavoClient(base_url=settings.bitvavo_base_url) if not args.validate_only else None

    summary: list[dict] = []

    for market in markets:
        logger.info("-" * 40)
        logger.info("Processing market: %s", market)

        if args.validate_only or args.report_gaps:
            csv_path, frame, report = validate_existing_ohlcv(
                settings.raw_data_dir, market, interval
            )
            logger.info("Market: %s | Rows: %s | Gaps: %s", market, len(frame), len(report.gaps))
            if args.report_gaps:
                for line in format_gap_report(report):
                    logger.info("  %s", line)
            summary.append(
                {
                    "market": market,
                    "rows": len(frame),
                    "gaps": len(report.gaps),
                    "mode": "validate",
                }
            )
            continue

        if args.backfill:
            result = backfill_and_store_ohlcv(
                client=client,
                raw_data_dir=settings.raw_data_dir,
                market=market,
                timeframe=interval,
                limit=limit,
                logger=logger,
                target_rows=args.target_rows,
            )
            summary.append(
                {
                    "market": market,
                    "rows_final": result.final_rows,
                    "rows_added": result.new_rows_added,
                    "gaps": len(result.validation.gaps),
                    "requests": result.request_count,
                    "mode": "backfill",
                }
            )
        else:
            result = fetch_and_store_ohlcv(
                client=client,
                raw_data_dir=settings.raw_data_dir,
                market=market,
                timeframe=interval,
                limit=limit,
                logger=logger,
            )
            summary.append(
                {
                    "market": market,
                    "rows_final": result.final_rows,
                    "rows_added": result.new_rows_added,
                    "gaps": len(result.validation.gaps),
                    "requests": result.request_count,
                    "mode": "fetch",
                }
            )

    logger.info("=" * 60)
    logger.info("SUMMARY — Multi-asset OHLCV fetch complete")
    for row in summary:
        if row.get("mode") == "validate":
            logger.info("  %s: %s rows, %s gaps", row["market"], row["rows"], row["gaps"])
        else:
            logger.info(
                "  %s: %s rows final (+%s new), %s gaps, %s requests",
                row["market"],
                row.get("rows_final", "?"),
                row.get("rows_added", "?"),
                row["gaps"],
                row.get("requests", "?"),
            )
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
