from __future__ import annotations

import argparse

from alpha_x.config.settings import get_settings
from alpha_x.data.bitvavo_client import BitvavoClient
from alpha_x.data.ohlcv_pipeline import (
    backfill_and_store_ohlcv,
    fetch_and_store_ohlcv,
    format_gap_report,
    repair_ohlcv_gaps,
    validate_existing_ohlcv,
)
from alpha_x.utils.logging_utils import configure_logging


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch or validate OHLCV data from Bitvavo.")
    parser.add_argument("--market", help="Bitvavo market, for example BTC-EUR.")
    parser.add_argument("--interval", help="OHLCV timeframe, for example 1h or 1d.")
    parser.add_argument("--limit", type=int, help="Number of candles to request.")
    parser.add_argument("--start", type=int, help="Start timestamp in milliseconds.")
    parser.add_argument("--end", type=int, help="End timestamp in milliseconds.")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Download historical OHLCV in multiple requests until target rows are reached.",
    )
    parser.add_argument(
        "--target-rows",
        type=int,
        help="Desired final row count for backfill mode.",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate the existing CSV without calling Bitvavo.",
    )
    parser.add_argument(
        "--report-gaps",
        action="store_true",
        help="Report detected OHLCV gaps for the persisted CSV.",
    )
    parser.add_argument(
        "--repair-gaps",
        action="store_true",
        help="Attempt to repair detected OHLCV gaps by re-downloading only missing windows.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    settings = get_settings()
    logger = configure_logging(settings.log_dir, settings.log_level, logger_name="alpha_x_data")

    market = args.market or settings.bitvavo_market
    timeframe = args.interval or settings.ohlcv_default_interval
    limit = args.limit or settings.ohlcv_default_limit

    mode_flags = [args.backfill, args.validate_only, args.report_gaps, args.repair_gaps]
    if sum(bool(flag) for flag in mode_flags) > 1:
        raise ValueError(
            "--backfill, --validate-only, --report-gaps and --repair-gaps are mutually exclusive."
        )

    if args.validate_only:
        csv_path, frame, report = validate_existing_ohlcv(settings.raw_data_dir, market, timeframe)
        logger.info("Validation completed for %s", csv_path.resolve())
        logger.info("Rows: %s", len(frame))
        logger.info("Sorted ascending: %s", report.is_sorted)
        logger.info("Unique timestamps: %s", report.has_unique_timestamps)
        logger.info("Gap count: %s", len(report.gaps))
        for gap in report.gaps:
            logger.warning(
                "Gap detected between %s and %s (%s missing intervals)",
                gap.previous_timestamp,
                gap.current_timestamp,
                gap.missing_intervals,
            )
        return

    if args.report_gaps:
        csv_path, frame, report = validate_existing_ohlcv(settings.raw_data_dir, market, timeframe)
        logger.info("Gap report for %s", csv_path.resolve())
        logger.info("Rows: %s", len(frame))
        for line in format_gap_report(report):
            logger.info(line)
        return

    client = BitvavoClient(base_url=settings.bitvavo_base_url)
    if args.repair_gaps:
        repair_ohlcv_gaps(
            client=client,
            raw_data_dir=settings.raw_data_dir,
            market=market,
            timeframe=timeframe,
            logger=logger,
        )
        return

    if args.backfill:
        target_rows = args.target_rows or 10_000
        backfill_and_store_ohlcv(
            client=client,
            raw_data_dir=settings.raw_data_dir,
            market=market,
            timeframe=timeframe,
            limit=limit,
            logger=logger,
            target_rows=target_rows,
            start=args.start,
            end=args.end,
        )
        return

    fetch_and_store_ohlcv(
        client=client,
        raw_data_dir=settings.raw_data_dir,
        market=market,
        timeframe=timeframe,
        limit=limit,
        logger=logger,
        start=args.start,
        end=args.end,
    )


if __name__ == "__main__":
    main()
