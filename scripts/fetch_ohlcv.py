from __future__ import annotations

import argparse

from alpha_x.config.settings import get_settings
from alpha_x.data.bitvavo_client import BitvavoClient
from alpha_x.data.ohlcv_pipeline import fetch_and_store_ohlcv, validate_existing_ohlcv
from alpha_x.utils.logging_utils import configure_logging


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch or validate OHLCV data from Bitvavo.")
    parser.add_argument("--market", help="Bitvavo market, for example BTC-EUR.")
    parser.add_argument("--interval", help="OHLCV timeframe, for example 1h or 1d.")
    parser.add_argument("--limit", type=int, help="Number of candles to request.")
    parser.add_argument("--start", type=int, help="Start timestamp in milliseconds.")
    parser.add_argument("--end", type=int, help="End timestamp in milliseconds.")
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate the existing CSV without calling Bitvavo.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    settings = get_settings()
    logger = configure_logging(settings.log_dir, settings.log_level, logger_name="alpha_x_data")

    market = args.market or settings.bitvavo_market
    timeframe = args.interval or settings.ohlcv_default_interval
    limit = args.limit or settings.ohlcv_default_limit

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

    client = BitvavoClient(base_url=settings.bitvavo_base_url)
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
