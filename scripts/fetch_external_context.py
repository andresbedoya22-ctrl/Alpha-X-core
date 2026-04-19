from __future__ import annotations

import argparse

from alpha_x.config.settings import get_settings
from alpha_x.external_data.etf_flows import BitboEtfFlowSource
from alpha_x.external_data.funding import BybitFundingSource
from alpha_x.multi_asset.config import OFFICIAL_MARKETS
from alpha_x.multi_asset.markets import MARKET_REGISTRY
from alpha_x.utils.logging_utils import configure_logging

FUNDING_SYMBOLS = [MARKET_REGISTRY[market].funding_symbol for market in OFFICIAL_MARKETS]
ETF_SERIES_KEYS = ["BTC_SPOT_ETF", "ETH_SPOT_ETF"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download external contextual data for multi-asset research."
    )
    parser.add_argument("--skip-funding", action="store_true", help="Skip funding rate download.")
    parser.add_argument("--skip-etf", action="store_true", help="Skip ETF flow download.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    settings = get_settings()
    logger = configure_logging(settings.log_dir, settings.log_level, logger_name="alpha_x_external")

    logger.info("=" * 60)
    logger.info("External context fetch")
    logger.info("Storage root: %s", settings.external_data_dir.resolve())
    logger.info("=" * 60)

    if not args.skip_funding:
        funding_source = BybitFundingSource(storage_dir=settings.external_data_dir)
        logger.info("Fetching funding rates from Bybit V5 for symbols: %s", FUNDING_SYMBOLS)
        for symbol in FUNDING_SYMBOLS:
            result = funding_source.fetch(key=symbol, logger=logger)
            logger.info(
                "%s | rows=%s | added=%s | start=%s | end=%s | freq=%s",
                symbol,
                result.rows_final,
                result.rows_added,
                result.start_dt,
                result.end_dt,
                result.frequency,
            )
    else:
        logger.info("Skipping funding download.")

    if not args.skip_etf:
        etf_source = BitboEtfFlowSource(storage_dir=settings.external_data_dir)
        logger.info("Fetching ETF flow context series: %s", ETF_SERIES_KEYS)
        for key in ETF_SERIES_KEYS:
            result = etf_source.fetch(key=key, logger=logger)
            logger.info(
                "%s | rows=%s | added=%s | start=%s | end=%s | freq=%s",
                key,
                result.rows_final,
                result.rows_added,
                result.start_dt,
                result.end_dt,
                result.frequency,
            )
    else:
        logger.info("Skipping ETF flow download.")

    logger.info("=" * 60)
    logger.info("External context fetch complete")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
