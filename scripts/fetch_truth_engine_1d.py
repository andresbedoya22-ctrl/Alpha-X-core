from __future__ import annotations

import argparse

import pandas as pd

from alpha_x.config.settings import get_settings
from alpha_x.data.bitvavo_client import BitvavoClient
from alpha_x.data.truth_engine_data import run_truth_engine_data_batch
from alpha_x.truth_engine.universe import OFFICIAL_UNIVERSE
from alpha_x.utils.logging_utils import configure_logging


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch, backfill or validate the official Truth Engine 1D universe "
            "from Bitvavo."
        )
    )
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--markets", nargs="+", default=list(OFFICIAL_UNIVERSE), metavar="MARKET")
    parser.add_argument("--target-rows", type=int, default=2500)
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--use-reserves", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = get_settings()
    logger = configure_logging(
        settings.log_dir,
        settings.log_level,
        logger_name="alpha_x_truth_engine_data",
    )
    client = BitvavoClient(base_url=settings.bitvavo_base_url)

    result = run_truth_engine_data_batch(
        client=client,
        raw_data_dir=settings.raw_data_dir,
        reports_dir=settings.reports_dir,
        logger=logger,
        run_id=args.run_id,
        created_at=pd.Timestamp.now(tz="UTC").floor("s"),
        markets=args.markets,
        target_rows=args.target_rows,
        limit=args.limit,
        validate_only=args.validate_only,
        use_reserves=args.use_reserves,
    )

    coverage = result.coverage_frame.loc[
        :,
        [
            "target_market",
            "effective_market",
            "rows",
            "gap_count",
            "status",
            "reason",
            "eligible_for_truth_engine",
        ],
    ]
    print(f"Run ID: {result.run_id}")
    print()
    print(coverage.to_string(index=False))
    print()
    print(f"Effective universe final: {result.summary['effective_universe_final']}")
    print(f"Report path: {result.report_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
