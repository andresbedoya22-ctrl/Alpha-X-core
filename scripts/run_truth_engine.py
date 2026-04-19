from __future__ import annotations

import argparse

import pandas as pd

from alpha_x.config.settings import get_settings
from alpha_x.truth_engine.comparison import TruthEngineConfig, run_truth_engine
from alpha_x.utils.logging_utils import configure_logging

COST_SCENARIOS = {
    "base": {
        "fee_rate": "truth_base_fee_rate",
        "slippage_rate": "truth_base_slippage_rate",
        "label": "fee 25 bps por lado + slippage explicito",
    },
    "mid": {
        "fee_rate": "truth_mid_fee_rate",
        "slippage_rate": "truth_mid_slippage_rate",
        "label": "mezcla maker/taker razonable con fee medio menor",
    },
    "stress": {
        "fee_rate": "truth_stress_fee_rate",
        "slippage_rate": "truth_stress_slippage_rate",
        "label": "stress con fee y slippage mas altos",
    },
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run ALPHA-X Phase 1 Truth Engine.")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--cost-scenario", choices=sorted(COST_SCENARIOS), default="base")
    parser.add_argument("--timeframe", default=None)
    parser.add_argument("--initial-capital", type=float, default=None)
    parser.add_argument("--no-export", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = get_settings()
    scenario = COST_SCENARIOS[args.cost_scenario]
    fee_rate = getattr(settings, scenario["fee_rate"])
    slippage_rate = getattr(settings, scenario["slippage_rate"])
    timeframe = args.timeframe or settings.truth_timeframe
    initial_capital = args.initial_capital or settings.truth_initial_capital

    logger = configure_logging(
        settings.log_dir, settings.log_level, logger_name="alpha_x_truth_engine"
    )
    logger.info(
        "Truth Engine start | timeframe=%s scenario=%s fee_rate=%.6f slippage_rate=%.6f",
        timeframe,
        args.cost_scenario,
        fee_rate,
        slippage_rate,
    )

    result = run_truth_engine(
        raw_data_dir=settings.raw_data_dir,
        reports_dir=settings.reports_dir,
        run_id=args.run_id,
        created_at=pd.Timestamp.now(tz="UTC").floor("s"),
        config=TruthEngineConfig(
            timeframe=timeframe,
            initial_capital=initial_capital,
            fee_rate=fee_rate,
            slippage_rate=slippage_rate,
            dca_amount=settings.benchmark_dca_amount,
        ),
        export_report=not args.no_export,
    )

    print(f"Run ID: {result.run_id}")
    print(f"Cost scenario: {args.cost_scenario} ({scenario['label']})")
    print(f"Eligible markets: {', '.join(result.eligible_markets)}")
    if result.skipped_markets:
        print(f"Skipped markets: {', '.join(result.skipped_markets)}")
    print()
    print(result.comparison_frame.to_string(index=False))
    if result.report_dir is not None:
        print()
        print(f"Report path: {result.report_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
