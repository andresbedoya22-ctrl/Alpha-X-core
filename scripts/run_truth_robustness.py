from __future__ import annotations

import argparse

import pandas as pd

from alpha_x.config.settings import get_settings
from alpha_x.reporting.io import build_run_id, write_json_file, write_table_csv
from alpha_x.truth_engine.comparison import TruthEngineConfig, run_truth_engine


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run minimal robustness checks for Truth Engine.")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--timeframe", default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = get_settings()
    created_at = pd.Timestamp.now(tz="UTC").floor("s")
    run_id = args.run_id or build_run_id(created_at)
    timeframe = args.timeframe or settings.truth_timeframe

    scenarios = [
        (
            "base",
            TruthEngineConfig(
                timeframe=timeframe,
                initial_capital=settings.truth_initial_capital,
                fee_rate=settings.truth_base_fee_rate,
                slippage_rate=settings.truth_base_slippage_rate,
            ),
        ),
        (
            "mid",
            TruthEngineConfig(
                timeframe=timeframe,
                initial_capital=settings.truth_initial_capital,
                fee_rate=settings.truth_mid_fee_rate,
                slippage_rate=settings.truth_mid_slippage_rate,
            ),
        ),
        (
            "stress",
            TruthEngineConfig(
                timeframe=timeframe,
                initial_capital=settings.truth_initial_capital,
                fee_rate=settings.truth_stress_fee_rate,
                slippage_rate=settings.truth_stress_slippage_rate,
            ),
        ),
    ]

    rows: list[pd.DataFrame] = []
    for label, config in scenarios:
        result = run_truth_engine(
            raw_data_dir=settings.raw_data_dir,
            reports_dir=settings.reports_dir,
            run_id=f"{run_id}-{label}",
            created_at=created_at,
            config=config,
            export_report=False,
        )
        frame = result.summary_frame.copy()
        frame.insert(0, "scenario", label)
        rows.append(frame)

    summary = pd.concat(rows, ignore_index=True)
    report_dir = settings.reports_dir / "truth_engine_robustness" / run_id
    report_dir.mkdir(parents=True, exist_ok=True)
    write_table_csv(report_dir / "summary.csv", summary)
    write_json_file(
        report_dir / "summary.json",
        {
            "run_id": run_id,
            "timeframe": timeframe,
            "scenarios": [label for label, _ in scenarios],
            "rows": summary.to_dict(orient="records"),
        },
    )

    print(f"Run ID: {run_id}")
    print(summary.to_string(index=False))
    print()
    print(f"Report path: {report_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
