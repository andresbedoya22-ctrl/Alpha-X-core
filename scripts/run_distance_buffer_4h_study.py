from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from alpha_x.config.settings import get_settings
from alpha_x.data.ohlcv_storage import build_ohlcv_csv_path
from alpha_x.reporting.io import build_run_id, create_report_directory
from alpha_x.research_4h.distance_buffer_study import (
    BASE_COST_MODEL,
    STRESS_COST_MODEL,
    build_common_comparison_table,
    build_full_sample_table,
    build_temporal_robustness_table,
    build_trade_log,
    derive_4h_from_1h,
    load_ohlcv_frame,
    summarize_trades,
    write_json,
    write_report_markdown,
)


def parse_args() -> argparse.Namespace:
    settings = get_settings()
    parser = argparse.ArgumentParser(description="Run BTC/EUR Distance buffer 4H research study.")
    parser.add_argument("--market", default="BTC-EUR")
    parser.add_argument("--exchange", default="bitvavo")
    parser.add_argument("--reports-dir", type=Path, default=settings.reports_dir)
    parser.add_argument("--run-id", default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    settings = get_settings()
    raw_data_dir = settings.raw_data_dir
    one_hour_path = build_ohlcv_csv_path(raw_data_dir, args.exchange, args.market, "1h")
    one_day_path = build_ohlcv_csv_path(raw_data_dir, args.exchange, args.market, "1d")

    frame_4h, quality_4h = derive_4h_from_1h(one_hour_path)
    frame_1d, quality_1d = load_ohlcv_frame(one_day_path, "1d")

    full_base = build_full_sample_table(frame_4h, frame_1d, cost_model=BASE_COST_MODEL)
    full_stress = build_full_sample_table(frame_4h, frame_1d, cost_model=STRESS_COST_MODEL)
    common_base = build_common_comparison_table(frame_4h, frame_1d, cost_model=BASE_COST_MODEL)
    common_stress = build_common_comparison_table(frame_4h, frame_1d, cost_model=STRESS_COST_MODEL)
    robustness = build_temporal_robustness_table(
        frame_4h,
        frame_1d,
        cost_model=BASE_COST_MODEL,
    )

    trades_4h, trade_notes_4h = build_trade_log(
        frame_4h,
        timeframe="4h",
        cost_model=BASE_COST_MODEL,
    )
    trades_1d, trade_notes_1d = build_trade_log(
        frame_1d,
        timeframe="1d",
        cost_model=BASE_COST_MODEL,
    )
    trade_summary_4h = summarize_trades(
        trades_4h,
        dataset_start=pd.to_datetime(frame_4h["datetime"].iloc[0], utc=True),
        dataset_end=pd.to_datetime(frame_4h["datetime"].iloc[-1], utc=True),
    )
    trade_summary_1d = summarize_trades(
        trades_1d,
        dataset_start=pd.to_datetime(frame_1d["datetime"].iloc[0], utc=True),
        dataset_end=pd.to_datetime(frame_1d["datetime"].iloc[-1], utc=True),
    )

    run_id = args.run_id or build_run_id()
    report_dir = create_report_directory(args.reports_dir, "distance_buffer_4h_study", run_id)

    full_base.to_csv(report_dir / "full_sample_base.csv", index=False)
    full_stress.to_csv(report_dir / "full_sample_stress.csv", index=False)
    common_base.to_csv(report_dir / "common_comparison_base.csv", index=False)
    common_stress.to_csv(report_dir / "common_comparison_stress.csv", index=False)
    robustness.to_csv(report_dir / "temporal_robustness_base.csv", index=False)
    trades_4h.to_csv(report_dir / "trade_log_4h_base.csv", index=False)
    trades_1d.to_csv(report_dir / "trade_log_1d_base.csv", index=False)

    manifest = {
        "market": args.market,
        "exchange": args.exchange,
        "data": {
            "4h": quality_4h.to_dict(),
            "1d": quality_1d.to_dict(),
        },
        "strategy": {
            "name": "Distance buffer",
            "asset": "BTC/EUR",
            "sma_window": 125,
            "buffer": 0.03,
            "signal_on": "close > SMA125 * 1.03",
            "signal_off": "close <= SMA125 * 1.03",
            "execution": "t+1 next candle close",
            "deadband": 0.10,
            "exposure_on": "100% BTC",
            "exposure_off": "0% BTC / 100% EUR",
        },
        "costs": {
            "base": {
                "fee_per_side": BASE_COST_MODEL.fee_per_side,
                "slippage_per_side": BASE_COST_MODEL.slippage_per_side,
            },
            "stress": {
                "fee_per_side": STRESS_COST_MODEL.fee_per_side,
                "slippage_per_side": STRESS_COST_MODEL.slippage_per_side,
            },
        },
        "trade_notes": {
            "4h": trade_notes_4h,
            "1d": trade_notes_1d,
        },
        "artifacts": [
            "full_sample_base.csv",
            "full_sample_stress.csv",
            "common_comparison_base.csv",
            "common_comparison_stress.csv",
            "temporal_robustness_base.csv",
            "trade_log_4h_base.csv",
            "trade_log_1d_base.csv",
            "manifest.json",
            "study.md",
        ],
    }
    write_json(report_dir / "manifest.json", manifest)
    write_report_markdown(
        report_dir / "study.md",
        manifest=manifest,
        full_base=full_base,
        full_stress=full_stress,
        common_base=common_base,
        robustness=robustness,
        trade_summary_4h=trade_summary_4h,
        trade_summary_1d=trade_summary_1d,
    )

    print(f"Report directory: {report_dir}")
    print("\nCommon base comparison")
    print(common_base.to_string(index=False))
    print("\nTemporal robustness")
    print(robustness.to_string(index=False))
    print("\n4H trade summary")
    print(pd.DataFrame([trade_summary_4h]).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
