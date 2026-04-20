from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from alpha_x.config.settings import get_settings
from alpha_x.data.ohlcv_storage import build_ohlcv_csv_path
from alpha_x.reporting.io import build_run_id, create_report_directory
from alpha_x.research_4h.distance_buffer_study import (
    BASE_COST_MODEL,
    BUY_HOLD_CONFIG_4H,
    OFFICIAL_1D_CONFIG,
    STRESS_COST_MODEL,
    build_4h_grid_results,
    build_block_bootstrap_table,
    build_common_comparison_table,
    build_expanding_robustness_table,
    build_temporal_robustness_table,
    build_trade_log,
    common_period_start,
    config_from_metrics,
    derive_4h_from_1h,
    evaluate_pro_layer,
    filter_trades_by_execution_period,
    load_ohlcv_frame,
    run_monthly_contribution_simulation,
    summarize_trades,
    write_json,
    write_report_markdown,
)


def parse_args() -> argparse.Namespace:
    settings = get_settings()
    parser = argparse.ArgumentParser(
        description="Run BTC/EUR corrected Distance Buffer 4H research study."
    )
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

    # First pass ranks every corrected 4H candidate on its own common-live period.
    ranking_base_full = build_4h_grid_results(frame_4h, cost_model=BASE_COST_MODEL)
    provisional_best = config_from_metrics(ranking_base_full.iloc[0])
    common_start = common_period_start(frame_4h, frame_1d, provisional_best)
    common_end = min(
        pd.to_datetime(frame_4h["datetime"].iloc[-1], utc=True),
        pd.to_datetime(frame_1d["datetime"].iloc[-1], utc=True),
    )

    # Final ranking uses the explicit common period against the official 1D reference.
    ranking_base = build_4h_grid_results(
        frame_4h,
        cost_model=BASE_COST_MODEL,
        start=common_start,
        end=common_end,
    )
    ranking_stress = build_4h_grid_results(
        frame_4h,
        cost_model=STRESS_COST_MODEL,
        start=common_start,
        end=common_end,
    )
    best_config = config_from_metrics(ranking_base.iloc[0])

    common_base = build_common_comparison_table(
        frame_4h,
        frame_1d,
        best_config,
        cost_model=BASE_COST_MODEL,
    )
    common_stress = build_common_comparison_table(
        frame_4h,
        frame_1d,
        best_config,
        cost_model=STRESS_COST_MODEL,
    )
    rolling = build_temporal_robustness_table(
        frame_4h,
        frame_1d,
        best_config,
        cost_model=BASE_COST_MODEL,
    )
    expanding = build_expanding_robustness_table(
        frame_4h,
        frame_1d,
        best_config,
        cost_model=BASE_COST_MODEL,
    )
    bootstrap = build_block_bootstrap_table(
        frame_4h,
        frame_1d,
        best_config,
        cost_model=BASE_COST_MODEL,
    )
    pro_layer = evaluate_pro_layer(
        frame_4h,
        best_config,
        cost_model=BASE_COST_MODEL,
        start=common_start,
        end=common_end,
    )

    trades_4h, trade_notes_4h = build_trade_log(
        frame_4h,
        best_config,
        cost_model=BASE_COST_MODEL,
    )
    trades_1d, trade_notes_1d = build_trade_log(
        frame_1d,
        OFFICIAL_1D_CONFIG,
        cost_model=BASE_COST_MODEL,
    )
    common_trades_4h = filter_trades_by_execution_period(
        trades_4h,
        start=common_start,
        end=common_end,
    )
    common_trades_1d = filter_trades_by_execution_period(
        trades_1d,
        start=common_start,
        end=common_end,
    )
    trade_summary_4h = summarize_trades(
        common_trades_4h,
        dataset_start=common_start,
        dataset_end=common_end,
    )
    trade_summary_1d = summarize_trades(
        common_trades_1d,
        dataset_start=common_start,
        dataset_end=common_end,
    )

    practical_2025 = pd.DataFrame(
        [
            run_monthly_contribution_simulation(
                frame_4h,
                best_config,
                cost_model=BASE_COST_MODEL,
            ),
            run_monthly_contribution_simulation(
                frame_1d,
                OFFICIAL_1D_CONFIG,
                cost_model=BASE_COST_MODEL,
            ),
            run_monthly_contribution_simulation(
                frame_4h,
                BUY_HOLD_CONFIG_4H,
                cost_model=BASE_COST_MODEL,
            ),
        ]
    )

    run_id = args.run_id or build_run_id()
    report_dir = create_report_directory(args.reports_dir, "distance_buffer_4h_corrected", run_id)

    ranking_base.to_csv(report_dir / "ranking_4h_base.csv", index=False)
    ranking_stress.to_csv(report_dir / "ranking_4h_stress.csv", index=False)
    common_base.to_csv(report_dir / "common_comparison_base.csv", index=False)
    common_stress.to_csv(report_dir / "common_comparison_stress.csv", index=False)
    rolling.to_csv(report_dir / "rolling_robustness_base.csv", index=False)
    expanding.to_csv(report_dir / "expanding_robustness_base.csv", index=False)
    bootstrap.to_csv(report_dir / "block_bootstrap_base.csv", index=False)
    pro_layer.to_csv(report_dir / "pro_layer_asymmetric_exit_base.csv", index=False)
    trades_4h.to_csv(report_dir / "trade_log_best_4h_base.csv", index=False)
    trades_1d.to_csv(report_dir / "trade_log_1d_official_base.csv", index=False)
    common_trades_4h.to_csv(report_dir / "trade_log_best_4h_common_base.csv", index=False)
    common_trades_1d.to_csv(report_dir / "trade_log_1d_official_common_base.csv", index=False)
    practical_2025.to_csv(report_dir / "practical_2025_simulation_base.csv", index=False)

    manifest = {
        "market": args.market,
        "exchange": args.exchange,
        "common_period": {
            "start": str(common_start),
            "end": str(common_end),
        },
        "data": {
            "4h": quality_4h.to_dict(),
            "1d": quality_1d.to_dict(),
        },
        "best_4h_config": {
            "sma_window": best_config.sma_window,
            "buffer": best_config.buffer,
            "entry_persistence": best_config.entry_persistence,
            "exit_persistence": best_config.exit_persistence,
            "confirmation_price": best_config.confirmation_price,
            "execution": "t+1 next candle close",
            "deadband": 0.10,
            "exposure_on": "100% BTC",
            "exposure_off": "0% BTC / 100% EUR",
        },
        "official_1d_reference": {
            "sma_window": OFFICIAL_1D_CONFIG.sma_window,
            "buffer": OFFICIAL_1D_CONFIG.buffer,
            "signal_on": "close > SMA125 * 1.03",
            "execution": "t+1 next daily candle close",
            "deadband": 0.10,
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
            "ranking_4h_base.csv",
            "ranking_4h_stress.csv",
            "common_comparison_base.csv",
            "common_comparison_stress.csv",
            "rolling_robustness_base.csv",
            "expanding_robustness_base.csv",
            "block_bootstrap_base.csv",
            "pro_layer_asymmetric_exit_base.csv",
            "trade_log_best_4h_base.csv",
            "trade_log_1d_official_base.csv",
            "trade_log_best_4h_common_base.csv",
            "trade_log_1d_official_common_base.csv",
            "practical_2025_simulation_base.csv",
            "manifest.json",
            "study.md",
        ],
    }
    write_json(report_dir / "manifest.json", manifest)
    write_report_markdown(
        report_dir / "study.md",
        manifest=manifest,
        ranking_base=ranking_base,
        ranking_stress=ranking_stress,
        common_base=common_base,
        common_stress=common_stress,
        rolling=rolling,
        expanding=expanding,
        bootstrap=bootstrap,
        pro_layer=pro_layer,
        trade_summary_4h=trade_summary_4h,
        trade_summary_1d=trade_summary_1d,
        practical_2025=practical_2025,
    )

    print(f"Report directory: {report_dir}")
    print("\nBest corrected 4H config")
    print(best_config)
    print("\n4H ranking base")
    print(ranking_base.to_string(index=False))
    print("\nCommon base comparison")
    print(common_base.to_string(index=False))
    print("\nRolling robustness")
    print(rolling.to_string(index=False))
    print("\n4H trade summary")
    print(pd.DataFrame([trade_summary_4h]).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
