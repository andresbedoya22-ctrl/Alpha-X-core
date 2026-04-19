from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from alpha_x.backtest.data_loader import load_backtest_dataset
from alpha_x.backtest.engine import run_long_flat_backtest
from alpha_x.backtest.metrics import benchmark_result_to_performance_row, calculate_backtest_metrics
from alpha_x.benchmarks.sma_baseline import run_sma_baseline
from alpha_x.config.settings import get_settings
from alpha_x.data.ohlcv_storage import build_ohlcv_csv_path
from alpha_x.refinements.catalog import RefinementDefinition, get_refinement_catalog
from alpha_x.refinements.confirmation import apply_entry_confirmation
from alpha_x.refinements.cooldown import apply_cooldown
from alpha_x.refinements.holding import apply_minimum_holding
from alpha_x.refinements.resample import resample_1h_to_4h
from alpha_x.reporting.io import (
    build_run_id,
    create_report_directory,
    list_report_files,
    write_json_file,
    write_table_csv,
)
from alpha_x.reporting.run_manifest import build_run_manifest
from alpha_x.reporting.serializers import serialize_value
from alpha_x.strategies.volatility import build_volatility_filter_signal
from alpha_x.utils.logging_utils import configure_logging
from alpha_x.validation.base import ValidationResultRow
from alpha_x.validation.reporting import build_validation_table, validation_rows_to_frame
from alpha_x.validation.splits import (
    build_temporal_splits,
    slice_frame_for_split,
    summarize_segment_gaps,
)
from alpha_x.validation.walk_forward import (
    build_expanding_walk_forward_windows,
    slice_test_frame,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    settings = get_settings()
    parser = argparse.ArgumentParser(
        description="Run V2 refinement experiments for lower churn and lower noise."
    )
    parser.add_argument("--market", default=settings.bitvavo_market, help="Market symbol.")
    parser.add_argument(
        "--timeframe",
        default=settings.ohlcv_default_interval,
        help="Base OHLCV timeframe. Refinements also derive 4h from 1h.",
    )
    fee_group = parser.add_mutually_exclusive_group()
    fee_group.add_argument(
        "--fee-bps",
        type=float,
        default=25.0,
        help="Trading fee in basis points. Default is intentionally aggressive.",
    )
    fee_group.add_argument("--fee", type=float, default=None, help="Trading fee as decimal rate.")
    slippage_group = parser.add_mutually_exclusive_group()
    slippage_group.add_argument(
        "--slippage-bps",
        type=float,
        default=10.0,
        help="Execution slippage in basis points.",
    )
    slippage_group.add_argument(
        "--slippage",
        type=float,
        default=None,
        help="Execution slippage as decimal rate.",
    )
    parser.add_argument(
        "--initial-capital",
        type=float,
        default=settings.benchmark_initial_capital,
        help="Initial capital for each segment backtest.",
    )
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--no-export", action="store_true")
    return parser.parse_args(argv)


def resolve_fee_rate(args: argparse.Namespace) -> float:
    if args.fee is not None:
        if args.fee < 0:
            raise ValueError("--fee must be non-negative.")
        return args.fee
    if args.fee_bps < 0:
        raise ValueError("--fee-bps must be non-negative.")
    return args.fee_bps / 10_000.0


def resolve_slippage_rate(args: argparse.Namespace) -> float:
    if args.slippage is not None:
        if args.slippage < 0:
            raise ValueError("--slippage must be non-negative.")
        return args.slippage
    if args.slippage_bps < 0:
        raise ValueError("--slippage-bps must be non-negative.")
    return args.slippage_bps / 10_000.0


def build_dataset_context(dataset: object, resampled_4h: pd.DataFrame) -> dict[str, object]:
    dataset_info = dataset.dataset_info
    gap_summary = dataset.gap_summary
    return {
        "path": dataset_info.path,
        "market": dataset_info.market,
        "timeframe": dataset_info.timeframe,
        "rows_1h": dataset_info.row_count,
        "rows_4h": len(resampled_4h),
        "start_timestamp": dataset_info.start_timestamp,
        "end_timestamp": dataset_info.end_timestamp,
        "gap_count_1h": gap_summary.gap_count,
        "total_missing_intervals_1h": gap_summary.total_missing_intervals,
        "largest_gap_1h": gap_summary.largest_gap,
    }


def parameter_label(parameters: dict[str, object]) -> str:
    return ",".join(f"{key}={value}" for key, value in parameters.items())


def apply_refinement_rules(signal: pd.Series, definition: RefinementDefinition) -> pd.Series:
    adjusted = signal.astype("int64")
    confirmation_bars = int(definition.parameters.get("confirmation_bars", 1))
    min_hold_bars = int(definition.parameters.get("min_hold_bars", 0))
    cooldown_bars = int(definition.parameters.get("cooldown_bars", 0))

    if confirmation_bars > 1:
        adjusted = apply_entry_confirmation(adjusted, confirmation_bars=confirmation_bars)
    if min_hold_bars > 0:
        adjusted = apply_minimum_holding(adjusted, min_hold_bars=min_hold_bars)
    if cooldown_bars > 0:
        adjusted = apply_cooldown(adjusted, cooldown_bars=cooldown_bars)
    return adjusted.astype("int64")


def build_signal_frame(frame: pd.DataFrame, definition: RefinementDefinition) -> pd.DataFrame:
    if definition.base_type == "volatility_filter":
        signal_frame = build_volatility_filter_signal(
            frame,
            base_slow_window=int(definition.parameters["base_slow_window"]),
            volatility_window=int(definition.parameters["volatility_window"]),
            min_volatility=float(definition.parameters["min_volatility"]),
            max_volatility=float(definition.parameters["max_volatility"]),
        )
        signal_frame["signal"] = apply_refinement_rules(signal_frame["signal"], definition)
        return signal_frame
    raise ValueError(f"Signal frame not supported for base_type={definition.base_type}")


def run_definition_on_frame(
    frame: pd.DataFrame,
    definition: RefinementDefinition,
    *,
    initial_capital: float,
    fee_rate: float,
    slippage_rate: float,
) -> object:
    if definition.base_type == "sma_baseline":
        return run_sma_baseline(
            frame,
            fee_rate=fee_rate,
            initial_capital=initial_capital,
            fast_window=int(definition.parameters["fast_window"]),
            slow_window=int(definition.parameters["slow_window"]),
        )

    signal_frame = build_signal_frame(frame, definition)
    return run_long_flat_backtest(
        signal_frame,
        initial_capital=initial_capital,
        fee_rate=fee_rate,
        slippage_rate=slippage_rate,
        name=definition.name,
    )


def performance_to_row(
    definition: RefinementDefinition,
    performance: object,
    *,
    segment: str,
    split_id: str,
    rows: int,
    start_timestamp: int,
    end_timestamp: int,
    gap_count: int,
    total_missing_intervals: int,
) -> ValidationResultRow:
    if definition.base_type == "sma_baseline":
        metrics = benchmark_result_to_performance_row(performance)
    else:
        metrics = calculate_backtest_metrics(performance)
    return ValidationResultRow(
        candidate_id=definition.refinement_id,
        candidate_name=definition.name,
        family=definition.base_type,
        source_type="refinement",
        mode="refinement_validation",
        segment=segment,
        split_id=split_id,
        parameter_set=parameter_label(definition.parameters),
        total_return=metrics.total_return,
        annualized_return=metrics.annualized_return,
        max_drawdown=metrics.max_drawdown,
        profit_factor=metrics.profit_factor,
        trades=metrics.trades,
        exposure=metrics.exposure,
        final_equity=metrics.final_equity,
        rows=rows,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        gap_count=gap_count,
        total_missing_intervals=total_missing_intervals,
    )


def get_frames_by_timeframe(dataset_frame: pd.DataFrame) -> dict[str, pd.DataFrame]:
    frame_1h = dataset_frame.copy()
    frame_4h = resample_1h_to_4h(frame_1h)
    if frame_4h.empty:
        raise ValueError("4h resample returned no rows.")
    return {"1h": frame_1h, "4h": frame_4h}


def run_refinement_validation(
    frames_by_timeframe: dict[str, pd.DataFrame],
    *,
    initial_capital: float,
    fee_rate: float,
    slippage_rate: float,
) -> list[ValidationResultRow]:
    rows: list[ValidationResultRow] = []
    for definition in get_refinement_catalog():
        frame = frames_by_timeframe[definition.timeframe]
        splits = build_temporal_splits(frame)
        oos_splits = [split for split in splits if split.segment in {"validation", "test"}]
        for split in oos_splits:
            split_frame = slice_frame_for_split(frame, split)
            gap_count, missing = summarize_segment_gaps(split_frame, timeframe=definition.timeframe)
            result = run_definition_on_frame(
                split_frame,
                definition,
                initial_capital=initial_capital,
                fee_rate=fee_rate,
                slippage_rate=slippage_rate,
            )
            rows.append(
                performance_to_row(
                    definition,
                    result,
                    segment=split.segment,
                    split_id=split.split_id,
                    rows=len(split_frame),
                    start_timestamp=int(split_frame["timestamp"].iloc[0]),
                    end_timestamp=int(split_frame["timestamp"].iloc[-1]),
                    gap_count=gap_count,
                    total_missing_intervals=missing,
                )
            )

        wf_windows = build_expanding_walk_forward_windows(
            frame,
            train_size=int(len(frame) * 0.5),
            test_size=int(len(frame) * 0.1),
        )
        for window in wf_windows:
            test_frame = slice_test_frame(frame, window)
            gap_count, missing = summarize_segment_gaps(test_frame, timeframe=definition.timeframe)
            result = run_definition_on_frame(
                test_frame,
                definition,
                initial_capital=initial_capital,
                fee_rate=fee_rate,
                slippage_rate=slippage_rate,
            )
            rows.append(
                performance_to_row(
                    definition,
                    result,
                    segment="walk_forward_test",
                    split_id=window.window_id,
                    rows=len(test_frame),
                    start_timestamp=int(test_frame["timestamp"].iloc[0]),
                    end_timestamp=int(test_frame["timestamp"].iloc[-1]),
                    gap_count=gap_count,
                    total_missing_intervals=missing,
                )
            )
    return rows


def build_oos_summary(rows: list[ValidationResultRow]) -> pd.DataFrame:
    frame = validation_rows_to_frame(rows)
    if frame.empty:
        return frame
    timeframe_map = {
        definition.refinement_id: definition.timeframe for definition in get_refinement_catalog()
    }
    summary = (
        frame.groupby(["candidate_id", "candidate_name", "family"], as_index=False)
        .agg(
            oos_segments=("segment", "count"),
            avg_total_return=("total_return", "mean"),
            median_total_return=("total_return", "median"),
            worst_total_return=("total_return", "min"),
            avg_max_drawdown=("max_drawdown", "mean"),
            avg_profit_factor=("profit_factor", "mean"),
            avg_trades=("trades", "mean"),
            avg_exposure=("exposure", "mean"),
            avg_final_equity=("final_equity", "mean"),
        )
        .sort_values(["avg_total_return", "worst_total_return"], ascending=[False, False])
        .reset_index(drop=True)
    )
    summary["timeframe"] = summary["candidate_id"].map(timeframe_map)
    return summary


def build_comparison_frame(summary: pd.DataFrame) -> pd.DataFrame:
    indexed = summary.set_index("candidate_id")
    records: list[dict[str, object]] = []
    for definition in get_refinement_catalog():
        if definition.baseline_id is None:
            continue
        current = indexed.loc[definition.refinement_id]
        baseline = indexed.loc[definition.baseline_id]
        records.append(
            {
                "candidate_name": definition.name,
                "baseline_name": baseline["candidate_name"],
                "avg_total_return": current["avg_total_return"],
                "baseline_avg_total_return": baseline["avg_total_return"],
                "return_delta": current["avg_total_return"] - baseline["avg_total_return"],
                "avg_trades": current["avg_trades"],
                "baseline_avg_trades": baseline["avg_trades"],
                "trade_delta": current["avg_trades"] - baseline["avg_trades"],
                "avg_exposure": current["avg_exposure"],
                "baseline_avg_exposure": baseline["avg_exposure"],
                "exposure_delta": current["avg_exposure"] - baseline["avg_exposure"],
            }
        )
    return pd.DataFrame.from_records(records)


def format_pct(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{float(value) * 100:.2f}%"


def build_comparison_table(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "No baseline comparisons."

    display = frame.copy()
    for column in [
        "avg_total_return",
        "baseline_avg_total_return",
        "return_delta",
        "avg_exposure",
        "baseline_avg_exposure",
        "exposure_delta",
    ]:
        display[column] = display[column].map(format_pct)
    display["avg_trades"] = display["avg_trades"].map(lambda value: f"{float(value):.1f}")
    display["baseline_avg_trades"] = display["baseline_avg_trades"].map(
        lambda value: f"{float(value):.1f}"
    )
    display["trade_delta"] = display["trade_delta"].map(lambda value: f"{float(value):.1f}")

    reduced = display.loc[
        :,
        [
            "candidate_name",
            "baseline_name",
            "avg_total_return",
            "baseline_avg_total_return",
            "return_delta",
            "avg_trades",
            "baseline_avg_trades",
            "trade_delta",
            "avg_exposure",
            "baseline_avg_exposure",
            "exposure_delta",
        ],
    ]
    headers = [
        "Candidate",
        "Baseline",
        "Avg Return",
        "Base Return",
        "Return Delta",
        "Avg Trades",
        "Base Trades",
        "Trade Delta",
        "Avg Exposure",
        "Base Exposure",
        "Exposure Delta",
    ]
    rows = reduced.values.tolist()
    widths = [len(header) for header in headers]
    for row in rows:
        widths = [max(width, len(str(cell))) for width, cell in zip(widths, row, strict=True)]

    def format_row(cells: list[str]) -> str:
        return " | ".join(str(cell).ljust(width) for cell, width in zip(cells, widths, strict=True))

    separator = "-+-".join("-" * width for width in widths)
    lines = [format_row(headers), separator]
    lines.extend(format_row([str(cell) for cell in row]) for row in rows)
    return "\n".join(lines)


def build_summary_table(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "No OOS summary rows."

    display = frame.copy()
    for column in [
        "avg_total_return",
        "median_total_return",
        "worst_total_return",
        "avg_max_drawdown",
        "avg_exposure",
    ]:
        display[column] = display[column].map(format_pct)
    display["avg_profit_factor"] = display["avg_profit_factor"].map(
        lambda value: "N/A" if pd.isna(value) else f"{float(value):.2f}"
    )
    display["avg_trades"] = display["avg_trades"].map(lambda value: f"{float(value):.1f}")
    display["avg_final_equity"] = display["avg_final_equity"].map(
        lambda value: f"{float(value):.2f}"
    )

    reduced = display.loc[
        :,
        [
            "candidate_name",
            "timeframe",
            "oos_segments",
            "avg_total_return",
            "median_total_return",
            "worst_total_return",
            "avg_max_drawdown",
            "avg_profit_factor",
            "avg_trades",
            "avg_exposure",
            "avg_final_equity",
        ],
    ]
    headers = [
        "Candidate",
        "TF",
        "OOS Segments",
        "Avg Return",
        "Median Return",
        "Worst Return",
        "Avg Max DD",
        "Avg PF",
        "Avg Trades",
        "Avg Exposure",
        "Avg Final Equity",
    ]
    rows = reduced.values.tolist()
    widths = [len(header) for header in headers]
    for row in rows:
        widths = [max(width, len(str(cell))) for width, cell in zip(widths, row, strict=True)]

    def format_row(cells: list[str]) -> str:
        return " | ".join(str(cell).ljust(width) for cell, width in zip(cells, widths, strict=True))

    separator = "-+-".join("-" * width for width in widths)
    lines = [format_row(headers), separator]
    lines.extend(format_row([str(cell) for cell in row]) for row in rows)
    return "\n".join(lines)


def export_report(
    *,
    run_id: str,
    created_at: pd.Timestamp,
    dataset_context: dict[str, object],
    parameters: dict[str, object],
    validation_rows: list[ValidationResultRow],
    summary: pd.DataFrame,
    comparisons: pd.DataFrame,
) -> Path:
    settings = get_settings()
    report_dir = create_report_directory(settings.reports_dir, "refinements", run_id)
    write_json_file(
        report_dir / "summary.json",
        {
            "run_id": run_id,
            "report_type": "refinements",
            "created_at": serialize_value(created_at),
            "dataset": serialize_value(dataset_context),
            "parameters": serialize_value(parameters),
            "oos_summary": serialize_value(summary.to_dict(orient="records")),
            "comparisons": serialize_value(comparisons.to_dict(orient="records")),
        },
    )
    write_table_csv(report_dir / "validation_rows.csv", validation_rows_to_frame(validation_rows))
    write_table_csv(report_dir / "oos_summary.csv", summary)
    write_table_csv(report_dir / "comparisons.csv", comparisons)
    artifacts = list_report_files(report_dir)
    manifest = build_run_manifest(
        run_id=run_id,
        report_type="refinements",
        created_at=created_at,
        dataset=dataset_context,
        parameters=parameters,
        summary_rows=[],
        artifacts=artifacts + ["manifest.json"],
    )
    write_json_file(report_dir / "manifest.json", serialize_value(manifest))
    return report_dir


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = get_settings()
    fee_rate = resolve_fee_rate(args)
    slippage_rate = resolve_slippage_rate(args)
    created_at = pd.Timestamp.now(tz="UTC").floor("s")
    run_id = args.run_id or build_run_id(created_at)
    logger = configure_logging(
        settings.log_dir,
        settings.log_level,
        logger_name="alpha_x_refinements",
    )

    dataset_path = build_ohlcv_csv_path(
        raw_data_dir=settings.raw_data_dir,
        exchange="bitvavo",
        market=args.market,
        timeframe=args.timeframe,
    )
    dataset = load_backtest_dataset(Path(dataset_path), args.timeframe)
    frames_by_timeframe = get_frames_by_timeframe(dataset.frame)
    validation_rows = run_refinement_validation(
        frames_by_timeframe,
        initial_capital=args.initial_capital,
        fee_rate=fee_rate,
        slippage_rate=slippage_rate,
    )
    summary = build_oos_summary(validation_rows)
    comparisons = build_comparison_frame(summary)
    dataset_context = build_dataset_context(dataset, frames_by_timeframe["4h"])
    parameters = {
        "fee_rate": fee_rate,
        "fee_bps": fee_rate * 10_000.0,
        "slippage_rate": slippage_rate,
        "slippage_bps": slippage_rate * 10_000.0,
        "initial_capital": args.initial_capital,
        "variants": [definition.parameters for definition in get_refinement_catalog()],
        "method_note": "OOS summary uses validation, test and walk_forward_test segments only.",
    }

    report_dir: Path | None = None
    if not args.no_export:
        report_dir = export_report(
            run_id=run_id,
            created_at=created_at,
            dataset_context=dataset_context,
            parameters=parameters,
            validation_rows=validation_rows,
            summary=summary,
            comparisons=comparisons,
        )

    logger.info("Loaded refinements dataset from %s", dataset_path)
    logger.info(
        "run_id=%s variants=%s rows_1h=%s rows_4h=%s",
        run_id,
        len(summary),
        len(dataset.frame),
        len(frames_by_timeframe["4h"]),
    )

    print(
        f"Dataset: {dataset_context['path']} | market={dataset_context['market']} | "
        f"rows_1h={dataset_context['rows_1h']} | rows_4h={dataset_context['rows_4h']}"
    )
    print(
        f"Gaps 1h: {dataset_context['gap_count_1h']} | "
        f"missing_1h={dataset_context['total_missing_intervals_1h']} | "
        f"largest_gap_1h={dataset_context['largest_gap_1h']}"
    )
    print(
        f"Fees: {fee_rate:.6f} ({fee_rate * 10_000.0:.2f} bps) | "
        f"Slippage: {slippage_rate:.6f} ({slippage_rate * 10_000.0:.2f} bps)"
    )
    if report_dir is not None:
        print(f"Report path: {report_dir}")
    print()
    print("OOS Summary")
    print(build_summary_table(summary))
    print()
    print("Churn Comparison vs Baseline")
    print(build_comparison_table(comparisons))
    print()
    print("Detailed OOS Rows")
    print(build_validation_table(validation_rows_to_frame(validation_rows)))
    print()
    churn_improves = not comparisons.empty and bool((comparisons["return_delta"] > 0).any())
    best_4h = summary.loc[
        summary["candidate_id"].str.contains("4h"),
        "avg_total_return",
    ].max()
    best_1h = summary.loc[
        summary["candidate_id"].str.contains("1h"),
        "avg_total_return",
    ].max()
    print("Interpretation Questions")
    print(
        "1. Reducir churn mejora materialmente la estabilidad OOS: "
        f"{'no claro' if not churn_improves else 'mixto'}"
    )
    print(
        "2. El 4h mejora frente al 1h: "
        f"{'no claro' if best_4h <= best_1h else 'parcial'}"
    )
    print(
        "3. La mejora viene de mejor estructura o solo de no operar: "
        "revisar trade_delta y exposure_delta."
    )
    print(
        "4. Evidencia minima para pasar a V3: solo si hay mejora OOS material "
        "sin colapso de exposicion."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
