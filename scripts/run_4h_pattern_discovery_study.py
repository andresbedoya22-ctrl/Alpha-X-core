from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from alpha_x.config.settings import get_settings
from alpha_x.data.ohlcv_storage import build_ohlcv_csv_path
from alpha_x.reporting.io import build_run_id, create_report_directory
from alpha_x.research_4h.distance_buffer_study import (
    BASE_COST_MODEL,
    BUY_HOLD_CONFIG_4H,
    DEADBAND,
    INITIAL_CAPITAL,
    OFFICIAL_1D_CONFIG,
    STRESS_COST_MODEL,
    CostModel,
    StrategyConfig,
    build_buy_and_hold_targets,
    build_distance_buffer_targets,
    calculate_metrics,
    common_period_start,
    derive_4h_from_1h,
    filter_trades_by_execution_period,
    load_ohlcv_frame,
    run_config,
    simulate_exposure_strategy,
    summarize_trades,
)

BASELINE_4H_PREVIOUS = StrategyConfig(
    name="Previous best 4H corrected",
    timeframe="4h",
    sma_window=750,
    buffer=0.035,
    entry_persistence=3,
    exit_persistence=3,
)

TRADING_DAYS_PER_YEAR = 365.25


@dataclass(frozen=True)
class DiscoveryCandidate:
    name: str
    pattern_source: str
    logic: str
    sma_window: int
    buffer: float
    entry_persistence: int
    exit_persistence: int
    variant: str

    def config(self) -> StrategyConfig:
        return StrategyConfig(
            name=self.name,
            timeframe="4h",
            sma_window=self.sma_window,
            buffer=self.buffer,
            entry_persistence=self.entry_persistence,
            exit_persistence=self.exit_persistence,
        )


def parse_args() -> argparse.Namespace:
    settings = get_settings()
    parser = argparse.ArgumentParser(
        description="Run BTC/EUR Bitvavo 4H deep pattern discovery study."
    )
    parser.add_argument("--market", default="BTC-EUR")
    parser.add_argument("--exchange", default="bitvavo")
    parser.add_argument("--reports-dir", type=Path, default=settings.reports_dir)
    parser.add_argument("--run-id", default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    settings = get_settings()
    one_hour_path = build_ohlcv_csv_path(
        settings.raw_data_dir,
        args.exchange,
        args.market,
        "1h",
    )
    one_day_path = build_ohlcv_csv_path(
        settings.raw_data_dir,
        args.exchange,
        args.market,
        "1d",
    )

    frame_4h, quality_4h = derive_4h_from_1h(one_hour_path)
    frame_1d, quality_1d = load_ohlcv_frame(one_day_path, "1d")
    features = add_4h_features(frame_4h)

    common_start = common_period_start(frame_4h, frame_1d, BASELINE_4H_PREVIOUS)
    common_end = min(
        pd.to_datetime(frame_4h["datetime"].iloc[-1], utc=True),
        pd.to_datetime(frame_1d["datetime"].iloc[-1], utc=True),
    )

    baseline_targets = build_distance_buffer_targets(frame_4h, BASELINE_4H_PREVIOUS)
    baseline_trades = build_custom_trade_log(
        frame_4h,
        baseline_targets,
        BASELINE_4H_PREVIOUS,
        BASE_COST_MODEL,
    )
    entry_features = build_entry_feature_table(
        features,
        baseline_targets,
        baseline_trades,
        cost_model=BASE_COST_MODEL,
    )
    pattern_summary = summarize_patterns(entry_features)
    exit_summary = summarize_exit_patterns(features, baseline_targets, baseline_trades)
    study_2025 = summarize_2025_trades(features, baseline_targets, baseline_trades)

    candidate_templates = build_candidate_templates(entry_features)
    candidate_rows, candidate_targets = evaluate_candidate_grid(
        frame_4h,
        features,
        candidate_templates,
        start=common_start,
        end=common_end,
        cost_model=BASE_COST_MODEL,
    )
    candidate_ranking = pd.DataFrame(candidate_rows).sort_values(
        ["calmar", "cagr", "top_3_concentration", "rebalances"],
        ascending=[False, False, True, True],
    )
    selected = select_relevant_candidates(candidate_ranking)

    comparison_base = build_comparison(
        frame_4h,
        frame_1d,
        features,
        selected,
        candidate_targets,
        cost_model=BASE_COST_MODEL,
        start=common_start,
        end=common_end,
    )
    comparison_stress = build_comparison(
        frame_4h,
        frame_1d,
        features,
        selected,
        candidate_targets,
        cost_model=STRESS_COST_MODEL,
        start=common_start,
        end=common_end,
    )
    best_row = selected.iloc[0]
    best_name = str(best_row["name"])
    best_targets = candidate_targets[best_name]
    best_config = StrategyConfig(
        name=best_name,
        timeframe="4h",
        sma_window=int(best_row["sma_window"]),
        buffer=float(best_row["buffer"]),
        entry_persistence=int(best_row["entry_persistence"]),
        exit_persistence=int(best_row["exit_persistence"]),
    )
    rolling = build_rolling_robustness(
        frame_4h,
        frame_1d,
        best_targets,
        best_config,
        cost_model=BASE_COST_MODEL,
        start=common_start,
        end=common_end,
    )
    expanding = build_expanding_robustness(
        frame_4h,
        frame_1d,
        best_targets,
        best_config,
        cost_model=BASE_COST_MODEL,
        start=common_start,
        end=common_end,
    )
    bootstrap = build_bootstrap_table(
        frame_4h,
        frame_1d,
        best_targets,
        best_config,
        cost_model=BASE_COST_MODEL,
        start=common_start,
        end=common_end,
    )

    best_trades = build_custom_trade_log(
        frame_4h,
        best_targets,
        best_config,
        BASE_COST_MODEL,
    )
    best_trades_common = filter_trades_by_execution_period(
        best_trades,
        start=common_start,
        end=common_end,
    )
    best_trade_summary = summarize_trades(
        best_trades_common,
        dataset_start=common_start,
        dataset_end=common_end,
    )
    baseline_trades_common = filter_trades_by_execution_period(
        baseline_trades,
        start=common_start,
        end=common_end,
    )
    baseline_trade_summary = summarize_trades(
        baseline_trades_common,
        dataset_start=common_start,
        dataset_end=common_end,
    )

    practical_2025 = pd.DataFrame(
        [
            run_custom_monthly_contribution_simulation(
                frame_4h,
                best_targets,
                best_config,
                cost_model=BASE_COST_MODEL,
            ),
            run_custom_monthly_contribution_simulation(
                frame_4h,
                build_distance_buffer_targets(frame_4h, BASELINE_4H_PREVIOUS),
                BASELINE_4H_PREVIOUS,
                cost_model=BASE_COST_MODEL,
            ),
            run_custom_monthly_contribution_simulation(
                frame_1d,
                build_distance_buffer_targets(frame_1d, OFFICIAL_1D_CONFIG),
                OFFICIAL_1D_CONFIG,
                cost_model=BASE_COST_MODEL,
            ),
            run_custom_monthly_contribution_simulation(
                frame_4h,
                build_buy_and_hold_targets(frame_4h),
                BUY_HOLD_CONFIG_4H,
                cost_model=BASE_COST_MODEL,
            ),
        ]
    )

    run_id = args.run_id or build_run_id()
    report_dir = create_report_directory(args.reports_dir, "4h_pattern_discovery", run_id)
    write_outputs(
        report_dir,
        manifest={
            "market": args.market,
            "exchange": args.exchange,
            "data": {
                "4h": quality_4h.to_dict(),
                "1d": quality_1d.to_dict(),
            },
            "common_period": {"start": str(common_start), "end": str(common_end)},
            "previous_4h": candidate_dict(BASELINE_4H_PREVIOUS),
            "best_candidate": best_row.to_dict(),
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
        },
        entry_features=entry_features,
        pattern_summary=pattern_summary,
        exit_summary=exit_summary,
        study_2025=study_2025,
        candidate_ranking=candidate_ranking,
        selected=selected,
        comparison_base=comparison_base,
        comparison_stress=comparison_stress,
        rolling=rolling,
        expanding=expanding,
        bootstrap=bootstrap,
        best_trades=best_trades_common,
        best_trade_summary=best_trade_summary,
        baseline_trade_summary=baseline_trade_summary,
        practical_2025=practical_2025,
        candidate_templates=candidate_templates,
    )

    print(f"Report directory: {report_dir}")
    print("\nSelected candidate ranking")
    print(selected.to_string(index=False))
    print("\nBase comparison")
    print(comparison_base.to_string(index=False))
    print("\n2025 practical simulation")
    print(practical_2025.to_string(index=False))
    return 0


def add_4h_features(frame: pd.DataFrame) -> pd.DataFrame:
    data = frame.copy().reset_index(drop=True)
    data["datetime"] = pd.to_datetime(data["datetime"], utc=True)
    prev_close = data["close"].shift(1)
    true_range = pd.concat(
        [
            data["high"] - data["low"],
            (data["high"] - prev_close).abs(),
            (data["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    data["atr14"] = true_range.rolling(14, min_periods=14).mean()
    data["atr_pct"] = data["atr14"] / data["close"]
    data["range_pct"] = (data["high"] - data["low"]) / data["close"]
    data["range20_pct"] = data["range_pct"].rolling(20, min_periods=20).mean()
    data["range120_pct"] = data["range_pct"].rolling(120, min_periods=120).mean()
    data["compression_20_120"] = data["range20_pct"] / data["range120_pct"]
    data["atr_pctile_180"] = rolling_percentile(data["atr_pct"], 180)
    body = (data["close"] - data["open"]).abs()
    candle_range = (data["high"] - data["low"]).replace(0.0, np.nan)
    data["body_pct"] = body / data["close"]
    data["body_to_range"] = body / candle_range
    data["close_position"] = (data["close"] - data["low"]) / candle_range
    data["green"] = data["close"] > data["open"]
    data["green_streak"] = streak(data["green"])
    for window in (20, 30, 50):
        data[f"break_prev_high_{window}"] = (
            data["close"] > data["high"].rolling(window, min_periods=window).max().shift(1)
        )
    for window in (700, 750, 800):
        sma = data["close"].rolling(window, min_periods=window).mean()
        data[f"sma{window}"] = sma
        data[f"sma{window}_slope30_pct"] = (sma - sma.shift(30)) / data["close"]
        data[f"sma{window}_slope30_atr"] = (sma - sma.shift(30)) / data["atr14"]
        data[f"dist_sma{window}"] = data["close"] / sma - 1.0
    return data


def rolling_percentile(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window, min_periods=window).apply(
        lambda values: pd.Series(values).rank(pct=True).iloc[-1],
        raw=False,
    )


def streak(condition: pd.Series) -> pd.Series:
    groups = condition.ne(condition.shift()).cumsum()
    return condition.astype(int).groupby(groups).cumsum()


def build_entry_feature_table(
    features: pd.DataFrame,
    targets: pd.DataFrame,
    trades: pd.DataFrame,
    *,
    cost_model: CostModel,
) -> pd.DataFrame:
    enriched = features.copy()
    enriched["threshold"] = targets["threshold"]
    enriched["entry_condition"] = targets["entry_condition"].astype(bool)
    enriched["entry_streak"] = targets["entry_streak"]
    enriched["target_exposure"] = targets["target_exposure"]
    enriched["prior_target"] = enriched["target_exposure"].shift(1, fill_value=0.0)
    below = ~enriched["entry_condition"]
    enriched["bars_below_threshold_before_entry"] = streak(below).shift(1)
    prior_entry_condition = enriched["entry_condition"].shift(fill_value=False)
    attempt_start = enriched["entry_condition"] & (~prior_entry_condition)
    enriched["failed_attempts_90"] = attempt_start.shift(1).rolling(90, min_periods=1).sum()
    enriched["dist_threshold"] = enriched["close"] / enriched["threshold"] - 1.0
    entries = enriched[
        (enriched["prior_target"] == 0.0) & (enriched["target_exposure"] == 1.0)
    ].copy()
    trades_working = trades.copy()
    if not trades_working.empty:
        trades_working["signal_entry_date"] = pd.to_datetime(
            trades_working["signal_entry_date"],
            utc=True,
        )
        entries = entries.merge(
            trades_working[
                [
                    "trade_id",
                    "signal_entry_date",
                    "execution_entry_date",
                    "execution_exit_date",
                    "net_return",
                    "gross_return",
                    "holding_days",
                ]
            ],
            left_on="datetime",
            right_on="signal_entry_date",
            how="left",
        )
    entries["outcome"] = np.where(entries["net_return"] > 0.0, "winner", "loser")
    entries["trend_trade"] = entries["net_return"] > (2.0 * cost_model.cost_per_side)
    return entries.reset_index(drop=True)


def summarize_patterns(entry_features: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "dist_threshold",
        "dist_sma750",
        "entry_streak",
        "break_prev_high_20",
        "break_prev_high_30",
        "break_prev_high_50",
        "body_pct",
        "body_to_range",
        "close_position",
        "green_streak",
        "atr_pct",
        "atr_pctile_180",
        "compression_20_120",
        "sma750_slope30_pct",
        "sma750_slope30_atr",
        "bars_below_threshold_before_entry",
        "failed_attempts_90",
    ]
    rows: list[dict[str, Any]] = []
    valid = entry_features.dropna(subset=["net_return"]).copy()
    for column in columns:
        series = valid[column]
        if series.dropna().empty:
            continue
        if series.dtype == bool:
            for value, label in ((True, "true"), (False, "false")):
                sample = valid[series == value]
                rows.append(pattern_row(column, label, sample))
            continue
        q25 = float(series.quantile(0.25))
        q50 = float(series.quantile(0.50))
        q75 = float(series.quantile(0.75))
        bins = [
            (series <= q25, f"<=p25 {q25:.6f}"),
            ((series > q25) & (series <= q50), f"p25-p50 {q25:.6f}..{q50:.6f}"),
            ((series > q50) & (series <= q75), f"p50-p75 {q50:.6f}..{q75:.6f}"),
            (series > q75, f">p75 {q75:.6f}"),
        ]
        for mask, label in bins:
            rows.append(pattern_row(column, label, valid[mask]))
    summary = pd.DataFrame(rows)
    if summary.empty:
        return summary
    summary["edge_vs_all"] = summary["mean_net_return"] - float(valid["net_return"].mean())
    return summary.sort_values(["edge_vs_all", "trade_count"], ascending=[False, False])


def pattern_row(column: str, bucket: str, sample: pd.DataFrame) -> dict[str, Any]:
    returns = sample["net_return"].dropna()
    return {
        "feature": column,
        "bucket": bucket,
        "trade_count": int(len(returns)),
        "win_rate": None if returns.empty else float((returns > 0.0).mean()),
        "mean_net_return": None if returns.empty else float(returns.mean()),
        "median_net_return": None if returns.empty else float(returns.median()),
        "total_net_return_sum": None if returns.empty else float(returns.sum()),
    }


def summarize_exit_patterns(
    features: pd.DataFrame,
    targets: pd.DataFrame,
    trades: pd.DataFrame,
) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame()
    enriched = features.copy()
    enriched["threshold"] = targets["threshold"]
    enriched["dist_threshold"] = enriched["close"] / enriched["threshold"] - 1.0
    rows = []
    for trade in trades.itertuples(index=False):
        exit_dt = pd.to_datetime(trade.signal_exit_date, utc=True)
        matches = enriched.index[enriched["datetime"] == exit_dt].tolist()
        if not matches:
            continue
        idx = matches[0]
        lookback = enriched.iloc[max(0, idx - 12) : idx + 1]
        rows.append(
            {
                "trade_id": int(trade.trade_id),
                "net_return": float(trade.net_return),
                "exit_dist_threshold": float(enriched["dist_threshold"].iloc[idx]),
                "exit_close_vs_prior_12_low": float(
                    enriched["close"].iloc[idx] / lookback["low"].min() - 1.0
                ),
                "exit_atr_pctile_180": float(enriched["atr_pctile_180"].iloc[idx]),
                "deterioration_3bar_return": float(
                    enriched["close"].iloc[idx] / enriched["close"].iloc[max(0, idx - 3)] - 1.0
                ),
                "gave_back_from_prior_20_high": float(
                    enriched["close"].iloc[idx]
                    / enriched["high"].iloc[max(0, idx - 20) : idx + 1].max()
                    - 1.0
                ),
            }
        )
    exits = pd.DataFrame(rows)
    if exits.empty:
        return exits
    exits["exit_type"] = np.where(exits["net_return"] > 0.0, "winner_exit", "false_or_loss_exit")
    return exits.groupby("exit_type", as_index=False).agg(
        trades=("trade_id", "count"),
        mean_net_return=("net_return", "mean"),
        median_exit_dist_threshold=("exit_dist_threshold", "median"),
        mean_deterioration_3bar_return=("deterioration_3bar_return", "mean"),
        mean_gave_back_from_prior_20_high=("gave_back_from_prior_20_high", "mean"),
        median_exit_atr_pctile_180=("exit_atr_pctile_180", "median"),
    )


def summarize_2025_trades(
    features: pd.DataFrame,
    targets: pd.DataFrame,
    trades: pd.DataFrame,
) -> pd.DataFrame:
    start = pd.Timestamp("2025-01-01", tz="UTC")
    end = pd.Timestamp("2026-01-01", tz="UTC")
    rows = []
    working = build_entry_feature_table(features, targets, trades, cost_model=BASE_COST_MODEL)
    working["execution_entry_date"] = pd.to_datetime(working["execution_entry_date"], utc=True)
    period = working[
        (working["execution_entry_date"] >= start) & (working["execution_entry_date"] < end)
    ].copy()
    for row in period.itertuples(index=False):
        rows.append(
            {
                "trade_id": row.trade_id,
                "execution_entry_date": row.execution_entry_date,
                "execution_exit_date": row.execution_exit_date,
                "net_return": row.net_return,
                "dist_threshold": row.dist_threshold,
                "sma750_slope30_atr": row.sma750_slope30_atr,
                "break_prev_high_30": row.break_prev_high_30,
                "body_to_range": row.body_to_range,
                "close_position": row.close_position,
                "atr_pctile_180": row.atr_pctile_180,
                "compression_20_120": row.compression_20_120,
                "failed_attempts_90": row.failed_attempts_90,
            }
        )
    return pd.DataFrame(rows)


def build_candidate_templates(entry_features: pd.DataFrame) -> list[DiscoveryCandidate]:
    valid = entry_features.dropna(subset=["net_return"]).copy()
    compression_limit = float(valid["compression_20_120"].quantile(0.50))
    close_position_limit = float(valid["close_position"].quantile(0.50))
    body_limit = float(valid["body_to_range"].quantile(0.50))
    failed_limit = float(valid["failed_attempts_90"].quantile(0.75))
    return [
        DiscoveryCandidate(
            name="4H slope recent-high quality",
            pattern_source=(
                "Breakouts with positive SMA slope and recent-high confirmation had "
                "better follow-through."
            ),
            logic=(
                "Long threshold only when SMA slope is positive, close breaks prior "
                "high, and close is in the upper candle half."
            ),
            sma_window=750,
            buffer=0.030,
            entry_persistence=2,
            exit_persistence=1,
            variant="slope_recent_high",
        ),
        DiscoveryCandidate(
            name="4H compression quality breakout",
            pattern_source=(
                "Best baseline trades came after non-expanded volatility and "
                "decisive candles."
            ),
            logic=(
                "Long threshold only after relative range compression, a prior-high break, "
                "body quality, and close near the high."
            ),
            sma_window=750,
            buffer=0.030,
            entry_persistence=1,
            exit_persistence=1,
            variant=(
                "compression_quality"
                f"|compression<={compression_limit:.6f}"
                f"|body>={body_limit:.6f}"
                f"|closepos>={close_position_limit:.6f}"
            ),
        ),
        DiscoveryCandidate(
            name="4H anti-whipsaw cooldown",
            pattern_source="Losses clustered after repeated failed threshold attempts.",
            logic=(
                "Require slope not clearly negative and skip entries after an "
                "elevated failed-attempt count."
            ),
            sma_window=750,
            buffer=0.030,
            entry_persistence=2,
            exit_persistence=1,
            variant=f"anti_whipsaw|failed<={failed_limit:.6f}",
        ),
        DiscoveryCandidate(
            name="4H asymmetric exit repair",
            pattern_source=(
                "False exits and late exits showed deterioration before the 3/3 "
                "exit confirmed."
            ),
            logic=(
                "Keep normal threshold entry, but exit on first confirmed threshold "
                "loss to reduce give-back."
            ),
            sma_window=750,
            buffer=0.035,
            entry_persistence=3,
            exit_persistence=1,
            variant="asymmetric_exit",
        ),
    ]


def evaluate_candidate_grid(
    frame: pd.DataFrame,
    features: pd.DataFrame,
    templates: list[DiscoveryCandidate],
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
    cost_model: CostModel,
) -> tuple[list[dict[str, Any]], dict[str, pd.DataFrame]]:
    rows: list[dict[str, Any]] = []
    targets_by_name: dict[str, pd.DataFrame] = {}
    for template in templates:
        for sma_window in (700, 750, 800):
            for buffer in (0.025, 0.030, 0.035):
                candidate = DiscoveryCandidate(
                    name=template.name,
                    pattern_source=template.pattern_source,
                    logic=template.logic,
                    sma_window=sma_window,
                    buffer=buffer,
                    entry_persistence=template.entry_persistence,
                    exit_persistence=template.exit_persistence,
                    variant=template.variant,
                )
                targets = build_candidate_targets(features, candidate)
                config = candidate.config()
                result = simulate_exposure_strategy(
                    frame,
                    targets,
                    config=config,
                    initial_capital=INITIAL_CAPITAL,
                    cost_model=cost_model,
                    dead_band=DEADBAND,
                )
                metrics = calculate_metrics(result, start=start, end=end)
                trades = build_custom_trade_log(frame, targets, config, cost_model)
                common_trades = filter_trades_by_execution_period(trades, start=start, end=end)
                summary = summarize_trades(common_trades, dataset_start=start, dataset_end=end)
                top3 = concentration_share(summary, 3)
                key = candidate_key(candidate)
                row = {
                    **metrics,
                    "candidate_family": template.name,
                    "variant": template.variant,
                    "pattern_source": template.pattern_source,
                    "logic": template.logic,
                    "complete_trades": summary.get("complete_trades", 0),
                    "trades_per_year": summary.get("trades_per_year", 0.0),
                    "profit_factor": summary.get("profit_factor"),
                    "expectancy": summary.get("expectancy_per_trade"),
                    "top_1_concentration": concentration_share(summary, 1),
                    "top_3_concentration": top3,
                    "top_5_concentration": concentration_share(summary, 5),
                    "name": key,
                }
                rows.append(row)
                targets_by_name[key] = targets
    return rows, targets_by_name


def build_candidate_targets(features: pd.DataFrame, candidate: DiscoveryCandidate) -> pd.DataFrame:
    data = features.copy()
    threshold = data[f"sma{candidate.sma_window}"] * (1.0 + candidate.buffer)
    entry_raw = (data["close"] > threshold) & threshold.notna()
    exit_raw = (data["close"] <= threshold) & threshold.notna()
    entry_streak = streak(entry_raw)
    exit_streak = streak(exit_raw)
    variant = candidate.variant
    entry_filter = pd.Series(True, index=data.index)
    if variant.startswith("slope_recent_high"):
        entry_filter = (
            (data[f"sma{candidate.sma_window}_slope30_atr"] > 0.0)
            & data["break_prev_high_30"]
            & (data["close_position"] >= 0.55)
        )
    elif variant.startswith("compression_quality"):
        parts = parse_variant_limits(variant)
        entry_filter = (
            (data["compression_20_120"] <= parts["compression"])
            & data["break_prev_high_20"]
            & (data["body_to_range"] >= parts["body"])
            & (data["close_position"] >= parts["closepos"])
        )
    elif variant.startswith("anti_whipsaw"):
        parts = parse_variant_limits(variant)
        attempt_start = entry_raw & (~entry_raw.shift(fill_value=False))
        failed_attempts = attempt_start.shift(1).rolling(90, min_periods=1).sum()
        entry_filter = (
            (data[f"sma{candidate.sma_window}_slope30_atr"] > -0.25)
            & (failed_attempts <= parts["failed"])
        )
    elif variant == "asymmetric_exit":
        entry_filter = pd.Series(True, index=data.index)
    else:
        raise ValueError(f"Unknown candidate variant: {variant}")

    state = 0.0
    exposure = []
    for index in range(len(data)):
        if (
            state == 0.0
            and bool(entry_raw.iloc[index])
            and int(entry_streak.iloc[index]) >= candidate.entry_persistence
            and bool(entry_filter.iloc[index])
        ):
            state = 1.0
        elif (
            state == 1.0
            and bool(exit_raw.iloc[index])
            and int(exit_streak.iloc[index]) >= candidate.exit_persistence
        ):
            state = 0.0
        exposure.append(state)
    targets = data.loc[:, ["timestamp", "datetime", "open", "high", "low", "close"]].copy()
    targets["sma"] = data[f"sma{candidate.sma_window}"]
    targets["threshold"] = threshold
    targets["entry_condition"] = entry_raw
    targets["exit_condition"] = exit_raw
    targets["entry_streak"] = entry_streak
    targets["exit_streak"] = exit_streak
    targets["target_exposure"] = exposure
    return targets


def parse_variant_limits(variant: str) -> dict[str, float]:
    values = {}
    for part in variant.split("|")[1:]:
        key, value = part.split("<=") if "<=" in part else part.split(">=")
        values[key] = float(value)
    return values


def candidate_key(candidate: DiscoveryCandidate) -> str:
    return (
        f"{candidate.name} | SMA{candidate.sma_window} | "
        f"buffer {candidate.buffer:.1%} | p{candidate.entry_persistence}/"
        f"{candidate.exit_persistence}"
    )


def select_relevant_candidates(ranking: pd.DataFrame) -> pd.DataFrame:
    selected_rows = []
    for _, family in ranking.groupby("candidate_family", sort=False):
        selected_rows.append(family.iloc[0])
    selected = pd.DataFrame(selected_rows).sort_values(
        ["calmar", "cagr", "top_3_concentration"],
        ascending=[False, False, True],
    )
    return selected.head(4).reset_index(drop=True)


def build_comparison(
    frame_4h: pd.DataFrame,
    frame_1d: pd.DataFrame,
    _features: pd.DataFrame,
    selected: pd.DataFrame,
    candidate_targets: dict[str, pd.DataFrame],
    *,
    cost_model: CostModel,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame:
    rows = []
    for row in selected.itertuples(index=False):
        config = StrategyConfig(
            name=str(row.name),
            timeframe="4h",
            sma_window=int(row.sma_window),
            buffer=float(row.buffer),
            entry_persistence=int(row.entry_persistence),
            exit_persistence=int(row.exit_persistence),
        )
        result = simulate_exposure_strategy(
            frame_4h,
            candidate_targets[str(row.name)],
            config=config,
            initial_capital=INITIAL_CAPITAL,
            cost_model=cost_model,
            dead_band=DEADBAND,
        )
        rows.append(calculate_metrics(result, start=start, end=end))
    for config, frame, targets in (
        (
            BASELINE_4H_PREVIOUS,
            frame_4h,
            build_distance_buffer_targets(frame_4h, BASELINE_4H_PREVIOUS),
        ),
        (OFFICIAL_1D_CONFIG, frame_1d, build_distance_buffer_targets(frame_1d, OFFICIAL_1D_CONFIG)),
        (BUY_HOLD_CONFIG_4H, frame_4h, build_buy_and_hold_targets(frame_4h)),
    ):
        result = simulate_exposure_strategy(
            frame,
            targets,
            config=config,
            initial_capital=INITIAL_CAPITAL,
            cost_model=cost_model,
            dead_band=DEADBAND,
        )
        rows.append(calculate_metrics(result, start=start, end=end))
    return pd.DataFrame(rows)


def build_rolling_robustness(
    frame_4h: pd.DataFrame,
    frame_1d: pd.DataFrame,
    best_targets: pd.DataFrame,
    best_config: StrategyConfig,
    *,
    cost_model: CostModel,
    start: pd.Timestamp,
    end: pd.Timestamp,
    window_days: int = 365,
    step_days: int = 182,
) -> pd.DataFrame:
    curves = comparison_curves(frame_4h, frame_1d, best_targets, best_config, cost_model)
    rows = []
    window_start = start.floor("D")
    number = 1
    while window_start + pd.Timedelta(days=window_days) <= end:
        window_end = window_start + pd.Timedelta(days=window_days)
        metrics = {
            name: calculate_metrics(result, start=window_start, end=window_end)
            for name, result in curves.items()
        }
        rows.append(robustness_row(f"rolling_{number:02d}", window_start, window_end, metrics))
        window_start += pd.Timedelta(days=step_days)
        number += 1
    return pd.DataFrame(rows)


def build_expanding_robustness(
    frame_4h: pd.DataFrame,
    frame_1d: pd.DataFrame,
    best_targets: pd.DataFrame,
    best_config: StrategyConfig,
    *,
    cost_model: CostModel,
    start: pd.Timestamp,
    end: pd.Timestamp,
    min_days: int = 365,
    step_days: int = 182,
) -> pd.DataFrame:
    curves = comparison_curves(frame_4h, frame_1d, best_targets, best_config, cost_model)
    rows = []
    window_end = start.floor("D") + pd.Timedelta(days=min_days)
    number = 1
    while window_end <= end:
        metrics = {
            name: calculate_metrics(result, start=start, end=window_end)
            for name, result in curves.items()
        }
        rows.append(robustness_row(f"expanding_{number:02d}", start, window_end, metrics))
        window_end += pd.Timedelta(days=step_days)
        number += 1
    return pd.DataFrame(rows)


def comparison_curves(
    frame_4h: pd.DataFrame,
    frame_1d: pd.DataFrame,
    best_targets: pd.DataFrame,
    best_config: StrategyConfig,
    cost_model: CostModel,
) -> dict[str, Any]:
    return {
        "new_4h": simulate_exposure_strategy(
            frame_4h,
            best_targets,
            config=best_config,
            initial_capital=INITIAL_CAPITAL,
            cost_model=cost_model,
            dead_band=DEADBAND,
        ),
        "previous_4h": run_config(frame_4h, BASELINE_4H_PREVIOUS, cost_model=cost_model),
        "official_1d": run_config(frame_1d, OFFICIAL_1D_CONFIG, cost_model=cost_model),
        "buy_hold": run_config(frame_4h, BUY_HOLD_CONFIG_4H, cost_model=cost_model),
    }


def robustness_row(
    label: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    metrics: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    calmars = {
        name: (-np.inf if row["calmar"] is None else float(row["calmar"]))
        for name, row in metrics.items()
    }
    returns = {name: float(row["total_return"]) for name, row in metrics.items()}
    return {
        "window": label,
        "start": str(start.date()),
        "end": str(end.date()),
        "winner_by_calmar": max(calmars, key=calmars.get),
        "all_negative_return": all(value < 0.0 for value in returns.values()),
        "new_4h_total_return": returns["new_4h"],
        "previous_4h_total_return": returns["previous_4h"],
        "official_1d_total_return": returns["official_1d"],
        "buy_hold_total_return": returns["buy_hold"],
        "new_4h_calmar": metrics["new_4h"]["calmar"],
        "previous_4h_calmar": metrics["previous_4h"]["calmar"],
        "official_1d_calmar": metrics["official_1d"]["calmar"],
        "buy_hold_calmar": metrics["buy_hold"]["calmar"],
        "new_4h_rebalances": metrics["new_4h"]["rebalances"],
        "previous_4h_rebalances": metrics["previous_4h"]["rebalances"],
        "official_1d_rebalances": metrics["official_1d"]["rebalances"],
    }


def build_bootstrap_table(
    frame_4h: pd.DataFrame,
    frame_1d: pd.DataFrame,
    best_targets: pd.DataFrame,
    best_config: StrategyConfig,
    *,
    cost_model: CostModel,
    start: pd.Timestamp,
    end: pd.Timestamp,
    iterations: int = 500,
    block_days: int = 30,
) -> pd.DataFrame:
    curves = comparison_curves(frame_4h, frame_1d, best_targets, best_config, cost_model)
    rows = []
    for name, result in curves.items():
        curve = result.equity_curve.copy()
        curve["datetime"] = pd.to_datetime(curve["datetime"], utc=True)
        curve = curve[(curve["datetime"] >= start) & (curve["datetime"] <= end)]
        returns = curve["equity"].pct_change().dropna().to_numpy()
        rows.append(
            {
                "name": name,
                "timeframe": result.config.timeframe,
                **bootstrap_return_metrics(
                    returns,
                    bars_per_year=bars_per_year(result.config.timeframe),
                    iterations=iterations,
                    block_bars=max(1, int(block_days / bar_days(result.config.timeframe))),
                    seed=17,
                ),
            }
        )
    return pd.DataFrame(rows)


def bootstrap_return_metrics(
    returns: np.ndarray,
    *,
    bars_per_year: float,
    iterations: int,
    block_bars: int,
    seed: int,
) -> dict[str, float]:
    rng = np.random.default_rng(seed)
    sharpes = []
    calmars = []
    starts = np.arange(0, len(returns))
    for _ in range(iterations):
        sampled: list[float] = []
        while len(sampled) < len(returns):
            start = int(rng.choice(starts))
            sampled.extend(returns[start : min(start + block_bars, len(returns))].tolist())
        sample = np.asarray(sampled[: len(returns)], dtype=float)
        std = float(sample.std(ddof=1))
        sharpes.append(np.nan if std <= 0.0 else sample.mean() / std * np.sqrt(bars_per_year))
        equity = np.cumprod(1.0 + sample)
        total_return = float(equity[-1] - 1.0)
        years = len(sample) / bars_per_year
        cagr = np.nan if total_return <= -1.0 else (1.0 + total_return) ** (1.0 / years) - 1.0
        max_drawdown = float((equity / np.maximum.accumulate(equity) - 1.0).min())
        calmars.append(np.nan if max_drawdown == 0.0 else cagr / abs(max_drawdown))
    return {
        "sharpe_p05": float(np.nanpercentile(sharpes, 5)),
        "sharpe_median": float(np.nanpercentile(sharpes, 50)),
        "sharpe_p95": float(np.nanpercentile(sharpes, 95)),
        "calmar_p05": float(np.nanpercentile(calmars, 5)),
        "calmar_median": float(np.nanpercentile(calmars, 50)),
        "calmar_p95": float(np.nanpercentile(calmars, 95)),
    }


def build_custom_trade_log(
    frame: pd.DataFrame,
    targets: pd.DataFrame,
    config: StrategyConfig,
    cost_model: CostModel,
) -> pd.DataFrame:
    signals = targets.copy().reset_index(drop=True)
    signals["datetime"] = pd.to_datetime(signals["datetime"], utc=True)
    signals["prior_target"] = signals["target_exposure"].shift(1, fill_value=0.0)
    trades = []
    open_entry: dict[str, Any] | None = None
    for signal_index, row in signals.iterrows():
        transition_on = row["prior_target"] == 0.0 and row["target_exposure"] == 1.0
        transition_off = row["prior_target"] == 1.0 and row["target_exposure"] == 0.0
        if not transition_on and not transition_off:
            continue
        execution_index = signal_index + 1
        if execution_index >= len(signals):
            continue
        execution = signals.iloc[execution_index]
        if transition_on and open_entry is None:
            open_entry = {
                "signal_entry_date": row["datetime"],
                "execution_entry_date": execution["datetime"],
                "entry_price_signal": float(row["close"]),
                "entry_price_execution": float(execution["close"]),
                "entry_execution_index": execution_index,
            }
            continue
        if transition_off and open_entry is not None:
            entry_price = float(open_entry["entry_price_execution"])
            exit_price = float(execution["close"])
            gross_return = exit_price / entry_price - 1.0
            net_return = (exit_price * (1.0 - cost_model.cost_per_side)) / (
                entry_price * (1.0 + cost_model.cost_per_side)
            ) - 1.0
            bars_held = execution_index - int(open_entry["entry_execution_index"])
            trades.append(
                {
                    "trade_id": len(trades) + 1,
                    "signal_entry_date": str(open_entry["signal_entry_date"]),
                    "execution_entry_date": str(open_entry["execution_entry_date"]),
                    "signal_exit_date": str(row["datetime"]),
                    "execution_exit_date": str(execution["datetime"]),
                    "entry_price_signal": float(open_entry["entry_price_signal"]),
                    "entry_price_execution": entry_price,
                    "exit_price_signal": float(row["close"]),
                    "exit_price_execution": exit_price,
                    "gross_return": gross_return,
                    "net_return": net_return,
                    "holding_bars": bars_held,
                    "holding_days": bars_held * bar_days(config.timeframe),
                    "fees_paid": cost_model.fee_per_side * 2.0,
                    "slippage_paid": cost_model.slippage_per_side * 2.0,
                }
            )
            open_entry = None
    return pd.DataFrame(trades)


def run_custom_monthly_contribution_simulation(
    frame: pd.DataFrame,
    targets: pd.DataFrame,
    config: StrategyConfig,
    *,
    cost_model: CostModel,
    start: str = "2025-01-01",
    end: str = "2025-12-31",
    initial_capital: float = 1_000.0,
    monthly_contribution: float = 500.0,
) -> dict[str, Any]:
    working = frame.loc[:, ["datetime", "close"]].copy()
    working["datetime"] = pd.to_datetime(working["datetime"], utc=True)
    working["target"] = targets["target_exposure"].astype(float)
    start_ts = pd.Timestamp(start, tz="UTC")
    end_ts = pd.Timestamp(end, tz="UTC") + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
    working = working[(working["datetime"] >= start_ts) & (working["datetime"] <= end_ts)]
    cash = initial_capital
    btc = 0.0
    invested = initial_capital
    last_month = working["datetime"].iloc[0].strftime("%Y-%m")
    fees = 0.0
    slippage = 0.0
    nav_rows = []
    for index, row in working.reset_index(drop=True).iterrows():
        dt = row["datetime"]
        price = float(row["close"])
        month = dt.strftime("%Y-%m")
        if index > 0 and month != last_month:
            cash += monthly_contribution
            invested += monthly_contribution
            last_month = month
        if index >= 1:
            scheduled_target = float(working["target"].iloc[index - 1])
            nav_before_trade = cash + btc * price
            current_value_exposure = (
                0.0 if nav_before_trade <= 0.0 else (btc * price) / nav_before_trade
            )
            if abs(scheduled_target - current_value_exposure) >= DEADBAND:
                target_btc_value = nav_before_trade * scheduled_target
                delta_value = target_btc_value - btc * price
                cost_base = abs(delta_value)
                fee = cost_base * cost_model.fee_per_side
                slip = cost_base * cost_model.slippage_per_side
                if delta_value > 0:
                    buy_value = max(0.0, delta_value - fee - slip)
                    cash -= delta_value
                    btc += buy_value / price
                else:
                    btc_to_sell = min(btc, abs(delta_value) / price)
                    proceeds = btc_to_sell * price - fee - slip
                    btc -= btc_to_sell
                    cash += proceeds
                fees += fee
                slippage += slip
        nav = cash + btc * price
        nav_rows.append({"datetime": dt, "nav": nav, "invested": invested})
    nav = pd.DataFrame(nav_rows)
    nav["tw_nav"] = nav["nav"] / nav["invested"]
    return {
        "name": config.name,
        "timeframe": config.timeframe,
        "final_value": float(nav["nav"].iloc[-1]),
        "invested_capital": float(nav["invested"].iloc[-1]),
        "return_on_contributions": float(nav["nav"].iloc[-1] / nav["invested"].iloc[-1] - 1.0),
        "time_weighted_nav_drawdown": float((nav["tw_nav"] / nav["tw_nav"].cummax() - 1.0).min()),
        "invested_capital_drawdown": float((nav["nav"] / nav["invested"] - 1.0).min()),
        "fee_drag": float((fees + slippage) / nav["invested"].iloc[-1]),
    }


def concentration_share(summary: dict[str, Any], top_n: int) -> float | None:
    for row in summary.get("concentration", []):
        if int(row["top_n"]) == top_n:
            value = row.get("share_of_total_net_return_sum")
            return None if value is None else float(value)
    return None


def bars_per_year(timeframe: str) -> float:
    if timeframe == "4h":
        return TRADING_DAYS_PER_YEAR * 6.0
    if timeframe == "1d":
        return TRADING_DAYS_PER_YEAR
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def bar_days(timeframe: str) -> float:
    if timeframe == "4h":
        return 4.0 / 24.0
    if timeframe == "1d":
        return 1.0
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def candidate_dict(config: StrategyConfig) -> dict[str, Any]:
    return {
        "name": config.name,
        "timeframe": config.timeframe,
        "sma_window": config.sma_window,
        "buffer": config.buffer,
        "entry_persistence": config.entry_persistence,
        "exit_persistence": config.exit_persistence,
    }


def write_outputs(
    report_dir: Path,
    *,
    manifest: dict[str, Any],
    entry_features: pd.DataFrame,
    pattern_summary: pd.DataFrame,
    exit_summary: pd.DataFrame,
    study_2025: pd.DataFrame,
    candidate_ranking: pd.DataFrame,
    selected: pd.DataFrame,
    comparison_base: pd.DataFrame,
    comparison_stress: pd.DataFrame,
    rolling: pd.DataFrame,
    expanding: pd.DataFrame,
    bootstrap: pd.DataFrame,
    best_trades: pd.DataFrame,
    best_trade_summary: dict[str, Any],
    baseline_trade_summary: dict[str, Any],
    practical_2025: pd.DataFrame,
    candidate_templates: list[DiscoveryCandidate],
) -> None:
    report_dir.mkdir(parents=True, exist_ok=True)
    artifacts = {
        "entry_feature_table.csv": entry_features,
        "pattern_summary.csv": pattern_summary,
        "exit_pattern_summary.csv": exit_summary,
        "baseline_2025_trade_features.csv": study_2025,
        "candidate_grid_base.csv": candidate_ranking,
        "selected_candidates_base.csv": selected,
        "comparison_base.csv": comparison_base,
        "comparison_stress.csv": comparison_stress,
        "rolling_robustness_base.csv": rolling,
        "expanding_robustness_base.csv": expanding,
        "block_bootstrap_base.csv": bootstrap,
        "best_candidate_trade_log_base.csv": best_trades,
        "practical_2025_simulation_base.csv": practical_2025,
    }
    for filename, frame in artifacts.items():
        frame.to_csv(report_dir / filename, index=False)
    manifest["artifacts"] = sorted([*artifacts, "manifest.json", "study.md"])
    (report_dir / "manifest.json").write_text(
        json.dumps(json_clean(manifest), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    (report_dir / "study.md").write_text(
        build_markdown_report(
            manifest=manifest,
            pattern_summary=pattern_summary,
            exit_summary=exit_summary,
            study_2025=study_2025,
            selected=selected,
            comparison_base=comparison_base,
            comparison_stress=comparison_stress,
            rolling=rolling,
            expanding=expanding,
            bootstrap=bootstrap,
            best_trade_summary=best_trade_summary,
            baseline_trade_summary=baseline_trade_summary,
            practical_2025=practical_2025,
            candidate_templates=candidate_templates,
        ),
        encoding="utf-8",
        newline="\n",
    )


def build_markdown_report(
    *,
    manifest: dict[str, Any],
    pattern_summary: pd.DataFrame,
    exit_summary: pd.DataFrame,
    study_2025: pd.DataFrame,
    selected: pd.DataFrame,
    comparison_base: pd.DataFrame,
    comparison_stress: pd.DataFrame,
    rolling: pd.DataFrame,
    expanding: pd.DataFrame,
    bootstrap: pd.DataFrame,
    best_trade_summary: dict[str, Any],
    baseline_trade_summary: dict[str, Any],
    practical_2025: pd.DataFrame,
    candidate_templates: list[DiscoveryCandidate],
) -> str:
    best_name = str(selected.iloc[0]["name"])
    verdict, verdict_reason = build_verdict(selected, comparison_base, practical_2025)
    lines = [
        "# 4H Deep Pattern Discovery & Strategy Improvement Study",
        "",
        "Research-only. No cambia la estrategia oficial 1D ni la capa operativa congelada.",
        "",
        "## Dataset y auditoria",
        "",
        (
            f"- Fuente principal: `{manifest['data']['4h']['dataset_path']}` "
            "derivado desde 1H Bitvavo BTC/EUR."
        ),
        (
            "- Agregacion 1H -> 4H: open primer open, high maximo, "
            "low minimo, close ultimo close, volume suma."
        ),
        "- Solo se conservan buckets completos de cuatro velas 1H.",
        f"- Alineacion UTC: {manifest['data']['4h']['bucket_alignment_utc']}.",
        (
            f"- 4H: {manifest['data']['4h']['start']} -> {manifest['data']['4h']['end']}, "
            f"{manifest['data']['4h']['row_count']} velas."
        ),
        (
            f"- Gaps 1H fuente: {manifest['data']['4h']['source_gap_count']} gaps, "
            f"{manifest['data']['4h']['source_total_missing_intervals']} intervalos faltantes."
        ),
        (
            f"- Gaps 4H derivados: {manifest['data']['4h']['gap_count']} gaps, "
            f"{manifest['data']['4h']['total_missing_intervals']} intervalos faltantes; "
            f"{manifest['data']['4h']['incomplete_4h_bins_dropped']} buckets "
            "incompletos descartados."
        ),
        (
            f"- Periodo comun de evaluacion: {manifest['common_period']['start']} "
            f"-> {manifest['common_period']['end']}."
        ),
        "",
        "## Patrones encontrados",
        "",
        pattern_read(pattern_summary),
        "",
        "### Evidencia cuantitativa principal",
        "",
        markdown_table(pattern_summary.head(18)),
        "",
        "## Salidas",
        "",
        markdown_table(exit_summary),
        "",
        "## 2025 especifico",
        "",
        study_2025_read(study_2025),
        "",
        markdown_table(study_2025),
        "",
        "## Candidatos construidos",
        "",
        candidate_read(candidate_templates),
        "",
        "## Ranking de candidatos",
        "",
        markdown_table(selected),
        "",
        "## Comparacion base",
        "",
        markdown_table(comparison_base),
        "",
        "## Comparacion stress",
        "",
        markdown_table(comparison_stress),
        "",
        "## Robustez temporal rolling",
        "",
        markdown_table(rolling),
        "",
        "## Robustez temporal expanding",
        "",
        markdown_table(expanding),
        "",
        "## Bootstrap block",
        "",
        markdown_table(bootstrap),
        "",
        "## Trade log analysis mejor candidato",
        "",
        f"Mejor candidato nuevo: `{best_name}`.",
        "",
        summary_lines(best_trade_summary),
        "",
        "### Concentracion vs 4H previa",
        "",
        concentration_read(best_trade_summary, baseline_trade_summary),
        "",
        "## Simulacion practica 2025",
        "",
        markdown_table(practical_2025),
        "",
        practical_2025_read(practical_2025),
        "",
        "## Veredicto final",
        "",
        f"**{verdict}**",
        "",
        verdict_reason,
        "",
    ]
    return "\n".join(lines)


def pattern_read(pattern_summary: pd.DataFrame) -> str:
    if pattern_summary.empty:
        return "- No hay suficientes trades cerrados para extraer patrones."
    top = pattern_summary.head(8)
    lines = []
    for row in top.itertuples(index=False):
        lines.append(
            f"- `{row.feature}` / {row.bucket}: {int(row.trade_count)} trades, "
            f"win rate {fmt(row.win_rate)}, media neta {fmt(row.mean_net_return)}, "
            f"edge vs universo {fmt(row.edge_vs_all)}."
        )
    return "\n".join(lines)


def study_2025_read(study_2025: pd.DataFrame) -> str:
    if study_2025.empty:
        return "- La 4H previa no tuvo trades cerrados con entrada ejecutada en 2025."
    returns = study_2025["net_return"].dropna()
    weak = int((returns <= 0.0).sum())
    positive = int((returns > 0.0).sum())
    avg_slope = float(study_2025["sma750_slope30_atr"].mean())
    avg_attempts = float(study_2025["failed_attempts_90"].mean())
    return "\n".join(
        [
            (
                f"- Trades 2025 en 4H previa: {len(returns)} cerrados; "
                f"{positive} positivos y {weak} no positivos."
            ),
            (
                f"- Retorno medio por trade 2025: {fmt(float(returns.mean()))}; "
                f"mediana {fmt(float(returns.median()))}."
            ),
            (
                f"- Slope SMA750 medio en entradas 2025: {avg_slope:.3f} ATR; "
                f"intentos previos 90 velas medio: {avg_attempts:.2f}."
            ),
            (
                "- Lectura: 2025 se evalua como chop si aparecen entradas cerca de "
                "threshold sin ruptura de maximos ni pendiente clara."
            ),
        ]
    )


def candidate_read(candidates: list[DiscoveryCandidate]) -> str:
    lines = []
    for candidate in candidates:
        lines.append(
            f"- `{candidate.name}`: {candidate.logic} Nace de: {candidate.pattern_source}"
        )
    return "\n".join(lines)


def summary_lines(summary: dict[str, Any]) -> str:
    keys = [
        "complete_trades",
        "trades_per_year",
        "win_rate",
        "loss_rate",
        "profit_factor",
        "expectancy_per_trade",
        "mean_return_per_trade",
        "median_return_per_trade",
        "std_return_per_trade",
        "winner_mean_holding_days",
        "loser_mean_holding_days",
        "longest_winning_streak",
        "longest_losing_streak",
    ]
    lines = []
    for key in keys:
        lines.append(f"- {key}: {fmt(summary.get(key))}")
    for label in ("best_trade", "worst_trade"):
        trade = summary.get(label, {})
        if trade:
            lines.append(
                f"- {label}: trade {trade.get('trade_id')}, "
                f"net_return {fmt(trade.get('net_return'))}, "
                f"holding_days {fmt(trade.get('holding_days'))}."
            )
    for row in summary.get("concentration", []):
        lines.append(
            f"- top_{row['top_n']}_concentration: "
            f"net_sum {fmt(row['net_return_sum'])}, "
            f"share {fmt(row['share_of_total_net_return_sum'])}."
        )
    return "\n".join(lines)


def concentration_read(best: dict[str, Any], baseline: dict[str, Any]) -> str:
    best_top3 = concentration_share(best, 3)
    baseline_top3 = concentration_share(baseline, 3)
    best_top1 = concentration_share(best, 1)
    baseline_top1 = concentration_share(baseline, 1)
    improved = (
        best_top3 is not None
        and baseline_top3 is not None
        and best_top3 < baseline_top3
        and best_top1 is not None
        and baseline_top1 is not None
        and best_top1 < baseline_top1
    )
    answer = "si" if improved else "no"
    return (
        f"- Dependencia extrema reducida: {answer}. "
        f"Top 1 nuevo {fmt(best_top1)} vs previo {fmt(baseline_top1)}; "
        f"top 3 nuevo {fmt(best_top3)} vs previo {fmt(baseline_top3)}."
    )


def practical_2025_read(practical_2025: pd.DataFrame) -> str:
    indexed = practical_2025.set_index("name")
    new_name = practical_2025.iloc[0]["name"]
    new_return = float(indexed.loc[new_name]["return_on_contributions"])
    previous_return = float(indexed.loc[BASELINE_4H_PREVIOUS.name]["return_on_contributions"])
    if new_return > previous_return:
        conclusion = "mejora 2025 frente a la 4H previa"
    else:
        conclusion = "no mejora 2025 frente a la 4H previa"
    return f"- El candidato nuevo {conclusion}: {fmt(new_return)} vs {fmt(previous_return)}."


def practical_2025_return(practical_2025: pd.DataFrame, name_contains: str) -> float:
    row = practical_2025[practical_2025["name"].str.contains(name_contains, regex=False)].iloc[0]
    return float(row["return_on_contributions"])


def build_verdict(
    selected: pd.DataFrame,
    comparison_base: pd.DataFrame,
    practical_2025: pd.DataFrame,
) -> tuple[str, str]:
    best = selected.iloc[0]
    metrics = comparison_base.set_index("name")
    previous = metrics.loc[BASELINE_4H_PREVIOUS.name]
    best_metrics = metrics.loc[str(best["name"])]
    cagr_delta = float(best_metrics["cagr"] - previous["cagr"])
    calmar_delta = float(best_metrics["calmar"] - previous["calmar"])
    new_2025 = float(practical_2025.iloc[0]["return_on_contributions"])
    old_2025 = practical_2025_return(practical_2025, BASELINE_4H_PREVIOUS.name)
    top3 = best.get("top_3_concentration")
    material = cagr_delta > 0.015 and calmar_delta > 0.10 and new_2025 > old_2025
    partial = cagr_delta > 0.0 or calmar_delta > 0.0 or new_2025 > old_2025
    if material and (pd.isna(top3) or float(top3) < 1.0):
        return (
            "Encontramos una mejora 4H material y defendible",
            (
                "Razon principal: mejora Calmar/CAGR y tambien la simulacion "
                "practica 2025 sin aumentar la concentracion extrema."
            ),
        )
    if partial:
        return (
            "Encontramos mejoras parciales pero no suficientes",
            (
                "Razon principal: algun eje mejora, pero no simultaneamente "
                "robustez, 2025 y dependencia de pocos trades."
            ),
        )
    return (
        "No encontramos una mejora real sobre la 4H corregida previa",
        (
            "Razon principal: los filtros derivados de patrones no superan a la "
            "4H previa en el conjunto operativo relevante."
        ),
    )


def markdown_table(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "_Sin filas._"
    display = frame.copy()
    for column in display.columns:
        if pd.api.types.is_float_dtype(display[column]):
            display[column] = display[column].map(fmt)
    headers = [escape_markdown_cell(str(column)) for column in display.columns]
    rows = [[escape_markdown_cell(str(value)) for value in row] for row in display.to_numpy()]
    table_rows = [headers, ["---"] * len(headers), *rows]
    return "\n".join("| " + " | ".join(row) + " |" for row in table_rows)


def fmt(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        if np.isnan(value):
            return ""
        return f"{value:.6f}"
    if isinstance(value, np.floating):
        if np.isnan(float(value)):
            return ""
        return f"{float(value):.6f}"
    return str(value)


def escape_markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")


def json_clean(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: json_clean(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_clean(item) for item in value]
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        value = float(value)
    if isinstance(value, float):
        if np.isnan(value):
            return None
        if np.isinf(value):
            return "inf" if value > 0 else "-inf"
    return value


if __name__ == "__main__":
    raise SystemExit(main())
