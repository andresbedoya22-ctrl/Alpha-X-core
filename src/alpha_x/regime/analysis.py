from __future__ import annotations

import pandas as pd

from alpha_x.backtest.engine import run_long_flat_backtest
from alpha_x.benchmarks.sma_baseline import run_sma_baseline
from alpha_x.config.settings import get_settings
from alpha_x.regime.base import RegimeDetectionResult
from alpha_x.strategies.catalog import get_strategy_catalog


def build_regime_summary(result: RegimeDetectionResult) -> pd.DataFrame:
    valid = result.frame.loc[result.frame["regime_is_valid"]].copy()
    if valid.empty:
        return pd.DataFrame(
            columns=["regime", "rows", "pct", "start_timestamp", "end_timestamp"]
        )

    summary = (
        valid.groupby("regime", dropna=False)
        .agg(
            rows=("regime", "size"),
            start_timestamp=("timestamp", "min"),
            end_timestamp=("timestamp", "max"),
        )
        .reset_index()
    )
    summary["pct"] = summary["rows"] / len(valid)
    return summary.sort_values(["rows", "regime"], ascending=[False, True]).reset_index(drop=True)


def build_regime_component_summary(result: RegimeDetectionResult) -> pd.DataFrame:
    valid = result.frame.loc[result.frame["regime_is_valid"]].copy()
    if valid.empty:
        return pd.DataFrame(columns=["component", "value", "rows", "pct"])

    frames = []
    for column in ("trend_state", "volatility_state", "compression_state"):
        summary = (
            valid.groupby(column, dropna=False)
            .size()
            .rename("rows")
            .reset_index()
            .rename(columns={column: "value"})
        )
        summary.insert(0, "component", column)
        summary["pct"] = summary["rows"] / len(valid)
        frames.append(summary)
    return pd.concat(frames, ignore_index=True)


def build_regime_label_table(frame: pd.DataFrame) -> pd.DataFrame:
    valid = frame.loc[frame["regime_is_valid"] & frame["tb_is_valid"]].copy()
    if valid.empty:
        return pd.DataFrame(
            columns=["regime", "label", "rows", "label_pct_within_regime", "avg_event_return"]
        )

    summary = (
        valid.groupby(["regime", "tb_label"], dropna=False)
        .agg(
            rows=("tb_label", "size"),
            avg_event_return=("tb_event_return", "mean"),
        )
        .reset_index()
        .rename(columns={"tb_label": "label"})
    )
    regime_totals = summary.groupby("regime")["rows"].transform("sum")
    summary["label_pct_within_regime"] = summary["rows"] / regime_totals
    return summary.sort_values(["regime", "label"]).reset_index(drop=True)


def build_regime_strategy_table(
    regime_frame: pd.DataFrame,
    dataset_frame: pd.DataFrame,
    *,
    fee_rate: float,
    slippage_rate: float,
    initial_capital: float,
) -> pd.DataFrame:
    strategy_tables = [
        _build_hypothesis_5_table(
            regime_frame,
            dataset_frame,
            fee_rate=fee_rate,
            slippage_rate=slippage_rate,
            initial_capital=initial_capital,
        ),
        _build_sma_baseline_table(
            regime_frame,
            dataset_frame,
            fee_rate=fee_rate,
            initial_capital=initial_capital,
        ),
    ]
    return pd.concat(strategy_tables, ignore_index=True)


def _build_hypothesis_5_table(
    regime_frame: pd.DataFrame,
    dataset_frame: pd.DataFrame,
    *,
    fee_rate: float,
    slippage_rate: float,
    initial_capital: float,
) -> pd.DataFrame:
    strategy = next(
        item for item in get_strategy_catalog() if item.strategy_id == "trend_volatility_filter"
    )
    signal_frame = strategy.build_signal(dataset_frame)
    backtest = run_long_flat_backtest(
        signal_frame,
        initial_capital=initial_capital,
        fee_rate=fee_rate,
        slippage_rate=slippage_rate,
        name=strategy.name,
    )
    return _summarize_strategy_equity_curve(
        strategy_id=strategy.strategy_id,
        strategy_name=strategy.name,
        regime_frame=regime_frame,
        equity_curve=backtest.equity_curve,
    )


def _build_sma_baseline_table(
    regime_frame: pd.DataFrame,
    dataset_frame: pd.DataFrame,
    *,
    fee_rate: float,
    initial_capital: float,
) -> pd.DataFrame:
    settings = get_settings()
    benchmark = run_sma_baseline(
        dataset_frame,
        fee_rate=fee_rate,
        initial_capital=initial_capital,
        fast_window=settings.benchmark_sma_fast,
        slow_window=settings.benchmark_sma_slow,
    )
    return _summarize_strategy_equity_curve(
        strategy_id="sma_crossover_baseline",
        strategy_name=benchmark.name,
        regime_frame=regime_frame,
        equity_curve=benchmark.equity_curve,
    )


def _summarize_strategy_equity_curve(
    *,
    strategy_id: str,
    strategy_name: str,
    regime_frame: pd.DataFrame,
    equity_curve: pd.DataFrame,
) -> pd.DataFrame:
    prepared_curve = equity_curve.copy()
    if "bar_return" not in prepared_curve.columns:
        prepared_curve["bar_return"] = prepared_curve["equity"].pct_change().fillna(0.0)
    if "trade_fee" not in prepared_curve.columns:
        prepared_curve["trade_fee"] = 0.0

    merged = regime_frame.loc[:, ["timestamp", "regime", "regime_is_valid"]].merge(
        prepared_curve.loc[:, ["timestamp", "signal", "position", "bar_return", "trade_fee"]],
        on="timestamp",
        how="left",
    )
    valid = merged.loc[merged["regime_is_valid"]].copy()
    if valid.empty:
        return pd.DataFrame(
            columns=[
                "strategy_id",
                "strategy_name",
                "regime",
                "rows",
                "signal_on_rate",
                "position_on_rate",
                "mean_bar_return",
                "cumulative_bar_return",
                "trade_fee_sum",
            ]
        )

    summary = (
        valid.groupby("regime", dropna=False)
        .agg(
            rows=("regime", "size"),
            signal_on_rate=("signal", "mean"),
            position_on_rate=("position", "mean"),
            mean_bar_return=("bar_return", "mean"),
            trade_fee_sum=("trade_fee", "sum"),
        )
        .reset_index()
    )
    cumulative = (
        valid.groupby("regime", dropna=False)["bar_return"]
        .apply(lambda series: float((1.0 + series).prod() - 1.0))
        .reset_index(name="cumulative_bar_return")
    )
    summary = summary.merge(cumulative, on="regime", how="left")
    summary.insert(0, "strategy_name", strategy_name)
    summary.insert(0, "strategy_id", strategy_id)
    return summary.sort_values("regime").reset_index(drop=True)
