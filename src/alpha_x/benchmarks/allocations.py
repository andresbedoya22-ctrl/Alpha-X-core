from __future__ import annotations

import numpy as np
import pandas as pd

from alpha_x.benchmarks import BenchmarkResult

ONE_QUARTER = 63


def run_equal_weight_basket(
    close_frame: pd.DataFrame,
    *,
    fee_rate: float,
    slippage_rate: float,
    initial_capital: float,
    rebalance_interval_days: int = ONE_QUARTER,
    name: str = "Benchmark D - Equal-Weight Basket (Quarterly)",
) -> BenchmarkResult:
    if close_frame.empty:
        raise ValueError("Equal-weight basket requires a non-empty close frame.")
    asset_columns = [
        column for column in close_frame.columns if column not in {"timestamp", "datetime"}
    ]
    if not asset_columns:
        raise ValueError("Equal-weight basket requires at least one asset column.")

    weights = pd.DataFrame(0.0, index=close_frame.index, columns=asset_columns)
    target = {asset: 1.0 / len(asset_columns) for asset in asset_columns}
    for index in range(0, len(close_frame), rebalance_interval_days):
        weights.loc[index, asset_columns] = pd.Series(target)
    positions = weights.replace(0.0, np.nan).ffill().fillna(0.0)
    return _simulate_weighted_benchmark(
        close_frame=close_frame,
        positions=positions,
        fee_rate=fee_rate,
        slippage_rate=slippage_rate,
        initial_capital=initial_capital,
        benchmark_id="equal_weight_quarterly",
        name=name,
        rebalance_interval_days=rebalance_interval_days,
    )


def run_fixed_mix_benchmark(
    close_frame: pd.DataFrame,
    *,
    allocations: dict[str, float],
    fee_rate: float,
    slippage_rate: float,
    initial_capital: float,
    rebalance_interval_days: int = ONE_QUARTER,
    name: str = "Benchmark E - BTC/ETH 60/40 (Quarterly)",
) -> BenchmarkResult:
    if close_frame.empty:
        raise ValueError("Fixed mix benchmark requires a non-empty close frame.")
    total_weight = sum(allocations.values())
    if total_weight <= 0:
        raise ValueError("Fixed mix benchmark requires positive allocations.")

    normalized = {asset: weight / total_weight for asset, weight in allocations.items()}
    missing_assets = [asset for asset in normalized if asset not in close_frame.columns]
    if missing_assets:
        raise ValueError(f"Missing assets for fixed mix benchmark: {missing_assets}")

    asset_columns = [
        column for column in close_frame.columns if column not in {"timestamp", "datetime"}
    ]
    weights = pd.DataFrame(0.0, index=close_frame.index, columns=asset_columns)
    for index in range(0, len(close_frame), rebalance_interval_days):
        for asset, weight in normalized.items():
            weights.at[index, asset] = weight
    positions = weights.replace(0.0, np.nan).ffill().fillna(0.0)
    return _simulate_weighted_benchmark(
        close_frame=close_frame,
        positions=positions,
        fee_rate=fee_rate,
        slippage_rate=slippage_rate,
        initial_capital=initial_capital,
        benchmark_id="btc_eth_60_40_quarterly",
        name=name,
        rebalance_interval_days=rebalance_interval_days,
    )


def _simulate_weighted_benchmark(
    *,
    close_frame: pd.DataFrame,
    positions: pd.DataFrame,
    fee_rate: float,
    slippage_rate: float,
    initial_capital: float,
    benchmark_id: str,
    name: str,
    rebalance_interval_days: int = ONE_QUARTER,
) -> BenchmarkResult:
    returns = close_frame.drop(columns=["timestamp", "datetime"]).pct_change().fillna(0.0)
    active_positions = positions.shift(1).fillna(0.0)
    turnover = active_positions.diff().abs().sum(axis=1).fillna(active_positions.abs().sum(axis=1))
    one_way_cost = fee_rate + slippage_rate
    bar_return = (active_positions * returns).sum(axis=1) - (turnover * one_way_cost)
    equity = initial_capital * (1.0 + bar_return).cumprod()

    equity_curve = close_frame.loc[:, ["timestamp", "datetime"]].copy()
    equity_curve["position"] = active_positions.sum(axis=1)
    equity_curve["equity"] = equity
    equity_curve["turnover"] = turnover
    equity_curve["trade_fee"] = turnover * one_way_cost * equity.shift(1).fillna(initial_capital)
    equity_curve["bar_return"] = bar_return

    return BenchmarkResult(
        name=name,
        equity_curve=equity_curve,
        metadata={
            "benchmark_id": benchmark_id,
            "initial_capital": initial_capital,
            "capital_base": initial_capital,
            "fee_rate": fee_rate,
            "slippage_rate": slippage_rate,
            "trades": int((turnover > 0).sum()),
            "exposure": float(active_positions.sum(axis=1).mean()),
            "rebalance_interval_days": rebalance_interval_days,
        },
    )
