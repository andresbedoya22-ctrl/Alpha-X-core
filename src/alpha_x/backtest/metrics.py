from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from alpha_x.backtest.models import BacktestResult
from alpha_x.benchmarks import BenchmarkResult
from alpha_x.benchmarks.metrics import (
    calculate_annualized_return,
    calculate_benchmark_metrics,
    calculate_max_drawdown,
)


@dataclass(frozen=True)
class PerformanceRow:
    name: str
    source_id: str
    total_return: float
    annualized_return: float | None
    max_drawdown: float
    profit_factor: float | None
    trades: int | None
    exposure: float | None
    final_equity: float


def calculate_profit_factor(trades: pd.DataFrame) -> float | None:
    if trades.empty or "net_pnl" not in trades.columns:
        return None

    gross_profit = float(trades.loc[trades["net_pnl"] > 0, "net_pnl"].sum())
    gross_loss = float(-trades.loc[trades["net_pnl"] < 0, "net_pnl"].sum())
    if gross_profit <= 0 or gross_loss <= 0:
        return None
    return gross_profit / gross_loss


def calculate_backtest_metrics(result: BacktestResult) -> PerformanceRow:
    curve = result.equity_curve
    capital_base = float(result.metadata["capital_base"])
    final_equity = float(curve["equity"].iloc[-1])
    total_return = (final_equity / capital_base) - 1.0

    return PerformanceRow(
        name=result.name,
        source_id="backtest",
        total_return=total_return,
        annualized_return=calculate_annualized_return(
            total_return,
            int(curve["timestamp"].iloc[0]),
            int(curve["timestamp"].iloc[-1]),
        ),
        max_drawdown=calculate_max_drawdown(curve["equity"]),
        profit_factor=calculate_profit_factor(result.trades),
        trades=int(result.metadata["trade_count"]),
        exposure=float(result.metadata["exposure"]),
        final_equity=final_equity,
    )


def benchmark_result_to_performance_row(
    result: BenchmarkResult,
    *,
    source_id: str | None = None,
) -> PerformanceRow:
    metrics = calculate_benchmark_metrics(result)
    final_equity = float(result.equity_curve["equity"].iloc[-1])
    return PerformanceRow(
        name=metrics.name,
        source_id=source_id or metrics.benchmark_id,
        total_return=metrics.total_return,
        annualized_return=metrics.annualized_return,
        max_drawdown=metrics.max_drawdown,
        profit_factor=None,
        trades=metrics.trades,
        exposure=metrics.exposure,
        final_equity=final_equity,
    )
