from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from alpha_x.benchmarks import BenchmarkResult


@dataclass(frozen=True)
class BenchmarkMetrics:
    name: str
    benchmark_id: str
    total_return: float
    annualized_return: float | None
    max_drawdown: float
    trades: int | None
    exposure: float | None


def calculate_max_drawdown(equity_curve: pd.Series) -> float:
    running_peak = equity_curve.cummax()
    drawdown = (equity_curve / running_peak) - 1.0
    return float(drawdown.min())


def calculate_total_return(final_equity: float, capital_base: float) -> float:
    if capital_base <= 0:
        raise ValueError("Capital base must be positive.")
    return (final_equity / capital_base) - 1.0


def calculate_annualized_return(
    total_return: float,
    start_timestamp: int,
    end_timestamp: int,
    *,
    minimum_days: float = 365.0,
) -> float | None:
    duration_days = (end_timestamp - start_timestamp) / 86_400_000
    if duration_days < minimum_days:
        return None

    years = duration_days / 365.25
    if years <= 0:
        return None

    return ((1.0 + total_return) ** (1.0 / years)) - 1.0


def calculate_benchmark_metrics(result: BenchmarkResult) -> BenchmarkMetrics:
    curve = result.equity_curve
    capital_base = float(result.metadata["capital_base"])
    total_return = calculate_total_return(float(curve["equity"].iloc[-1]), capital_base)
    benchmark_id = str(result.metadata["benchmark_id"])

    annualized_return: float | None
    if result.metadata.get("cash_flow_strategy"):
        annualized_return = None
    else:
        annualized_return = calculate_annualized_return(
            total_return,
            int(curve["timestamp"].iloc[0]),
            int(curve["timestamp"].iloc[-1]),
        )

    return BenchmarkMetrics(
        name=result.name,
        benchmark_id=benchmark_id,
        total_return=total_return,
        annualized_return=annualized_return,
        max_drawdown=calculate_max_drawdown(curve["equity"]),
        trades=int(result.metadata["trades"]) if "trades" in result.metadata else None,
        exposure=float(result.metadata["exposure"]) if "exposure" in result.metadata else None,
    )
