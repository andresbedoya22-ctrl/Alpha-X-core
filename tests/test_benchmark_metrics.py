from __future__ import annotations

import pandas as pd
import pytest

from alpha_x.benchmarks import BenchmarkResult
from alpha_x.benchmarks.metrics import (
    calculate_annualized_return,
    calculate_benchmark_metrics,
    calculate_max_drawdown,
)


def test_calculate_max_drawdown_returns_peak_to_trough_loss() -> None:
    equity = pd.Series([100.0, 120.0, 90.0, 150.0], dtype="float64")
    assert calculate_max_drawdown(equity) == -0.25


def test_calculate_annualized_return_returns_none_for_short_history() -> None:
    assert calculate_annualized_return(0.1, 0, 30 * 86_400_000) is None


def test_calculate_benchmark_metrics_uses_capital_base() -> None:
    result = BenchmarkResult(
        name="Example",
        equity_curve=pd.DataFrame(
            {
                "timestamp": [0, 366 * 86_400_000],
                "equity": [100.0, 120.0],
            }
        ),
        metadata={
            "benchmark_id": "buy_and_hold",
            "capital_base": 100.0,
            "trades": 1,
            "exposure": 1.0,
        },
    )

    metrics = calculate_benchmark_metrics(result)

    assert metrics.total_return == pytest.approx(0.2)
    assert metrics.annualized_return is not None
