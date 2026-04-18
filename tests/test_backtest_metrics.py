from __future__ import annotations

import pandas as pd
import pytest

from alpha_x.backtest.engine import run_long_flat_backtest
from alpha_x.backtest.metrics import calculate_backtest_metrics


def test_calculate_backtest_metrics_returns_base_fields() -> None:
    timestamps = [index * 60_000 for index in range(4)]
    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "datetime": pd.to_datetime(timestamps, unit="ms", utc=True),
            "close": [100.0, 110.0, 121.0, 121.0],
            "signal": [1, 1, 0, 0],
        }
    )

    result = run_long_flat_backtest(
        frame,
        initial_capital=100.0,
        fee_rate=0.0,
        slippage_rate=0.0,
        name="Example",
    )

    metrics = calculate_backtest_metrics(result)

    assert metrics.name == "Example"
    assert metrics.total_return == pytest.approx(0.1)
    assert metrics.annualized_return is None
    assert metrics.max_drawdown == pytest.approx(0.0)
    assert metrics.profit_factor is None
    assert metrics.trades == 1
    assert metrics.exposure == pytest.approx(0.5)
    assert metrics.final_equity == pytest.approx(110.0)
