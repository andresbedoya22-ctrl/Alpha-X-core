from __future__ import annotations

import pandas as pd
import pytest

from alpha_x.backtest.engine import run_long_flat_backtest
from alpha_x.strategies.base import StrategyDefinition
from alpha_x.strategies.catalog import get_strategy_catalog


def _build_ohlcv_frame(length: int = 260) -> pd.DataFrame:
    timestamps = [index * 3_600_000 for index in range(length)]
    close = []
    for index in range(length):
        trend = 100.0 + (index * 0.25)
        seasonal = ((index % 12) - 6) * 0.4
        shock = -8.0 if index in {70, 150, 220} else 0.0
        breakout = 6.0 if index in {120, 200} else 0.0
        close.append(trend + seasonal + shock + breakout)

    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "datetime": pd.to_datetime(timestamps, unit="ms", utc=True),
            "open": close,
            "high": [value + 1.0 for value in close],
            "low": [value - 1.0 for value in close],
            "close": close,
            "volume": [10.0 + (index % 5) for index in range(length)],
        }
    )


@pytest.mark.parametrize("strategy", get_strategy_catalog(), ids=lambda item: item.strategy_id)
def test_each_strategy_generates_binary_signal(strategy: StrategyDefinition) -> None:
    frame = _build_ohlcv_frame()

    signal_frame = strategy.build_signal(frame)

    assert {"timestamp", "datetime", "close", "signal"}.issubset(signal_frame.columns)
    assert set(signal_frame["signal"].unique().tolist()).issubset({0, 1})
    assert signal_frame["signal"].dtype == "int64"


@pytest.mark.parametrize("strategy", get_strategy_catalog(), ids=lambda item: item.strategy_id)
def test_each_strategy_avoids_basic_lookahead(strategy: StrategyDefinition) -> None:
    frame = _build_ohlcv_frame()
    mutated = frame.copy()
    mutated.loc[mutated.index[-1], "close"] = mutated["close"].iloc[-1] * 4.0

    base_signal = strategy.build_signal(frame)
    mutated_signal = strategy.build_signal(mutated)

    pd.testing.assert_series_equal(
        base_signal["signal"].iloc[:-1],
        mutated_signal["signal"].iloc[:-1],
        check_names=False,
    )


@pytest.mark.parametrize("strategy", get_strategy_catalog(), ids=lambda item: item.strategy_id)
def test_each_strategy_is_backtester_compatible(strategy: StrategyDefinition) -> None:
    frame = _build_ohlcv_frame()
    signal_frame = strategy.build_signal(frame)

    result = run_long_flat_backtest(
        signal_frame,
        initial_capital=10_000.0,
        fee_rate=0.001,
        slippage_rate=0.0005,
        name=strategy.name,
    )

    assert result.name == strategy.name
    assert len(result.equity_curve) == len(frame)
    assert (
        result.metadata["execution_rule"]
        == "Signal observed on close[t], executed on close[t+1]."
    )
