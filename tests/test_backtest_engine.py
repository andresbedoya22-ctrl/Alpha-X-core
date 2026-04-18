from __future__ import annotations

import pandas as pd
import pytest

from alpha_x.backtest.engine import run_long_flat_backtest


def _build_frame(closes: list[float], signals: list[int]) -> pd.DataFrame:
    timestamps = [index * 60_000 for index in range(len(closes))]
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "datetime": pd.to_datetime(timestamps, unit="ms", utc=True),
            "close": closes,
            "signal": signals,
        }
    )


def test_run_long_flat_backtest_executes_long_flat_with_one_bar_lag() -> None:
    frame = _build_frame([100.0, 110.0, 121.0, 121.0], [1, 1, 0, 0])

    result = run_long_flat_backtest(
        frame,
        initial_capital=100.0,
        fee_rate=0.0,
        slippage_rate=0.0,
    )

    assert list(result.equity_curve["position"]) == [0, 1, 1, 0]
    assert result.metadata["trade_count"] == 1
    assert result.equity_curve["equity"].iloc[-1] == pytest.approx(110.0)


def test_run_long_flat_backtest_applies_fees() -> None:
    frame = _build_frame([100.0, 100.0, 100.0, 100.0], [1, 1, 0, 0])

    result = run_long_flat_backtest(
        frame,
        initial_capital=100.0,
        fee_rate=0.1,
        slippage_rate=0.0,
    )

    assert result.equity_curve["equity"].iloc[-1] == pytest.approx(81.81818181818181)
    assert result.trades["entry_fee"].iloc[0] == pytest.approx(9.090909090909093)
    assert result.trades["exit_fee"].iloc[0] == pytest.approx(9.090909090909092)


def test_run_long_flat_backtest_applies_slippage() -> None:
    frame = _build_frame([100.0, 100.0, 100.0, 100.0], [1, 1, 0, 0])

    result = run_long_flat_backtest(
        frame,
        initial_capital=100.0,
        fee_rate=0.0,
        slippage_rate=0.1,
    )

    assert result.equity_curve["equity"].iloc[-1] == pytest.approx(81.81818181818181)


def test_run_long_flat_backtest_avoids_basic_lookahead() -> None:
    frame = _build_frame([100.0, 120.0, 120.0], [1, 0, 0])

    result = run_long_flat_backtest(
        frame,
        initial_capital=100.0,
        fee_rate=0.0,
        slippage_rate=0.0,
    )

    assert list(result.equity_curve["position"]) == [0, 1, 0]
    assert result.equity_curve["equity"].iloc[-1] == pytest.approx(100.0)
