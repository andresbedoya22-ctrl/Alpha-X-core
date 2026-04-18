from __future__ import annotations

import pandas as pd

from alpha_x.benchmarks import BenchmarkResult


def run_sma_baseline(
    frame: pd.DataFrame,
    fee_rate: float,
    initial_capital: float,
    fast_window: int,
    slow_window: int,
) -> BenchmarkResult:
    if frame.empty:
        raise ValueError("SMA baseline requires a non-empty dataset.")
    if fast_window <= 0 or slow_window <= 0:
        raise ValueError("SMA windows must be positive.")
    if fast_window >= slow_window:
        raise ValueError("SMA fast window must be smaller than slow window.")

    equity_curve = frame.loc[:, ["timestamp", "datetime", "close"]].copy()
    fast_roll = equity_curve["close"].rolling(window=fast_window, min_periods=fast_window)
    slow_roll = equity_curve["close"].rolling(window=slow_window, min_periods=slow_window)
    equity_curve["sma_fast"] = fast_roll.mean()
    equity_curve["sma_slow"] = slow_roll.mean()
    equity_curve["signal"] = (
        (equity_curve["sma_fast"] > equity_curve["sma_slow"])
        & equity_curve["sma_fast"].notna()
        & equity_curve["sma_slow"].notna()
    ).astype("float64")

    cash = initial_capital
    units = 0.0
    trades = 0
    positions: list[float] = []
    equities: list[float] = []

    previous_signal = 0.0
    for signal, close in zip(equity_curve["signal"], equity_curve["close"], strict=True):
        price = float(close)
        signal_value = float(signal)

        if signal_value != previous_signal:
            if signal_value == 1.0:
                units = (cash * (1.0 - fee_rate)) / price
                cash = 0.0
                trades += 1
            else:
                cash = units * price * (1.0 - fee_rate)
                units = 0.0
                trades += 1

        previous_signal = signal_value
        positions.append(signal_value)
        equities.append(cash + (units * price * (1.0 - fee_rate)))

    equity_curve["position"] = positions
    equity_curve["equity"] = equities

    return BenchmarkResult(
        name="Benchmark C - SMA Crossover Baseline",
        equity_curve=equity_curve,
        metadata={
            "benchmark_id": "sma_crossover",
            "initial_capital": initial_capital,
            "capital_base": initial_capital,
            "fee_rate": fee_rate,
            "fast_window": fast_window,
            "slow_window": slow_window,
            "trades": trades,
            "exposure": float(pd.Series(positions, dtype="float64").mean()),
            "baseline_label": f"SMA close crossover ({fast_window}/{slow_window})",
        },
    )
