from __future__ import annotations

import pandas as pd

from alpha_x.benchmarks import BenchmarkResult


def run_buy_and_hold(
    frame: pd.DataFrame,
    fee_rate: float,
    initial_capital: float,
) -> BenchmarkResult:
    if frame.empty:
        raise ValueError("Buy & Hold requires a non-empty dataset.")

    entry_price = float(frame["close"].iloc[0])
    units = (initial_capital * (1.0 - fee_rate)) / entry_price

    equity_curve = frame.loc[:, ["timestamp", "datetime", "close"]].copy()
    equity_curve["position"] = 1.0
    equity_curve["equity"] = equity_curve["close"] * units * (1.0 - fee_rate)

    return BenchmarkResult(
        name="Benchmark A - Buy & Hold BTC/EUR",
        equity_curve=equity_curve,
        metadata={
            "benchmark_id": "buy_and_hold",
            "initial_capital": initial_capital,
            "capital_base": initial_capital,
            "entry_price": entry_price,
            "fee_rate": fee_rate,
            "trades": 1,
            "exposure": 1.0,
        },
    )
