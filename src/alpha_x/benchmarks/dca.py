from __future__ import annotations

import pandas as pd

from alpha_x.benchmarks import BenchmarkResult


def run_monthly_dca(frame: pd.DataFrame, fee_rate: float, contribution: float) -> BenchmarkResult:
    if frame.empty:
        raise ValueError("Monthly DCA requires a non-empty dataset.")
    if contribution <= 0:
        raise ValueError("Monthly DCA contribution must be positive.")

    equity_curve = frame.loc[:, ["timestamp", "datetime", "close"]].copy()
    month_keys = equity_curve["datetime"].dt.strftime("%Y-%m")
    contribution_mask = ~month_keys.duplicated()

    units = 0.0
    total_contributed = 0.0
    invested_flags: list[float] = []
    contribution_flags: list[int] = []
    equity_values: list[float] = []

    for should_contribute, close in zip(contribution_mask, equity_curve["close"], strict=True):
        if bool(should_contribute):
            units += (contribution * (1.0 - fee_rate)) / float(close)
            total_contributed += contribution
            contribution_flags.append(1)
        else:
            contribution_flags.append(0)

        invested_flags.append(1.0 if units > 0 else 0.0)
        equity_values.append(units * float(close) * (1.0 - fee_rate))

    equity_curve["contribution_event"] = contribution_flags
    equity_curve["position"] = invested_flags
    equity_curve["equity"] = equity_values
    contribution_count = contribution_mask.astype("int64").cumsum()
    equity_curve["cumulative_contribution"] = contribution_count * contribution

    exposure = float(pd.Series(invested_flags, dtype="float64").mean())
    trade_count = int(sum(contribution_flags))

    return BenchmarkResult(
        name="Benchmark B - Monthly DCA BTC/EUR",
        equity_curve=equity_curve,
        metadata={
            "benchmark_id": "monthly_dca",
            "contribution": contribution,
            "capital_base": total_contributed,
            "fee_rate": fee_rate,
            "trades": trade_count,
            "exposure": exposure,
            "cash_flow_strategy": True,
        },
    )
