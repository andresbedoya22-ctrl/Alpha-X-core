from __future__ import annotations

import pandas as pd

from alpha_x.benchmarks.buy_and_hold import run_buy_and_hold


def test_run_buy_and_hold_builds_equity_curve() -> None:
    frame = pd.DataFrame(
        {
            "timestamp": [0, 1],
            "datetime": pd.to_datetime([0, 1], unit="ms", utc=True),
            "close": [100.0, 120.0],
        }
    )

    result = run_buy_and_hold(frame, fee_rate=0.01, initial_capital=1_000.0)

    assert result.metadata["trades"] == 1
    assert result.equity_curve["equity"].iloc[0] == 980.1
    assert result.equity_curve["equity"].iloc[-1] == 1_176.12
