from __future__ import annotations

import pandas as pd

from alpha_x.benchmarks.sma_baseline import run_sma_baseline


def test_run_sma_baseline_tracks_entries_and_exits() -> None:
    closes = [10.0, 10.0, 12.0, 12.0, 9.0]
    frame = pd.DataFrame(
        {
            "timestamp": list(range(len(closes))),
            "datetime": pd.to_datetime(list(range(len(closes))), unit="ms", utc=True),
            "close": closes,
        }
    )

    result = run_sma_baseline(
        frame,
        fee_rate=0.0,
        initial_capital=100.0,
        fast_window=2,
        slow_window=3,
    )

    assert result.metadata["trades"] == 2
    assert list(result.equity_curve["position"]) == [0.0, 0.0, 1.0, 1.0, 0.0]
    assert result.equity_curve["equity"].iloc[-1] == 75.0
