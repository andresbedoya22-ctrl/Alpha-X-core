from __future__ import annotations

import pandas as pd

from alpha_x.benchmarks.dca import run_monthly_dca


def test_run_monthly_dca_uses_first_available_bar_of_each_month() -> None:
    frame = pd.DataFrame(
        {
            "timestamp": [0, 1, 2],
            "datetime": pd.to_datetime(
                ["2024-01-01T00:00:00Z", "2024-01-10T00:00:00Z", "2024-02-01T00:00:00Z"],
                utc=True,
            ),
            "close": [100.0, 110.0, 200.0],
        }
    )

    result = run_monthly_dca(frame, fee_rate=0.0, contribution=100.0)

    assert list(result.equity_curve["contribution_event"]) == [1, 0, 1]
    assert result.metadata["capital_base"] == 200.0
    assert result.equity_curve["equity"].iloc[-1] == 300.0
