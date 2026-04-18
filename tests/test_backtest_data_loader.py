from __future__ import annotations

from pathlib import Path

import pandas as pd

from alpha_x.backtest.data_loader import load_backtest_dataset


def test_load_backtest_dataset_keeps_residual_gap_report(tmp_path: Path) -> None:
    path = tmp_path / "btc-eur_1h.csv"
    pd.DataFrame(
        [
            {"timestamp": 0, "open": 10, "high": 11, "low": 9, "close": 10, "volume": 1},
            {"timestamp": 3_600_000, "open": 11, "high": 12, "low": 10, "close": 11, "volume": 2},
            {"timestamp": 10_800_000, "open": 12, "high": 13, "low": 11, "close": 12, "volume": 3},
        ]
    ).to_csv(path, index=False)

    loaded = load_backtest_dataset(path, "1h")

    assert list(loaded.frame.columns) == [
        "timestamp",
        "datetime",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]
    assert loaded.gap_summary.gap_count == 1
    assert loaded.gap_summary.total_missing_intervals == 1
