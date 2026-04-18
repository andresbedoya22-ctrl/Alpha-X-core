from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from alpha_x.benchmarks.data_loader import load_benchmark_dataset


def test_load_benchmark_dataset_validates_and_prepares_frame(tmp_path: Path) -> None:
    path = tmp_path / "btc-eur_1h.csv"
    frame = pd.DataFrame(
        [
            {"timestamp": 0, "open": 10, "high": 11, "low": 9, "close": 10, "volume": 1},
            {"timestamp": 3_600_000, "open": 11, "high": 12, "low": 10, "close": 11, "volume": 2},
        ]
    )
    frame.to_csv(path, index=False)

    loaded, info = load_benchmark_dataset(path, "1h")

    assert list(loaded.columns) == [
        "timestamp",
        "datetime",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]
    assert len(loaded) == 2
    assert info.row_count == 2
    assert info.timeframe == "1h"
    assert loaded["close"].iloc[-1] == pytest.approx(11.0)
