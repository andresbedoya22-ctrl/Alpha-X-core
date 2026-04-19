from __future__ import annotations

import pandas as pd

from alpha_x.refinements.resample import resample_1h_to_4h


def test_resample_1h_to_4h_aggregates_complete_buckets_only() -> None:
    timestamps = [index * 3_600_000 for index in range(8)]
    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "datetime": pd.to_datetime(timestamps, unit="ms", utc=True),
            "open": [1, 2, 3, 4, 5, 6, 7, 8],
            "high": [2, 3, 4, 5, 6, 7, 8, 9],
            "low": [0, 1, 2, 3, 4, 5, 6, 7],
            "close": [1, 2, 3, 4, 5, 6, 7, 8],
            "volume": [10] * 8,
        }
    )

    result = resample_1h_to_4h(frame)

    assert len(result) == 2
    assert result["open"].tolist() == [1.0, 5.0]
    assert result["close"].tolist() == [4.0, 8.0]
    assert result["high"].tolist() == [5.0, 9.0]
    assert result["low"].tolist() == [0.0, 4.0]
    assert result["volume"].tolist() == [40.0, 40.0]
