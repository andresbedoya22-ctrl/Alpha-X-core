import pandas as pd

from alpha_x.data.ohlcv_storage import merge_ohlcv_frames


def test_merge_ohlcv_frames_deduplicates_by_timestamp() -> None:
    existing = pd.DataFrame(
        [
            {"timestamp": 1000, "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 10},
            {"timestamp": 2000, "open": 2, "high": 3, "low": 1.5, "close": 2.5, "volume": 20},
        ]
    )
    incoming = pd.DataFrame(
        [
            {"timestamp": 2000, "open": 9, "high": 9, "low": 9, "close": 9, "volume": 99},
            {"timestamp": 3000, "open": 3, "high": 4, "low": 2.5, "close": 3.5, "volume": 30},
        ]
    )

    merged = merge_ohlcv_frames(existing, incoming)

    assert list(merged["timestamp"]) == [1000, 2000, 3000]
    assert merged.loc[merged["timestamp"] == 2000, "close"].iloc[0] == 9.0
