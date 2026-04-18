import pandas as pd

from alpha_x.data.ohlcv_validation import validate_temporal_integrity


def test_validate_temporal_integrity_requires_ascending_timestamps() -> None:
    frame = pd.DataFrame(
        [
            {"timestamp": 2000, "open": 2, "high": 2, "low": 2, "close": 2, "volume": 2},
            {"timestamp": 1000, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1},
        ]
    )

    report = validate_temporal_integrity(frame, "1m")

    assert report.is_sorted is False


def test_validate_temporal_integrity_detects_gaps() -> None:
    frame = pd.DataFrame(
        [
            {"timestamp": 0, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1},
            {"timestamp": 60000, "open": 2, "high": 2, "low": 2, "close": 2, "volume": 2},
            {"timestamp": 180000, "open": 3, "high": 3, "low": 3, "close": 3, "volume": 3},
        ]
    )

    report = validate_temporal_integrity(frame, "1m")

    assert len(report.gaps) == 1
    assert report.gaps[0].missing_intervals == 1
