from __future__ import annotations

import pandas as pd

from alpha_x.labeling.fixed_horizon import build_fixed_horizon_labels


def _build_frame(closes: list[float], timestamps: list[int] | None = None) -> pd.DataFrame:
    values = timestamps or [index * 3_600_000 for index in range(len(closes))]
    return pd.DataFrame(
        {
            "timestamp": values,
            "datetime": pd.to_datetime(values, unit="ms", utc=True),
            "open": closes,
            "high": [value + 1.0 for value in closes],
            "low": [value - 1.0 for value in closes],
            "close": closes,
            "volume": [10.0] * len(closes),
        }
    )


def test_fixed_horizon_labels_use_future_return() -> None:
    frame = _build_frame([100.0, 101.0, 103.0, 101.0, 98.0])

    labels = build_fixed_horizon_labels(
        frame,
        timeframe="1h",
        horizon_bars=2,
        positive_threshold=0.02,
        negative_threshold=-0.02,
    )

    assert labels.loc[0, "label"] == 1
    assert labels.loc[1, "label"] == 0
    assert labels.loc[2, "label"] == -1


def test_fixed_horizon_discards_gap_crossing_window() -> None:
    frame = _build_frame(
        [100.0, 101.0, 102.0, 103.0],
        timestamps=[0, 3_600_000, 10_800_000, 14_400_000],
    )

    labels = build_fixed_horizon_labels(frame, timeframe="1h", horizon_bars=2)

    assert bool(labels.loc[0, "is_valid"]) is False
    assert labels.loc[0, "discard_reason"] == "future_gap"
    assert labels.loc[2, "discard_reason"] == "insufficient_future_bars"
