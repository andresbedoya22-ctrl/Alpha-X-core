from __future__ import annotations

import pandas as pd

from alpha_x.labeling.next_bar import build_next_bar_labels


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


def test_next_bar_labels_positive_neutral_negative() -> None:
    frame = _build_frame([100.0, 102.0, 101.0, 101.02])

    labels = build_next_bar_labels(
        frame,
        timeframe="1h",
        positive_threshold=0.01,
        negative_threshold=-0.01,
    )

    assert labels.loc[0, "label"] == 1
    assert labels.loc[1, "label"] == 0
    assert labels.loc[2, "label"] == 0


def test_next_bar_discards_gap_and_tail_rows() -> None:
    frame = _build_frame(
        [100.0, 101.0, 102.0],
        timestamps=[0, 3_600_000, 10_800_000],
    )

    labels = build_next_bar_labels(frame, timeframe="1h")

    assert bool(labels.loc[1, "is_valid"]) is False
    assert labels.loc[1, "discard_reason"] == "future_gap"
    assert labels.loc[2, "discard_reason"] == "insufficient_future_bars"
