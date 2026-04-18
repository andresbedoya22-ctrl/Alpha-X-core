from __future__ import annotations

import pandas as pd

from alpha_x.labeling.triple_barrier import build_triple_barrier_labels


def _build_frame(
    closes: list[float],
    highs: list[float] | None = None,
    lows: list[float] | None = None,
    timestamps: list[int] | None = None,
) -> pd.DataFrame:
    values = timestamps or [index * 3_600_000 for index in range(len(closes))]
    high_values = highs or [value + 1.0 for value in closes]
    low_values = lows or [value - 1.0 for value in closes]
    return pd.DataFrame(
        {
            "timestamp": values,
            "datetime": pd.to_datetime(values, unit="ms", utc=True),
            "open": closes,
            "high": high_values,
            "low": low_values,
            "close": closes,
            "volume": [10.0] * len(closes),
        }
    )


def test_triple_barrier_labels_first_upper_touch() -> None:
    frame = _build_frame(
        closes=[100.0, 101.0, 102.0, 103.0],
        highs=[100.5, 102.5, 103.0, 104.0],
        lows=[99.5, 100.5, 101.0, 102.0],
    )

    labels = build_triple_barrier_labels(
        frame,
        timeframe="1h",
        horizon_bars=2,
        upper_barrier_pct=0.02,
        lower_barrier_pct=0.02,
    )

    assert labels.loc[0, "label"] == 1
    assert labels.loc[0, "hit_barrier"] == "upper"


def test_triple_barrier_labels_first_lower_touch() -> None:
    frame = _build_frame(
        closes=[100.0, 99.0, 98.0, 97.0],
        highs=[100.5, 99.5, 98.5, 97.5],
        lows=[99.5, 97.5, 96.5, 95.5],
    )

    labels = build_triple_barrier_labels(
        frame,
        timeframe="1h",
        horizon_bars=2,
        upper_barrier_pct=0.02,
        lower_barrier_pct=0.02,
    )

    assert labels.loc[0, "label"] == -1
    assert labels.loc[0, "hit_barrier"] == "lower"


def test_triple_barrier_discards_gap_and_tail_rows() -> None:
    frame = _build_frame(
        closes=[100.0, 100.0, 100.0, 100.0],
        timestamps=[0, 3_600_000, 10_800_000, 14_400_000],
    )

    labels = build_triple_barrier_labels(frame, timeframe="1h", horizon_bars=2)

    assert bool(labels.loc[0, "is_valid"]) is False
    assert labels.loc[0, "discard_reason"] == "future_gap"
    assert labels.loc[2, "discard_reason"] == "insufficient_future_bars"
