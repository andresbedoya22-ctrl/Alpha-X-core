from __future__ import annotations

import pandas as pd

from alpha_x.data.ohlcv_validation import summarize_gaps, validate_temporal_integrity
from alpha_x.validation.base import TemporalSplit


def build_temporal_splits(
    frame: pd.DataFrame,
    *,
    train_ratio: float = 0.6,
    validation_ratio: float = 0.2,
    test_ratio: float = 0.2,
) -> list[TemporalSplit]:
    total_ratio = train_ratio + validation_ratio + test_ratio
    if abs(total_ratio - 1.0) > 1e-9:
        raise ValueError("train_ratio, validation_ratio and test_ratio must sum to 1.0.")
    if frame.empty:
        raise ValueError("Temporal splits require a non-empty frame.")

    total_rows = len(frame)
    train_end = int(total_rows * train_ratio)
    validation_end = train_end + int(total_rows * validation_ratio)
    boundaries = [
        ("train", 0, train_end),
        ("validation", train_end, validation_end),
        ("test", validation_end, total_rows),
    ]

    splits: list[TemporalSplit] = []
    for index, (segment, start_idx, end_idx) in enumerate(boundaries, start=1):
        if end_idx <= start_idx:
            raise ValueError(f"Temporal split {segment} is empty. Adjust the ratios.")
        segment_frame = frame.iloc[start_idx:end_idx]
        splits.append(
            TemporalSplit(
                split_id=f"split_{index}_{segment}",
                segment=segment,
                start_index=start_idx,
                end_index=end_idx - 1,
                start_timestamp=int(segment_frame["timestamp"].iloc[0]),
                end_timestamp=int(segment_frame["timestamp"].iloc[-1]),
                row_count=len(segment_frame),
            )
        )
    return splits


def slice_frame_for_split(frame: pd.DataFrame, split: TemporalSplit) -> pd.DataFrame:
    return frame.iloc[split.start_index : split.end_index + 1].copy().reset_index(drop=True)


def summarize_segment_gaps(frame: pd.DataFrame, *, timeframe: str) -> tuple[int, int]:
    report = validate_temporal_integrity(frame, timeframe)
    summary = summarize_gaps(report)
    return summary.gap_count, summary.total_missing_intervals
