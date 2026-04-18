from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from alpha_x.data.ohlcv_models import normalize_ohlcv_frame, timeframe_to_timedelta


@dataclass(frozen=True)
class OhlcvGap:
    previous_timestamp: int
    current_timestamp: int
    missing_intervals: int


@dataclass(frozen=True)
class OhlcvValidationReport:
    row_count: int
    is_sorted: bool
    has_unique_timestamps: bool
    gaps: list[OhlcvGap]

    @property
    def is_valid(self) -> bool:
        return self.is_sorted and self.has_unique_timestamps and not self.gaps


def validate_temporal_integrity(frame: pd.DataFrame, timeframe: str) -> OhlcvValidationReport:
    if timeframe == "1M":
        raise ValueError("Gap detection for timeframe 1M is not supported in F1.2.")

    if frame.empty:
        normalized = normalize_ohlcv_frame(frame)
        return OhlcvValidationReport(
            row_count=0,
            is_sorted=True,
            has_unique_timestamps=True,
            gaps=[],
        )

    original_timestamps = pd.to_numeric(frame["timestamp"], errors="raise").astype("int64")
    is_sorted = original_timestamps.is_monotonic_increasing
    has_unique_timestamps = original_timestamps.is_unique
    normalized = normalize_ohlcv_frame(frame)
    if normalized.empty:
        return OhlcvValidationReport(
            row_count=0,
            is_sorted=True,
            has_unique_timestamps=True,
            gaps=[],
        )

    timestamp_series = normalized["timestamp"]

    expected_delta_ms = int(timeframe_to_timedelta(timeframe).total_seconds() * 1000)
    differences = timestamp_series.diff().dropna().astype("int64")

    gaps: list[OhlcvGap] = []
    for index, difference in differences.items():
        if difference > expected_delta_ms:
            missing_intervals = (difference // expected_delta_ms) - 1
            gaps.append(
                OhlcvGap(
                    previous_timestamp=int(timestamp_series.iloc[index - 1]),
                    current_timestamp=int(timestamp_series.iloc[index]),
                    missing_intervals=int(missing_intervals),
                )
            )

    return OhlcvValidationReport(
        row_count=len(normalized),
        is_sorted=is_sorted,
        has_unique_timestamps=has_unique_timestamps,
        gaps=gaps,
    )
