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


@dataclass(frozen=True)
class OhlcvGapSummary:
    gap_count: int
    total_missing_intervals: int
    smallest_gap: int | None
    largest_gap: int | None
    affected_start: int | None
    affected_end: int | None
    size_buckets: dict[str, int]


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


def summarize_gaps(report: OhlcvValidationReport) -> OhlcvGapSummary:
    if not report.gaps:
        return OhlcvGapSummary(
            gap_count=0,
            total_missing_intervals=0,
            smallest_gap=None,
            largest_gap=None,
            affected_start=None,
            affected_end=None,
            size_buckets={"1": 0, "2-3": 0, "4-12": 0, "13+": 0},
        )

    missing_sizes = [gap.missing_intervals for gap in report.gaps]
    size_buckets = {"1": 0, "2-3": 0, "4-12": 0, "13+": 0}
    for size in missing_sizes:
        if size == 1:
            size_buckets["1"] += 1
        elif size <= 3:
            size_buckets["2-3"] += 1
        elif size <= 12:
            size_buckets["4-12"] += 1
        else:
            size_buckets["13+"] += 1

    return OhlcvGapSummary(
        gap_count=len(report.gaps),
        total_missing_intervals=sum(missing_sizes),
        smallest_gap=min(missing_sizes),
        largest_gap=max(missing_sizes),
        affected_start=min(gap.previous_timestamp for gap in report.gaps),
        affected_end=max(gap.current_timestamp for gap in report.gaps),
        size_buckets=size_buckets,
    )
