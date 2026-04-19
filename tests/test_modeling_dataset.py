from __future__ import annotations

from pathlib import Path

import pandas as pd

from alpha_x.backtest.models import LoadedBacktestDataset
from alpha_x.benchmarks import DatasetInfo
from alpha_x.data.ohlcv_validation import OhlcvGapSummary, OhlcvValidationReport
from alpha_x.modeling.dataset import TARGET_COLUMN, build_supervised_dataset


def _build_loaded_dataset(rows: int = 400) -> LoadedBacktestDataset:
    timestamps = [index * 3_600_000 for index in range(rows)]
    close = []
    for index in range(rows):
        trend = 100.0 + (index * 0.2)
        cycle = ((index % 24) - 12) * 0.25
        close.append(trend + cycle)

    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "datetime": pd.to_datetime(timestamps, unit="ms", utc=True),
            "open": close,
            "high": [value + 1.0 for value in close],
            "low": [value - 1.0 for value in close],
            "close": close,
            "volume": [100.0 + (index % 5) for index in range(rows)],
        }
    )
    return LoadedBacktestDataset(
        frame=frame,
        dataset_info=DatasetInfo(
            path=Path("dummy.csv"),
            market="BTC-EUR",
            timeframe="1h",
            row_count=len(frame),
            start_timestamp=int(frame["timestamp"].iloc[0]),
            end_timestamp=int(frame["timestamp"].iloc[-1]),
        ),
        validation_report=OhlcvValidationReport(
            row_count=len(frame),
            is_sorted=True,
            has_unique_timestamps=True,
            gaps=[],
        ),
        gap_summary=OhlcvGapSummary(
            gap_count=0,
            total_missing_intervals=0,
            smallest_gap=None,
            largest_gap=None,
            affected_start=None,
            affected_end=None,
            size_buckets={"1": 0, "2-3": 0, "4-12": 0, "13+": 0},
        ),
    )


def test_build_supervised_dataset_creates_binary_target_and_filters_rows() -> None:
    dataset = _build_loaded_dataset()

    full_frame, supervised_frame, feature_columns, categorical_columns, summary = (
        build_supervised_dataset(dataset, timeframe="1h")
    )

    assert TARGET_COLUMN in supervised_frame.columns
    assert set(supervised_frame[TARGET_COLUMN].unique().tolist()).issubset({0, 1})
    assert "regime" in categorical_columns
    assert len(feature_columns) == 24
    assert summary["supervised_rows"] > 0
    assert full_frame["supervised_discard_reason"].isin(
        ["valid", "feature_warmup", "regime_unavailable", "label_unavailable", "target_missing"]
    ).all()
