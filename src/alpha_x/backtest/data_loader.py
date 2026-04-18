from __future__ import annotations

from pathlib import Path

import pandas as pd

from alpha_x.backtest.models import LoadedBacktestDataset
from alpha_x.benchmarks import DatasetInfo
from alpha_x.data.ohlcv_models import OHLCV_COLUMNS, normalize_ohlcv_frame
from alpha_x.data.ohlcv_validation import summarize_gaps, validate_temporal_integrity

REQUIRED_COLUMNS = tuple(OHLCV_COLUMNS)


def load_backtest_dataset(path: Path, timeframe: str) -> LoadedBacktestDataset:
    if not path.exists():
        raise FileNotFoundError(f"Backtest dataset not found: {path}")

    frame = pd.read_csv(path)
    missing_columns = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing_columns:
        raise ValueError(f"Missing required OHLCV columns: {missing_columns}")

    normalized = normalize_ohlcv_frame(frame)
    report = validate_temporal_integrity(normalized, timeframe)
    if not report.is_sorted or not report.has_unique_timestamps:
        raise ValueError(
            "Dataset failed structural validation: "
            f"sorted={report.is_sorted}, unique={report.has_unique_timestamps}"
        )

    prepared = normalized.copy()
    prepared["datetime"] = pd.to_datetime(prepared["timestamp"], unit="ms", utc=True)
    prepared = prepared.loc[:, ["timestamp", "datetime", *OHLCV_COLUMNS[1:]]]

    dataset_info = DatasetInfo(
        path=path,
        market=path.stem.rsplit("_", maxsplit=1)[0].upper(),
        timeframe=timeframe,
        row_count=len(prepared),
        start_timestamp=int(prepared["timestamp"].iloc[0]),
        end_timestamp=int(prepared["timestamp"].iloc[-1]),
    )
    return LoadedBacktestDataset(
        frame=prepared,
        dataset_info=dataset_info,
        validation_report=report,
        gap_summary=summarize_gaps(report),
    )
