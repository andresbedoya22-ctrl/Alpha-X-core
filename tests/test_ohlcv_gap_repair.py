from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from alpha_x.data.ohlcv_pipeline import (
    format_gap_report,
    repair_ohlcv_gaps,
    validate_existing_ohlcv,
)
from alpha_x.data.ohlcv_storage import build_ohlcv_csv_path, save_ohlcv_csv
from alpha_x.data.ohlcv_validation import validate_temporal_integrity


def _frame_from_timestamps(timestamps: list[int]) -> pd.DataFrame:
    rows = []
    for index, timestamp in enumerate(timestamps, start=1):
        price = float(index)
        rows.append(
            {
                "timestamp": timestamp,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": price,
            }
        )
    return pd.DataFrame(rows)


class GapRepairClient:
    max_candles_per_request = 1000

    def __init__(self, responses: list[pd.DataFrame]) -> None:
        self.responses = responses
        self.calls: list[dict[str, int | None | str]] = []

    def fetch_candles(
        self,
        market: str,
        interval: str,
        limit: int,
        start: int | None = None,
        end: int | None = None,
    ) -> pd.DataFrame:
        self.calls.append(
            {"market": market, "interval": interval, "limit": limit, "start": start, "end": end}
        )
        if not self.responses:
            return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
        return self.responses.pop(0)


def test_repair_ohlcv_gaps_refetches_gap_windows_without_synthetic_fill(tmp_path: Path) -> None:
    raw_data_dir = tmp_path / "raw"
    csv_path = build_ohlcv_csv_path(raw_data_dir, "bitvavo", "BTC-EUR", "1h")
    save_ohlcv_csv(_frame_from_timestamps([0, 3_600_000, 10_800_000]), csv_path)
    client = GapRepairClient([_frame_from_timestamps([7_200_000])])
    logger = logging.getLogger("test_repair_ohlcv_gaps_refetches_only_missing_windows")

    result = repair_ohlcv_gaps(
        client=client,
        raw_data_dir=raw_data_dir,
        market="BTC-EUR",
        timeframe="1h",
        logger=logger,
    )

    _csv_path, frame, report = validate_existing_ohlcv(raw_data_dir, "BTC-EUR", "1h")
    assert len(client.calls) == 1
    assert client.calls[0]["start"] == 3_600_000
    assert client.calls[0]["end"] == 10_800_000
    assert len(frame) == 4
    assert report.is_valid is True
    assert result.repaired_gaps == 1


def test_repair_ohlcv_gaps_leaves_real_gap_when_exchange_returns_nothing(tmp_path: Path) -> None:
    raw_data_dir = tmp_path / "raw"
    csv_path = build_ohlcv_csv_path(raw_data_dir, "bitvavo", "BTC-EUR", "1h")
    save_ohlcv_csv(_frame_from_timestamps([0, 3_600_000, 10_800_000]), csv_path)
    empty_response = pd.DataFrame(
        columns=["timestamp", "open", "high", "low", "close", "volume"]
    )
    client = GapRepairClient([empty_response])
    logger = logging.getLogger(
        "test_repair_ohlcv_gaps_leaves_real_gap_when_exchange_returns_nothing"
    )

    result = repair_ohlcv_gaps(
        client=client,
        raw_data_dir=raw_data_dir,
        market="BTC-EUR",
        timeframe="1h",
        logger=logger,
    )

    assert result.repaired_gaps == 0
    assert result.remaining_gaps == 1
    assert result.remaining_missing_intervals == 1


def test_format_gap_report_includes_summary_and_rows() -> None:
    frame = _frame_from_timestamps([0, 3_600_000, 10_800_000])
    report = validate_temporal_integrity(frame, "1h")

    lines = format_gap_report(report)

    assert "Gap count: 1" in lines[0]
    assert any("Total missing intervals: 1" in line for line in lines)
    assert any("Detected gaps:" == line for line in lines)
    assert any("missing_intervals=1" in line for line in lines)
