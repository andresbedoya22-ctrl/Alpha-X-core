from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from alpha_x.data.ohlcv_pipeline import backfill_and_store_ohlcv, validate_existing_ohlcv


def _build_frame(start_timestamp: int, count: int, step: int = 3_600_000) -> pd.DataFrame:
    rows = []
    for index in range(count):
        timestamp = start_timestamp + (index * step)
        price = float(index + 1)
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


class FakeBitvavoClient:
    max_candles_per_request = 3

    def __init__(self, frames: list[pd.DataFrame]) -> None:
        self.frames = frames
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
            {
                "market": market,
                "interval": interval,
                "limit": limit,
                "start": start,
                "end": end,
            }
        )
        if not self.frames:
            return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
        return self.frames.pop(0)


def test_backfill_and_store_ohlcv_fetches_multiple_blocks(tmp_path: Path) -> None:
    logger = logging.getLogger("test_backfill_and_store_ohlcv_fetches_multiple_blocks")
    raw_data_dir = tmp_path / "raw"
    client = FakeBitvavoClient(
        frames=[
            _build_frame(6 * 3_600_000, 3),
            _build_frame(3 * 3_600_000, 3),
            _build_frame(0, 3),
        ]
    )

    result = backfill_and_store_ohlcv(
        client=client,
        raw_data_dir=raw_data_dir,
        market="BTC-EUR",
        timeframe="1h",
        limit=3,
        logger=logger,
        target_rows=8,
    )

    assert result.request_count == 3
    assert result.final_rows == 9
    assert result.new_rows_added == 9
    assert len(client.calls) == 3
    assert client.calls[1]["end"] == (6 * 3_600_000) - 3_600_000


def test_backfill_persistence_is_idempotent_for_existing_rows(tmp_path: Path) -> None:
    logger = logging.getLogger("test_backfill_persistence_is_idempotent_for_existing_rows")
    raw_data_dir = tmp_path / "raw"
    client = FakeBitvavoClient(frames=[_build_frame(0, 3)])

    first = backfill_and_store_ohlcv(
        client=client,
        raw_data_dir=raw_data_dir,
        market="BTC-EUR",
        timeframe="1h",
        limit=3,
        logger=logger,
        target_rows=3,
    )

    second_client = FakeBitvavoClient(frames=[])
    second = backfill_and_store_ohlcv(
        client=second_client,
        raw_data_dir=raw_data_dir,
        market="BTC-EUR",
        timeframe="1h",
        limit=3,
        logger=logger,
        target_rows=3,
    )

    assert first.final_rows == 3
    assert second.final_rows == 3
    assert second.new_rows_added == 0
    assert second.request_count == 0


def test_validate_existing_ohlcv_after_backfill_is_gap_free(tmp_path: Path) -> None:
    logger = logging.getLogger("test_validate_existing_ohlcv_after_backfill_is_gap_free")
    raw_data_dir = tmp_path / "raw"
    client = FakeBitvavoClient(frames=[_build_frame(0, 4)])

    backfill_and_store_ohlcv(
        client=client,
        raw_data_dir=raw_data_dir,
        market="BTC-EUR",
        timeframe="1h",
        limit=4,
        logger=logger,
        target_rows=4,
    )

    _csv_path, frame, report = validate_existing_ohlcv(raw_data_dir, "BTC-EUR", "1h")

    assert len(frame) == 4
    assert report.is_valid is True
