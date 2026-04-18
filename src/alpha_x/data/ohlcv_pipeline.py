from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from alpha_x.data.bitvavo_client import BitvavoClient
from alpha_x.data.ohlcv_storage import (
    build_ohlcv_csv_path,
    load_ohlcv_csv,
    merge_ohlcv_frames,
    save_ohlcv_csv,
)
from alpha_x.data.ohlcv_validation import OhlcvValidationReport, validate_temporal_integrity


@dataclass(frozen=True)
class OhlcvPipelineResult:
    csv_path: Path
    downloaded_rows: int
    existing_rows: int
    final_rows: int
    validation: OhlcvValidationReport


def fetch_and_store_ohlcv(
    client: BitvavoClient,
    raw_data_dir: Path,
    market: str,
    timeframe: str,
    limit: int,
    logger: logging.Logger,
    start: int | None = None,
    end: int | None = None,
) -> OhlcvPipelineResult:
    csv_path = build_ohlcv_csv_path(
        raw_data_dir,
        exchange="bitvavo",
        market=market,
        timeframe=timeframe,
    )
    existing = load_ohlcv_csv(csv_path)
    downloaded = client.fetch_candles(
        market=market,
        interval=timeframe,
        limit=limit,
        start=start,
        end=end,
    )
    merged = merge_ohlcv_frames(existing, downloaded)
    validation = validate_temporal_integrity(merged, timeframe)

    if not validation.is_sorted:
        raise RuntimeError("Temporal validation failed: timestamps are not sorted ascending.")
    if not validation.has_unique_timestamps:
        raise RuntimeError("Temporal validation failed: duplicate timestamps remain after merge.")

    save_ohlcv_csv(merged, csv_path)
    _log_summary(
        logger=logger,
        market=market,
        timeframe=timeframe,
        csv_path=csv_path,
        existing=existing,
        downloaded=downloaded,
        merged=merged,
        validation=validation,
    )

    return OhlcvPipelineResult(
        csv_path=csv_path,
        downloaded_rows=len(downloaded),
        existing_rows=len(existing),
        final_rows=len(merged),
        validation=validation,
    )


def validate_existing_ohlcv(
    raw_data_dir: Path,
    market: str,
    timeframe: str,
) -> tuple[Path, pd.DataFrame, OhlcvValidationReport]:
    csv_path = build_ohlcv_csv_path(
        raw_data_dir,
        exchange="bitvavo",
        market=market,
        timeframe=timeframe,
    )
    frame = load_ohlcv_csv(csv_path)
    report = validate_temporal_integrity(frame, timeframe)
    return csv_path, frame, report


def _log_summary(
    logger: logging.Logger,
    market: str,
    timeframe: str,
    csv_path: Path,
    existing: pd.DataFrame,
    downloaded: pd.DataFrame,
    merged: pd.DataFrame,
    validation: OhlcvValidationReport,
) -> None:
    logger.info("OHLCV pipeline completed for %s %s", market, timeframe)
    logger.info("CSV path: %s", csv_path.resolve())
    logger.info("Existing rows: %s", len(existing))
    logger.info("Downloaded rows: %s", len(downloaded))
    logger.info("Final rows: %s", len(merged))

    if merged.empty:
        logger.info("Dataset is empty.")
        return

    logger.info(
        "Range: %s -> %s",
        int(merged.iloc[0]["timestamp"]),
        int(merged.iloc[-1]["timestamp"]),
    )
    logger.info("Gap count: %s", len(validation.gaps))
    for gap in validation.gaps:
        logger.warning(
            "Gap detected between %s and %s (%s missing intervals)",
            gap.previous_timestamp,
            gap.current_timestamp,
            gap.missing_intervals,
        )
