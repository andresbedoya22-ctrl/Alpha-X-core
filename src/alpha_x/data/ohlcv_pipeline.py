from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from alpha_x.data.bitvavo_client import BitvavoClient
from alpha_x.data.ohlcv_models import timeframe_to_timedelta
from alpha_x.data.ohlcv_storage import (
    build_ohlcv_csv_path,
    load_ohlcv_csv,
    merge_ohlcv_frames,
    save_ohlcv_csv,
)
from alpha_x.data.ohlcv_validation import (
    OhlcvValidationReport,
    summarize_gaps,
    validate_temporal_integrity,
)


@dataclass(frozen=True)
class OhlcvPipelineResult:
    csv_path: Path
    downloaded_rows: int
    existing_rows: int
    final_rows: int
    new_rows_added: int
    request_count: int
    validation: OhlcvValidationReport


@dataclass(frozen=True)
class OhlcvGapRepairResult:
    csv_path: Path
    requests_made: int
    downloaded_rows: int
    rows_added: int
    repaired_gaps: int
    remaining_gaps: int
    remaining_missing_intervals: int
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
        limit=min(limit, client.max_candles_per_request),
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
        downloaded_rows=len(downloaded),
        merged=merged,
        validation=validation,
        request_count=1,
    )

    return OhlcvPipelineResult(
        csv_path=csv_path,
        downloaded_rows=len(downloaded),
        existing_rows=len(existing),
        final_rows=len(merged),
        new_rows_added=len(merged) - len(existing),
        request_count=1,
        validation=validation,
    )


def backfill_and_store_ohlcv(
    client: BitvavoClient,
    raw_data_dir: Path,
    market: str,
    timeframe: str,
    limit: int,
    logger: logging.Logger,
    target_rows: int,
    start: int | None = None,
    end: int | None = None,
) -> OhlcvPipelineResult:
    if target_rows <= 0:
        raise ValueError("target_rows must be positive.")

    csv_path = build_ohlcv_csv_path(
        raw_data_dir,
        exchange="bitvavo",
        market=market,
        timeframe=timeframe,
    )
    existing = load_ohlcv_csv(csv_path)
    merged = existing.copy()
    request_limit = min(limit, client.max_candles_per_request)
    candle_ms = int(timeframe_to_timedelta(timeframe).total_seconds() * 1000)
    request_count = 0
    downloaded_rows = 0

    if len(merged) >= target_rows:
        validation = validate_temporal_integrity(merged, timeframe)
        _log_summary(
            logger=logger,
            market=market,
            timeframe=timeframe,
            csv_path=csv_path,
            existing=existing,
            downloaded_rows=0,
            merged=merged,
            validation=validation,
            request_count=0,
        )
        return OhlcvPipelineResult(
            csv_path=csv_path,
            downloaded_rows=0,
            existing_rows=len(existing),
            final_rows=len(merged),
            new_rows_added=0,
            request_count=0,
            validation=validation,
        )

    next_end = _resolve_initial_backfill_end(merged, candle_ms, end)

    while len(merged) < target_rows:
        current_start = start
        if next_end is not None and current_start is not None and current_start > next_end:
            break

        logger.info(
            "Backfill request %s | market=%s interval=%s limit=%s start=%s end=%s",
            request_count + 1,
            market,
            timeframe,
            request_limit,
            current_start,
            next_end,
        )
        batch = client.fetch_candles(
            market=market,
            interval=timeframe,
            limit=request_limit,
            start=current_start,
            end=next_end,
        )
        request_count += 1
        downloaded_rows += len(batch)

        if batch.empty:
            logger.info("Backfill stopped: Bitvavo returned no rows.")
            break

        previous_rows = len(merged)
        merged = merge_ohlcv_frames(merged, batch)
        batch_new_rows = len(merged) - previous_rows
        logger.info(
            "Backfill response %s | downloaded=%s new_rows=%s cumulative_rows=%s",
            request_count,
            len(batch),
            batch_new_rows,
            len(merged),
        )

        oldest_timestamp = int(batch["timestamp"].iloc[0])
        if current_start is not None and oldest_timestamp <= current_start:
            logger.info("Backfill stopped: reached requested start boundary.")
            break

        if len(batch) < request_limit:
            logger.info("Backfill stopped: response shorter than request limit.")
            break

        next_end = oldest_timestamp - candle_ms

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
        downloaded_rows=downloaded_rows,
        merged=merged,
        validation=validation,
        request_count=request_count,
    )

    return OhlcvPipelineResult(
        csv_path=csv_path,
        downloaded_rows=downloaded_rows,
        existing_rows=len(existing),
        final_rows=len(merged),
        new_rows_added=len(merged) - len(existing),
        request_count=request_count,
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


def repair_ohlcv_gaps(
    client: BitvavoClient,
    raw_data_dir: Path,
    market: str,
    timeframe: str,
    logger: logging.Logger,
) -> OhlcvGapRepairResult:
    csv_path, existing, initial_report = validate_existing_ohlcv(raw_data_dir, market, timeframe)
    initial_summary = summarize_gaps(initial_report)
    if not initial_report.gaps:
        logger.info("No gaps detected for %s %s. Repair skipped.", market, timeframe)
        return OhlcvGapRepairResult(
            csv_path=csv_path,
            requests_made=0,
            downloaded_rows=0,
            rows_added=0,
            repaired_gaps=0,
            remaining_gaps=0,
            remaining_missing_intervals=0,
            validation=initial_report,
        )

    candle_ms = int(timeframe_to_timedelta(timeframe).total_seconds() * 1000)
    merged = existing.copy()
    requests_made = 0
    downloaded_rows = 0

    for index, gap in enumerate(initial_report.gaps, start=1):
        missing_start = gap.previous_timestamp + candle_ms
        missing_end = gap.current_timestamp - candle_ms
        requested_missing = gap.missing_intervals
        window_start = gap.previous_timestamp
        window_end = gap.current_timestamp
        cursor_start = window_start

        logger.info(
            "Repair gap %s/%s | missing_start=%s missing_end=%s missing_intervals=%s",
            index,
            len(initial_report.gaps),
            missing_start,
            missing_end,
            requested_missing,
        )

        while cursor_start <= window_end:
            remaining_intervals = ((window_end - cursor_start) // candle_ms) + 1
            request_limit = min(client.max_candles_per_request, int(remaining_intervals))
            try:
                batch = client.fetch_candles(
                    market=market,
                    interval=timeframe,
                    limit=request_limit,
                    start=cursor_start,
                    end=window_end,
                )
            except RuntimeError as error:
                logger.warning(
                    "Repair request failed for gap %s at start=%s end=%s: %s",
                    index,
                    cursor_start,
                    window_end,
                    error,
                )
                break
            requests_made += 1
            downloaded_rows += len(batch)

            if batch.empty:
                logger.info(
                    "Repair request returned no rows for gap %s at start=%s end=%s",
                    index,
                    cursor_start,
                    window_end,
                )
                break

            previous_rows = len(merged)
            merged = merge_ohlcv_frames(merged, batch)
            logger.info(
                "Repair response gap %s | downloaded=%s new_rows=%s",
                index,
                len(batch),
                len(merged) - previous_rows,
            )

            last_timestamp = int(batch["timestamp"].iloc[-1])
            next_start = last_timestamp + candle_ms
            if next_start <= cursor_start:
                break
            cursor_start = next_start

            if len(batch) < request_limit:
                break

    final_validation = validate_temporal_integrity(merged, timeframe)
    if not final_validation.is_sorted:
        raise RuntimeError("Temporal validation failed: timestamps are not sorted ascending.")
    if not final_validation.has_unique_timestamps:
        raise RuntimeError("Temporal validation failed: duplicate timestamps remain after merge.")

    save_ohlcv_csv(merged, csv_path)

    final_summary = summarize_gaps(final_validation)
    repaired_gaps = initial_summary.gap_count - final_summary.gap_count
    rows_added = len(merged) - len(existing)

    logger.info("Gap repair completed for %s %s", market, timeframe)
    logger.info("CSV path: %s", csv_path.resolve())
    logger.info("Requests made: %s", requests_made)
    logger.info("Downloaded rows: %s", downloaded_rows)
    logger.info("Rows added: %s", rows_added)
    logger.info("Gaps repaired: %s", repaired_gaps)
    logger.info("Remaining gaps: %s", final_summary.gap_count)
    logger.info("Remaining missing intervals: %s", final_summary.total_missing_intervals)

    return OhlcvGapRepairResult(
        csv_path=csv_path,
        requests_made=requests_made,
        downloaded_rows=downloaded_rows,
        rows_added=rows_added,
        repaired_gaps=repaired_gaps,
        remaining_gaps=final_summary.gap_count,
        remaining_missing_intervals=final_summary.total_missing_intervals,
        validation=final_validation,
    )


def format_gap_report(report: OhlcvValidationReport) -> list[str]:
    summary = summarize_gaps(report)
    lines = [
        f"Gap count: {summary.gap_count}",
        f"Total missing intervals: {summary.total_missing_intervals}",
        f"Gap size buckets: 1={summary.size_buckets['1']}, 2-3={summary.size_buckets['2-3']}, "
        f"4-12={summary.size_buckets['4-12']}, 13+={summary.size_buckets['13+']}",
    ]

    if summary.gap_count == 0:
        lines.append("Affected range: N/A")
        return lines

    lines.extend(
        [
            f"Affected range: {summary.affected_start} -> {summary.affected_end}",
            f"Smallest gap: {summary.smallest_gap}",
            f"Largest gap: {summary.largest_gap}",
            "Detected gaps:",
        ]
    )
    for gap in report.gaps:
        lines.append(
            f"- previous={gap.previous_timestamp} current={gap.current_timestamp} "
            f"missing_intervals={gap.missing_intervals}"
        )
    return lines


def _resolve_initial_backfill_end(
    existing: pd.DataFrame,
    candle_ms: int,
    requested_end: int | None,
) -> int | None:
    if requested_end is not None:
        return requested_end
    if existing.empty:
        return None
    return int(existing["timestamp"].iloc[0]) - candle_ms


def _log_summary(
    logger: logging.Logger,
    market: str,
    timeframe: str,
    csv_path: Path,
    existing: pd.DataFrame,
    downloaded_rows: int,
    merged: pd.DataFrame,
    validation: OhlcvValidationReport,
    request_count: int,
) -> None:
    logger.info("OHLCV pipeline completed for %s %s", market, timeframe)
    logger.info("CSV path: %s", csv_path.resolve())
    logger.info("Requests made: %s", request_count)
    logger.info("Existing rows: %s", len(existing))
    logger.info("Downloaded rows: %s", downloaded_rows)
    logger.info("New rows added: %s", len(merged) - len(existing))
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
