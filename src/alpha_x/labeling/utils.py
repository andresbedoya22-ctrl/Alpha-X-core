from __future__ import annotations

from dataclasses import asdict
from typing import Any

import pandas as pd

from alpha_x.data.ohlcv_models import timeframe_to_timedelta
from alpha_x.labeling.base import LabelingSummaryRow


def prepare_labeling_frame(frame: pd.DataFrame) -> pd.DataFrame:
    required_columns = {"timestamp", "datetime", "close", "high", "low"}
    missing_columns = sorted(required_columns - set(frame.columns))
    if missing_columns:
        raise ValueError(f"Missing required labeling columns: {missing_columns}")

    columns = ["timestamp", "datetime", "open", "high", "low", "close", "volume"]
    prepared = frame.loc[:, columns].copy()
    prepared["timestamp"] = pd.to_numeric(prepared["timestamp"], errors="raise").astype("int64")
    prepared = prepared.sort_values("timestamp", ascending=True).reset_index(drop=True)
    return prepared


def add_gap_flags(frame: pd.DataFrame, *, timeframe: str) -> tuple[pd.DataFrame, int]:
    prepared = frame.copy()
    candle_ms = int(timeframe_to_timedelta(timeframe).total_seconds() * 1000)
    deltas = prepared["timestamp"].diff()
    prepared["is_contiguous_prev"] = deltas.eq(candle_ms)
    prepared.loc[0, "is_contiguous_prev"] = False
    return prepared, candle_ms


def build_contiguous_window_mask(frame: pd.DataFrame, *, horizon_bars: int) -> pd.Series:
    if horizon_bars <= 0:
        raise ValueError("horizon_bars must be positive.")

    if len(frame) <= horizon_bars:
        return pd.Series(False, index=frame.index, dtype="bool")

    contiguous_prev = frame["is_contiguous_prev"].astype("int64")
    rolling_sum = contiguous_prev.rolling(window=horizon_bars, min_periods=horizon_bars).sum()
    mask = rolling_sum.shift(-horizon_bars).eq(horizon_bars).fillna(False)
    return mask.astype("bool")


def assign_sign_label(
    returns: pd.Series,
    *,
    positive_threshold: float,
    negative_threshold: float,
) -> pd.Series:
    if positive_threshold < 0:
        raise ValueError("positive_threshold must be non-negative.")
    if negative_threshold > 0:
        raise ValueError("negative_threshold must be non-positive.")

    labels = pd.Series(0, index=returns.index, dtype="int64")
    labels.loc[returns > positive_threshold] = 1
    labels.loc[returns < negative_threshold] = -1
    return labels


def finalize_labels(
    frame: pd.DataFrame,
    *,
    labeling_id: str,
    labeling_name: str,
    method: str,
    parameters: dict[str, Any],
) -> pd.DataFrame:
    finalized = frame.copy()
    finalized.insert(0, "labeling_id", labeling_id)
    finalized.insert(1, "labeling_name", labeling_name)
    finalized.insert(2, "method", method)
    finalized["label"] = finalized["label"].astype("Int64")
    finalized["is_valid"] = finalized["is_valid"].astype("bool")
    finalized["discard_reason"] = finalized["discard_reason"].astype("string")
    finalized["parameters"] = str(parameters)
    return finalized


def summarize_labels(frame: pd.DataFrame, *, name: str, method: str) -> LabelingSummaryRow:
    valid = frame.loc[frame["is_valid"]].copy()
    labeled_rows = len(valid)
    positive_count = int(valid["label"].eq(1).sum())
    neutral_count = int(valid["label"].eq(0).sum())
    negative_count = int(valid["label"].eq(-1).sum())

    def ratio(value: int) -> float:
        if labeled_rows == 0:
            return 0.0
        return value / labeled_rows

    if labeled_rows == 0:
        start_timestamp = None
        end_timestamp = None
    else:
        start_timestamp = int(valid["timestamp"].iloc[0])
        end_timestamp = int(valid["timestamp"].iloc[-1])

    return LabelingSummaryRow(
        name=name,
        method=method,
        total_rows=len(frame),
        labeled_rows=labeled_rows,
        discarded_rows=int((~frame["is_valid"]).sum()),
        positive_count=positive_count,
        neutral_count=neutral_count,
        negative_count=negative_count,
        positive_pct=ratio(positive_count),
        neutral_pct=ratio(neutral_count),
        negative_pct=ratio(negative_count),
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )


def summary_rows_to_frame(rows: list[LabelingSummaryRow]) -> pd.DataFrame:
    return pd.DataFrame([asdict(row) for row in rows])
