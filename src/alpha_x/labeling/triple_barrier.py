from __future__ import annotations

import pandas as pd

from alpha_x.labeling.utils import (
    add_gap_flags,
    build_contiguous_window_mask,
    finalize_labels,
    prepare_labeling_frame,
)


def build_triple_barrier_labels(
    frame: pd.DataFrame,
    *,
    timeframe: str,
    horizon_bars: int = 24,
    upper_barrier_pct: float = 0.02,
    lower_barrier_pct: float = 0.02,
) -> pd.DataFrame:
    if horizon_bars <= 0:
        raise ValueError("horizon_bars must be positive.")
    if upper_barrier_pct <= 0 or lower_barrier_pct <= 0:
        raise ValueError("Barrier percentages must be positive.")

    prepared = prepare_labeling_frame(frame)
    prepared, _ = add_gap_flags(prepared, timeframe=timeframe)
    prepared["future_timestamp"] = prepared["timestamp"].shift(-horizon_bars).astype("Int64")
    prepared["is_valid"] = build_contiguous_window_mask(prepared, horizon_bars=horizon_bars)
    prepared["discard_reason"] = "valid"
    prepared.loc[prepared["future_timestamp"].isna(), "discard_reason"] = "insufficient_future_bars"
    prepared.loc[
        prepared["future_timestamp"].notna() & ~prepared["is_valid"],
        "discard_reason",
    ] = "future_gap"
    prepared["label"] = pd.Series(pd.NA, index=prepared.index, dtype="Int64")
    prepared["hit_barrier"] = pd.Series(pd.NA, index=prepared.index, dtype="string")
    prepared["hit_timestamp"] = pd.Series(pd.NA, index=prepared.index, dtype="Int64")
    prepared["event_return"] = pd.Series(pd.NA, index=prepared.index, dtype="Float64")

    for index in prepared.index[prepared["is_valid"]]:
        entry_close = float(prepared.at[index, "close"])
        upper_price = entry_close * (1.0 + upper_barrier_pct)
        lower_price = entry_close * (1.0 - lower_barrier_pct)
        window = prepared.iloc[index + 1 : index + horizon_bars + 1]

        label = 0
        hit_barrier = "time"
        hit_timestamp = int(window["timestamp"].iloc[-1])
        event_return = (float(window["close"].iloc[-1]) / entry_close) - 1.0

        for _, row in window.iterrows():
            current_high = float(row["high"])
            current_low = float(row["low"])
            current_close = float(row["close"])
            current_timestamp = int(row["timestamp"])

            if current_high >= upper_price and current_low <= lower_price:
                label = 0
                hit_barrier = "both_same_bar"
                hit_timestamp = current_timestamp
                event_return = (current_close / entry_close) - 1.0
                break
            if current_high >= upper_price:
                label = 1
                hit_barrier = "upper"
                hit_timestamp = current_timestamp
                event_return = (upper_price / entry_close) - 1.0
                break
            if current_low <= lower_price:
                label = -1
                hit_barrier = "lower"
                hit_timestamp = current_timestamp
                event_return = (lower_price / entry_close) - 1.0
                break

        prepared.at[index, "label"] = label
        prepared.at[index, "hit_barrier"] = hit_barrier
        prepared.at[index, "hit_timestamp"] = hit_timestamp
        prepared.at[index, "event_return"] = event_return

    return finalize_labels(
        prepared,
        labeling_id="triple_barrier",
        labeling_name="Triple-Barrier Labeling",
        method="triple_barrier",
        parameters={
            "horizon_bars": horizon_bars,
            "upper_barrier_pct": upper_barrier_pct,
            "lower_barrier_pct": lower_barrier_pct,
        },
    )
