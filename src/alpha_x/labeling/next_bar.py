from __future__ import annotations

import pandas as pd

from alpha_x.labeling.utils import (
    add_gap_flags,
    assign_sign_label,
    finalize_labels,
    prepare_labeling_frame,
)


def build_next_bar_labels(
    frame: pd.DataFrame,
    *,
    timeframe: str,
    positive_threshold: float = 0.001,
    negative_threshold: float = -0.001,
) -> pd.DataFrame:
    prepared = prepare_labeling_frame(frame)
    prepared, _ = add_gap_flags(prepared, timeframe=timeframe)

    prepared["future_close"] = prepared["close"].shift(-1)
    prepared["future_timestamp"] = prepared["timestamp"].shift(-1).astype("Int64")
    prepared["forward_return"] = (prepared["future_close"] / prepared["close"]) - 1.0
    prepared["is_valid"] = prepared["is_contiguous_prev"].shift(-1).eq(True)
    prepared["discard_reason"] = "valid"
    prepared.loc[prepared["future_close"].isna(), "discard_reason"] = "insufficient_future_bars"
    prepared.loc[
        prepared["future_close"].notna() & ~prepared["is_valid"],
        "discard_reason",
    ] = "future_gap"
    prepared["label"] = pd.Series(pd.NA, index=prepared.index, dtype="Int64")

    valid_returns = prepared.loc[prepared["is_valid"], "forward_return"]
    prepared.loc[prepared["is_valid"], "label"] = assign_sign_label(
        valid_returns,
        positive_threshold=positive_threshold,
        negative_threshold=negative_threshold,
    ).astype("Int64")

    return finalize_labels(
        prepared,
        labeling_id="next_bar",
        labeling_name="Next-Bar Labeling",
        method="next_bar",
        parameters={
            "positive_threshold": positive_threshold,
            "negative_threshold": negative_threshold,
        },
    )
