from __future__ import annotations

import pandas as pd

from alpha_x.validation.base import WalkForwardWindow


def build_expanding_walk_forward_windows(
    frame: pd.DataFrame,
    *,
    train_size: int,
    test_size: int,
    step_size: int | None = None,
) -> list[WalkForwardWindow]:
    if frame.empty:
        raise ValueError("Walk-forward windows require a non-empty frame.")
    if train_size <= 0 or test_size <= 0:
        raise ValueError("train_size and test_size must be positive.")

    step = step_size or test_size
    if step <= 0:
        raise ValueError("step_size must be positive.")

    windows: list[WalkForwardWindow] = []
    total_rows = len(frame)
    test_start = train_size
    window_number = 1

    while test_start + test_size <= total_rows:
        train_end = test_start - 1
        test_end = test_start + test_size - 1
        windows.append(
            WalkForwardWindow(
                window_id=f"wf_{window_number}",
                train_start_index=0,
                train_end_index=train_end,
                test_start_index=test_start,
                test_end_index=test_end,
                train_start_timestamp=int(frame["timestamp"].iloc[0]),
                train_end_timestamp=int(frame["timestamp"].iloc[train_end]),
                test_start_timestamp=int(frame["timestamp"].iloc[test_start]),
                test_end_timestamp=int(frame["timestamp"].iloc[test_end]),
            )
        )
        test_start += step
        window_number += 1

    if not windows:
        raise ValueError("Not enough rows to build at least one walk-forward window.")
    return windows


def slice_train_frame(frame: pd.DataFrame, window: WalkForwardWindow) -> pd.DataFrame:
    return frame.iloc[window.train_start_index : window.train_end_index + 1].copy().reset_index(
        drop=True
    )


def slice_test_frame(frame: pd.DataFrame, window: WalkForwardWindow) -> pd.DataFrame:
    return frame.iloc[window.test_start_index : window.test_end_index + 1].copy().reset_index(
        drop=True
    )
