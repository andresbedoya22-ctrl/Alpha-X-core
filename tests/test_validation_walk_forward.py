from __future__ import annotations

import pandas as pd

from alpha_x.validation.walk_forward import build_expanding_walk_forward_windows


def _build_frame(length: int = 120) -> pd.DataFrame:
    timestamps = [index * 3_600_000 for index in range(length)]
    return pd.DataFrame({"timestamp": timestamps})


def test_walk_forward_windows_expand_and_keep_test_oos() -> None:
    frame = _build_frame()

    windows = build_expanding_walk_forward_windows(frame, train_size=60, test_size=20)

    assert len(windows) == 3
    assert windows[0].train_start_index == 0
    assert windows[0].train_end_index == 59
    assert windows[0].test_start_index == 60
    assert windows[0].test_end_index == 79
    assert windows[1].train_end_index > windows[0].train_end_index
    assert windows[1].test_start_index == 80
