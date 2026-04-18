from __future__ import annotations

import pandas as pd

from alpha_x.validation.splits import build_temporal_splits


def _build_frame(length: int = 100) -> pd.DataFrame:
    timestamps = [index * 3_600_000 for index in range(length)]
    return pd.DataFrame({"timestamp": timestamps})


def test_temporal_splits_are_ordered_and_non_overlapping() -> None:
    frame = _build_frame()

    splits = build_temporal_splits(frame)

    assert [split.segment for split in splits] == ["train", "validation", "test"]
    assert splits[0].end_index < splits[1].start_index
    assert splits[1].end_index < splits[2].start_index
    assert splits[0].start_timestamp < splits[1].start_timestamp < splits[2].start_timestamp
