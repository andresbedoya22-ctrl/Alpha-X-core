from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import pandas as pd

LabelingBuilder = Callable[..., pd.DataFrame]


@dataclass(frozen=True)
class LabelingDefinition:
    labeling_id: str
    name: str
    method: str
    description: str
    parameters: dict[str, Any]
    builder: LabelingBuilder

    def build_labels(self, frame: pd.DataFrame, *, timeframe: str) -> pd.DataFrame:
        return self.builder(frame, timeframe=timeframe, **self.parameters)


@dataclass(frozen=True)
class LabelingSummaryRow:
    name: str
    method: str
    total_rows: int
    labeled_rows: int
    discarded_rows: int
    positive_count: int
    neutral_count: int
    negative_count: int
    positive_pct: float
    neutral_pct: float
    negative_pct: float
    start_timestamp: int | None
    end_timestamp: int | None
