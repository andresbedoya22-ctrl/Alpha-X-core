from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class TemporalSplit:
    split_id: str
    segment: str
    start_index: int
    end_index: int
    start_timestamp: int
    end_timestamp: int
    row_count: int


@dataclass(frozen=True)
class WalkForwardWindow:
    window_id: str
    train_start_index: int
    train_end_index: int
    test_start_index: int
    test_end_index: int
    train_start_timestamp: int
    train_end_timestamp: int
    test_start_timestamp: int
    test_end_timestamp: int


@dataclass(frozen=True)
class ValidationCandidate:
    candidate_id: str
    name: str
    family: str
    source_type: str
    parameters: dict[str, Any]


@dataclass(frozen=True)
class ValidationResultRow:
    candidate_id: str
    candidate_name: str
    family: str
    source_type: str
    mode: str
    segment: str
    split_id: str
    parameter_set: str
    total_return: float
    annualized_return: float | None
    max_drawdown: float
    profit_factor: float | None
    trades: int | None
    exposure: float | None
    final_equity: float
    rows: int
    start_timestamp: int
    end_timestamp: int
    gap_count: int
    total_missing_intervals: int
