from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass
class BenchmarkResult:
    name: str
    equity_curve: pd.DataFrame
    metadata: dict[str, Any]


@dataclass(frozen=True)
class DatasetInfo:
    path: Path
    market: str
    timeframe: str
    row_count: int
    start_timestamp: int
    end_timestamp: int


__all__ = ["BenchmarkResult", "DatasetInfo"]
