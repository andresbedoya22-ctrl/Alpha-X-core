from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from alpha_x.benchmarks import DatasetInfo
from alpha_x.data.ohlcv_validation import OhlcvGapSummary, OhlcvValidationReport


@dataclass(frozen=True)
class BacktestConfig:
    initial_capital: float
    fee_rate: float
    slippage_rate: float
    signal_column: str = "signal"


@dataclass(frozen=True)
class LoadedBacktestDataset:
    frame: pd.DataFrame
    dataset_info: DatasetInfo
    validation_report: OhlcvValidationReport
    gap_summary: OhlcvGapSummary


@dataclass(frozen=True)
class BacktestTrade:
    entry_timestamp: int
    exit_timestamp: int
    entry_datetime: pd.Timestamp
    exit_datetime: pd.Timestamp
    entry_price: float
    exit_price: float
    quantity: float
    entry_fee: float
    exit_fee: float
    gross_proceeds: float
    net_proceeds: float
    net_pnl: float
    return_pct: float
    bars_held: int


@dataclass
class BacktestResult:
    name: str
    equity_curve: pd.DataFrame
    trades: pd.DataFrame
    metadata: dict[str, Any]
