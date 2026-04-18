from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

OHLCV_COLUMNS = [
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
]

NUMERIC_COLUMNS = ["open", "high", "low", "close", "volume"]

TIMEFRAME_TO_PANDAS = {
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1h",
    "2h": "2h",
    "4h": "4h",
    "6h": "6h",
    "8h": "8h",
    "12h": "12h",
    "1d": "1D",
    "1W": "7D",
}


@dataclass(frozen=True)
class OhlcvRecord:
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float


def timeframe_to_timedelta(timeframe: str) -> pd.Timedelta:
    try:
        return pd.Timedelta(TIMEFRAME_TO_PANDAS[timeframe])
    except KeyError as error:
        raise ValueError(f"Unsupported timeframe: {timeframe}") from error


def normalize_ohlcv_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=OHLCV_COLUMNS)

    missing_columns = [column for column in OHLCV_COLUMNS if column not in frame.columns]
    if missing_columns:
        raise ValueError(f"Missing OHLCV columns: {missing_columns}")

    normalized = frame.loc[:, OHLCV_COLUMNS].copy()
    normalized["timestamp"] = pd.to_numeric(normalized["timestamp"], errors="raise").astype("int64")

    for column in NUMERIC_COLUMNS:
        normalized[column] = pd.to_numeric(normalized[column], errors="raise").astype("float64")

    normalized = normalized.sort_values("timestamp", ascending=True).reset_index(drop=True)
    return normalized
