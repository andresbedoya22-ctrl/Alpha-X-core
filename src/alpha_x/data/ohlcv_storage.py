from __future__ import annotations

from pathlib import Path

import pandas as pd

from alpha_x.data.ohlcv_models import OHLCV_COLUMNS, normalize_ohlcv_frame


def build_ohlcv_csv_path(raw_data_dir: Path, exchange: str, market: str, timeframe: str) -> Path:
    safe_market = market.lower().replace("/", "-")
    target_dir = raw_data_dir / exchange.lower()
    target_dir.mkdir(parents=True, exist_ok=True)
    return target_dir / f"{safe_market}_{timeframe}.csv"


def load_ohlcv_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=OHLCV_COLUMNS)

    frame = pd.read_csv(path)
    return normalize_ohlcv_frame(frame)


def merge_ohlcv_frames(existing: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
    if existing.empty:
        return normalize_ohlcv_frame(incoming)
    if incoming.empty:
        return normalize_ohlcv_frame(existing)

    merged = pd.concat([existing, incoming], ignore_index=True)
    merged = normalize_ohlcv_frame(merged)
    merged = merged.drop_duplicates(subset=["timestamp"], keep="last")
    merged = merged.sort_values("timestamp", ascending=True).reset_index(drop=True)
    return merged


def save_ohlcv_csv(frame: pd.DataFrame, path: Path) -> None:
    normalized = normalize_ohlcv_frame(frame)
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized.to_csv(path, index=False)
