from __future__ import annotations

from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from alpha_x.backtest.metrics import PerformanceRow


def serialize_value(value: Any) -> Any:
    if is_dataclass(value):
        return serialize_value(asdict(value))
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): serialize_value(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [serialize_value(item) for item in value]
    if hasattr(value, "item") and callable(value.item):
        try:
            return value.item()
        except (TypeError, ValueError):
            return str(value)
    return value


def performance_rows_to_frame(rows: list[PerformanceRow]) -> pd.DataFrame:
    records = [serialize_value(row) for row in rows]
    return pd.DataFrame(records)


def build_equity_curves_frame(named_curves: list[tuple[str, pd.DataFrame]]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for strategy_name, curve in named_curves:
        prepared = curve.copy()
        prepared.insert(0, "strategy", strategy_name)
        frames.append(prepared)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def build_summary_payload(
    *,
    run_id: str,
    report_type: str,
    created_at: pd.Timestamp,
    dataset: dict[str, Any],
    parameters: dict[str, Any],
    summary_rows: list[PerformanceRow],
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "report_type": report_type,
        "created_at": serialize_value(created_at),
        "dataset": serialize_value(dataset),
        "parameters": serialize_value(parameters),
        "summary": serialize_value(summary_rows),
    }
