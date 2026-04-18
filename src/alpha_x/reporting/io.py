from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def build_run_id(created_at: pd.Timestamp | None = None) -> str:
    timestamp = created_at or pd.Timestamp.now(tz="UTC")
    normalized = (
        timestamp.tz_localize("UTC") if timestamp.tzinfo is None else timestamp.tz_convert("UTC")
    ).floor("s")
    return normalized.strftime("%Y%m%dT%H%M%SZ")


def create_report_directory(reports_dir: Path, report_type: str, run_id: str) -> Path:
    path = reports_dir / report_type / run_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_json_file(path: Path, payload: dict[str, Any] | list[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    return path


def write_table_csv(path: Path, frame: pd.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)
    return path


def list_report_files(report_dir: Path) -> list[str]:
    return sorted(
        str(path.relative_to(report_dir))
        for path in report_dir.rglob("*")
        if path.is_file()
    )
