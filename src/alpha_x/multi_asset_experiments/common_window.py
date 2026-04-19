from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class CommonWindowDefinition:
    audit_run_id: str
    report_path: Path
    start: pd.Timestamp
    end: pd.Timestamp
    markets: list[str]
    source_summary: dict[str, object]

    @property
    def row_count_estimate(self) -> int:
        return int(((self.end - self.start).total_seconds() / 3600) + 1)


def load_common_enriched_window(
    reports_dir: Path,
    *,
    audit_run_id: str | None = None,
) -> CommonWindowDefinition:
    summary_path = _resolve_summary_path(reports_dir, audit_run_id=audit_run_id)
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    common_window = payload["common_windows"]["ohlcv_plus_funding_plus_etf_flows"]
    start = pd.Timestamp(common_window["start"])
    end = pd.Timestamp(common_window["end"])
    if start >= end:
        raise ValueError("Common enriched window is empty or invalid.")

    return CommonWindowDefinition(
        audit_run_id=str(payload["run_id"]),
        report_path=summary_path,
        start=start,
        end=end,
        markets=list(payload["markets_audited"]),
        source_summary=payload,
    )


def apply_common_window(
    frame: pd.DataFrame,
    common_window: CommonWindowDefinition,
) -> pd.DataFrame:
    if "datetime" not in frame.columns:
        raise ValueError("Frame must include a 'datetime' column.")
    filtered = frame.loc[
        frame["datetime"].ge(common_window.start) & frame["datetime"].le(common_window.end)
    ].copy()
    return filtered.reset_index(drop=True)


def _resolve_summary_path(reports_dir: Path, *, audit_run_id: str | None) -> Path:
    base_dir = reports_dir / "multi_asset_data"
    if audit_run_id is not None:
        summary_path = base_dir / audit_run_id / "summary.json"
        if not summary_path.exists():
            raise FileNotFoundError(f"Audit summary not found: {summary_path}")
        return summary_path

    candidates = sorted(
        (
            path / "summary.json"
            for path in base_dir.iterdir()
            if path.is_dir() and (path / "summary.json").exists()
        ),
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(
            "No multi-asset audit summary found under reports/multi_asset_data/."
        )
    return candidates[0]
