from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from alpha_x.multi_asset_experiments.common_window import (
    apply_common_window,
    load_common_enriched_window,
)


def test_load_common_enriched_window_from_latest_report(tmp_path: Path) -> None:
    report_dir = tmp_path / "reports" / "multi_asset_data" / "20260419T000000Z"
    report_dir.mkdir(parents=True)
    summary = {
        "run_id": "20260419T000000Z",
        "markets_audited": ["BTC-EUR", "ETH-EUR"],
        "common_windows": {
            "ohlcv_plus_funding_plus_etf_flows": {
                "start": "2024-01-01T00:00:00+00:00",
                "end": "2024-01-03T00:00:00+00:00",
            }
        },
    }
    (report_dir / "summary.json").write_text(json.dumps(summary), encoding="utf-8")

    loaded = load_common_enriched_window(tmp_path / "reports")

    assert loaded.audit_run_id == "20260419T000000Z"
    assert loaded.start == pd.Timestamp("2024-01-01T00:00:00+00:00")
    assert loaded.end == pd.Timestamp("2024-01-03T00:00:00+00:00")


def test_apply_common_window_is_inclusive(tmp_path: Path) -> None:
    common_window = load_common_enriched_window(
        _write_summary(tmp_path / "reports"),
        audit_run_id="20260419T000001Z",
    )
    frame = pd.DataFrame(
        {
            "datetime": pd.to_datetime(
                [
                    "2023-12-31T23:00:00+00:00",
                    "2024-01-01T00:00:00+00:00",
                    "2024-01-02T00:00:00+00:00",
                    "2024-01-03T00:00:00+00:00",
                    "2024-01-03T01:00:00+00:00",
                ],
                utc=True,
            )
        }
    )

    filtered = apply_common_window(frame, common_window)

    assert len(filtered) == 3
    assert filtered["datetime"].iloc[0] == common_window.start
    assert filtered["datetime"].iloc[-1] == common_window.end


def _write_summary(reports_dir: Path) -> Path:
    report_dir = reports_dir / "multi_asset_data" / "20260419T000001Z"
    report_dir.mkdir(parents=True)
    summary = {
        "run_id": "20260419T000001Z",
        "markets_audited": ["BTC-EUR", "ETH-EUR"],
        "common_windows": {
            "ohlcv_plus_funding_plus_etf_flows": {
                "start": "2024-01-01T00:00:00+00:00",
                "end": "2024-01-03T00:00:00+00:00",
            }
        },
    }
    (report_dir / "summary.json").write_text(json.dumps(summary), encoding="utf-8")
    return reports_dir
