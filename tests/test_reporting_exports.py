from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from alpha_x.backtest.metrics import PerformanceRow
from alpha_x.reporting.io import (
    build_run_id,
    create_report_directory,
    list_report_files,
    write_json_file,
    write_table_csv,
)
from alpha_x.reporting.run_manifest import build_run_manifest
from alpha_x.reporting.serializers import (
    build_equity_curves_frame,
    build_summary_payload,
    performance_rows_to_frame,
    serialize_value,
)


def _sample_rows() -> list[PerformanceRow]:
    return [
        PerformanceRow(
            name="Example",
            source_id="backtest",
            total_return=0.1,
            annualized_return=None,
            max_drawdown=-0.05,
            profit_factor=1.5,
            trades=2,
            exposure=0.5,
            final_equity=110.0,
        )
    ]


def test_create_report_directory_creates_expected_path(tmp_path: Path) -> None:
    report_dir = create_report_directory(tmp_path, "backtests", "run-001")

    assert report_dir == tmp_path / "backtests" / "run-001"
    assert report_dir.is_dir()


def test_export_summary_writes_json_and_csv(tmp_path: Path) -> None:
    summary_rows = _sample_rows()
    created_at = pd.Timestamp("2026-04-18T12:00:00Z")
    payload = build_summary_payload(
        run_id="run-001",
        report_type="backtests",
        created_at=created_at,
        dataset={"path": "data/raw/bitvavo/btc-eur_1h.csv", "rows": 3, "gap_count": 1},
        parameters={"fee_rate": 0.001, "slippage_rate": 0.0005},
        summary_rows=summary_rows,
    )
    frame = performance_rows_to_frame(summary_rows)

    json_path = write_json_file(tmp_path / "summary.json", payload)
    csv_path = write_table_csv(tmp_path / "summary.csv", frame)

    loaded = json.loads(json_path.read_text(encoding="utf-8"))
    csv_frame = pd.read_csv(csv_path)

    assert loaded["run_id"] == "run-001"
    assert loaded["summary"][0]["name"] == "Example"
    assert "total_return" in csv_frame.columns
    assert csv_frame["name"].iloc[0] == "Example"


def test_export_equity_curve_writes_csv(tmp_path: Path) -> None:
    equity_curves = build_equity_curves_frame(
        [
            (
                "Example",
                pd.DataFrame(
                    {
                        "timestamp": [0, 1],
                        "equity": [100.0, 110.0],
                    }
                ),
            )
        ]
    )

    csv_path = write_table_csv(tmp_path / "equity_curves.csv", equity_curves)
    loaded = pd.read_csv(csv_path)

    assert list(loaded.columns) == ["strategy", "timestamp", "equity"]
    assert loaded["strategy"].iloc[0] == "Example"


def test_export_trades_writes_csv(tmp_path: Path) -> None:
    trades = pd.DataFrame(
        {
            "entry_timestamp": [1],
            "exit_timestamp": [2],
            "net_pnl": [10.0],
        }
    )

    csv_path = write_table_csv(tmp_path / "trades.csv", trades)
    loaded = pd.read_csv(csv_path)

    assert "net_pnl" in loaded.columns
    assert loaded["net_pnl"].iloc[0] == 10.0


def test_manifest_is_reproducible_with_basic_metadata(tmp_path: Path) -> None:
    created_at = pd.Timestamp("2026-04-18T12:00:00Z")
    manifest = build_run_manifest(
        run_id=build_run_id(created_at),
        report_type="backtests",
        created_at=created_at,
        dataset={"path": "data/raw/bitvavo/btc-eur_1h.csv", "rows": 10510, "gap_count": 12},
        parameters={"fee_rate": 0.001, "slippage_rate": 0.0005},
        summary_rows=_sample_rows(),
        artifacts=["summary.json", "summary.csv", "equity_curve.csv", "trades.csv"],
    )

    manifest_path = write_json_file(tmp_path / "manifest.json", serialize_value(manifest))
    loaded = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert loaded["run_id"] == "20260418T120000Z"
    assert loaded["dataset"]["gap_count"] == 12
    assert loaded["parameters"]["slippage_rate"] == 0.0005
    assert loaded["artifacts"] == ["summary.json", "summary.csv", "equity_curve.csv", "trades.csv"]
    assert list_report_files(tmp_path) == ["manifest.json"]
