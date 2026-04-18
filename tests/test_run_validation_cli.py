from __future__ import annotations

from pathlib import Path

import pandas as pd
from scripts.run_validation import main

from alpha_x.config.settings import get_settings
from alpha_x.data.ohlcv_storage import save_ohlcv_csv


def _build_dataset() -> pd.DataFrame:
    timestamps = [index * 3_600_000 for index in range(400)]
    close = []
    for index in range(400):
        trend = 100.0 + (index * 0.08)
        swing = ((index % 24) - 12) * 0.35
        dip = -6.0 if index in {150, 260, 330} else 0.0
        close.append(trend + swing + dip)

    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": close,
            "high": [value + 1.0 for value in close],
            "low": [value - 1.0 for value in close],
            "close": close,
            "volume": [100.0 + (index % 10) for index in range(400)],
        }
    )


def test_run_validation_main_exports_report(
    tmp_path: Path,
    monkeypatch: object,
    capsys: object,
) -> None:
    raw_data_dir = tmp_path / "raw"
    reports_dir = tmp_path / "reports"
    log_dir = tmp_path / "logs"

    monkeypatch.setenv("RAW_DATA_DIR", str(raw_data_dir))
    monkeypatch.setenv("REPORTS_DIR", str(reports_dir))
    monkeypatch.setenv("LOG_DIR", str(log_dir))
    get_settings.cache_clear()

    dataset_path = raw_data_dir / "bitvavo" / "btc-eur_1h.csv"
    save_ohlcv_csv(_build_dataset(), dataset_path)

    exit_code = main(["--run-id", "test-validation-run"])
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "Benchmark C - SMA Crossover Baseline" in output
    assert "Hypothesis 1 - Trend Following" in output
    assert "OOS Aggregate" in output
    assert (reports_dir / "validation" / "test-validation-run" / "summary.json").exists()
    assert (reports_dir / "validation" / "test-validation-run" / "validation_rows.csv").exists()
    assert (reports_dir / "validation" / "test-validation-run" / "oos_aggregate.csv").exists()

    get_settings.cache_clear()
