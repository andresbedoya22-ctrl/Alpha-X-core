from __future__ import annotations

from pathlib import Path

import pandas as pd
from scripts.run_labeling import main

from alpha_x.config.settings import get_settings
from alpha_x.data.ohlcv_storage import save_ohlcv_csv


def _build_dataset() -> pd.DataFrame:
    timestamps = [index * 3_600_000 for index in range(120)]
    close = []
    for index in range(120):
        trend = 100.0 + (index * 0.15)
        wave = ((index % 12) - 6) * 0.4
        close.append(trend + wave)

    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": close,
            "high": [value + 1.0 for value in close],
            "low": [value - 1.0 for value in close],
            "close": close,
            "volume": [100.0 + (index % 10) for index in range(120)],
        }
    )


def test_run_labeling_main_exports_report(
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

    exit_code = main(["--run-id", "test-labeling-run"])
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "Labeling A - Next Bar" in output
    assert "Labeling C - Triple Barrier 24h" in output
    assert "Report path:" in output
    assert (reports_dir / "labeling" / "test-labeling-run" / "summary.json").exists()
    assert (reports_dir / "labeling" / "test-labeling-run" / "summary.csv").exists()
    assert (reports_dir / "labeling" / "test-labeling-run" / "labels.csv").exists()

    get_settings.cache_clear()
