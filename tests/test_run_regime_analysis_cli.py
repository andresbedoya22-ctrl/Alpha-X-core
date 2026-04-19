from __future__ import annotations

from pathlib import Path

import pandas as pd
from scripts.run_regime_analysis import main

from alpha_x.config.settings import get_settings
from alpha_x.data.ohlcv_storage import save_ohlcv_csv


def _build_dataset() -> pd.DataFrame:
    timestamps = [index * 3_600_000 for index in range(400)]
    close = []
    for index in range(400):
        trend = 100.0 + (index * 0.18)
        cycle = ((index % 24) - 12) * 0.4
        shock = 10.0 if index in {220, 221, 222} else 0.0
        drawdown = -8.0 if index in {300, 301, 302} else 0.0
        close.append(trend + cycle + shock + drawdown)

    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": close,
            "high": [value + 1.5 for value in close],
            "low": [value - 1.5 for value in close],
            "close": close,
            "volume": [100.0 + (index % 8) for index in range(400)],
        }
    )


def test_run_regime_analysis_main_exports_report(
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

    exit_code = main(["--run-id", "test-regime-run"])
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "Distribucion de regimenes:" in output
    assert "Regime x labels" in output
    assert "Regime x estrategias:" in output
    assert "Report path:" in output
    assert (reports_dir / "regime" / "test-regime-run" / "summary.json").exists()
    assert (reports_dir / "regime" / "test-regime-run" / "regime_table.csv").exists()
    assert (reports_dir / "regime" / "test-regime-run" / "regime_label_table.csv").exists()
    assert (reports_dir / "regime" / "test-regime-run" / "regime_strategy_table.csv").exists()

    get_settings.cache_clear()
