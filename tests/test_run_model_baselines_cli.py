from __future__ import annotations

from pathlib import Path

import pandas as pd
from scripts.run_model_baselines import main

from alpha_x.config.settings import get_settings
from alpha_x.data.ohlcv_storage import save_ohlcv_csv


def _build_dataset() -> pd.DataFrame:
    timestamps = [index * 3_600_000 for index in range(500)]
    close = []
    for index in range(500):
        trend = 100.0 + (index * 0.16)
        cycle = ((index % 24) - 12) * 0.45
        shock_up = 6.0 if index in {250, 251, 252} else 0.0
        shock_down = -7.0 if index in {360, 361, 362} else 0.0
        close.append(trend + cycle + shock_up + shock_down)

    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": close,
            "high": [value + 1.2 for value in close],
            "low": [value - 1.2 for value in close],
            "close": close,
            "volume": [100.0 + (index % 8) for index in range(500)],
        }
    )


def test_run_model_baselines_main_exports_report(
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

    exit_code = main(["--run-id", "test-modeling-run"])
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "Metricas por modelo:" in output
    assert "Mejor modelo base:" in output
    assert "Backtest comparativo en test:" in output
    assert (reports_dir / "modeling" / "test-modeling-run" / "summary.json").exists()
    assert (reports_dir / "modeling" / "test-modeling-run" / "model_metrics.csv").exists()
    assert (reports_dir / "modeling" / "test-modeling-run" / "backtest_comparison.csv").exists()

    get_settings.cache_clear()
