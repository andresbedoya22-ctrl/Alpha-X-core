from __future__ import annotations

from pathlib import Path

import pandas as pd
from scripts.run_hypotheses import main

from alpha_x.config.settings import get_settings
from alpha_x.data.ohlcv_storage import save_ohlcv_csv


def _build_dataset() -> pd.DataFrame:
    timestamps = [index * 3_600_000 for index in range(300)]
    close = []
    for index in range(300):
        trend = 100.0 + (index * 0.2)
        swing = ((index % 24) - 12) * 0.35
        dip = -7.0 if index in {90, 180, 250} else 0.0
        jump = 5.0 if index in {120, 210} else 0.0
        close.append(trend + swing + dip + jump)

    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": close,
            "high": [value + 1.0 for value in close],
            "low": [value - 1.0 for value in close],
            "close": close,
            "volume": [100.0 + (index % 10) for index in range(300)],
        }
    )


def test_run_hypotheses_main_exports_report(
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

    exit_code = main(["--run-id", "test-hypotheses-run"])
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "Hypothesis 1 - Trend Following" in output
    assert "Benchmark A - Buy & Hold BTC/EUR" in output
    assert "Report path:" in output
    assert (reports_dir / "hypotheses" / "test-hypotheses-run" / "summary.json").exists()
    assert (reports_dir / "hypotheses" / "test-hypotheses-run" / "summary.csv").exists()
    assert (reports_dir / "hypotheses" / "test-hypotheses-run" / "equity_curves.csv").exists()
    assert (reports_dir / "hypotheses" / "test-hypotheses-run" / "signals.csv").exists()

    get_settings.cache_clear()
