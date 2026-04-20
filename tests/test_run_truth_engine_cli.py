from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from scripts.run_truth_engine import main

from alpha_x.config.settings import get_settings

SAMPLE_MARKETS = ["BTC-EUR", "ETH-EUR", "SOL-EUR", "XRP-EUR"]


def test_run_truth_engine_cli(tmp_path: Path, monkeypatch: object, capsys: object) -> None:
    raw_data_dir = tmp_path / "raw"
    reports_dir = tmp_path / "reports"
    log_dir = tmp_path / "logs"
    monkeypatch.setenv("RAW_DATA_DIR", str(raw_data_dir))
    monkeypatch.setenv("REPORTS_DIR", str(reports_dir))
    monkeypatch.setenv("LOG_DIR", str(log_dir))
    get_settings.cache_clear()
    _write_data_summary(reports_dir)

    for market in SAMPLE_MARKETS:
        _write_daily_ohlcv(
            raw_data_dir / "bitvavo" / f"{market.lower()}_1d.csv", periods=520, seed=len(market)
        )

    exit_code = main(["--run-id", "cli-truth", "--no-export"])
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "Run ID: cli-truth" in output
    assert "Eligible markets:" in output
    get_settings.cache_clear()


def _write_data_summary(reports_dir: Path) -> None:
    report_dir = reports_dir / "truth_engine_data" / "20260419T000000Z"
    report_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "run_id": "20260419T000000Z",
        "timeframe": "1d",
        "effective_universe_final": SAMPLE_MARKETS,
    }
    (report_dir / "summary.json").write_text(json.dumps(payload), encoding="utf-8")


def _write_daily_ohlcv(path: Path, *, periods: int, seed: int) -> None:
    rows = []
    start = pd.Timestamp("2022-01-01T00:00:00+00:00")
    price = 100.0 + seed * 5.0
    for index in range(periods):
        dt = start + pd.Timedelta(days=index)
        drift = 0.0015 + (seed % 3) * 0.0002
        shock = (((index + seed) % 11) - 5) * 0.002
        price *= 1.0 + drift + shock
        rows.append(
            {
                "timestamp": int(dt.timestamp() * 1000),
                "open": price * 0.99,
                "high": price * 1.01,
                "low": price * 0.98,
                "close": price,
                "volume": 8000 + ((index + seed) % 17) * 300,
            }
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)
