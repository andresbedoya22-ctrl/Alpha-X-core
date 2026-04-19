from __future__ import annotations

from pathlib import Path

import pandas as pd
from scripts.run_multi_asset_data_audit import main

from alpha_x.config.settings import get_settings


def _write_ohlcv(path: Path, n_rows: int, start_ms: int = 0) -> None:
    rows = []
    for i in range(n_rows):
        rows.append(
            {
                "timestamp": start_ms + i * 3_600_000,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 1.0,
            }
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_funding(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "timestamp_ms": [0, 8 * 3_600_000, 16 * 3_600_000],
            "funding_rate": [0.0001, 0.0002, 0.0003],
        }
    ).to_csv(path, index=False)


def _write_etf(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "timestamp_ms": [0],
            "date": ["2024-01-12"],
            "effective_timestamp_ms": [24 * 3_600_000],
            "effective_date": ["2024-01-13"],
            "series_key": ["BTC_SPOT_ETF"],
            "btc_etf_flow_usd": [10_000_000.0],
            "btc_etf_flow_usd_millions": [10.0],
        }
    ).to_csv(path, index=False)


def test_run_multi_asset_data_audit_main(
    tmp_path: Path, monkeypatch: object, capsys: object
) -> None:
    raw_data_dir = tmp_path / "raw"
    external_data_dir = tmp_path / "external"
    reports_dir = tmp_path / "reports"
    log_dir = tmp_path / "logs"

    monkeypatch.setenv("RAW_DATA_DIR", str(raw_data_dir))
    monkeypatch.setenv("EXTERNAL_DATA_DIR", str(external_data_dir))
    monkeypatch.setenv("REPORTS_DIR", str(reports_dir))
    monkeypatch.setenv("LOG_DIR", str(log_dir))
    get_settings.cache_clear()

    for market in ["BTC-EUR", "ETH-EUR", "XRP-EUR", "SOL-EUR"]:
        _write_ohlcv(raw_data_dir / "bitvavo" / f"{market.lower()}_1h.csv", 48)
        base_asset = market.split("-")[0]
        _write_funding(external_data_dir / "funding" / f"{base_asset}USDT_funding_8h.csv")
    _write_etf(external_data_dir / "etf_flows" / "btc_spot_etf_daily.csv")
    (external_data_dir / "etf_flows" / "eth_spot_etf_daily.csv").parent.mkdir(
        parents=True, exist_ok=True
    )
    pd.DataFrame(
        columns=[
            "timestamp_ms",
            "date",
            "effective_timestamp_ms",
            "effective_date",
            "series_key",
            "btc_etf_flow_usd",
            "btc_etf_flow_usd_millions",
        ]
    ).to_csv(external_data_dir / "etf_flows" / "eth_spot_etf_daily.csv", index=False)

    exit_code = main(["--output-dir", str(reports_dir / "multi_asset_data" / "test-run")])
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "Markets audited:" in output
    assert "Common windows" in output
    assert (reports_dir / "multi_asset_data" / "test-run" / "summary.json").exists()
    assert (reports_dir / "multi_asset_data" / "test-run" / "manifest.json").exists()

    get_settings.cache_clear()
