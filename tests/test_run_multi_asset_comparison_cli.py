from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from scripts.run_multi_asset_comparison import main

from alpha_x.config.settings import get_settings


def test_run_multi_asset_comparison_main(
    tmp_path: Path,
    monkeypatch: object,
    capsys: object,
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

    _write_audit_summary(reports_dir)
    for market in ["BTC-EUR", "ETH-EUR", "XRP-EUR", "SOL-EUR"]:
        _write_ohlcv(raw_data_dir / "bitvavo" / f"{market.lower()}_1h.csv", periods=720)
        _write_funding(
            external_data_dir / "funding" / f"{market.split('-')[0]}USDT_funding_8h.csv",
            periods=240,
        )
    _write_etf(external_data_dir / "etf_flows" / "btc_spot_etf_daily.csv", periods=40)
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

    exit_code = main(["--run-id", "test-multi-asset-comparison"])
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "Ventana comun enriquecida:" in output
    assert "Resumen comparativo por activo:" in output
    assert "Conclusion:" in output
    assert (
        reports_dir
        / "multi_asset_comparison"
        / "test-multi-asset-comparison"
        / "summary.json"
    ).exists()
    assert (
        reports_dir
        / "multi_asset_comparison"
        / "test-multi-asset-comparison"
        / "asset_comparison.csv"
    ).exists()

    get_settings.cache_clear()


def _write_audit_summary(reports_dir: Path) -> None:
    report_dir = reports_dir / "multi_asset_data" / "20260419T000000Z"
    report_dir.mkdir(parents=True, exist_ok=True)
    summary = {
        "run_id": "20260419T000000Z",
        "markets_audited": ["BTC-EUR", "ETH-EUR", "XRP-EUR", "SOL-EUR"],
        "common_windows": {
            "ohlcv_plus_funding_plus_etf_flows": {
                "start": "2024-01-10T00:00:00+00:00",
                "end": "2024-01-25T00:00:00+00:00",
            }
        },
    }
    (report_dir / "summary.json").write_text(json.dumps(summary), encoding="utf-8")


def _write_ohlcv(path: Path, *, periods: int) -> None:
    rows = []
    start = pd.Timestamp("2024-01-01T00:00:00+00:00")
    for index in range(periods):
        dt = start + pd.Timedelta(hours=index)
        wave = ((index % 24) - 12) * 0.2
        base = 100 + index * 0.05 + wave
        rows.append(
            {
                "timestamp": int(dt.timestamp() * 1000),
                "open": base,
                "high": base + 1.0,
                "low": base - 1.0,
                "close": base + ((index % 5) - 2) * 0.1,
                "volume": 1000 + (index % 12) * 10,
            }
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_funding(path: Path, *, periods: int) -> None:
    start = pd.Timestamp("2024-01-01T00:00:00+00:00")
    rows = []
    for index in range(periods):
        dt = start + pd.Timedelta(hours=8 * index)
        rows.append(
            {
                "timestamp_ms": int(dt.timestamp() * 1000),
                "funding_rate": 0.0001 * ((index % 7) - 3),
            }
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_etf(path: Path, *, periods: int) -> None:
    start = pd.Timestamp("2024-01-01T00:00:00+00:00")
    rows = []
    for index in range(periods):
        flow_day = start + pd.Timedelta(days=index)
        effective_day = flow_day + pd.Timedelta(days=1)
        rows.append(
            {
                "timestamp_ms": int(flow_day.timestamp() * 1000),
                "date": flow_day.strftime("%Y-%m-%d"),
                "effective_timestamp_ms": int(effective_day.timestamp() * 1000),
                "effective_date": effective_day.strftime("%Y-%m-%d"),
                "series_key": "BTC_SPOT_ETF",
                "btc_etf_flow_usd": 1_000_000.0 + index * 1_000.0,
                "btc_etf_flow_usd_millions": 1.0 + index * 0.001,
            }
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)
