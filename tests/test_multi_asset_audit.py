from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from alpha_x.external_data.reporting import compute_external_coverage, export_coverage_report
from alpha_x.multi_asset.dataset import load_multi_asset_ohlcv


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


def _funding(start_ms: int, periods: int) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp_ms": [start_ms + i * 8 * 3_600_000 for i in range(periods)],
            "funding_rate": [0.0001] * periods,
        }
    )


def _etf(start_ms: int, days: int) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp_ms": [start_ms + i * 24 * 3_600_000 for i in range(days)],
            "effective_timestamp_ms": [start_ms + (i + 1) * 24 * 3_600_000 for i in range(days)],
            "btc_etf_flow_usd": [10_000_000.0] * days,
        }
    )


def test_compute_and_export_coverage_report(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    _write_ohlcv(raw_dir / "bitvavo" / "btc-eur_1h.csv", 72)
    _write_ohlcv(raw_dir / "bitvavo" / "eth-eur_1h.csv", 60, start_ms=12 * 3_600_000)

    dataset = load_multi_asset_ohlcv(raw_dir, markets=["BTC-EUR", "ETH-EUR"])
    report = compute_external_coverage(
        dataset=dataset,
        funding_frames={"BTC": _funding(0, 20), "ETH": _funding(0, 20)},
        etf_flow_frames={"BTC_SPOT_ETF": _etf(0, 5), "ETH_SPOT_ETF": pd.DataFrame()},
        global_etf_key="BTC_SPOT_ETF",
        run_id="test-run",
    )

    assert report.common_window_ohlcv_start is not None
    assert report.comparable_in_common_ohlcv
    assert report.known_limitations

    paths = export_coverage_report(report, tmp_path / "reports")
    assert paths["summary"].exists()
    assert paths["coverage_csv"].exists()
    assert paths["common_windows_csv"].exists()
    assert paths["comparability_csv"].exists()
    summary = json.loads(paths["summary"].read_text(encoding="utf-8"))
    assert "known_limitations" in summary
    assert "common_windows" in summary
