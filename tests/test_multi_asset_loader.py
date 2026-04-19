from __future__ import annotations

from pathlib import Path

import pandas as pd

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


def test_load_multi_asset_ohlcv_includes_asset_columns(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    _write_ohlcv(raw_dir / "bitvavo" / "btc-eur_1h.csv", 10)
    _write_ohlcv(raw_dir / "bitvavo" / "eth-eur_1h.csv", 8, start_ms=2 * 3_600_000)

    dataset = load_multi_asset_ohlcv(raw_dir, markets=["BTC-EUR", "ETH-EUR"])
    btc_frame = dataset.results["BTC-EUR"].frame

    assert dataset.results["BTC-EUR"].available
    assert dataset.results["ETH-EUR"].available
    assert {"market", "asset", "exchange", "timeframe"}.issubset(btc_frame.columns)
    assert btc_frame["asset"].iloc[0] == "BTC"


def test_common_window_uses_intersection(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    _write_ohlcv(raw_dir / "bitvavo" / "btc-eur_1h.csv", 10, start_ms=0)
    _write_ohlcv(raw_dir / "bitvavo" / "eth-eur_1h.csv", 6, start_ms=4 * 3_600_000)

    dataset = load_multi_asset_ohlcv(raw_dir, markets=["BTC-EUR", "ETH-EUR"])
    start, end = dataset.common_window

    assert start == pd.Timestamp(4 * 3_600_000, unit="ms", tz="UTC")
    assert end == pd.Timestamp(9 * 3_600_000, unit="ms", tz="UTC")
