from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from alpha_x.truth_engine.comparison import TruthEngineConfig, run_truth_engine

OFFICIAL_SAMPLE_MARKETS = ["BTC-EUR", "ETH-EUR", "SOL-EUR", "XRP-EUR"]
MISSING_MARKETS = [
    "LINK-EUR",
    "ADA-EUR",
    "AVAX-EUR",
    "DOT-EUR",
    "LTC-EUR",
    "UNI-EUR",
    "AAVE-EUR",
    "ATOM-EUR",
    "XLM-EUR",
    "BCH-EUR",
]


def test_run_truth_engine_smoke(tmp_path: Path) -> None:
    raw_data_dir = tmp_path / "raw"
    reports_dir = tmp_path / "reports"
    _write_data_summary(reports_dir)
    for market in OFFICIAL_SAMPLE_MARKETS:
        _write_daily_ohlcv(
            raw_data_dir / "bitvavo" / f"{market.lower()}_1d.csv", periods=520, seed=len(market)
        )

    result = run_truth_engine(
        raw_data_dir=raw_data_dir,
        reports_dir=reports_dir,
        run_id="truth-smoke",
        config=TruthEngineConfig(
            timeframe="1d", initial_capital=1.0, fee_rate=0.0025, slippage_rate=0.0005
        ),
    )

    assert result.run_id == "truth-smoke"
    assert "BTC-EUR" in result.eligible_markets
    assert result.manifest["universe_resolved"] == OFFICIAL_SAMPLE_MARKETS
    assert result.skipped_markets == []
    assert (reports_dir / "truth_engine" / "truth-smoke" / "summary.json").exists()
    assert not result.comparison_frame.empty
    assert {"family", "benchmark"}.issubset(set(result.comparison_frame["source_type"]))


def _write_data_summary(reports_dir: Path) -> None:
    report_dir = reports_dir / "truth_engine_data" / "20260419T000000Z"
    report_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "run_id": "20260419T000000Z",
        "timeframe": "1d",
        "effective_universe_final": OFFICIAL_SAMPLE_MARKETS,
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
        volume = 8000 + ((index + seed) % 17) * 300
        rows.append(
            {
                "timestamp": int(dt.timestamp() * 1000),
                "open": price * 0.99,
                "high": price * 1.01,
                "low": price * 0.98,
                "close": price,
                "volume": volume,
            }
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)
