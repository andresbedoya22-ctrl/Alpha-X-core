from __future__ import annotations

from pathlib import Path

import pandas as pd

from alpha_x.data.truth_engine_data import run_truth_engine_data_batch
from alpha_x.truth_engine.comparison import TruthEngineConfig, run_truth_engine


class StubBitvavoClient:
    max_candles_per_request = 1000

    def __init__(self, frames: dict[str, pd.DataFrame]) -> None:
        self.frames = frames

    def list_markets(self) -> list[str]:
        return sorted(self.frames)

    def fetch_candles(
        self,
        market: str,
        interval: str,
        limit: int,
        start: int | None = None,
        end: int | None = None,
    ) -> pd.DataFrame:
        frame = self.frames.get(market, pd.DataFrame())
        if frame.empty:
            return frame
        filtered = frame.copy()
        if start is not None:
            filtered = filtered.loc[filtered["timestamp"] >= start]
        if end is not None:
            filtered = filtered.loc[filtered["timestamp"] <= end]
        return filtered.tail(limit).reset_index(drop=True)


class SilentLogger:
    def info(self, *args: object, **kwargs: object) -> None:
        return None

    def warning(self, *args: object, **kwargs: object) -> None:
        return None

    def error(self, *args: object, **kwargs: object) -> None:
        return None


def test_truth_engine_data_batch_writes_coverage_and_summary(tmp_path: Path) -> None:
    raw_data_dir = tmp_path / "raw"
    reports_dir = tmp_path / "reports"
    client = StubBitvavoClient(
        {
            "BTC-EUR": _build_daily_frame(periods=720, seed=1),
            "ETH-EUR": _build_daily_frame(periods=520, seed=2),
        }
    )

    result = run_truth_engine_data_batch(
        client=client,
        raw_data_dir=raw_data_dir,
        reports_dir=reports_dir,
        logger=SilentLogger(),
        run_id="truth-data-test",
        markets=["BTC-EUR", "ETH-EUR", "LINK-EUR"],
        target_rows=700,
    )

    coverage = result.coverage_frame.set_index("target_market")
    assert coverage.loc["BTC-EUR", "status"] == "ready"
    assert coverage.loc["ETH-EUR", "status"] == "partial"
    assert coverage.loc["LINK-EUR", "status"] == "unavailable"
    assert result.summary["effective_universe_final"] == ["BTC-EUR", "ETH-EUR"]
    assert (result.report_dir / "asset_coverage.csv").exists()
    assert (result.report_dir / "summary.json").exists()
    assert (raw_data_dir / "bitvavo" / "btc-eur_1d.csv").exists()


def test_run_truth_engine_uses_latest_data_batch_effective_universe(tmp_path: Path) -> None:
    raw_data_dir = tmp_path / "raw"
    reports_dir = tmp_path / "reports"
    client = StubBitvavoClient(
        {
            "BTC-EUR": _build_daily_frame(periods=720, seed=1),
            "ETH-EUR": _build_daily_frame(periods=520, seed=2),
        }
    )
    run_truth_engine_data_batch(
        client=client,
        raw_data_dir=raw_data_dir,
        reports_dir=reports_dir,
        logger=SilentLogger(),
        run_id="truth-data-test",
        markets=["BTC-EUR", "ETH-EUR", "LINK-EUR"],
        target_rows=700,
    )

    result = run_truth_engine(
        raw_data_dir=raw_data_dir,
        reports_dir=reports_dir,
        run_id="truth-run",
        config=TruthEngineConfig(timeframe="1d", initial_capital=1.0),
    )

    assert result.manifest["data_batch_run_id"] == "truth-data-test"
    assert result.manifest["universe_resolved"] == ["BTC-EUR", "ETH-EUR"]
    assert "BTC-EUR" in result.eligible_markets


def _build_daily_frame(*, periods: int, seed: int) -> pd.DataFrame:
    start = pd.Timestamp("2021-01-01T00:00:00+00:00")
    rows = []
    price = 100.0 + seed
    for index in range(periods):
        dt = start + pd.Timedelta(days=index)
        price *= 1.0 + 0.001 + (((index + seed) % 9) - 4) * 0.001
        rows.append(
            {
                "timestamp": int(dt.timestamp() * 1000),
                "open": price * 0.99,
                "high": price * 1.01,
                "low": price * 0.98,
                "close": price,
                "volume": 5_000 + ((index + seed) % 13) * 250,
            }
        )
    return pd.DataFrame(rows)
