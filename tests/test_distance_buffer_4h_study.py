from __future__ import annotations

import pandas as pd
import pytest

from alpha_x.research_4h.distance_buffer_study import (
    CostModel,
    StrategyConfig,
    build_distance_buffer_targets,
    build_trade_log,
    derive_4h_from_1h,
)


def _write_1h(path, closes: list[float]) -> None:
    frame = pd.DataFrame(
        {
            "timestamp": [index * 3_600_000 for index in range(len(closes))],
            "open": closes,
            "high": [close + 1 for close in closes],
            "low": [close - 1 for close in closes],
            "close": closes,
            "volume": [10.0] * len(closes),
        }
    )
    frame.to_csv(path, index=False)


def test_derive_4h_from_1h_uses_only_complete_buckets(tmp_path) -> None:
    path = tmp_path / "btc-eur_1h.csv"
    _write_1h(path, [100, 101, 102, 103, 104, 105])

    frame, quality = derive_4h_from_1h(path)

    assert len(frame) == 1
    assert frame["timestamp"].tolist() == [0]
    assert frame["open"].iloc[0] == 100
    assert frame["high"].iloc[0] == 104
    assert frame["low"].iloc[0] == 99
    assert frame["close"].iloc[0] == 103
    assert frame["volume"].iloc[0] == 40
    assert quality.incomplete_4h_bins_dropped == 1


def test_4h_trade_log_uses_next_bar_execution() -> None:
    timestamps = [index * 4 * 3_600_000 for index in range(132)]
    closes = [100.0] * 125 + [110.0, 120.0, 130.0, 90.0, 80.0, 70.0, 60.0]
    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "datetime": pd.to_datetime(timestamps, unit="ms", utc=True),
            "open": closes,
            "high": closes,
            "low": closes,
            "close": closes,
            "volume": [1.0] * len(closes),
        }
    )

    trades, _ = build_trade_log(
        frame,
        StrategyConfig(
            name="Distance buffer 4H corrected",
            timeframe="4h",
            sma_window=125,
            buffer=0.03,
        ),
        cost_model=CostModel("zero", fee_per_side=0.0, slippage_per_side=0.0),
    )

    assert len(trades) == 1
    trade = trades.iloc[0]
    assert trade["entry_price_execution"] == pytest.approx(120.0)
    assert trade["exit_price_execution"] == pytest.approx(80.0)


def test_persistence_requires_consecutive_entry_and_exit_conditions() -> None:
    timestamps = [index * 4 * 3_600_000 for index in range(132)]
    closes = [100.0] * 125 + [110.0, 100.0, 112.0, 113.0, 90.0, 89.0, 120.0]
    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "datetime": pd.to_datetime(timestamps, unit="ms", utc=True),
            "open": closes,
            "high": closes,
            "low": closes,
            "close": closes,
            "volume": [1.0] * len(closes),
        }
    )

    targets = build_distance_buffer_targets(
        frame,
        StrategyConfig(
            name="Distance buffer 4H corrected",
            timeframe="4h",
            sma_window=125,
            buffer=0.03,
            entry_persistence=2,
            exit_persistence=2,
        ),
    )

    assert targets["target_exposure"].iloc[125] == 0.0
    assert targets["target_exposure"].iloc[127] == 0.0
    assert targets["target_exposure"].iloc[128] == 1.0
    assert targets["target_exposure"].iloc[129] == 1.0
    assert targets["target_exposure"].iloc[130] == 0.0
