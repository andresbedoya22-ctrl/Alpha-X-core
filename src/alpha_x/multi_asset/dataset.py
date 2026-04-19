from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from alpha_x.data.ohlcv_storage import build_ohlcv_csv_path, load_ohlcv_csv
from alpha_x.data.ohlcv_validation import (
    OhlcvValidationReport,
    summarize_gaps,
    validate_temporal_integrity,
)
from alpha_x.multi_asset.config import OFFICIAL_INTERVAL, OFFICIAL_MARKETS
from alpha_x.multi_asset.markets import get_market_info


@dataclass
class MarketOhlcvInfo:
    market: str
    csv_path: Path
    frame: pd.DataFrame
    row_count: int
    start_ts: int | None
    end_ts: int | None
    start_dt: pd.Timestamp | None
    end_dt: pd.Timestamp | None
    validation: OhlcvValidationReport
    gap_count: int
    missing_intervals: int
    available: bool


@dataclass
class MultiAssetDataset:
    markets: list[str]
    interval: str
    results: dict[str, MarketOhlcvInfo] = field(default_factory=dict)

    @property
    def available_markets(self) -> list[str]:
        return [market for market, result in self.results.items() if result.available]

    @property
    def common_window(self) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
        starts = [
            result.start_dt
            for result in self.results.values()
            if result.available and result.start_dt
        ]
        ends = [
            result.end_dt for result in self.results.values() if result.available and result.end_dt
        ]
        if len(starts) < 2 or len(ends) < 2:
            return None, None
        return max(starts), min(ends)

    def comparable_in_common_window(self) -> bool:
        start, end = self.common_window
        if start is None or end is None:
            return False
        return start < end and len(self.available_markets) == len(self.markets)

    def depth_report(self) -> list[dict]:
        rows: list[dict] = []
        for market in self.markets:
            info = self.results.get(market)
            if info is None or not info.available:
                rows.append(
                    {
                        "market": market,
                        "rows": 0,
                        "start": None,
                        "end": None,
                        "gaps": 0,
                        "missing_intervals": 0,
                        "available": False,
                    }
                )
                continue
            rows.append(
                {
                    "market": market,
                    "rows": info.row_count,
                    "start": str(info.start_dt) if info.start_dt else None,
                    "end": str(info.end_dt) if info.end_dt else None,
                    "gaps": info.gap_count,
                    "missing_intervals": info.missing_intervals,
                    "available": True,
                }
            )
        return rows


def load_multi_asset_ohlcv(
    raw_data_dir: Path,
    markets: list[str] | None = None,
    interval: str | None = None,
) -> MultiAssetDataset:
    markets = markets or OFFICIAL_MARKETS
    interval = interval or OFFICIAL_INTERVAL
    dataset = MultiAssetDataset(markets=markets, interval=interval)

    for market in markets:
        market_info = get_market_info(market)
        csv_path = build_ohlcv_csv_path(
            raw_data_dir,
            exchange=market_info.exchange,
            market=market,
            timeframe=interval,
        )
        frame = load_ohlcv_csv(csv_path)

        if frame.empty:
            dataset.results[market] = MarketOhlcvInfo(
                market=market,
                csv_path=csv_path,
                frame=frame,
                row_count=0,
                start_ts=None,
                end_ts=None,
                start_dt=None,
                end_dt=None,
                validation=validate_temporal_integrity(frame, interval),
                gap_count=0,
                missing_intervals=0,
                available=False,
            )
            continue

        validation = validate_temporal_integrity(frame, interval)
        gap_summary = summarize_gaps(validation)
        enriched = frame.copy()
        enriched["market"] = market
        enriched["asset"] = market_info.base_asset
        enriched["exchange"] = market_info.exchange
        enriched["timeframe"] = interval

        start_ts = int(frame["timestamp"].iloc[0])
        end_ts = int(frame["timestamp"].iloc[-1])
        dataset.results[market] = MarketOhlcvInfo(
            market=market,
            csv_path=csv_path,
            frame=enriched,
            row_count=len(frame),
            start_ts=start_ts,
            end_ts=end_ts,
            start_dt=pd.Timestamp(start_ts, unit="ms", tz="UTC"),
            end_dt=pd.Timestamp(end_ts, unit="ms", tz="UTC"),
            validation=validation,
            gap_count=len(validation.gaps),
            missing_intervals=gap_summary.total_missing_intervals,
            available=True,
        )

    return dataset
