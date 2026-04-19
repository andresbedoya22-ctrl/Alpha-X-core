from __future__ import annotations

import logging
import math
import re
from pathlib import Path

import pandas as pd
import requests

from alpha_x.external_data.base import ExternalDataSource, ExternalFetchResult

_BITBO_BTC_ETF_FLOWS_URL = "https://bitbo.io/treasuries/etf-flows/"
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; research-bot/1.0)",
    "Accept": "text/html,application/xhtml+xml",
}

_LIMITATIONS = (
    "ETF flows are sourced from Bitbo's published BTC spot ETF history page. "
    "This yields BTC ETF daily flow context only. "
    "ETH ETF flows are not ingested in this phase because a stable free no-auth historical source "
    "was not confirmed for automated use. XRP and SOL have no "
    "equivalent spot ETF flow series. The BTC ETF flow series is therefore reused as global crypto "
    "institutional sentiment context for all assets. "
    "Daily flows are aligned to 1h OHLCV from the next "
    "UTC day onward to avoid leakage."
)


class BitboEtfFlowSource(ExternalDataSource):
    def __init__(self, storage_dir: Path, timeout: int = 30) -> None:
        super().__init__(storage_dir / "etf_flows", timeout)
        self._session = requests.Session()
        self._session.headers.update(_HEADERS)

    @property
    def source_name(self) -> str:
        return "Bitbo BTC ETF flows"

    def fetch(
        self,
        key: str,
        logger: logging.Logger,
        start_ms: int | None = None,
        end_ms: int | None = None,
    ) -> ExternalFetchResult:
        del end_ms
        csv_path = self._build_csv_path(f"{key.lower()}_daily.csv")
        existing = self._load_csv(csv_path)
        rows_existing = len(existing)

        if key != "BTC_SPOT_ETF":
            if not csv_path.exists():
                pd.DataFrame(columns=self._columns()).to_csv(csv_path, index=False)
            logger.info(
                "ETF flow series %s unavailable in this phase; keeping empty placeholder.", key
            )
            return self._empty_result(key, csv_path, rows_existing)

        try:
            response = self._session.get(_BITBO_BTC_ETF_FLOWS_URL, timeout=self.timeout)
            response.raise_for_status()
        except requests.RequestException as exc:
            logger.warning("Bitbo ETF flow request failed for %s: %s", key, exc)
            return self._empty_result(key, csv_path, rows_existing)

        try:
            downloaded = self._parse_bitbo_history(response.text, key)
        except ValueError as exc:
            logger.warning("Bitbo ETF flow parse failed for %s: %s", key, exc)
            return self._empty_result(key, csv_path, rows_existing)

        if start_ms is not None and not downloaded.empty:
            downloaded = downloaded.loc[downloaded["timestamp_ms"] >= start_ms].reset_index(
                drop=True
            )

        merged = self._merge_frames(existing, downloaded, ts_col="timestamp_ms")
        merged.to_csv(csv_path, index=False)

        start_dt = end_dt = None
        if not merged.empty:
            start_dt = pd.Timestamp(int(merged["timestamp_ms"].iloc[0]), unit="ms", tz="UTC")
            end_dt = pd.Timestamp(int(merged["timestamp_ms"].iloc[-1]), unit="ms", tz="UTC")

        return ExternalFetchResult(
            source_name=self.source_name,
            key=key,
            csv_path=csv_path,
            rows_downloaded=len(downloaded),
            rows_existing=rows_existing,
            rows_final=len(merged),
            rows_added=len(merged) - rows_existing,
            start_dt=start_dt,
            end_dt=end_dt,
            frequency="1d",
            source_url=_BITBO_BTC_ETF_FLOWS_URL,
            limitations=_LIMITATIONS,
        )

    def load(self, key: str) -> pd.DataFrame:
        return self._load_csv(self._build_csv_path(f"{key.lower()}_daily.csv"))

    @staticmethod
    def _parse_bitbo_history(html: str, key: str) -> pd.DataFrame:
        marker = "const historyUsd = ["
        start = html.find(marker)
        if start == -1:
            raise ValueError("historyUsd array not found")
        end = html.find("];", start)
        if end == -1:
            raise ValueError("historyUsd array terminator not found")
        block = html[start:end]

        pattern = re.compile(
            r"getPreviousBusinessDay\((?P<ts>\d+)\)\s*,\s*truncate\((?P<a>-?\d+(?:\.\d+)?)\s*\*\s*(?P<b>-?\d+(?:\.\d+)?)\s*,\s*2\)"
        )
        rows: list[dict] = []
        for match in pattern.finditer(block):
            raw_timestamp_ms = int(match.group("ts"))
            flow_date = BitboEtfFlowSource._previous_business_day_utc(raw_timestamp_ms)
            effective_date = flow_date + pd.Timedelta(days=1)
            flow_usd = math.floor(float(match.group("a")) * float(match.group("b")) * 100) / 100
            rows.append(
                {
                    "timestamp_ms": int(flow_date.timestamp() * 1000),
                    "date": flow_date.strftime("%Y-%m-%d"),
                    "effective_timestamp_ms": int(effective_date.timestamp() * 1000),
                    "effective_date": effective_date.strftime("%Y-%m-%d"),
                    "series_key": key,
                    "btc_etf_flow_usd": flow_usd,
                    "btc_etf_flow_usd_millions": flow_usd / 1_000_000,
                }
            )

        if not rows:
            raise ValueError("no rows parsed from Bitbo history")
        return pd.DataFrame(rows).sort_values("timestamp_ms").reset_index(drop=True)

    @staticmethod
    def _previous_business_day_utc(timestamp_ms: int) -> pd.Timestamp:
        date = pd.Timestamp(timestamp_ms, unit="ms", tz="UTC").normalize()
        weekday = date.weekday()
        if weekday == 6:
            return date - pd.Timedelta(days=2)
        if weekday == 0:
            return date - pd.Timedelta(days=3)
        return date - pd.Timedelta(days=1)

    def _empty_result(self, key: str, csv_path: Path, rows_existing: int) -> ExternalFetchResult:
        existing = self._load_csv(csv_path)
        start_dt = end_dt = None
        if not existing.empty:
            start_dt = pd.Timestamp(int(existing["timestamp_ms"].iloc[0]), unit="ms", tz="UTC")
            end_dt = pd.Timestamp(int(existing["timestamp_ms"].iloc[-1]), unit="ms", tz="UTC")

        return ExternalFetchResult(
            source_name=self.source_name,
            key=key,
            csv_path=csv_path,
            rows_downloaded=0,
            rows_existing=rows_existing,
            rows_final=len(existing),
            rows_added=0,
            start_dt=start_dt,
            end_dt=end_dt,
            frequency="1d",
            source_url=_BITBO_BTC_ETF_FLOWS_URL,
            limitations=_LIMITATIONS,
        )

    @staticmethod
    def _load_csv(path: Path) -> pd.DataFrame:
        if not path.exists():
            return pd.DataFrame(columns=BitboEtfFlowSource._columns())
        frame = pd.read_csv(path)
        if frame.empty:
            return pd.DataFrame(columns=BitboEtfFlowSource._columns())
        frame["timestamp_ms"] = frame["timestamp_ms"].astype(int)
        if "effective_timestamp_ms" in frame.columns:
            frame["effective_timestamp_ms"] = frame["effective_timestamp_ms"].astype(int)
        return frame.sort_values("timestamp_ms").reset_index(drop=True)

    @staticmethod
    def _columns() -> list[str]:
        return [
            "timestamp_ms",
            "date",
            "effective_timestamp_ms",
            "effective_date",
            "series_key",
            "btc_etf_flow_usd",
            "btc_etf_flow_usd_millions",
        ]
