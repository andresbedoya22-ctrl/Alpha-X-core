from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from alpha_x.external_data.base import ExternalDataSource, ExternalFetchResult

_BYBIT_FUNDING_URL = "https://api.bybit.com/v5/market/funding/history"
_MAX_RECORDS_PER_REQUEST = 200
FUNDING_INTERVAL_MS = 8 * 3_600_000

_LIMITATIONS = (
    "Funding rates are sourced from Bybit linear perpetual futures. "
    "Bitvavo is spot-only, so funding is used as global crypto-derivatives context rather than "
    "EUR-spot-native information. Coverage depth differs by asset and listing history. "
    "Funding is published every 8h and aligned to 1h OHLCV with a maximum carry of 8 bars."
)


class BybitFundingSource(ExternalDataSource):
    def __init__(self, storage_dir: Path, timeout: int = 30) -> None:
        super().__init__(storage_dir / "funding", timeout)
        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json"})

    @property
    def source_name(self) -> str:
        return "Bybit V5 funding history"

    def fetch(
        self,
        key: str,
        logger: logging.Logger,
        start_ms: int | None = None,
        end_ms: int | None = None,
    ) -> ExternalFetchResult:
        csv_path = self._build_csv_path(f"{key}_funding_8h.csv")
        existing = self._load_csv(csv_path)
        rows_existing = len(existing)

        cursor_end = end_ms
        if not existing.empty and cursor_end is None:
            cursor_end = int(existing["timestamp_ms"].iloc[0]) - 1

        all_batches: list[pd.DataFrame] = []
        request_count = 0

        while True:
            params: dict[str, Any] = {
                "category": "linear",
                "symbol": key,
                "limit": _MAX_RECORDS_PER_REQUEST,
            }
            if cursor_end is not None:
                params["endTime"] = int(cursor_end)

            try:
                response = self._session.get(
                    _BYBIT_FUNDING_URL, params=params, timeout=self.timeout
                )
                response.raise_for_status()
            except requests.RequestException as exc:
                logger.warning("Bybit funding request failed for %s: %s", key, exc)
                break

            payload = response.json()
            request_count += 1
            rows = payload.get("result", {}).get("list", [])
            if payload.get("retCode") != 0:
                logger.warning("Bybit funding returned error for %s: %s", key, payload)
                break
            if not rows:
                logger.info(
                    "Bybit funding: no more rows for %s after %s requests", key, request_count
                )
                break

            batch = self._parse_payload(rows)
            all_batches.append(batch)
            oldest_in_batch = int(batch["timestamp_ms"].iloc[0])
            logger.info(
                "Funding %s batch %s: %s rows (oldest=%s)",
                key,
                request_count,
                len(batch),
                pd.Timestamp(oldest_in_batch, unit="ms", tz="UTC"),
            )

            if start_ms is not None and oldest_in_batch <= start_ms:
                logger.info("Funding %s reached start boundary", key)
                break
            if len(rows) < _MAX_RECORDS_PER_REQUEST:
                logger.info("Funding %s reached historical limit", key)
                break

            cursor_end = oldest_in_batch - 1
            time.sleep(0.2)

        downloaded = (
            pd.concat(all_batches, ignore_index=True)
            if all_batches
            else pd.DataFrame(columns=["timestamp_ms", "funding_rate"])
        )
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
            frequency="8h",
            source_url=_BYBIT_FUNDING_URL,
            limitations=_LIMITATIONS,
        )

    def load(self, key: str) -> pd.DataFrame:
        return self._load_csv(self._build_csv_path(f"{key}_funding_8h.csv"))

    @staticmethod
    def _parse_payload(rows: list[dict]) -> pd.DataFrame:
        frame = pd.DataFrame(
            [
                {
                    "timestamp_ms": int(row["fundingRateTimestamp"]),
                    "funding_rate": float(row["fundingRate"]),
                }
                for row in rows
            ]
        )
        return frame.sort_values("timestamp_ms").reset_index(drop=True)

    @staticmethod
    def _load_csv(path: Path) -> pd.DataFrame:
        if not path.exists():
            return pd.DataFrame(columns=["timestamp_ms", "funding_rate"])
        frame = pd.read_csv(path)
        if frame.empty:
            return pd.DataFrame(columns=["timestamp_ms", "funding_rate"])
        frame["timestamp_ms"] = frame["timestamp_ms"].astype(int)
        frame["funding_rate"] = frame["funding_rate"].astype(float)
        return frame.sort_values("timestamp_ms").reset_index(drop=True)
