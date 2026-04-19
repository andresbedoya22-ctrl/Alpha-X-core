from __future__ import annotations

from typing import Any

import pandas as pd
import requests

from alpha_x.data.ohlcv_models import OhlcvRecord, normalize_ohlcv_frame


class BitvavoClient:
    max_candles_per_request = 1000

    def __init__(self, base_url: str, timeout: int = 30) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

    def fetch_candles(
        self,
        market: str,
        interval: str,
        limit: int,
        start: int | None = None,
        end: int | None = None,
    ) -> pd.DataFrame:
        if limit <= 0:
            raise ValueError("limit must be positive.")
        if limit > self.max_candles_per_request:
            raise ValueError(
                f"limit exceeds Bitvavo max_candles_per_request={self.max_candles_per_request}"
            )

        params: dict[str, Any] = {"interval": interval, "limit": limit}
        if start is not None:
            params["start"] = start
        if end is not None:
            params["end"] = end

        payload = self._get_json(f"/{market}/candles", params=params)
        if not isinstance(payload, list):
            raise RuntimeError("Unexpected Bitvavo response format for candles endpoint.")

        records = [self._to_record(row) for row in payload]
        frame = pd.DataFrame([record.__dict__ for record in records])
        return normalize_ohlcv_frame(frame)

    def list_markets(self) -> list[str]:
        payload = self._get_json("/markets")
        if not isinstance(payload, list):
            raise RuntimeError("Unexpected Bitvavo response format for markets endpoint.")
        markets: list[str] = []
        for row in payload:
            if isinstance(row, dict) and "market" in row:
                markets.append(str(row["market"]).upper())
        return sorted(set(markets))

    def _get_json(self, path: str, *, params: dict[str, Any] | None = None) -> Any:
        try:
            response = self.session.get(
                f"{self.base_url}{path}",
                params=params,
                timeout=self.timeout,
            )
            response.raise_for_status()
        except requests.HTTPError as error:
            message = response.text.strip() or str(error)
            raise RuntimeError(f"Bitvavo request failed: {message}") from error
        except requests.RequestException as error:
            raise RuntimeError(f"Bitvavo request failed: {error}") from error
        return response.json()

    @staticmethod
    def _to_record(row: list[str]) -> OhlcvRecord:
        if len(row) != 6:
            raise RuntimeError(f"Unexpected OHLCV row length from Bitvavo: {row}")

        return OhlcvRecord(
            timestamp=int(row[0]),
            open=float(row[1]),
            high=float(row[2]),
            low=float(row[3]),
            close=float(row[4]),
            volume=float(row[5]),
        )
