from __future__ import annotations

from typing import Any

import pandas as pd
import requests

from alpha_x.data.ohlcv_models import OhlcvRecord, normalize_ohlcv_frame


class BitvavoClient:
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
        params: dict[str, Any] = {"interval": interval, "limit": limit}
        if start is not None:
            params["start"] = start
        if end is not None:
            params["end"] = end

        try:
            response = self.session.get(
                f"{self.base_url}/{market}/candles",
                params=params,
                timeout=self.timeout,
            )
            response.raise_for_status()
        except requests.HTTPError as error:
            message = response.text.strip() or str(error)
            raise RuntimeError(f"Bitvavo request failed: {message}") from error
        except requests.RequestException as error:
            raise RuntimeError(f"Bitvavo request failed: {error}") from error

        payload = response.json()
        if not isinstance(payload, list):
            raise RuntimeError("Unexpected Bitvavo response format for candles endpoint.")

        records = [self._to_record(row) for row in payload]
        frame = pd.DataFrame([record.__dict__ for record in records])
        return normalize_ohlcv_frame(frame)

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
