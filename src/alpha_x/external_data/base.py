from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass
class ExternalFetchResult:
    """Result of fetching one external data series."""

    source_name: str
    key: str
    """Logical key for this series, e.g. 'BTCUSDT' or 'BTC_SPOT_ETF'."""

    csv_path: Path
    rows_downloaded: int
    rows_existing: int
    rows_final: int
    rows_added: int
    start_dt: pd.Timestamp | None
    end_dt: pd.Timestamp | None
    frequency: str
    """Human-readable frequency, e.g. '8h' or '1d'."""

    source_url: str
    limitations: str
    """Explicit statement of data limitations and caveats."""


class ExternalDataSource(ABC):
    """Base class for all external / contextual data sources.

    Implementors must:
    - Store data in a single CSV per logical series, deduplicated on timestamp.
    - Never use future data to fill past values.
    - Document source, frequency, and limitations.
    """

    def __init__(self, storage_dir: Path, timeout: int = 30) -> None:
        self.storage_dir = storage_dir
        self.timeout = timeout
        self.storage_dir.mkdir(parents=True, exist_ok=True)

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Human-readable source name."""

    @abstractmethod
    def fetch(
        self,
        key: str,
        logger: logging.Logger,
        start_ms: int | None = None,
        end_ms: int | None = None,
    ) -> ExternalFetchResult:
        """Download and persist external data for `key` (e.g. asset symbol or ETF ticker)."""

    @abstractmethod
    def load(self, key: str) -> pd.DataFrame:
        """Load previously persisted data for `key`. Returns empty DataFrame if not available."""

    def _build_csv_path(self, filename: str) -> Path:
        return self.storage_dir / filename

    @staticmethod
    def _merge_frames(existing: pd.DataFrame, incoming: pd.DataFrame, ts_col: str) -> pd.DataFrame:
        """Merge two frames on ts_col, deduplicating and sorting ascending."""
        if existing.empty:
            return incoming.sort_values(ts_col).reset_index(drop=True)
        if incoming.empty:
            return existing.sort_values(ts_col).reset_index(drop=True)
        merged = pd.concat([existing, incoming], ignore_index=True)
        merged = merged.drop_duplicates(subset=[ts_col], keep="last")
        return merged.sort_values(ts_col).reset_index(drop=True)
