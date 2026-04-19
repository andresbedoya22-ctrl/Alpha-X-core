from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

import pandas as pd

FEATURE_BASE_COLUMNS = ("timestamp", "datetime", "open", "high", "low", "close", "volume")

FeatureBuilder = Callable[[pd.DataFrame], pd.Series]


@dataclass(frozen=True)
class FeatureDefinition:
    feature_id: str
    family: str
    description: str
    parameters: dict[str, Any]
    warmup_bars: int
    builder: FeatureBuilder

    @property
    def column_name(self) -> str:
        return self.feature_id

    def build(self, frame: pd.DataFrame) -> pd.Series:
        series = self.builder(frame)
        if not isinstance(series, pd.Series):
            raise TypeError(f"Feature builder must return a pandas Series: {self.feature_id}")
        if len(series) != len(frame):
            raise ValueError(
                f"Feature series length mismatch for {self.feature_id}: "
                f"{len(series)} != {len(frame)}"
            )
        built = series.copy()
        built.name = self.column_name
        return built


def validate_feature_input_frame(frame: pd.DataFrame) -> pd.DataFrame:
    missing_columns = sorted(set(FEATURE_BASE_COLUMNS) - set(frame.columns))
    if missing_columns:
        raise ValueError(f"Missing required feature columns: {missing_columns}")

    prepared = frame.loc[:, FEATURE_BASE_COLUMNS].copy()
    prepared["timestamp"] = pd.to_numeric(prepared["timestamp"], errors="raise").astype("int64")
    prepared["datetime"] = pd.to_datetime(prepared["datetime"], utc=True)
    for column in FEATURE_BASE_COLUMNS[2:]:
        prepared[column] = pd.to_numeric(prepared[column], errors="raise").astype("float64")

    return prepared.sort_values("timestamp", ascending=True).reset_index(drop=True)


def build_metadata_frame(definitions: Sequence[FeatureDefinition]) -> pd.DataFrame:
    records = [
        {
            "feature_id": definition.feature_id,
            "column_name": definition.column_name,
            "family": definition.family,
            "description": definition.description,
            "warmup_bars": definition.warmup_bars,
            "parameters": str(definition.parameters),
        }
        for definition in definitions
    ]
    return pd.DataFrame.from_records(records)
