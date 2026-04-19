from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class SupervisedDatasetResult:
    full_frame: pd.DataFrame
    supervised_frame: pd.DataFrame
    feature_columns: list[str]
    categorical_columns: list[str]
    target_column: str
    dataset_summary: dict[str, Any]


@dataclass(frozen=True)
class ModelSpec:
    model_id: str
    name: str
    family: str
    description: str
    parameters: dict[str, Any]
    use_scaling: bool
    estimator: Any


@dataclass(frozen=True)
class ModelEvaluationRow:
    model_id: str
    model_name: str
    segment: str
    rows: int
    positive_rate: float
    balanced_accuracy: float
    macro_f1: float
    precision_positive: float
    recall_positive: float
    predicted_positive_rate: float
    confusion_tn: int
    confusion_fp: int
    confusion_fn: int
    confusion_tp: int
