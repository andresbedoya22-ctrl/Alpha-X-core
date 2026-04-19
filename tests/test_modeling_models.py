from __future__ import annotations

import pandas as pd

from alpha_x.modeling.evaluation import build_temporal_model_splits, fit_and_evaluate_models


def _build_supervised_frame(rows: int = 240) -> tuple[pd.DataFrame, list[str], list[str]]:
    timestamps = [index * 3_600_000 for index in range(rows)]
    data = {
        "timestamp": timestamps,
        "datetime": pd.to_datetime(timestamps, unit="ms", utc=True),
        "regime": [
            "trend_up_high_vol" if index % 3 == 0 else "range_low_vol" for index in range(rows)
        ],
        "target_positive_tb": [1 if index % 5 in {0, 1} else 0 for index in range(rows)],
    }
    feature_columns = []
    for index in range(24):
        column = f"feature_{index}"
        feature_columns.append(column)
        data[column] = [((row + index) % 11) / 10.0 for row in range(rows)]

    return pd.DataFrame(data), feature_columns, ["regime"]


def test_temporal_model_splits_preserve_order() -> None:
    frame, _feature_columns, _categorical_columns = _build_supervised_frame()

    splits = build_temporal_model_splits(frame)

    assert list(splits.keys()) == ["train", "validation", "test"]
    assert splits["train"]["timestamp"].iloc[-1] < splits["validation"]["timestamp"].iloc[0]
    assert splits["validation"]["timestamp"].iloc[-1] < splits["test"]["timestamp"].iloc[0]


def test_fit_and_evaluate_models_returns_validation_and_test_metrics() -> None:
    frame, feature_columns, categorical_columns = _build_supervised_frame()

    rows, _models, best_model_id = fit_and_evaluate_models(
        frame,
        feature_columns=feature_columns,
        categorical_columns=categorical_columns,
        target_column="target_positive_tb",
    )

    assert len(rows) == 4
    assert best_model_id in {"logreg_l2", "random_forest_small"}
    assert {row.segment for row in rows} == {"validation", "test"}
