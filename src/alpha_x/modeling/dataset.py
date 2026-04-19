from __future__ import annotations

import pandas as pd

from alpha_x.backtest.models import LoadedBacktestDataset
from alpha_x.features.engine import build_feature_frame_for_export
from alpha_x.regime.catalog import get_default_regime_rule_set
from alpha_x.regime.rules import detect_regimes

TARGET_COLUMN = "target_positive_tb"
REGIME_COLUMN = "regime"
REGIME_VALID_COLUMN = "regime_is_valid"


def build_supervised_dataset(
    dataset: LoadedBacktestDataset,
    *,
    timeframe: str,
) -> tuple[pd.DataFrame, pd.DataFrame, list[str], list[str], dict[str, object]]:
    feature_result = build_feature_frame_for_export(
        dataset,
        timeframe=timeframe,
        join_labels=True,
    )
    regime_result = detect_regimes(
        feature_result.feature_frame,
        get_default_regime_rule_set(),
    )
    frame = regime_result.frame.copy()

    frame[TARGET_COLUMN] = pd.Series(pd.NA, index=frame.index, dtype="Int64")
    valid_labels = frame["tb_is_valid"].fillna(False)
    frame.loc[valid_labels, TARGET_COLUMN] = (
        frame.loc[valid_labels, "tb_label"].eq(1).astype("int64")
    )
    frame["supervised_is_valid"] = (
        frame["valid_feature_row"] & frame[REGIME_VALID_COLUMN] & valid_labels
    )
    frame["supervised_discard_reason"] = "valid"
    frame.loc[~frame["valid_feature_row"], "supervised_discard_reason"] = "feature_warmup"
    frame.loc[
        frame["valid_feature_row"] & ~frame[REGIME_VALID_COLUMN],
        "supervised_discard_reason",
    ] = "regime_unavailable"
    frame.loc[
        frame["valid_feature_row"] & frame[REGIME_VALID_COLUMN] & ~valid_labels,
        "supervised_discard_reason",
    ] = "label_unavailable"
    frame.loc[
        frame["valid_feature_row"]
        & frame[REGIME_VALID_COLUMN]
        & valid_labels
        & frame[TARGET_COLUMN].isna(),
        "supervised_discard_reason",
    ] = "target_missing"

    feature_columns = feature_result.feature_names
    categorical_columns = [REGIME_COLUMN]
    supervised = frame.loc[frame["supervised_is_valid"]].copy().reset_index(drop=True)
    supervised[TARGET_COLUMN] = supervised[TARGET_COLUMN].astype("int64")
    supervised["row_id"] = range(len(supervised))

    discard_counts = (
        frame.groupby("supervised_discard_reason", dropna=False)
        .size()
        .rename("rows")
        .reset_index()
        .sort_values("rows", ascending=False)
        .reset_index(drop=True)
    )
    target_distribution = (
        supervised.groupby(TARGET_COLUMN)
        .size()
        .rename("rows")
        .reset_index()
        .sort_values(TARGET_COLUMN)
        .reset_index(drop=True)
    )
    dataset_summary = {
        "target_definition": "Binary target: 1 when triple-barrier label is +1, else 0.",
        "full_rows": len(frame),
        "supervised_rows": len(supervised),
        "discarded_rows": int((~frame["supervised_is_valid"]).sum()),
        "discard_pct": 0.0 if len(frame) == 0 else float((~frame["supervised_is_valid"]).mean()),
        "discard_counts": discard_counts.to_dict(orient="records"),
        "target_distribution": target_distribution.to_dict(orient="records"),
        "feature_count": len(feature_columns),
        "categorical_columns": categorical_columns,
    }
    return frame, supervised, feature_columns, categorical_columns, dataset_summary
