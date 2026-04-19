from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from alpha_x.backtest.data_loader import load_backtest_dataset
from alpha_x.backtest.models import LoadedBacktestDataset
from alpha_x.features.base import (
    FeatureDefinition,
    build_metadata_frame,
    validate_feature_input_frame,
)
from alpha_x.features.catalog import get_feature_catalog
from alpha_x.labeling.catalog import get_labeling_catalog
from alpha_x.reporting.io import (
    create_report_directory,
    list_report_files,
    write_json_file,
    write_table_csv,
)
from alpha_x.reporting.run_manifest import build_run_manifest
from alpha_x.reporting.serializers import serialize_value


@dataclass(frozen=True)
class FeatureEngineResult:
    feature_frame: pd.DataFrame
    feature_names: list[str]
    metadata_frame: pd.DataFrame
    summary: dict[str, Any]


def build_dataset_context(dataset: LoadedBacktestDataset) -> dict[str, Any]:
    dataset_info = dataset.dataset_info
    gap_summary = dataset.gap_summary
    return {
        "path": dataset_info.path,
        "market": dataset_info.market,
        "timeframe": dataset_info.timeframe,
        "rows": dataset_info.row_count,
        "start_timestamp": dataset_info.start_timestamp,
        "end_timestamp": dataset_info.end_timestamp,
        "gap_count": gap_summary.gap_count,
        "total_missing_intervals": gap_summary.total_missing_intervals,
        "largest_gap": gap_summary.largest_gap,
    }


def run_feature_engine(
    frame: pd.DataFrame,
    *,
    catalog: list[FeatureDefinition] | None = None,
) -> FeatureEngineResult:
    definitions = catalog or get_feature_catalog()
    prepared = validate_feature_input_frame(frame)
    feature_table = prepared.copy()

    for definition in definitions:
        feature_table[definition.column_name] = definition.build(prepared)

    feature_names = [definition.column_name for definition in definitions]
    feature_table["valid_feature_row"] = feature_table[feature_names].notna().all(axis=1)
    valid_rows = int(feature_table["valid_feature_row"].sum())
    total_rows = len(feature_table)

    summary = {
        "total_rows": total_rows,
        "valid_rows": valid_rows,
        "warmup_rows": total_rows - valid_rows,
        "warmup_loss_pct": 0.0 if total_rows == 0 else (total_rows - valid_rows) / total_rows,
        "feature_count": len(feature_names),
        "feature_names": feature_names,
        "feature_families": sorted({definition.family for definition in definitions}),
        "max_warmup_bars": max((definition.warmup_bars for definition in definitions), default=0),
        "range_start_timestamp": int(feature_table["timestamp"].iloc[0]) if total_rows else None,
        "range_end_timestamp": int(feature_table["timestamp"].iloc[-1]) if total_rows else None,
        "range_start_datetime": (
            feature_table["datetime"].iloc[0].isoformat() if total_rows else None
        ),
        "range_end_datetime": (
            feature_table["datetime"].iloc[-1].isoformat() if total_rows else None
        ),
    }
    return FeatureEngineResult(
        feature_frame=feature_table,
        feature_names=feature_names,
        metadata_frame=build_metadata_frame(definitions),
        summary=summary,
    )


def join_triple_barrier_labels(
    feature_frame: pd.DataFrame,
    dataset: LoadedBacktestDataset,
    *,
    timeframe: str,
) -> pd.DataFrame:
    triple_barrier = next(
        definition for definition in get_labeling_catalog() if definition.method == "triple_barrier"
    )
    labels = triple_barrier.build_labels(dataset.frame, timeframe=timeframe)
    label_columns = [
        "timestamp",
        "labeling_id",
        "labeling_name",
        "method",
        "label",
        "is_valid",
        "discard_reason",
        "hit_barrier",
        "hit_timestamp",
        "event_return",
        "parameters",
    ]
    renamed = labels.loc[:, label_columns].rename(
        columns={
            "labeling_id": "tb_labeling_id",
            "labeling_name": "tb_labeling_name",
            "method": "tb_method",
            "label": "tb_label",
            "is_valid": "tb_is_valid",
            "discard_reason": "tb_discard_reason",
            "hit_barrier": "tb_hit_barrier",
            "hit_timestamp": "tb_hit_timestamp",
            "event_return": "tb_event_return",
            "parameters": "tb_parameters",
        }
    )
    return feature_frame.merge(renamed, on="timestamp", how="left")


def load_feature_dataset(path: Path, timeframe: str) -> LoadedBacktestDataset:
    return load_backtest_dataset(path, timeframe)


def export_feature_report(
    *,
    reports_dir: Path,
    run_id: str,
    created_at: pd.Timestamp,
    dataset: LoadedBacktestDataset,
    engine_result: FeatureEngineResult,
    parameters: dict[str, Any],
) -> Path:
    dataset_context = build_dataset_context(dataset)
    report_dir = create_report_directory(reports_dir, "features", run_id)

    summary_payload = {
        "run_id": run_id,
        "report_type": "features",
        "created_at": serialize_value(created_at),
        "dataset": serialize_value(dataset_context),
        "parameters": serialize_value(parameters),
        "summary": serialize_value(engine_result.summary),
        "features": serialize_value(engine_result.metadata_frame.to_dict(orient="records")),
    }
    write_json_file(report_dir / "summary.json", summary_payload)
    write_table_csv(report_dir / "feature_table.csv", engine_result.feature_frame)
    write_table_csv(report_dir / "feature_catalog.csv", engine_result.metadata_frame)

    artifacts = list_report_files(report_dir)
    manifest = build_run_manifest(
        run_id=run_id,
        report_type="features",
        created_at=created_at,
        dataset=dataset_context,
        parameters=parameters,
        summary_rows=[engine_result.summary],  # type: ignore[list-item]
        artifacts=artifacts + ["manifest.json"],
    )
    write_json_file(report_dir / "manifest.json", serialize_value(manifest))
    return report_dir


def build_feature_frame_for_export(
    dataset: LoadedBacktestDataset,
    *,
    timeframe: str,
    join_labels: bool,
) -> FeatureEngineResult:
    engine_result = run_feature_engine(dataset.frame)
    if not join_labels:
        return engine_result

    joined_frame = join_triple_barrier_labels(
        engine_result.feature_frame,
        dataset,
        timeframe=timeframe,
    )
    summary = dict(engine_result.summary)
    summary["joined_label_columns"] = [
        column for column in joined_frame.columns if column.startswith("tb_")
    ]
    return FeatureEngineResult(
        feature_frame=joined_frame,
        feature_names=engine_result.feature_names,
        metadata_frame=engine_result.metadata_frame,
        summary=summary,
    )
