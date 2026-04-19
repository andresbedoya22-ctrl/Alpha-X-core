from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from alpha_x.reporting.io import (
    create_report_directory,
    list_report_files,
    write_json_file,
    write_table_csv,
)
from alpha_x.reporting.run_manifest import build_run_manifest
from alpha_x.reporting.serializers import serialize_value


def export_regime_report(
    *,
    reports_dir: Path,
    run_id: str,
    created_at: pd.Timestamp,
    dataset_context: dict[str, Any],
    parameters: dict[str, Any],
    summary_payload: dict[str, Any],
    regime_frame: pd.DataFrame,
    regime_summary: pd.DataFrame,
    component_summary: pd.DataFrame,
    regime_label_table: pd.DataFrame,
    regime_strategy_table: pd.DataFrame,
) -> Path:
    report_dir = create_report_directory(reports_dir, "regime", run_id)

    write_json_file(
        report_dir / "summary.json",
        {
            "run_id": run_id,
            "report_type": "regime",
            "created_at": serialize_value(created_at),
            "dataset": serialize_value(dataset_context),
            "parameters": serialize_value(parameters),
            "summary": serialize_value(summary_payload),
        },
    )
    write_table_csv(report_dir / "regime_table.csv", regime_frame)
    write_table_csv(report_dir / "regime_summary.csv", regime_summary)
    write_table_csv(report_dir / "regime_component_summary.csv", component_summary)
    write_table_csv(report_dir / "regime_label_table.csv", regime_label_table)
    write_table_csv(report_dir / "regime_strategy_table.csv", regime_strategy_table)

    artifacts = list_report_files(report_dir)
    manifest = build_run_manifest(
        run_id=run_id,
        report_type="regime",
        created_at=created_at,
        dataset=dataset_context,
        parameters=parameters,
        summary_rows=[summary_payload],  # type: ignore[list-item]
        artifacts=artifacts + ["manifest.json"],
    )
    write_json_file(report_dir / "manifest.json", serialize_value(manifest))
    return report_dir
