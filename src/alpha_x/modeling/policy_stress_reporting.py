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


def export_policy_stress_report(
    *,
    reports_dir: Path,
    run_id: str,
    created_at: pd.Timestamp,
    dataset_context: dict[str, Any],
    parameters: dict[str, Any],
    summary_payload: dict[str, Any],
    scored_test_frame: pd.DataFrame,
    stress_signal_frame: pd.DataFrame,
    stress_summary_frame: pd.DataFrame,
    subperiod_frame: pd.DataFrame,
    comparison_frame: pd.DataFrame,
) -> Path:
    report_dir = create_report_directory(reports_dir, "policy_stress", run_id)
    write_json_file(
        report_dir / "summary.json",
        {
            "run_id": run_id,
            "report_type": "policy_stress",
            "created_at": serialize_value(created_at),
            "dataset": serialize_value(dataset_context),
            "parameters": serialize_value(parameters),
            "summary": serialize_value(summary_payload),
        },
    )
    write_table_csv(report_dir / "scored_test_frame.csv", scored_test_frame)
    write_table_csv(report_dir / "stress_policy_signals.csv", stress_signal_frame)
    write_table_csv(report_dir / "stress_summary.csv", stress_summary_frame)
    write_table_csv(report_dir / "subperiod_stability.csv", subperiod_frame)
    write_table_csv(report_dir / "comparison.csv", comparison_frame)

    artifacts = list_report_files(report_dir)
    manifest = build_run_manifest(
        run_id=run_id,
        report_type="policy_stress",
        created_at=created_at,
        dataset=dataset_context,
        parameters=parameters,
        summary_rows=[summary_payload],  # type: ignore[list-item]
        artifacts=artifacts + ["manifest.json"],
    )
    write_json_file(report_dir / "manifest.json", serialize_value(manifest))
    return report_dir
