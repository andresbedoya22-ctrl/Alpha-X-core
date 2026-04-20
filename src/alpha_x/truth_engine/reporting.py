from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any

import pandas as pd

from alpha_x.reporting.io import (
    create_report_directory,
    list_report_files,
    write_json_file,
    write_table_csv,
)
from alpha_x.reporting.serializers import serialize_value


def export_truth_engine_report(
    *,
    reports_dir: Path,
    run_id: str,
    created_at: pd.Timestamp,
    summary_frame: pd.DataFrame,
    manifest_payload: dict[str, Any],
    eligibility_frame: pd.DataFrame,
    comparison_frame: pd.DataFrame,
    split_frame: pd.DataFrame,
    family_curves: dict[str, pd.DataFrame],
    benchmark_curves: dict[str, pd.DataFrame],
    decision_logs: dict[str, pd.DataFrame],
) -> Path:
    report_dir = create_report_directory(reports_dir, "truth_engine", run_id)
    write_table_csv(report_dir / "summary.csv", summary_frame)
    write_json_file(
        report_dir / "summary.json",
        {
            "run_id": run_id,
            "created_at": serialize_value(created_at),
            "summary": serialize_value(summary_frame.to_dict(orient="records")),
            "manifest": serialize_value(manifest_payload),
        },
    )
    write_table_csv(report_dir / "eligibility.csv", eligibility_frame)
    write_table_csv(report_dir / "comparison.csv", comparison_frame)
    write_table_csv(report_dir / "split_comparison.csv", split_frame)

    for name, frame in family_curves.items():
        write_table_csv(report_dir / "families" / f"{name}.csv", frame)
    for name, frame in benchmark_curves.items():
        write_table_csv(report_dir / "benchmarks" / f"{name}.csv", frame)
    for name, frame in decision_logs.items():
        write_table_csv(report_dir / "decisions" / f"{name}.csv", frame)

    manifest = dict(manifest_payload)
    manifest["artifacts"] = list_report_files(report_dir) + ["manifest.json"]
    write_json_file(report_dir / "manifest.json", serialize_value(manifest))
    return report_dir


def metrics_to_frame(metrics: list[Any]) -> pd.DataFrame:
    return pd.DataFrame(serialize_value(asdict(item)) for item in metrics)
