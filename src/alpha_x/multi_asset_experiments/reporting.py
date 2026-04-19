from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from alpha_x.multi_asset_experiments.comparison import MultiAssetComparisonResult
from alpha_x.reporting.io import (
    create_report_directory,
    list_report_files,
    write_json_file,
    write_table_csv,
)
from alpha_x.reporting.serializers import serialize_value


def export_multi_asset_comparison_report(
    *,
    reports_dir: Path,
    run_id: str,
    created_at: pd.Timestamp,
    parameters: dict[str, Any],
    result: MultiAssetComparisonResult,
) -> Path:
    report_dir = create_report_directory(reports_dir, "multi_asset_comparison", run_id)

    dataset_summary_frame = pd.DataFrame(
        [asset_result.dataset_summary for asset_result in result.asset_results]
    )
    model_metrics_frame = pd.concat(
        [
            asset_result.evaluation_frame.assign(
                market=asset_result.market,
                asset=asset_result.asset,
            )
            for asset_result in result.asset_results
        ],
        ignore_index=True,
    )
    regime_metrics_frame = pd.concat(
        [
            asset_result.regime_metrics.assign(
                market=asset_result.market,
                asset=asset_result.asset,
            )
            for asset_result in result.asset_results
        ],
        ignore_index=True,
    )
    backtest_metrics_frame = pd.concat(
        [
            asset_result.backtest_comparison.assign(
                market=asset_result.market,
                asset=asset_result.asset,
            )
            for asset_result in result.asset_results
        ],
        ignore_index=True,
    )

    write_json_file(
        report_dir / "summary.json",
        {
            "run_id": run_id,
            "report_type": "multi_asset_comparison",
            "created_at": serialize_value(created_at),
            "parameters": serialize_value(parameters),
            "common_window": {
                "audit_run_id": result.common_window.audit_run_id,
                "start": result.common_window.start.isoformat(),
                "end": result.common_window.end.isoformat(),
                "row_count_estimate": result.common_window.row_count_estimate,
            },
            "summary": {
                "policy_threshold": result.policy_threshold,
                "conclusion": result.conclusion,
            },
        },
    )
    write_table_csv(report_dir / "asset_dataset_summary.csv", dataset_summary_frame)
    write_table_csv(report_dir / "asset_model_metrics.csv", model_metrics_frame)
    write_table_csv(report_dir / "asset_regime_metrics.csv", regime_metrics_frame)
    write_table_csv(report_dir / "asset_backtest_metrics.csv", backtest_metrics_frame)
    write_table_csv(report_dir / "asset_comparison.csv", result.comparison_frame)
    write_table_csv(report_dir / "asset_promisingness.csv", result.promisingness_frame)

    artifacts = list_report_files(report_dir)
    write_json_file(
        report_dir / "manifest.json",
        {
            "run_id": run_id,
            "report_type": "multi_asset_comparison",
            "artifacts": artifacts + ["manifest.json"],
        },
    )
    return report_dir
