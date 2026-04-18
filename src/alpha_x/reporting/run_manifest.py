from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from alpha_x.backtest.metrics import PerformanceRow
from alpha_x.reporting.serializers import serialize_value


@dataclass(frozen=True)
class RunManifest:
    run_id: str
    report_type: str
    created_at: str
    dataset: dict[str, Any]
    parameters: dict[str, Any]
    summary: list[dict[str, Any]]
    artifacts: list[str]


def build_run_manifest(
    *,
    run_id: str,
    report_type: str,
    created_at: pd.Timestamp,
    dataset: dict[str, Any],
    parameters: dict[str, Any],
    summary_rows: list[PerformanceRow],
    artifacts: list[str],
) -> RunManifest:
    return RunManifest(
        run_id=run_id,
        report_type=report_type,
        created_at=serialize_value(created_at),
        dataset=serialize_value(dataset),
        parameters=serialize_value(parameters),
        summary=serialize_value(summary_rows),
        artifacts=serialize_value(artifacts),
    )
