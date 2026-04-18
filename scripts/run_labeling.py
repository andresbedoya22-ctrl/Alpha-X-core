from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from alpha_x.backtest.data_loader import load_backtest_dataset
from alpha_x.config.settings import get_settings
from alpha_x.data.ohlcv_storage import build_ohlcv_csv_path
from alpha_x.labeling.base import LabelingSummaryRow
from alpha_x.labeling.catalog import get_labeling_catalog
from alpha_x.labeling.utils import summarize_labels, summary_rows_to_frame
from alpha_x.reporting.io import (
    build_run_id,
    create_report_directory,
    list_report_files,
    write_json_file,
    write_table_csv,
)
from alpha_x.reporting.run_manifest import build_run_manifest
from alpha_x.reporting.serializers import serialize_value
from alpha_x.utils.logging_utils import configure_logging


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    settings = get_settings()
    parser = argparse.ArgumentParser(
        description="Run V2/F2.2 labeling methods on the persisted BTC-EUR 1h dataset."
    )
    parser.add_argument("--market", default=settings.bitvavo_market, help="Market symbol.")
    parser.add_argument(
        "--timeframe",
        default=settings.ohlcv_default_interval,
        help="OHLCV timeframe.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional run identifier. If omitted, a UTC timestamp-based run_id is used.",
    )
    parser.add_argument(
        "--no-export",
        action="store_true",
        help="Skip report export under reports/labeling/<run_id>/.",
    )
    return parser.parse_args(argv)


def build_dataset_context(dataset: object) -> dict[str, object]:
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


def build_summary_text(row: LabelingSummaryRow) -> list[str]:
    return [
        f"Method: {row.name}",
        (
            f"  labeled_rows={row.labeled_rows} | "
            f"discarded_rows={row.discarded_rows} | total_rows={row.total_rows}"
        ),
        (
            "  labels: "
            f"+1={row.positive_count} ({row.positive_pct * 100:.2f}%) | "
            f"0={row.neutral_count} ({row.neutral_pct * 100:.2f}%) | "
            f"-1={row.negative_count} ({row.negative_pct * 100:.2f}%)"
        ),
        f"  covered_range={row.start_timestamp} -> {row.end_timestamp}",
    ]


def export_labeling_report(
    *,
    run_id: str,
    created_at: pd.Timestamp,
    dataset: object,
    parameters: dict[str, object],
    summary_rows: list[LabelingSummaryRow],
    label_frames: list[tuple[str, pd.DataFrame]],
) -> Path:
    settings = get_settings()
    dataset_context = build_dataset_context(dataset)
    report_dir = create_report_directory(settings.reports_dir, "labeling", run_id)
    summary_payload = {
        "run_id": run_id,
        "report_type": "labeling",
        "created_at": serialize_value(created_at),
        "dataset": serialize_value(dataset_context),
        "parameters": serialize_value(parameters),
        "summary": serialize_value(summary_rows),
    }
    write_json_file(report_dir / "summary.json", summary_payload)
    write_table_csv(report_dir / "summary.csv", summary_rows_to_frame(summary_rows))
    write_table_csv(
        report_dir / "labels.csv",
        pd.concat([frame for _, frame in label_frames], ignore_index=True),
    )

    artifacts = list_report_files(report_dir)
    manifest = build_run_manifest(
        run_id=run_id,
        report_type="labeling",
        created_at=created_at,
        dataset=dataset_context,
        parameters=parameters,
        summary_rows=summary_rows,  # type: ignore[arg-type]
        artifacts=artifacts + ["manifest.json"],
    )
    write_json_file(report_dir / "manifest.json", serialize_value(manifest))
    return report_dir


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = get_settings()
    created_at = pd.Timestamp.now(tz="UTC").floor("s")
    run_id = args.run_id or build_run_id(created_at)
    logger = configure_logging(
        settings.log_dir,
        settings.log_level,
        logger_name="alpha_x_labeling",
    )

    dataset_path = build_ohlcv_csv_path(
        raw_data_dir=settings.raw_data_dir,
        exchange="bitvavo",
        market=args.market,
        timeframe=args.timeframe,
    )
    dataset = load_backtest_dataset(Path(dataset_path), args.timeframe)
    summary_rows: list[LabelingSummaryRow] = []
    label_frames: list[tuple[str, pd.DataFrame]] = []

    for labeling in get_labeling_catalog():
        frame = labeling.build_labels(dataset.frame, timeframe=args.timeframe)
        summary = summarize_labels(frame, name=labeling.name, method=labeling.method)
        summary_rows.append(summary)
        label_frames.append((labeling.name, frame))

    parameters = {
        "timeframe": args.timeframe,
        "methods": [labeling.parameters for labeling in get_labeling_catalog()],
        "gap_policy": "Rows are discarded when the forward path crosses a real dataset gap.",
        "tail_policy": "Rows without enough future data are discarded explicitly.",
    }

    report_dir: Path | None = None
    if not args.no_export:
        report_dir = export_labeling_report(
            run_id=run_id,
            created_at=created_at,
            dataset=dataset,
            parameters=parameters,
            summary_rows=summary_rows,
            label_frames=label_frames,
        )

    dataset_context = build_dataset_context(dataset)
    logger.info("Loaded labeling dataset from %s", dataset_path)
    logger.info(
        "run_id=%s rows=%s gaps=%s methods=%s",
        run_id,
        dataset_context["rows"],
        dataset_context["gap_count"],
        len(summary_rows),
    )

    print(
        f"Dataset: {dataset_context['path']} | market={dataset_context['market']} | "
        f"timeframe={dataset_context['timeframe']} | rows={dataset_context['rows']}"
    )
    print(
        f"Gaps residuales: {dataset_context['gap_count']} | "
        f"missing_intervals={dataset_context['total_missing_intervals']} | "
        f"largest_gap={dataset_context['largest_gap']}"
    )
    print("Gap policy: discard rows whose forward labeling window crosses a real gap.")
    print("Tail policy: discard rows without enough future bars.")
    if report_dir is not None:
        print(f"Report path: {report_dir}")
    print()
    for labeling, summary in zip(get_labeling_catalog(), summary_rows, strict=True):
        for line in build_summary_text(summary):
            print(line)
        print(f"  parameters={labeling.parameters}")
        print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
