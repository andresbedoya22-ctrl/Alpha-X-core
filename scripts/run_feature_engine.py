from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from alpha_x.config.settings import get_settings
from alpha_x.data.ohlcv_storage import build_ohlcv_csv_path
from alpha_x.features.engine import (
    build_dataset_context,
    build_feature_frame_for_export,
    export_feature_report,
    load_feature_dataset,
)
from alpha_x.reporting.io import build_run_id
from alpha_x.utils.logging_utils import configure_logging


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    settings = get_settings()
    parser = argparse.ArgumentParser(
        description="Run V3/F3.1 feature engine on the persisted BTC-EUR dataset."
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
        "--join-triple-barrier-labels",
        action="store_true",
        help="Join the existing triple-barrier labels into the exported feature table.",
    )
    parser.add_argument(
        "--no-export",
        action="store_true",
        help="Skip report export under reports/features/<run_id>/.",
    )
    return parser.parse_args(argv)


def format_summary(dataset_context: dict[str, object], summary: dict[str, object]) -> list[str]:
    return [
        (
            f"Dataset: {dataset_context['path']} | market={dataset_context['market']} | "
            f"timeframe={dataset_context['timeframe']} | rows={dataset_context['rows']}"
        ),
        (
            f"Gaps residuales: {dataset_context['gap_count']} | "
            f"missing_intervals={dataset_context['total_missing_intervals']} | "
            f"largest_gap={dataset_context['largest_gap']}"
        ),
        (
            f"Filas totales: {summary['total_rows']} | "
            f"filas validas tras warmup: {summary['valid_rows']} | "
            "perdida warmup: "
            f"{summary['warmup_rows']} ({float(summary['warmup_loss_pct']) * 100:.2f}%)"
        ),
        f"Numero total de features: {summary['feature_count']}",
        (
            f"Rango temporal: {summary['range_start_datetime']} -> "
            f"{summary['range_end_datetime']}"
        ),
        "Features generadas: " + ", ".join(summary["feature_names"]),
    ]


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = get_settings()
    created_at = pd.Timestamp.now(tz="UTC").floor("s")
    run_id = args.run_id or build_run_id(created_at)
    logger = configure_logging(
        settings.log_dir,
        settings.log_level,
        logger_name="alpha_x_features",
    )

    dataset_path = build_ohlcv_csv_path(
        raw_data_dir=settings.raw_data_dir,
        exchange="bitvavo",
        market=args.market,
        timeframe=args.timeframe,
    )
    dataset = load_feature_dataset(Path(dataset_path), args.timeframe)
    engine_result = build_feature_frame_for_export(
        dataset,
        timeframe=args.timeframe,
        join_labels=args.join_triple_barrier_labels,
    )

    parameters = {
        "timeframe": args.timeframe,
        "join_triple_barrier_labels": args.join_triple_barrier_labels,
        "feature_count": engine_result.summary["feature_count"],
        "feature_names": engine_result.summary["feature_names"],
    }

    report_dir: Path | None = None
    if not args.no_export:
        report_dir = export_feature_report(
            reports_dir=settings.reports_dir,
            run_id=run_id,
            created_at=created_at,
            dataset=dataset,
            engine_result=engine_result,
            parameters=parameters,
        )

    dataset_context = build_dataset_context(dataset)
    logger.info(
        "run_id=%s rows=%s valid_rows=%s features=%s join_labels=%s",
        run_id,
        dataset_context["rows"],
        engine_result.summary["valid_rows"],
        engine_result.summary["feature_count"],
        args.join_triple_barrier_labels,
    )

    for line in format_summary(dataset_context, engine_result.summary):
        print(line)
    if report_dir is not None:
        print(f"Report path: {report_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
