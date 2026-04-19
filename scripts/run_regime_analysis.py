from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from alpha_x.config.settings import get_settings
from alpha_x.data.ohlcv_storage import build_ohlcv_csv_path
from alpha_x.features.engine import (
    build_dataset_context,
    build_feature_frame_for_export,
    load_feature_dataset,
)
from alpha_x.regime.analysis import (
    build_regime_component_summary,
    build_regime_label_table,
    build_regime_strategy_table,
    build_regime_summary,
)
from alpha_x.regime.catalog import get_default_regime_rule_set
from alpha_x.regime.reporting import export_regime_report
from alpha_x.regime.rules import detect_regimes
from alpha_x.reporting.io import build_run_id
from alpha_x.utils.logging_utils import configure_logging


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    settings = get_settings()
    parser = argparse.ArgumentParser(
        description="Run V3/F3.2a simple regime analysis on the persisted BTC-EUR dataset."
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
        help="Skip report export under reports/regime/<run_id>/.",
    )
    return parser.parse_args(argv)


def _format_regime_distribution(summary: pd.DataFrame) -> list[str]:
    lines: list[str] = []
    for row in summary.itertuples(index=False):
        lines.append(f"  {row.regime}: rows={row.rows} ({row.pct * 100:.2f}%)")
    return lines


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = get_settings()
    created_at = pd.Timestamp.now(tz="UTC").floor("s")
    run_id = args.run_id or build_run_id(created_at)
    logger = configure_logging(
        settings.log_dir,
        settings.log_level,
        logger_name="alpha_x_regime",
    )

    dataset_path = build_ohlcv_csv_path(
        raw_data_dir=settings.raw_data_dir,
        exchange="bitvavo",
        market=args.market,
        timeframe=args.timeframe,
    )
    dataset = load_feature_dataset(Path(dataset_path), args.timeframe)
    feature_result = build_feature_frame_for_export(
        dataset,
        timeframe=args.timeframe,
        join_labels=True,
    )
    rule_set = get_default_regime_rule_set()
    detection = detect_regimes(feature_result.feature_frame, rule_set)
    regime_summary = build_regime_summary(detection)
    component_summary = build_regime_component_summary(detection)
    regime_label_table = build_regime_label_table(detection.frame)
    regime_strategy_table = build_regime_strategy_table(
        detection.frame,
        dataset.frame,
        fee_rate=settings.benchmark_fee_rate,
        slippage_rate=0.0005,
        initial_capital=settings.benchmark_initial_capital,
    )

    dataset_context = build_dataset_context(dataset)
    summary_payload = {
        "assigned_rows": detection.assigned_rows,
        "discarded_rows": detection.discarded_rows,
        "discard_pct": detection.discard_pct,
        "regime_names": detection.regime_names,
        "rules_used": detection.rules_used,
        "regime_distribution": regime_summary.to_dict(orient="records"),
    }
    parameters = {
        "timeframe": args.timeframe,
        "fee_rate": settings.benchmark_fee_rate,
        "slippage_rate": 0.0005,
        "initial_capital": settings.benchmark_initial_capital,
        "rule_set_id": rule_set.rule_set_id,
    }

    report_dir: Path | None = None
    if not args.no_export:
        report_dir = export_regime_report(
            reports_dir=settings.reports_dir,
            run_id=run_id,
            created_at=created_at,
            dataset_context=dataset_context,
            parameters=parameters,
            summary_payload=summary_payload,
            regime_frame=detection.frame,
            regime_summary=regime_summary,
            component_summary=component_summary,
            regime_label_table=regime_label_table,
            regime_strategy_table=regime_strategy_table,
        )

    logger.info(
        "run_id=%s rows=%s assigned=%s discarded=%s regimes=%s",
        run_id,
        dataset_context["rows"],
        detection.assigned_rows,
        detection.discarded_rows,
        len(detection.regime_names),
    )

    print(
        f"Dataset: {dataset_context['path']} | market={dataset_context['market']} | "
        f"timeframe={dataset_context['timeframe']} | rows={dataset_context['rows']}"
    )
    print(
        f"Filas con regimen asignado: {detection.assigned_rows} | "
        f"filas descartadas: {detection.discarded_rows} ({detection.discard_pct * 100:.2f}%)"
    )
    print("Distribucion de regimenes:")
    for line in _format_regime_distribution(regime_summary):
        print(line)
    print("Reglas usadas:")
    print(f"  rule_set_id={rule_set.rule_set_id}")
    for key, value in rule_set.parameters.items():
        print(f"  {key}={value}")
    print()
    print("Regime x labels (triple barrier):")
    print(regime_label_table.to_string(index=False))
    print()
    print("Regime x estrategias:")
    print(regime_strategy_table.to_string(index=False))
    if report_dir is not None:
        print()
        print(f"Report path: {report_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
