from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import pandas as pd

from alpha_x.data.ohlcv_pipeline import backfill_and_store_ohlcv, validate_existing_ohlcv
from alpha_x.reporting.io import (
    build_run_id,
    create_report_directory,
    list_report_files,
    write_json_file,
    write_table_csv,
)
from alpha_x.reporting.serializers import serialize_value
from alpha_x.truth_engine.eligibility import EligibilityConfig, evaluate_asset_eligibility
from alpha_x.truth_engine.universe import OFFICIAL_UNIVERSE, RESERVE_UNIVERSE


class BitvavoDataClient(Protocol):
    max_candles_per_request: int

    def fetch_candles(
        self,
        market: str,
        interval: str,
        limit: int,
        start: int | None = None,
        end: int | None = None,
    ) -> pd.DataFrame: ...

    def list_markets(self) -> list[str]: ...


@dataclass(frozen=True)
class TruthEngineDataBatchResult:
    run_id: str
    report_dir: Path
    coverage_frame: pd.DataFrame
    summary: dict[str, Any]


def run_truth_engine_data_batch(
    *,
    client: BitvavoDataClient,
    raw_data_dir: Path,
    reports_dir: Path,
    logger: Any,
    run_id: str | None = None,
    created_at: pd.Timestamp | None = None,
    markets: list[str] | tuple[str, ...] = OFFICIAL_UNIVERSE,
    reserve_markets: list[str] | tuple[str, ...] = RESERVE_UNIVERSE,
    timeframe: str = "1d",
    target_rows: int = 2500,
    limit: int | None = None,
    validate_only: bool = False,
    use_reserves: bool = False,
    eligibility_config: EligibilityConfig | None = None,
) -> TruthEngineDataBatchResult:
    if timeframe.lower() != "1d":
        raise ValueError("Truth Engine data batch only supports timeframe=1d.")

    created = created_at or pd.Timestamp.now(tz="UTC").floor("s")
    resolved_run_id = run_id or build_run_id(created)
    effective_limit = min(limit or client.max_candles_per_request, client.max_candles_per_request)
    available_markets = set(client.list_markets())
    eligibility = eligibility_config or EligibilityConfig(timeframe=timeframe)

    planned_markets, substitutions = resolve_target_markets(
        target_markets=list(markets),
        available_markets=available_markets,
        reserve_markets=list(reserve_markets),
        use_reserves=use_reserves,
    )

    rows: list[dict[str, Any]] = []
    for original_market, actual_market in planned_markets:
        row = _process_market(
            client=client,
            raw_data_dir=raw_data_dir,
            logger=logger,
            original_market=original_market,
            actual_market=actual_market,
            timeframe=timeframe,
            target_rows=target_rows,
            limit=effective_limit,
            validate_only=validate_only,
            is_available=actual_market in available_markets,
            substitution=substitutions.get(original_market),
            eligibility_config=eligibility,
        )
        rows.append(row)

    coverage_frame = pd.DataFrame(rows)
    effective_universe = (
        coverage_frame.loc[coverage_frame["eligible_for_truth_engine"], "effective_market"]
        .dropna()
        .astype(str)
        .tolist()
    )
    target_universe = [market for market, _ in planned_markets]
    unavailable_universe = (
        coverage_frame.loc[~coverage_frame["available_on_bitvavo"], "target_market"]
        .dropna()
        .astype(str)
        .tolist()
    )
    successful_universe = (
        coverage_frame.loc[coverage_frame["status"].isin(["ready", "partial"]), "effective_market"]
        .dropna()
        .astype(str)
        .tolist()
    )

    known_limitations = _build_known_limitations(
        coverage_frame=coverage_frame,
        target_rows=target_rows,
        effective_universe=effective_universe,
    )
    summary = {
        "run_id": resolved_run_id,
        "report_type": "truth_engine_data",
        "created_at": serialize_value(created),
        "timeframe": timeframe,
        "target_universe": target_universe,
        "available_universe": sorted(available_markets.intersection(target_universe)),
        "effective_universe_final": effective_universe,
        "successful_downloads": successful_universe,
        "missing_assets": unavailable_universe,
        "substitutions": substitutions,
        "total_files_generated": int(coverage_frame["csv_exists"].sum()),
        "rows_per_asset": {
            row["effective_market"]: int(row["rows"])
            for row in coverage_frame.to_dict(orient="records")
            if row.get("effective_market")
        },
        "known_limitations": known_limitations,
    }

    report_dir = create_report_directory(reports_dir, "truth_engine_data", resolved_run_id)
    write_table_csv(report_dir / "asset_coverage.csv", coverage_frame)
    write_json_file(report_dir / "summary.json", summary)
    write_json_file(
        report_dir / "manifest.json",
        {
            "run_id": resolved_run_id,
            "report_type": "truth_engine_data",
            "created_at": serialize_value(created),
            "artifacts": list_report_files(report_dir) + ["manifest.json"],
        },
    )
    return TruthEngineDataBatchResult(
        run_id=resolved_run_id,
        report_dir=report_dir,
        coverage_frame=coverage_frame,
        summary=summary,
    )


def resolve_target_markets(
    *,
    target_markets: list[str],
    available_markets: set[str],
    reserve_markets: list[str],
    use_reserves: bool,
) -> tuple[list[tuple[str, str]], dict[str, str]]:
    substitutions: dict[str, str] = {}
    remaining_reserves = [market for market in reserve_markets if market in available_markets]
    planned: list[tuple[str, str]] = []

    for market in target_markets:
        if market in available_markets or not use_reserves:
            planned.append((market, market))
            continue
        replacement = remaining_reserves.pop(0) if remaining_reserves else market
        substitutions[market] = replacement
        planned.append((market, replacement))
    return planned, substitutions


def load_latest_truth_engine_data_summary(reports_dir: Path) -> dict[str, Any] | None:
    base_dir = reports_dir / "truth_engine_data"
    if not base_dir.exists():
        return None

    candidates = sorted(
        path / "summary.json"
        for path in base_dir.iterdir()
        if path.is_dir() and (path / "summary.json").exists()
    )
    if not candidates:
        return None
    latest = candidates[-1]
    with latest.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _process_market(
    *,
    client: BitvavoDataClient,
    raw_data_dir: Path,
    logger: Any,
    original_market: str,
    actual_market: str,
    timeframe: str,
    target_rows: int,
    limit: int,
    validate_only: bool,
    is_available: bool,
    substitution: str | None,
    eligibility_config: EligibilityConfig,
) -> dict[str, Any]:
    if not is_available:
        return _empty_coverage_row(
            target_market=original_market,
            effective_market=actual_market,
            status="unavailable",
            reason="market_not_listed_by_bitvavo",
            substitution=substitution,
        )

    try:
        if not validate_only:
            backfill_and_store_ohlcv(
                client=client,
                raw_data_dir=raw_data_dir,
                market=actual_market,
                timeframe=timeframe,
                limit=limit,
                logger=logger,
                target_rows=target_rows,
            )

        csv_path, frame, report = validate_existing_ohlcv(raw_data_dir, actual_market, timeframe)
        asset_eligibility = evaluate_asset_eligibility(actual_market, frame, eligibility_config)
        row_count = len(frame)
        if row_count == 0:
            return _empty_coverage_row(
                target_market=original_market,
                effective_market=actual_market,
                status="empty",
                reason="no_rows_persisted",
                csv_path=csv_path,
                substitution=substitution,
                available_on_bitvavo=True,
            )

        status, reason = classify_market_status(
            row_count=row_count,
            target_rows=target_rows,
            gap_count=len(report.gaps),
            eligible_for_truth_engine=asset_eligibility.eligible,
            eligibility_reasons=asset_eligibility.reasons,
        )
        return {
            "target_market": original_market,
            "effective_market": actual_market,
            "substitution": substitution or "",
            "csv_path": str(csv_path),
            "csv_exists": csv_path.exists(),
            "rows": row_count,
            "first_timestamp": int(frame["timestamp"].iloc[0]),
            "last_timestamp": int(frame["timestamp"].iloc[-1]),
            "gap_count": len(report.gaps),
            "status": status,
            "reason": reason,
            "available_on_bitvavo": True,
            "eligible_for_truth_engine": asset_eligibility.eligible,
        }
    except Exception as error:  # noqa: BLE001
        logger.error("Truth Engine 1D batch failed for %s: %s", actual_market, error)
        return _empty_coverage_row(
            target_market=original_market,
            effective_market=actual_market,
            status="failed",
            reason=str(error),
            substitution=substitution,
            available_on_bitvavo=True,
        )


def classify_market_status(
    *,
    row_count: int,
    target_rows: int,
    gap_count: int,
    eligible_for_truth_engine: bool,
    eligibility_reasons: list[str],
) -> tuple[str, str]:
    reasons: list[str] = []
    if row_count < target_rows:
        reasons.append("rows_below_target")
    if gap_count > 0:
        reasons.append("dataset_has_gaps")
    reasons.extend(eligibility_reasons)

    if eligible_for_truth_engine and gap_count == 0 and row_count >= target_rows:
        return "ready", "ok"
    if row_count > 0:
        return "partial", ",".join(dict.fromkeys(reasons)) or "partial_history"
    return "empty", "no_rows_persisted"


def _empty_coverage_row(
    *,
    target_market: str,
    effective_market: str,
    status: str,
    reason: str,
    substitution: str | None,
    csv_path: Path | None = None,
    available_on_bitvavo: bool = False,
) -> dict[str, Any]:
    return {
        "target_market": target_market,
        "effective_market": effective_market,
        "substitution": substitution or "",
        "csv_path": str(csv_path) if csv_path is not None else "",
        "csv_exists": bool(csv_path and csv_path.exists()),
        "rows": 0,
        "first_timestamp": None,
        "last_timestamp": None,
        "gap_count": 0,
        "status": status,
        "reason": reason,
        "available_on_bitvavo": available_on_bitvavo,
        "eligible_for_truth_engine": False,
    }


def _build_known_limitations(
    *,
    coverage_frame: pd.DataFrame,
    target_rows: int,
    effective_universe: list[str],
) -> list[str]:
    limitations: list[str] = []
    partial = coverage_frame.loc[coverage_frame["status"] == "partial", "effective_market"].tolist()
    unavailable = coverage_frame.loc[
        coverage_frame["status"] == "unavailable", "target_market"
    ].tolist()
    failed = coverage_frame.loc[coverage_frame["status"] == "failed", "target_market"].tolist()

    if partial:
        limitations.append(
            f"Activos con historia o calidad parcial frente al target_rows={target_rows}: {partial}"
        )
    if unavailable:
        limitations.append(f"Activos no listados por Bitvavo: {unavailable}")
    if failed:
        limitations.append(f"Activos con fallo operativo durante el batch: {failed}")
    if not effective_universe:
        limitations.append("No hubo universo efectivo elegible para Truth Engine.")
    return limitations
