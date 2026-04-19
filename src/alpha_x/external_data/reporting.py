from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path

import pandas as pd

from alpha_x.external_data.alignment import (
    ETF_PROXY_ALIGNMENT_POLICY,
    FUNDING_ALIGNMENT_POLICY,
    align_external_to_ohlcv,
    compute_coverage_stats,
)
from alpha_x.multi_asset.dataset import MultiAssetDataset
from alpha_x.multi_asset.markets import MARKET_REGISTRY
from alpha_x.reporting.io import write_json_file, write_table_csv


@dataclass
class AssetCoverageRow:
    market: str
    ohlcv_rows: int
    ohlcv_start: str | None
    ohlcv_end: str | None
    ohlcv_gaps: int
    ohlcv_missing_intervals: int
    funding_rows: int
    funding_start: str | None
    funding_end: str | None
    funding_coverage_pct: float
    etf_flow_rows: int
    etf_flow_start: str | None
    etf_flow_end: str | None
    etf_flow_coverage_pct: float
    rows_with_full_context: int
    full_context_pct: float


@dataclass
class CoverageReport:
    run_id: str
    generated_at: str
    markets_audited: list[str]
    asset_rows: list[AssetCoverageRow]
    common_window_ohlcv_start: str | None
    common_window_ohlcv_end: str | None
    common_window_ohlcv_rows_approx: int | None
    common_window_ohlcv_funding_start: str | None
    common_window_ohlcv_funding_end: str | None
    common_window_ohlcv_funding_etf_start: str | None
    common_window_ohlcv_funding_etf_end: str | None
    comparable_in_common_ohlcv: bool
    comparable_in_common_ohlcv_funding: bool
    comparable_in_common_ohlcv_funding_etf: bool
    known_limitations: list[str]


def compute_external_coverage(
    dataset: MultiAssetDataset,
    funding_frames: dict[str, pd.DataFrame],
    etf_flow_frames: dict[str, pd.DataFrame],
    global_etf_key: str,
    run_id: str,
) -> CoverageReport:
    asset_rows: list[AssetCoverageRow] = []

    for market in dataset.markets:
        info = dataset.results.get(market)
        if info is None or not info.available:
            asset_rows.append(
                AssetCoverageRow(
                    market=market,
                    ohlcv_rows=0,
                    ohlcv_start=None,
                    ohlcv_end=None,
                    ohlcv_gaps=0,
                    ohlcv_missing_intervals=0,
                    funding_rows=0,
                    funding_start=None,
                    funding_end=None,
                    funding_coverage_pct=0.0,
                    etf_flow_rows=0,
                    etf_flow_start=None,
                    etf_flow_end=None,
                    etf_flow_coverage_pct=0.0,
                    rows_with_full_context=0,
                    full_context_pct=0.0,
                )
            )
            continue

        market_info = MARKET_REGISTRY[market]
        funding_frame = funding_frames.get(market_info.base_asset, pd.DataFrame())
        etf_key = market_info.etf_ticker or global_etf_key
        etf_frame = etf_flow_frames.get(etf_key, pd.DataFrame())
        if etf_frame.empty and etf_key != global_etf_key:
            etf_frame = etf_flow_frames.get(global_etf_key, pd.DataFrame())

        with_funding = align_external_to_ohlcv(info.frame, funding_frame, FUNDING_ALIGNMENT_POLICY)
        funding_stats = compute_coverage_stats(with_funding, FUNDING_ALIGNMENT_POLICY.aligned_col)

        with_context = align_external_to_ohlcv(with_funding, etf_frame, ETF_PROXY_ALIGNMENT_POLICY)
        etf_stats = compute_coverage_stats(with_context, ETF_PROXY_ALIGNMENT_POLICY.aligned_col)

        full_mask = (
            with_context[FUNDING_ALIGNMENT_POLICY.aligned_col].notna()
            & with_context[ETF_PROXY_ALIGNMENT_POLICY.aligned_col].notna()
        )
        rows_with_full_context = int(full_mask.sum())
        full_context_pct = round(rows_with_full_context / len(with_context) * 100, 2)

        funding_start, funding_end = _frame_bounds(funding_frame)
        etf_start, etf_end = _frame_bounds(etf_frame)
        asset_rows.append(
            AssetCoverageRow(
                market=market,
                ohlcv_rows=info.row_count,
                ohlcv_start=str(info.start_dt) if info.start_dt else None,
                ohlcv_end=str(info.end_dt) if info.end_dt else None,
                ohlcv_gaps=info.gap_count,
                ohlcv_missing_intervals=info.missing_intervals,
                funding_rows=len(funding_frame),
                funding_start=funding_start,
                funding_end=funding_end,
                funding_coverage_pct=funding_stats["coverage_pct"],
                etf_flow_rows=len(etf_frame),
                etf_flow_start=etf_start,
                etf_flow_end=etf_end,
                etf_flow_coverage_pct=etf_stats["coverage_pct"],
                rows_with_full_context=rows_with_full_context,
                full_context_pct=full_context_pct,
            )
        )

    common_ohlcv_start, common_ohlcv_end = dataset.common_window
    common_ohlcv_rows = None
    if common_ohlcv_start and common_ohlcv_end:
        common_ohlcv_rows = int(
            ((common_ohlcv_end - common_ohlcv_start).total_seconds() / 3600) + 1
        )

    common_funding_start, common_funding_end = _intersection_window(
        common_ohlcv_start,
        common_ohlcv_end,
        [row.funding_start for row in asset_rows],
        [row.funding_end for row in asset_rows],
    )
    common_funding_etf_start, common_funding_etf_end = _intersection_window(
        common_funding_start,
        common_funding_end,
        [row.etf_flow_start for row in asset_rows],
        [row.etf_flow_end for row in asset_rows],
    )

    return CoverageReport(
        run_id=run_id,
        generated_at=pd.Timestamp.now(tz="UTC").isoformat(),
        markets_audited=dataset.markets,
        asset_rows=asset_rows,
        common_window_ohlcv_start=str(common_ohlcv_start) if common_ohlcv_start else None,
        common_window_ohlcv_end=str(common_ohlcv_end) if common_ohlcv_end else None,
        common_window_ohlcv_rows_approx=common_ohlcv_rows,
        common_window_ohlcv_funding_start=(
            str(common_funding_start) if common_funding_start is not None else None
        ),
        common_window_ohlcv_funding_end=(
            str(common_funding_end) if common_funding_end is not None else None
        ),
        common_window_ohlcv_funding_etf_start=(
            str(common_funding_etf_start) if common_funding_etf_start is not None else None
        ),
        common_window_ohlcv_funding_etf_end=(
            str(common_funding_etf_end) if common_funding_etf_end is not None else None
        ),
        comparable_in_common_ohlcv=dataset.comparable_in_common_window(),
        comparable_in_common_ohlcv_funding=(
            common_funding_start is not None and common_funding_end is not None
        ),
        comparable_in_common_ohlcv_funding_etf=(
            common_funding_etf_start is not None and common_funding_etf_end is not None
        ),
        known_limitations=_build_limitations(dataset, asset_rows, global_etf_key),
    )


def export_coverage_report(report: CoverageReport, output_dir: Path) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    summary = {
        "run_id": report.run_id,
        "generated_at": report.generated_at,
        "markets_audited": report.markets_audited,
        "common_windows": {
            "ohlcv": {
                "start": report.common_window_ohlcv_start,
                "end": report.common_window_ohlcv_end,
                "rows_approx": report.common_window_ohlcv_rows_approx,
            },
            "ohlcv_plus_funding": {
                "start": report.common_window_ohlcv_funding_start,
                "end": report.common_window_ohlcv_funding_end,
            },
            "ohlcv_plus_funding_plus_etf_flows": {
                "start": report.common_window_ohlcv_funding_etf_start,
                "end": report.common_window_ohlcv_funding_etf_end,
            },
        },
        "comparability": {
            "full_individual_window": False,
            "common_ohlcv_window": report.comparable_in_common_ohlcv,
            "common_ohlcv_funding_window": report.comparable_in_common_ohlcv_funding,
            "common_ohlcv_funding_etf_window": report.comparable_in_common_ohlcv_funding_etf,
        },
        "asset_coverage": [asdict(row) for row in report.asset_rows],
        "known_limitations": report.known_limitations,
    }
    summary_path = write_json_file(output_dir / "summary.json", summary)

    coverage_frame = pd.DataFrame([asdict(row) for row in report.asset_rows])
    coverage_path = write_table_csv(output_dir / "asset_coverage.csv", coverage_frame)
    common_windows_path = write_table_csv(
        output_dir / "common_windows.csv",
        pd.DataFrame(
            [
                {
                    "window": "ohlcv",
                    "start": report.common_window_ohlcv_start,
                    "end": report.common_window_ohlcv_end,
                    "comparable": report.comparable_in_common_ohlcv,
                },
                {
                    "window": "ohlcv_plus_funding",
                    "start": report.common_window_ohlcv_funding_start,
                    "end": report.common_window_ohlcv_funding_end,
                    "comparable": report.comparable_in_common_ohlcv_funding,
                },
                {
                    "window": "ohlcv_plus_funding_plus_etf_flows",
                    "start": report.common_window_ohlcv_funding_etf_start,
                    "end": report.common_window_ohlcv_funding_etf_end,
                    "comparable": report.comparable_in_common_ohlcv_funding_etf,
                },
            ]
        ),
    )
    comparability_path = write_table_csv(
        output_dir / "comparability.csv",
        pd.DataFrame(
            [
                {
                    "scope": "full_individual_window",
                    "comparable": False,
                    "note": (
                        "Not fair for cross-asset comparison because history "
                        "and coverage differ."
                    ),
                },
                {
                    "scope": "common_ohlcv_window",
                    "comparable": report.comparable_in_common_ohlcv,
                    "note": "Fair for OHLCV-only comparison.",
                },
                {
                    "scope": "common_ohlcv_funding_window",
                    "comparable": report.comparable_in_common_ohlcv_funding,
                    "note": "Fair when funding is required.",
                },
                {
                    "scope": "common_ohlcv_funding_etf_window",
                    "comparable": report.comparable_in_common_ohlcv_funding_etf,
                    "note": "Fair when both funding and ETF flow context are required.",
                },
            ]
        ),
    )
    manifest_path = write_json_file(
        output_dir / "manifest.json",
        {
            "run_id": report.run_id,
            "report_type": "multi_asset_data",
            "artifacts": [
                "summary.json",
                "asset_coverage.csv",
                "common_windows.csv",
                "comparability.csv",
            ],
        },
    )
    return {
        "summary": summary_path,
        "coverage_csv": coverage_path,
        "common_windows_csv": common_windows_path,
        "comparability_csv": comparability_path,
        "manifest": manifest_path,
    }


def _frame_bounds(frame: pd.DataFrame) -> tuple[str | None, str | None]:
    if frame.empty or "timestamp_ms" not in frame.columns:
        return None, None
    start = pd.Timestamp(int(frame["timestamp_ms"].iloc[0]), unit="ms", tz="UTC")
    end = pd.Timestamp(int(frame["timestamp_ms"].iloc[-1]), unit="ms", tz="UTC")
    return str(start), str(end)


def _intersection_window(
    base_start: pd.Timestamp | None,
    base_end: pd.Timestamp | None,
    starts: list[str | None],
    ends: list[str | None],
) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    if base_start is None or base_end is None:
        return None, None

    valid_starts = [pd.Timestamp(start) for start in starts if start]
    valid_ends = [pd.Timestamp(end) for end in ends if end]
    if not valid_starts or not valid_ends:
        return None, None

    start = max(base_start, max(valid_starts))
    end = min(base_end, min(valid_ends))
    if start >= end:
        return None, None
    return start, end


def _build_limitations(
    dataset: MultiAssetDataset,
    asset_rows: list[AssetCoverageRow],
    global_etf_key: str,
) -> list[str]:
    limitations: list[str] = []

    row_counts = {row.market: row.ohlcv_rows for row in asset_rows if row.ohlcv_rows > 0}
    if row_counts:
        max_rows = max(row_counts.values())
        for market, rows in row_counts.items():
            if rows < max_rows:
                limitations.append(
                    f"{market} has {rows} OHLCV rows versus {max_rows} rows in the deepest market."
                )

    limitations.append(
        "Funding is sourced from Bybit perps and represents global derivatives "
        "context, not Bitvavo EUR spot."
    )
    limitations.append(
        f"ETF flow context uses the BTC series '{global_etf_key}' as a global "
        "institutional signal for all assets."
    )
    limitations.append(
        "ETH ETF flows are not ingested in this phase because a stable free no-auth "
        "historical source was not confirmed."
    )
    limitations.append(
        "Daily ETF flows become available only from the next UTC day onward. "
        "No same-day leakage is allowed."
    )
    limitations.append(
        "Rows beyond the fill limits remain NaN: funding max 8 bars, ETF flows max 168 bars."
    )
    limitations.append(
        "Cross-asset comparisons are only fair inside the exported common windows, "
        "not on full individual histories."
    )

    missing_markets = [
        market for market in dataset.markets if market not in dataset.available_markets
    ]
    if missing_markets:
        limitations.append(f"Missing OHLCV data for markets: {missing_markets}.")

    return limitations
