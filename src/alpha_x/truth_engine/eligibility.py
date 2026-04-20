from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from alpha_x.data.ohlcv_validation import summarize_gaps, validate_temporal_integrity


@dataclass(frozen=True)
class EligibilityConfig:
    timeframe: str = "1d"
    min_rows: int = 365
    min_median_turnover_eur: float = 250_000.0
    max_gap_count: int = 3
    max_missing_intervals: int = 5
    max_zero_volume_share: float = 0.02
    max_stale_price_share: float = 0.10


@dataclass(frozen=True)
class AssetEligibility:
    market: str
    eligible: bool
    reasons: list[str]
    row_count: int
    median_turnover_eur: float
    gap_count: int
    missing_intervals: int
    zero_volume_share: float
    stale_price_share: float


def evaluate_asset_eligibility(
    market: str,
    frame: pd.DataFrame,
    config: EligibilityConfig,
) -> AssetEligibility:
    reasons: list[str] = []
    if frame.empty:
        return AssetEligibility(
            market=market,
            eligible=False,
            reasons=["missing_dataset"],
            row_count=0,
            median_turnover_eur=0.0,
            gap_count=0,
            missing_intervals=0,
            zero_volume_share=1.0,
            stale_price_share=1.0,
        )

    validation = validate_temporal_integrity(frame, config.timeframe)
    gap_summary = summarize_gaps(validation)
    turnover = pd.to_numeric(frame["close"], errors="coerce") * pd.to_numeric(
        frame["volume"], errors="coerce"
    )
    median_turnover = float(turnover.median()) if not turnover.dropna().empty else 0.0
    zero_volume_share = float((pd.to_numeric(frame["volume"], errors="coerce") <= 0).mean())
    stale_price_share = float((frame["close"].pct_change().fillna(0.0) == 0.0).mean())

    if len(frame) < config.min_rows:
        reasons.append("insufficient_history")
    if median_turnover < config.min_median_turnover_eur:
        reasons.append("insufficient_turnover")
    if gap_summary.gap_count > config.max_gap_count:
        reasons.append("too_many_gaps")
    if gap_summary.total_missing_intervals > config.max_missing_intervals:
        reasons.append("too_many_missing_intervals")
    if zero_volume_share > config.max_zero_volume_share:
        reasons.append("too_many_zero_volume_bars")
    if stale_price_share > config.max_stale_price_share:
        reasons.append("too_many_stale_prices")

    return AssetEligibility(
        market=market,
        eligible=not reasons,
        reasons=reasons,
        row_count=len(frame),
        median_turnover_eur=median_turnover,
        gap_count=gap_summary.gap_count,
        missing_intervals=gap_summary.total_missing_intervals,
        zero_volume_share=zero_volume_share,
        stale_price_share=stale_price_share,
    )


def build_eligibility_table(
    market_frames: dict[str, pd.DataFrame],
    config: EligibilityConfig,
) -> pd.DataFrame:
    rows = [
        evaluate_asset_eligibility(market, frame, config) for market, frame in market_frames.items()
    ]
    return pd.DataFrame(
        {
            "market": row.market,
            "eligible": row.eligible,
            "reasons": ",".join(row.reasons),
            "row_count": row.row_count,
            "median_turnover_eur": row.median_turnover_eur,
            "gap_count": row.gap_count,
            "missing_intervals": row.missing_intervals,
            "zero_volume_share": row.zero_volume_share,
            "stale_price_share": row.stale_price_share,
        }
        for row in rows
    )
