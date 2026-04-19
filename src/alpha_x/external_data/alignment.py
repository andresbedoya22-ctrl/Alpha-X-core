from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from alpha_x.multi_asset.config import ETF_FLOWS_FFILL_LIMIT, FUNDING_FFILL_LIMIT


@dataclass(frozen=True)
class AlignmentPolicy:
    source_name: str
    external_col: str
    aligned_col: str
    ts_col: str
    ffill_limit: int
    frequency_note: str


FUNDING_ALIGNMENT_POLICY = AlignmentPolicy(
    source_name="Bybit funding rate",
    external_col="funding_rate",
    aligned_col="funding_rate",
    ts_col="timestamp_ms",
    ffill_limit=FUNDING_FFILL_LIMIT,
    frequency_note="every 8h; max fill = 8 bars",
)

ETF_PROXY_ALIGNMENT_POLICY = AlignmentPolicy(
    source_name="BTC ETF flows",
    external_col="btc_etf_flow_usd",
    aligned_col="btc_etf_flow_usd",
    ts_col="effective_timestamp_ms",
    ffill_limit=ETF_FLOWS_FFILL_LIMIT,
    frequency_note="daily business series; effective next UTC day; max fill = 168 bars",
)


def align_external_to_ohlcv(
    ohlcv: pd.DataFrame,
    external: pd.DataFrame,
    policy: AlignmentPolicy,
) -> pd.DataFrame:
    if ohlcv.empty:
        result = ohlcv.copy()
        result[policy.aligned_col] = pd.Series(dtype=float)
        return result

    if (
        external.empty
        or policy.external_col not in external.columns
        or policy.ts_col not in external.columns
    ):
        result = ohlcv.copy()
        result[policy.aligned_col] = float("nan")
        return result

    ohlcv_sorted = ohlcv.sort_values("timestamp").reset_index(drop=True)
    external_sorted = (
        external[[policy.ts_col, policy.external_col]]
        .dropna(subset=[policy.ts_col])
        .sort_values(policy.ts_col)
        .reset_index(drop=True)
    )
    if external_sorted.empty:
        result = ohlcv.copy()
        result[policy.aligned_col] = float("nan")
        return result

    external_prepared = external_sorted.copy()
    external_prepared["_ext_src_ts"] = external_prepared[policy.ts_col].astype("int64")
    external_prepared = external_prepared.rename(
        columns={policy.ts_col: "timestamp", policy.external_col: policy.aligned_col}
    )

    merged = pd.merge_asof(
        ohlcv_sorted,
        external_prepared[["timestamp", policy.aligned_col, "_ext_src_ts"]],
        on="timestamp",
        direction="backward",
    )

    lag_bars = (merged["timestamp"] - merged["_ext_src_ts"]) / 3_600_000
    stale_mask = lag_bars > policy.ffill_limit
    merged.loc[stale_mask, policy.aligned_col] = float("nan")
    return merged.drop(columns=["_ext_src_ts"])


def compute_coverage_stats(ohlcv: pd.DataFrame, aligned_col: str) -> dict:
    total = len(ohlcv)
    if total == 0:
        return {"total_rows": 0, "rows_with_data": 0, "coverage_pct": 0.0, "rows_missing": 0}

    rows_with_data = int(ohlcv[aligned_col].notna().sum())
    return {
        "total_rows": total,
        "rows_with_data": rows_with_data,
        "coverage_pct": round(rows_with_data / total * 100, 2),
        "rows_missing": total - rows_with_data,
    }
