from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from alpha_x.backtest.models import LoadedBacktestDataset
from alpha_x.benchmarks import DatasetInfo
from alpha_x.data.ohlcv_storage import build_ohlcv_csv_path
from alpha_x.data.ohlcv_validation import summarize_gaps, validate_temporal_integrity
from alpha_x.external_data.alignment import (
    ETF_PROXY_ALIGNMENT_POLICY,
    FUNDING_ALIGNMENT_POLICY,
    align_external_to_ohlcv,
)
from alpha_x.external_data.etf_flows import BitboEtfFlowSource
from alpha_x.external_data.funding import BybitFundingSource
from alpha_x.features.engine import build_feature_frame_for_export, load_feature_dataset
from alpha_x.modeling.dataset import REGIME_COLUMN, REGIME_VALID_COLUMN, TARGET_COLUMN
from alpha_x.multi_asset.markets import MarketInfo, get_market_info
from alpha_x.multi_asset_experiments.common_window import (
    CommonWindowDefinition,
    apply_common_window,
)
from alpha_x.regime.catalog import get_default_regime_rule_set
from alpha_x.regime.rules import detect_regimes

EXTERNAL_FEATURE_COLUMNS = ["funding_rate", "btc_etf_flow_usd"]


@dataclass(frozen=True)
class AssetExperimentDataset:
    market: str
    market_info: MarketInfo
    dataset: LoadedBacktestDataset
    full_frame: pd.DataFrame
    supervised_frame: pd.DataFrame
    feature_columns: list[str]
    categorical_columns: list[str]
    target_column: str
    dataset_summary: dict[str, Any]
    dataset_context: dict[str, Any]


def build_asset_experiment_dataset(
    *,
    raw_data_dir: Path,
    external_data_dir: Path,
    market: str,
    timeframe: str,
    common_window: CommonWindowDefinition,
) -> AssetExperimentDataset:
    market_info = get_market_info(market)
    dataset_path = build_ohlcv_csv_path(
        raw_data_dir=raw_data_dir,
        exchange=market_info.exchange,
        market=market,
        timeframe=timeframe,
    )
    loaded = load_feature_dataset(dataset_path, timeframe)
    windowed_frame = apply_common_window(loaded.frame, common_window)
    if windowed_frame.empty:
        raise ValueError(f"Common window produced an empty dataset for {market}.")

    windowed_dataset = _build_windowed_dataset(
        loaded,
        windowed_frame,
        market=market,
        timeframe=timeframe,
    )
    feature_result = build_feature_frame_for_export(
        windowed_dataset,
        timeframe=timeframe,
        join_labels=True,
    )
    enriched_frame = _align_external_context(
        feature_result.feature_frame,
        external_data_dir=external_data_dir,
        market_info=market_info,
    )
    regime_result = detect_regimes(enriched_frame, get_default_regime_rule_set())
    frame = regime_result.frame.copy()

    frame[TARGET_COLUMN] = pd.Series(pd.NA, index=frame.index, dtype="Int64")
    valid_labels = frame["tb_is_valid"].fillna(False)
    frame.loc[valid_labels, TARGET_COLUMN] = (
        frame.loc[valid_labels, "tb_label"].eq(1).astype("int64")
    )
    frame["external_context_is_valid"] = frame[EXTERNAL_FEATURE_COLUMNS].notna().all(axis=1)
    frame["supervised_is_valid"] = (
        frame["valid_feature_row"]
        & frame[REGIME_VALID_COLUMN]
        & valid_labels
        & frame["external_context_is_valid"]
    )
    frame["supervised_discard_reason"] = "valid"
    frame.loc[~frame["valid_feature_row"], "supervised_discard_reason"] = "feature_warmup"
    frame.loc[
        frame["valid_feature_row"] & ~frame[REGIME_VALID_COLUMN],
        "supervised_discard_reason",
    ] = "regime_unavailable"
    frame.loc[
        frame["valid_feature_row"] & frame[REGIME_VALID_COLUMN] & ~valid_labels,
        "supervised_discard_reason",
    ] = "label_unavailable"
    frame.loc[
        frame["valid_feature_row"]
        & frame[REGIME_VALID_COLUMN]
        & valid_labels
        & ~frame["external_context_is_valid"],
        "supervised_discard_reason",
    ] = "external_context_unavailable"

    feature_columns = feature_result.feature_names + EXTERNAL_FEATURE_COLUMNS
    categorical_columns = [REGIME_COLUMN]
    supervised = frame.loc[frame["supervised_is_valid"]].copy().reset_index(drop=True)
    if supervised.empty:
        raise ValueError(f"No supervised rows available for {market} inside the common window.")
    supervised[TARGET_COLUMN] = supervised[TARGET_COLUMN].astype("int64")
    supervised["row_id"] = range(len(supervised))

    target_distribution = (
        supervised.groupby(TARGET_COLUMN)
        .size()
        .rename("rows")
        .reset_index()
        .sort_values(TARGET_COLUMN)
        .reset_index(drop=True)
    )
    discard_counts = (
        frame.groupby("supervised_discard_reason", dropna=False)
        .size()
        .rename("rows")
        .reset_index()
        .sort_values("rows", ascending=False)
        .reset_index(drop=True)
    )
    funding_coverage_pct = float(frame["funding_rate"].notna().mean() * 100)
    etf_coverage_pct = float(frame["btc_etf_flow_usd"].notna().mean() * 100)
    dataset_summary = {
        "market": market,
        "asset": market_info.base_asset,
        "target_definition": "Binary target: 1 when triple-barrier label is +1, else 0.",
        "common_window_start": common_window.start.isoformat(),
        "common_window_end": common_window.end.isoformat(),
        "window_rows": len(frame),
        "supervised_rows": len(supervised),
        "discarded_rows": int((~frame["supervised_is_valid"]).sum()),
        "discard_pct": 0.0 if len(frame) == 0 else float((~frame["supervised_is_valid"]).mean()),
        "discard_counts": discard_counts.to_dict(orient="records"),
        "target_distribution": target_distribution.to_dict(orient="records"),
        "feature_count": len(feature_columns),
        "technical_feature_count": len(feature_result.feature_names),
        "external_feature_columns": EXTERNAL_FEATURE_COLUMNS,
        "funding_coverage_pct": round(funding_coverage_pct, 2),
        "etf_flow_coverage_pct": round(etf_coverage_pct, 2),
        "rows_with_full_context": int(frame["external_context_is_valid"].sum()),
    }
    dataset_context = {
        "path": str(dataset_path),
        "market": market,
        "asset": market_info.base_asset,
        "timeframe": timeframe,
        "window_rows": len(frame),
        "supervised_rows": len(supervised),
        "common_window_start": common_window.start.isoformat(),
        "common_window_end": common_window.end.isoformat(),
        "gap_count": windowed_dataset.gap_summary.gap_count,
        "total_missing_intervals": windowed_dataset.gap_summary.total_missing_intervals,
    }
    return AssetExperimentDataset(
        market=market,
        market_info=market_info,
        dataset=windowed_dataset,
        full_frame=frame,
        supervised_frame=supervised,
        feature_columns=feature_columns,
        categorical_columns=categorical_columns,
        target_column=TARGET_COLUMN,
        dataset_summary=dataset_summary,
        dataset_context=dataset_context,
    )


def _build_windowed_dataset(
    dataset: LoadedBacktestDataset,
    frame: pd.DataFrame,
    *,
    market: str,
    timeframe: str,
) -> LoadedBacktestDataset:
    validation = validate_temporal_integrity(frame, timeframe)
    dataset_info = DatasetInfo(
        path=dataset.dataset_info.path,
        market=market,
        timeframe=timeframe,
        row_count=len(frame),
        start_timestamp=int(frame["timestamp"].iloc[0]),
        end_timestamp=int(frame["timestamp"].iloc[-1]),
    )
    return LoadedBacktestDataset(
        frame=frame.reset_index(drop=True),
        dataset_info=dataset_info,
        validation_report=validation,
        gap_summary=summarize_gaps(validation),
    )


def _align_external_context(
    frame: pd.DataFrame,
    *,
    external_data_dir: Path,
    market_info: MarketInfo,
) -> pd.DataFrame:
    funding_source = BybitFundingSource(storage_dir=external_data_dir)
    etf_source = BitboEtfFlowSource(storage_dir=external_data_dir)

    funding_frame = funding_source.load(market_info.funding_symbol)
    etf_key = market_info.etf_ticker or "BTC_SPOT_ETF"
    etf_frame = etf_source.load(etf_key)
    if etf_frame.empty and etf_key != "BTC_SPOT_ETF":
        etf_frame = etf_source.load("BTC_SPOT_ETF")

    aligned = align_external_to_ohlcv(frame, funding_frame, FUNDING_ALIGNMENT_POLICY)
    aligned = align_external_to_ohlcv(aligned, etf_frame, ETF_PROXY_ALIGNMENT_POLICY)
    return aligned
