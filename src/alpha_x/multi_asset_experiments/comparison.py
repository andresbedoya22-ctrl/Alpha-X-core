from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from alpha_x.modeling.evaluation import (
    build_best_model_regime_metrics,
    build_temporal_model_splits,
    build_test_baseline_comparison,
    build_test_signal_backtest,
    fit_and_evaluate_models,
    refit_best_model_for_test_signal,
)
from alpha_x.multi_asset.config import OFFICIAL_INTERVAL, OFFICIAL_MARKETS
from alpha_x.multi_asset_experiments.common_window import CommonWindowDefinition
from alpha_x.multi_asset_experiments.datasets import (
    AssetExperimentDataset,
    build_asset_experiment_dataset,
)

COMMON_POLICY_THRESHOLD = 0.55


@dataclass(frozen=True)
class AssetComparisonResult:
    market: str
    asset: str
    dataset_summary: dict[str, Any]
    best_model_id: str
    best_validation_metrics: dict[str, Any]
    best_test_metrics: dict[str, Any]
    best_backtest_metrics: dict[str, Any]
    best_baseline_metrics: dict[str, Any]
    evaluation_frame: pd.DataFrame
    regime_metrics: pd.DataFrame
    backtest_comparison: pd.DataFrame
    test_predictions: pd.DataFrame


@dataclass(frozen=True)
class MultiAssetComparisonResult:
    common_window: CommonWindowDefinition
    policy_threshold: float
    asset_results: list[AssetComparisonResult]
    comparison_frame: pd.DataFrame
    promisingness_frame: pd.DataFrame
    conclusion: str


def run_multi_asset_comparison(
    *,
    raw_data_dir: Path,
    external_data_dir: Path,
    common_window: CommonWindowDefinition,
    markets: list[str] | None = None,
    timeframe: str = OFFICIAL_INTERVAL,
    threshold: float = COMMON_POLICY_THRESHOLD,
    fee_rate: float,
    slippage_rate: float,
    initial_capital: float,
    sma_fast: int,
    sma_slow: int,
) -> MultiAssetComparisonResult:
    market_list = markets or OFFICIAL_MARKETS
    asset_results: list[AssetComparisonResult] = []

    for market in market_list:
        dataset_result = build_asset_experiment_dataset(
            raw_data_dir=raw_data_dir,
            external_data_dir=external_data_dir,
            market=market,
            timeframe=timeframe,
            common_window=common_window,
        )
        asset_results.append(
            _evaluate_asset(
                dataset_result,
                threshold=threshold,
                fee_rate=fee_rate,
                slippage_rate=slippage_rate,
                initial_capital=initial_capital,
                sma_fast=sma_fast,
                sma_slow=sma_slow,
            )
        )

    comparison_frame = build_asset_comparison_frame(asset_results)
    promisingness_frame = build_promisingness_frame(comparison_frame)
    conclusion = build_comparison_conclusion(comparison_frame)
    return MultiAssetComparisonResult(
        common_window=common_window,
        policy_threshold=threshold,
        asset_results=asset_results,
        comparison_frame=comparison_frame,
        promisingness_frame=promisingness_frame,
        conclusion=conclusion,
    )


def build_asset_comparison_frame(asset_results: list[AssetComparisonResult]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for result in asset_results:
        rows.append(
            {
                "market": result.market,
                "asset": result.asset,
                "window_rows": result.dataset_summary["window_rows"],
                "supervised_rows": result.dataset_summary["supervised_rows"],
                "funding_coverage_pct": result.dataset_summary["funding_coverage_pct"],
                "etf_flow_coverage_pct": result.dataset_summary["etf_flow_coverage_pct"],
                "target_distribution": str(result.dataset_summary["target_distribution"]),
                "best_model_id": result.best_model_id,
                "validation_balanced_accuracy": result.best_validation_metrics["balanced_accuracy"],
                "validation_macro_f1": result.best_validation_metrics["macro_f1"],
                "test_balanced_accuracy": result.best_test_metrics["balanced_accuracy"],
                "test_macro_f1": result.best_test_metrics["macro_f1"],
                "policy_threshold": result.best_backtest_metrics["threshold"],
                "supervised_total_return": result.best_backtest_metrics["total_return"],
                "supervised_max_drawdown": result.best_backtest_metrics["max_drawdown"],
                "supervised_trades": result.best_backtest_metrics["trades"],
                "supervised_exposure": result.best_backtest_metrics["exposure"],
                "supervised_final_equity": result.best_backtest_metrics["final_equity"],
                "best_baseline_name": result.best_baseline_metrics["name"],
                "best_baseline_total_return": result.best_baseline_metrics["total_return"],
                "delta_vs_best_baseline": (
                    result.best_backtest_metrics["total_return"]
                    - result.best_baseline_metrics["total_return"]
                ),
            }
        )
    return pd.DataFrame(rows).sort_values("market").reset_index(drop=True)


def build_promisingness_frame(comparison_frame: pd.DataFrame) -> pd.DataFrame:
    btc_row = comparison_frame.loc[comparison_frame["market"].eq("BTC-EUR")].iloc[0]
    rows: list[dict[str, Any]] = []
    for _, row in comparison_frame.iterrows():
        rows.append(
            {
                "market": row["market"],
                "asset": row["asset"],
                "predictive_vs_btc": _compare_with_tolerance(
                    float(row["test_balanced_accuracy"]),
                    float(btc_row["test_balanced_accuracy"]),
                    tolerance=0.005,
                ),
                "operability_vs_btc": _compare_with_tolerance(
                    float(row["supervised_total_return"]),
                    float(btc_row["supervised_total_return"]),
                    tolerance=0.01,
                ),
                "baseline_relative": _compare_with_tolerance(
                    float(row["delta_vs_best_baseline"]),
                    float(btc_row["delta_vs_best_baseline"]),
                    tolerance=0.01,
                ),
            }
        )
    return pd.DataFrame(rows)


def build_comparison_conclusion(comparison_frame: pd.DataFrame) -> str:
    ranked = comparison_frame.sort_values(
        ["delta_vs_best_baseline", "supervised_total_return", "test_balanced_accuracy"],
        ascending=False,
    ).reset_index(drop=True)
    leader = ranked.iloc[0]
    if (
        float(leader["delta_vs_best_baseline"]) <= 0.0
        and float(leader["supervised_total_return"]) <= 0.0
    ):
        return (
            "Ningun activo muestra una mejora operable clara con la base enriquecida actual. "
            "El problema parece seguir siendo compartido entre el activo y el set "
            "de features/contexto."
        )
    return (
        f"El activo relativamente mas prometedor en esta pasada es {leader['market']}: "
        f"lidera por delta frente a baseline ({leader['delta_vs_best_baseline']:.4f}) "
        f"y retorno supervisado ({leader['supervised_total_return']:.4f}). "
        "Aun asi, esto debe leerse como priorizacion de investigacion, "
        "no como evidencia final de edge."
    )


def _evaluate_asset(
    dataset_result: AssetExperimentDataset,
    *,
    threshold: float,
    fee_rate: float,
    slippage_rate: float,
    initial_capital: float,
    sma_fast: int,
    sma_slow: int,
) -> AssetComparisonResult:
    evaluation_rows, _fitted_models, best_model_id = fit_and_evaluate_models(
        dataset_result.supervised_frame,
        feature_columns=dataset_result.feature_columns,
        categorical_columns=dataset_result.categorical_columns,
        target_column=dataset_result.target_column,
    )
    evaluation_frame = pd.DataFrame([asdict(row) for row in evaluation_rows])
    validation_row = _select_best_segment_row(evaluation_frame, best_model_id, "validation")
    test_row = _select_best_segment_row(evaluation_frame, best_model_id, "test")

    _pipeline, test_predictions = refit_best_model_for_test_signal(
        dataset_result.supervised_frame,
        feature_columns=dataset_result.feature_columns,
        categorical_columns=dataset_result.categorical_columns,
        target_column=dataset_result.target_column,
        best_model_id=best_model_id,
    )
    test_backtest, _equity_curve = build_test_signal_backtest(
        dataset_result.dataset.frame,
        test_predictions,
        threshold=threshold,
        fee_rate=fee_rate,
        slippage_rate=slippage_rate,
        initial_capital=initial_capital,
    )

    split_frames = build_temporal_model_splits(dataset_result.supervised_frame)
    test_start_timestamp = int(split_frames["test"]["timestamp"].iloc[0])
    test_dataset_frame = dataset_result.dataset.frame.loc[
        dataset_result.dataset.frame["timestamp"] >= test_start_timestamp
    ].copy()
    baseline_rows = build_test_baseline_comparison(
        test_dataset_frame,
        fee_rate=fee_rate,
        slippage_rate=slippage_rate,
        initial_capital=initial_capital,
        sma_fast=sma_fast,
        sma_slow=sma_slow,
    )
    backtest_comparison = pd.DataFrame([asdict(test_backtest), *baseline_rows])
    best_baseline = max(baseline_rows, key=lambda row: float(row["total_return"]))
    regime_metrics = build_best_model_regime_metrics(
        test_predictions,
        target_column=dataset_result.target_column,
        threshold=threshold,
    )

    return AssetComparisonResult(
        market=dataset_result.market,
        asset=dataset_result.market_info.base_asset,
        dataset_summary=dataset_result.dataset_summary,
        best_model_id=best_model_id,
        best_validation_metrics=validation_row.to_dict(),
        best_test_metrics=test_row.to_dict(),
        best_backtest_metrics={**asdict(test_backtest), "threshold": threshold},
        best_baseline_metrics=best_baseline,
        evaluation_frame=evaluation_frame,
        regime_metrics=regime_metrics,
        backtest_comparison=backtest_comparison,
        test_predictions=test_predictions,
    )


def _select_best_segment_row(
    evaluation_frame: pd.DataFrame,
    best_model_id: str,
    segment: str,
) -> pd.Series:
    return (
        evaluation_frame.loc[
            evaluation_frame["model_id"].eq(best_model_id)
            & evaluation_frame["segment"].eq(segment)
        ]
        .iloc[0]
    )


def _compare_with_tolerance(value: float, reference: float, *, tolerance: float) -> str:
    if value > reference + tolerance:
        return "mejor"
    if value < reference - tolerance:
        return "peor"
    return "igual"
