from __future__ import annotations

from dataclasses import asdict

import pandas as pd
from sklearn.metrics import (
    balanced_accuracy_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
)
from sklearn.pipeline import Pipeline

from alpha_x.backtest.engine import run_long_flat_backtest
from alpha_x.backtest.metrics import PerformanceRow, calculate_backtest_metrics
from alpha_x.benchmarks.metrics import calculate_benchmark_metrics
from alpha_x.benchmarks.sma_baseline import run_sma_baseline
from alpha_x.modeling.base import ModelEvaluationRow
from alpha_x.modeling.models import get_model_specs
from alpha_x.modeling.preprocessing import build_preprocessor
from alpha_x.strategies.catalog import get_strategy_catalog
from alpha_x.validation.splits import build_temporal_splits, slice_frame_for_split


def build_temporal_model_splits(frame: pd.DataFrame) -> dict[str, pd.DataFrame]:
    splits = build_temporal_splits(frame)
    return {split.segment: slice_frame_for_split(frame, split) for split in splits}


def fit_and_evaluate_models(
    supervised_frame: pd.DataFrame,
    *,
    feature_columns: list[str],
    categorical_columns: list[str],
    target_column: str,
) -> tuple[list[ModelEvaluationRow], dict[str, Pipeline], str]:
    split_frames = build_temporal_model_splits(supervised_frame)
    X_train = split_frames["train"][feature_columns + categorical_columns]
    y_train = split_frames["train"][target_column]

    evaluation_rows: list[ModelEvaluationRow] = []
    fitted_models: dict[str, Pipeline] = {}
    best_model_id: str | None = None
    best_score: tuple[float, float] | None = None

    for spec in get_model_specs():
        pipeline = Pipeline(
            steps=[
                (
                    "preprocessor",
                    build_preprocessor(
                        numeric_columns=feature_columns,
                        categorical_columns=categorical_columns,
                        use_scaling=spec.use_scaling,
                    ),
                ),
                ("model", spec.estimator),
            ]
        )
        pipeline.fit(X_train, y_train)
        fitted_models[spec.model_id] = pipeline

        for segment in ("validation", "test"):
            split_frame = split_frames[segment]
            scores = evaluate_classifier(
                model_id=spec.model_id,
                model_name=spec.name,
                segment=segment,
                pipeline=pipeline,
                frame=split_frame,
                feature_columns=feature_columns,
                categorical_columns=categorical_columns,
                target_column=target_column,
            )
            evaluation_rows.append(scores)
            if segment == "validation":
                candidate_score = (scores.balanced_accuracy, scores.macro_f1)
                if best_score is None or candidate_score > best_score:
                    best_score = candidate_score
                    best_model_id = spec.model_id

    if best_model_id is None:
        raise RuntimeError("No model was evaluated.")
    return evaluation_rows, fitted_models, best_model_id


def evaluate_classifier(
    *,
    model_id: str,
    model_name: str,
    segment: str,
    pipeline: Pipeline,
    frame: pd.DataFrame,
    feature_columns: list[str],
    categorical_columns: list[str],
    target_column: str,
) -> ModelEvaluationRow:
    X = frame[feature_columns + categorical_columns]
    y_true = frame[target_column]
    y_pred = pipeline.predict(X)
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[0, 1]).ravel()
    return ModelEvaluationRow(
        model_id=model_id,
        model_name=model_name,
        segment=segment,
        rows=len(frame),
        positive_rate=float(y_true.mean()),
        balanced_accuracy=float(balanced_accuracy_score(y_true, y_pred)),
        macro_f1=float(f1_score(y_true, y_pred, average="macro", zero_division=0)),
        precision_positive=float(precision_score(y_true, y_pred, zero_division=0)),
        recall_positive=float(recall_score(y_true, y_pred, zero_division=0)),
        predicted_positive_rate=float(pd.Series(y_pred).mean()),
        confusion_tn=int(tn),
        confusion_fp=int(fp),
        confusion_fn=int(fn),
        confusion_tp=int(tp),
    )


def refit_best_model_for_test_signal(
    supervised_frame: pd.DataFrame,
    *,
    feature_columns: list[str],
    categorical_columns: list[str],
    target_column: str,
    best_model_id: str,
) -> tuple[Pipeline, pd.DataFrame]:
    split_frames = build_temporal_model_splits(supervised_frame)
    train_validation = pd.concat(
        [split_frames["train"], split_frames["validation"]],
        ignore_index=True,
    )
    test_frame = split_frames["test"].copy().reset_index(drop=True)
    spec = next(item for item in get_model_specs() if item.model_id == best_model_id)
    pipeline = Pipeline(
        steps=[
            (
                "preprocessor",
                build_preprocessor(
                    numeric_columns=feature_columns,
                    categorical_columns=categorical_columns,
                    use_scaling=spec.use_scaling,
                ),
            ),
            ("model", spec.estimator),
        ]
    )
    pipeline.fit(
        train_validation[feature_columns + categorical_columns],
        train_validation[target_column],
    )
    probabilities = pipeline.predict_proba(test_frame[feature_columns + categorical_columns])[:, 1]
    test_frame["predicted_proba"] = probabilities
    return pipeline, test_frame


def build_test_signal_backtest(
    dataset_frame: pd.DataFrame,
    test_predictions: pd.DataFrame,
    *,
    threshold: float,
    fee_rate: float,
    slippage_rate: float,
    initial_capital: float,
) -> tuple[PerformanceRow, pd.DataFrame]:
    merged = dataset_frame.loc[:, ["timestamp", "datetime", "close"]].merge(
        test_predictions.loc[:, ["timestamp", "predicted_proba"]],
        on="timestamp",
        how="left",
    )
    merged["signal"] = merged["predicted_proba"].fillna(0.0).ge(threshold).astype("int64")
    result = run_long_flat_backtest(
        merged,
        initial_capital=initial_capital,
        fee_rate=fee_rate,
        slippage_rate=slippage_rate,
        name=f"Supervised Signal (p>{threshold:.2f})",
    )
    return calculate_backtest_metrics(result), result.equity_curve


def build_test_baseline_comparison(
    test_dataset_frame: pd.DataFrame,
    *,
    fee_rate: float,
    slippage_rate: float,
    initial_capital: float,
    sma_fast: int,
    sma_slow: int,
) -> list[dict[str, object]]:
    strategy = next(
        item for item in get_strategy_catalog() if item.strategy_id == "trend_volatility_filter"
    )
    h5_signal = strategy.build_signal(test_dataset_frame)
    h5_backtest = run_long_flat_backtest(
        h5_signal,
        initial_capital=initial_capital,
        fee_rate=fee_rate,
        slippage_rate=slippage_rate,
        name=strategy.name,
    )
    h5_metrics = asdict(calculate_backtest_metrics(h5_backtest))

    sma_result = run_sma_baseline(
        test_dataset_frame,
        fee_rate=fee_rate,
        initial_capital=initial_capital,
        fast_window=sma_fast,
        slow_window=sma_slow,
    )
    sma_metrics = calculate_benchmark_metrics(sma_result)
    return [
        h5_metrics,
        {
            "name": sma_result.name,
            "source_id": "sma_crossover",
            "total_return": sma_metrics.total_return,
            "annualized_return": sma_metrics.annualized_return,
            "max_drawdown": sma_metrics.max_drawdown,
            "profit_factor": None,
            "trades": sma_metrics.trades,
            "exposure": sma_metrics.exposure,
            "final_equity": float(sma_result.equity_curve["equity"].iloc[-1]),
        },
    ]


def build_best_model_regime_metrics(
    test_predictions: pd.DataFrame,
    *,
    target_column: str,
    threshold: float,
) -> pd.DataFrame:
    frame = test_predictions.copy()
    frame["predicted_label"] = frame["predicted_proba"].ge(threshold).astype("int64")
    rows = []
    for regime, regime_frame in frame.groupby("regime", dropna=False):
        if regime_frame.empty:
            continue
        y_true = regime_frame[target_column]
        y_pred = regime_frame["predicted_label"]
        tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[0, 1]).ravel()
        rows.append(
            {
                "regime": regime,
                "rows": len(regime_frame),
                "positive_rate": float(y_true.mean()),
                "balanced_accuracy": float(balanced_accuracy_score(y_true, y_pred)),
                "macro_f1": float(f1_score(y_true, y_pred, average="macro", zero_division=0)),
                "predicted_positive_rate": float(y_pred.mean()),
                "confusion_tn": int(tn),
                "confusion_fp": int(fp),
                "confusion_fn": int(fn),
                "confusion_tp": int(tp),
            }
        )
    return pd.DataFrame(rows)
