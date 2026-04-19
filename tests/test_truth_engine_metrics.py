from __future__ import annotations

import pandas as pd
import pytest

from alpha_x.truth_engine.metrics import calculate_truth_metrics


def test_truth_metrics_nominal_and_normalized_curves_match_percentages() -> None:
    normalized = _equity_curve([1.0, 1.1, 1.21], trade_fees=[0.0, 0.001, 0.002])
    nominal = _equity_curve([10_000.0, 11_000.0, 12_100.0], trade_fees=[0.0, 10.0, 20.0])

    normalized_metrics = calculate_truth_metrics(
        normalized,
        name="normalized",
        source_type="test",
        rebalance_count=1,
        trade_count=1,
        capital_base=1.0,
    )
    nominal_metrics = calculate_truth_metrics(
        nominal,
        name="nominal",
        source_type="test",
        rebalance_count=1,
        trade_count=1,
        capital_base=10_000.0,
    )

    assert normalized_metrics.total_return == nominal_metrics.total_return
    assert normalized_metrics.max_drawdown == nominal_metrics.max_drawdown
    assert normalized_metrics.fee_drag == nominal_metrics.fee_drag
    assert nominal_metrics.total_return == pytest.approx(0.21)


def test_truth_metrics_total_return_uses_capital_base_not_one() -> None:
    curve = _equity_curve([10_000.0, 12_000.0], trade_fees=[0.0, 25.0])

    metrics = calculate_truth_metrics(
        curve,
        name="nominal",
        source_type="test",
        rebalance_count=1,
        trade_count=1,
        capital_base=10_000.0,
    )

    assert metrics.initial_equity == 10_000.0
    assert metrics.capital_base == 10_000.0
    assert metrics.final_equity == 12_000.0
    assert metrics.total_return == pytest.approx(0.20)
    assert metrics.total_return != 11_999.0


def test_truth_metrics_cash_flow_strategy_disables_risk_ratios() -> None:
    curve = _equity_curve([250.0, 500.0, 1_200.0])

    metrics = calculate_truth_metrics(
        curve,
        name="cash-flow",
        source_type="benchmark",
        rebalance_count=3,
        trade_count=3,
        capital_base=1_000.0,
        cash_flow_strategy=True,
    )

    assert metrics.total_return == pytest.approx(0.20)
    assert metrics.cagr is None
    assert metrics.sharpe is None
    assert metrics.sortino is None
    assert metrics.calmar is None


def _equity_curve(values: list[float], trade_fees: list[float] | None = None) -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            "timestamp": [index * 86_400_000 for index in range(len(values))],
            "equity": values,
        }
    )
    frame["bar_return"] = frame["equity"].pct_change().fillna(0.0)
    frame["position"] = 1.0
    frame["turnover"] = 0.0
    frame["trade_fee"] = trade_fees or [0.0 for _ in values]
    return frame
