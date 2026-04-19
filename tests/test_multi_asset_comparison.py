from __future__ import annotations

import pandas as pd

from alpha_x.multi_asset_experiments.comparison import (
    build_comparison_conclusion,
    build_promisingness_frame,
)


def test_promisingness_compares_against_btc() -> None:
    comparison = pd.DataFrame(
        [
            {
                "market": "BTC-EUR",
                "asset": "BTC",
                "window_rows": 100,
                "supervised_rows": 80,
                "funding_coverage_pct": 100.0,
                "etf_flow_coverage_pct": 100.0,
                "target_distribution": "[]",
                "best_model_id": "random_forest_small",
                "validation_balanced_accuracy": 0.51,
                "validation_macro_f1": 0.5,
                "test_balanced_accuracy": 0.50,
                "test_macro_f1": 0.49,
                "policy_threshold": 0.55,
                "supervised_total_return": -0.02,
                "supervised_max_drawdown": -0.05,
                "supervised_trades": 10,
                "supervised_exposure": 0.3,
                "supervised_final_equity": 9800.0,
                "best_baseline_name": "Hypothesis 5",
                "best_baseline_total_return": 0.01,
                "delta_vs_best_baseline": -0.03,
            },
            {
                "market": "ETH-EUR",
                "asset": "ETH",
                "window_rows": 100,
                "supervised_rows": 80,
                "funding_coverage_pct": 100.0,
                "etf_flow_coverage_pct": 100.0,
                "target_distribution": "[]",
                "best_model_id": "random_forest_small",
                "validation_balanced_accuracy": 0.56,
                "validation_macro_f1": 0.54,
                "test_balanced_accuracy": 0.53,
                "test_macro_f1": 0.52,
                "policy_threshold": 0.55,
                "supervised_total_return": 0.03,
                "supervised_max_drawdown": -0.04,
                "supervised_trades": 9,
                "supervised_exposure": 0.25,
                "supervised_final_equity": 10300.0,
                "best_baseline_name": "Hypothesis 5",
                "best_baseline_total_return": 0.01,
                "delta_vs_best_baseline": 0.02,
            },
        ]
    )

    promisingness = build_promisingness_frame(comparison)

    btc = promisingness.loc[promisingness["market"].eq("BTC-EUR")].iloc[0]
    eth = promisingness.loc[promisingness["market"].eq("ETH-EUR")].iloc[0]
    assert btc["predictive_vs_btc"] == "igual"
    assert eth["predictive_vs_btc"] == "mejor"
    assert eth["operability_vs_btc"] == "mejor"


def test_comparison_conclusion_handles_all_assets_bad() -> None:
    comparison = pd.DataFrame(
        [
            {
                "market": "BTC-EUR",
                "delta_vs_best_baseline": -0.02,
                "supervised_total_return": -0.01,
                "test_balanced_accuracy": 0.5,
            },
            {
                "market": "ETH-EUR",
                "delta_vs_best_baseline": -0.01,
                "supervised_total_return": -0.02,
                "test_balanced_accuracy": 0.51,
            },
        ]
    )
    conclusion = build_comparison_conclusion(comparison)
    assert "Ningun activo" in conclusion
