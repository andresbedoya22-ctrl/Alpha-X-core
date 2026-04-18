from __future__ import annotations

from alpha_x.validation.base import ValidationCandidate


def get_validation_candidates() -> list[ValidationCandidate]:
    return [
        ValidationCandidate(
            candidate_id="benchmark_sma_crossover",
            name="Benchmark C - SMA Crossover Baseline",
            family="benchmark",
            source_type="benchmark",
            parameters={"fast_window": 20, "slow_window": 50},
        ),
        ValidationCandidate(
            candidate_id="trend_sma200",
            name="Hypothesis 1 - Trend Following (Close > SMA 200)",
            family="trend_following",
            source_type="strategy",
            parameters={"slow_window": 200},
        ),
        ValidationCandidate(
            candidate_id="trend_volatility_filter",
            name="Hypothesis 5 - Volatility Filter (Trend + vol band)",
            family="volatility_filter",
            source_type="strategy",
            parameters={
                "base_slow_window": 200,
                "volatility_window": 24,
                "min_volatility": 0.008,
                "max_volatility": 0.05,
            },
        ),
    ]


def get_parameter_sensitivity_grid(candidate: ValidationCandidate) -> list[dict[str, float | int]]:
    if candidate.candidate_id == "benchmark_sma_crossover":
        return [
            {"fast_window": 15, "slow_window": 40},
            {"fast_window": 20, "slow_window": 50},
            {"fast_window": 25, "slow_window": 60},
        ]
    if candidate.candidate_id == "trend_sma200":
        return [{"slow_window": 150}, {"slow_window": 200}, {"slow_window": 250}]
    if candidate.candidate_id == "trend_volatility_filter":
        return [
            {
                "base_slow_window": 150,
                "volatility_window": 24,
                "min_volatility": 0.006,
                "max_volatility": 0.04,
            },
            {
                "base_slow_window": 200,
                "volatility_window": 24,
                "min_volatility": 0.008,
                "max_volatility": 0.05,
            },
            {
                "base_slow_window": 250,
                "volatility_window": 36,
                "min_volatility": 0.01,
                "max_volatility": 0.06,
            },
        ]
    raise ValueError(f"Unsupported candidate for sensitivity analysis: {candidate.candidate_id}")
