from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RefinementDefinition:
    refinement_id: str
    name: str
    timeframe: str
    base_type: str
    baseline_id: str | None
    parameters: dict[str, Any]


def get_refinement_catalog() -> list[RefinementDefinition]:
    return [
        RefinementDefinition(
            refinement_id="vol_filter_1h_base",
            name="Volatility Filter 1h Base",
            timeframe="1h",
            base_type="volatility_filter",
            baseline_id=None,
            parameters={
                "base_slow_window": 200,
                "volatility_window": 24,
                "min_volatility": 0.008,
                "max_volatility": 0.05,
                "confirmation_bars": 1,
                "min_hold_bars": 0,
                "cooldown_bars": 0,
            },
        ),
        RefinementDefinition(
            refinement_id="vol_filter_1h_hold",
            name="Volatility Filter 1h + Holding",
            timeframe="1h",
            base_type="volatility_filter",
            baseline_id="vol_filter_1h_base",
            parameters={
                "base_slow_window": 200,
                "volatility_window": 24,
                "min_volatility": 0.008,
                "max_volatility": 0.05,
                "confirmation_bars": 1,
                "min_hold_bars": 12,
                "cooldown_bars": 0,
            },
        ),
        RefinementDefinition(
            refinement_id="vol_filter_1h_hold_cooldown",
            name="Volatility Filter 1h + Holding + Cooldown",
            timeframe="1h",
            base_type="volatility_filter",
            baseline_id="vol_filter_1h_base",
            parameters={
                "base_slow_window": 200,
                "volatility_window": 24,
                "min_volatility": 0.008,
                "max_volatility": 0.05,
                "confirmation_bars": 1,
                "min_hold_bars": 12,
                "cooldown_bars": 12,
            },
        ),
        RefinementDefinition(
            refinement_id="vol_filter_4h_base",
            name="Volatility Filter 4h Base",
            timeframe="4h",
            base_type="volatility_filter",
            baseline_id=None,
            parameters={
                "base_slow_window": 50,
                "volatility_window": 12,
                "min_volatility": 0.012,
                "max_volatility": 0.08,
                "confirmation_bars": 1,
                "min_hold_bars": 0,
                "cooldown_bars": 0,
            },
        ),
        RefinementDefinition(
            refinement_id="vol_filter_4h_hold_cooldown",
            name="Volatility Filter 4h + Holding + Cooldown",
            timeframe="4h",
            base_type="volatility_filter",
            baseline_id="vol_filter_4h_base",
            parameters={
                "base_slow_window": 50,
                "volatility_window": 12,
                "min_volatility": 0.012,
                "max_volatility": 0.08,
                "confirmation_bars": 1,
                "min_hold_bars": 3,
                "cooldown_bars": 3,
            },
        ),
        RefinementDefinition(
            refinement_id="vol_filter_4h_confirm_hold_cooldown",
            name="Volatility Filter 4h + Confirmation + Holding + Cooldown",
            timeframe="4h",
            base_type="volatility_filter",
            baseline_id="vol_filter_4h_base",
            parameters={
                "base_slow_window": 50,
                "volatility_window": 12,
                "min_volatility": 0.012,
                "max_volatility": 0.08,
                "confirmation_bars": 2,
                "min_hold_bars": 3,
                "cooldown_bars": 3,
            },
        ),
        RefinementDefinition(
            refinement_id="sma_baseline_4h_control",
            name="SMA Baseline 4h Control",
            timeframe="4h",
            base_type="sma_baseline",
            baseline_id=None,
            parameters={"fast_window": 5, "slow_window": 15},
        ),
    ]
