from __future__ import annotations

from dataclasses import dataclass

from alpha_x.truth_engine.rebalance import RebalanceConfig
from alpha_x.truth_engine.weighting import WeightingConfig


@dataclass(frozen=True)
class FamilyDefinition:
    family_id: str
    name: str
    score_column: str
    description: str
    weighting: WeightingConfig
    rebalance: RebalanceConfig


OFFICIAL_FAMILIES: tuple[FamilyDefinition, ...] = (
    FamilyDefinition(
        family_id="family_a",
        name="Familia A - Momentum simple long-only",
        score_column="family_a_score",
        description="Momentum 30/90/180d con filtro absoluto 30d > 0.",
        weighting=WeightingConfig(method="equal_weight", top_n=4),
        rebalance=RebalanceConfig(min_net_advantage=0.0025, no_trade_buffer=0.0, turnover_cap=1.0),
    ),
    FamilyDefinition(
        family_id="family_b",
        name="Familia B - Momentum + regimen",
        score_column="family_b_score",
        description="Momentum simple modulado por regimen BTC trend/vol.",
        weighting=WeightingConfig(method="blend", top_n=4),
        rebalance=RebalanceConfig(
            min_net_advantage=0.0025, no_trade_buffer=0.05, turnover_cap=0.70
        ),
    ),
    FamilyDefinition(
        family_id="family_c",
        name="Familia C - CTREND-light interpretable",
        score_column="family_c_score",
        description=(
            "Score compuesto interpretable con tendencia, breakout, volumen "
            "y penalizacion de volatilidad."
        ),
        weighting=WeightingConfig(method="inverse_volatility", top_n=4),
        rebalance=RebalanceConfig(
            min_net_advantage=0.0030, no_trade_buffer=0.05, turnover_cap=0.60
        ),
    ),
    FamilyDefinition(
        family_id="family_d",
        name="Familia D - Momentum con no-trade logic",
        score_column="family_b_score",
        description="Momentum + regimen con no-trade threshold y turnover cap conservador.",
        weighting=WeightingConfig(method="blend", top_n=4),
        rebalance=RebalanceConfig(
            min_net_advantage=0.0040, no_trade_buffer=0.10, turnover_cap=0.35
        ),
    ),
)
