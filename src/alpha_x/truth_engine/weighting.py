from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

OPTIONAL_WEIGHTING_HOOKS = ("hrp_hook", "weighted_entropy_hook")


@dataclass(frozen=True)
class WeightingConfig:
    method: str = "blend"
    top_n: int = 4
    min_score: float = 0.0


def compute_target_weights(
    candidates: pd.DataFrame,
    *,
    score_column: str,
    volatility_column: str = "realized_vol_30d",
    config: WeightingConfig | None = None,
    gross_exposure: float = 1.0,
) -> dict[str, float]:
    cfg = config or WeightingConfig()
    if candidates.empty or gross_exposure <= 0:
        return {}

    filtered = candidates.loc[candidates[score_column] > cfg.min_score].copy()
    filtered = filtered.sort_values(score_column, ascending=False).head(cfg.top_n)
    if filtered.empty:
        return {}

    if cfg.method == "equal_weight":
        raw = _equal_weight(filtered)
    elif cfg.method == "inverse_volatility":
        raw = _inverse_volatility(filtered, volatility_column)
    elif cfg.method == "blend":
        equal = _equal_weight(filtered)
        inverse = _inverse_volatility(filtered, volatility_column)
        raw = {
            asset: 0.5 * equal.get(asset, 0.0) + 0.5 * inverse.get(asset, 0.0)
            for asset in filtered["market"]
        }
    else:
        raise ValueError(f"Unsupported weighting method: {cfg.method}")

    return {asset: weight * gross_exposure for asset, weight in raw.items()}


def _equal_weight(filtered: pd.DataFrame) -> dict[str, float]:
    weight = 1.0 / len(filtered)
    return {asset: weight for asset in filtered["market"]}


def _inverse_volatility(filtered: pd.DataFrame, volatility_column: str) -> dict[str, float]:
    inverse = 1.0 / filtered[volatility_column].clip(lower=1e-6)
    total = float(inverse.sum())
    if total <= 0:
        return _equal_weight(filtered)
    return {
        asset: float(value / total)
        for asset, value in zip(filtered["market"], inverse, strict=True)
    }
