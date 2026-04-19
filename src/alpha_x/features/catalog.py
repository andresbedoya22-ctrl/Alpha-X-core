from __future__ import annotations

from alpha_x.features.base import FeatureDefinition
from alpha_x.features.compression import get_compression_features
from alpha_x.features.price_structure import get_price_structure_features
from alpha_x.features.returns import get_return_features
from alpha_x.features.trend import get_trend_features
from alpha_x.features.volatility import get_volatility_features


def get_feature_catalog() -> list[FeatureDefinition]:
    catalog = [
        *get_return_features(),
        *get_trend_features(),
        *get_volatility_features(),
        *get_compression_features(),
        *get_price_structure_features(),
    ]
    feature_ids = [definition.feature_id for definition in catalog]
    if len(feature_ids) != len(set(feature_ids)):
        raise ValueError("Feature catalog contains duplicated feature_id values.")
    return catalog
