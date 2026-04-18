from __future__ import annotations

from alpha_x.labeling.base import LabelingDefinition
from alpha_x.labeling.fixed_horizon import build_fixed_horizon_labels
from alpha_x.labeling.next_bar import build_next_bar_labels
from alpha_x.labeling.triple_barrier import build_triple_barrier_labels


def get_labeling_catalog() -> list[LabelingDefinition]:
    return [
        LabelingDefinition(
            labeling_id="next_bar",
            name="Labeling A - Next Bar",
            method="next_bar",
            description="Labels by the next observed bar return with a neutral zone.",
            parameters={
                "positive_threshold": 0.001,
                "negative_threshold": -0.001,
            },
            builder=build_next_bar_labels,
        ),
        LabelingDefinition(
            labeling_id="fixed_horizon_24h",
            name="Labeling B - Fixed Horizon 24h",
            method="fixed_horizon",
            description="Labels by the 24-bar forward return with explicit thresholds.",
            parameters={
                "horizon_bars": 24,
                "positive_threshold": 0.01,
                "negative_threshold": -0.01,
            },
            builder=build_fixed_horizon_labels,
        ),
        LabelingDefinition(
            labeling_id="triple_barrier_24h",
            name="Labeling C - Triple Barrier 24h",
            method="triple_barrier",
            description="Labels by first touch of upper, lower, or time barrier over 24 bars.",
            parameters={
                "horizon_bars": 24,
                "upper_barrier_pct": 0.02,
                "lower_barrier_pct": 0.02,
            },
            builder=build_triple_barrier_labels,
        ),
    ]
