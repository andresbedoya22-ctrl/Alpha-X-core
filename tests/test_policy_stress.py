from __future__ import annotations

import pandas as pd

from alpha_x.modeling.policy_stress import (
    get_policy_stress_variants,
    split_test_frame_into_subperiods,
)


def _build_test_predictions(rows: int = 9) -> pd.DataFrame:
    timestamps = [index * 3_600_000 for index in range(rows)]
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "datetime": pd.to_datetime(timestamps, unit="ms", utc=True),
            "close": [100.0 + index for index in range(rows)],
            "predicted_proba": [0.55 + (index * 0.02) for index in range(rows)],
            "regime": ["trend_up_high_vol"] * rows,
        }
    )


def test_threshold_stress_variants_are_local_and_ordered() -> None:
    variants = get_policy_stress_variants()

    assert [variant.threshold for variant in variants] == [0.60, 0.65, 0.70]
    assert {variant.allowed_regime for variant in variants} == {"trend_up_high_vol"}


def test_split_test_frame_into_three_ordered_subperiods() -> None:
    subperiods = split_test_frame_into_subperiods(_build_test_predictions(), parts=3)

    assert len(subperiods) == 3
    assert subperiods[0][1]["timestamp"].iloc[-1] < subperiods[1][1]["timestamp"].iloc[0]
    assert subperiods[1][1]["timestamp"].iloc[-1] < subperiods[2][1]["timestamp"].iloc[0]
