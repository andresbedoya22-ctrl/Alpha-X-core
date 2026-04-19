from __future__ import annotations

import pandas as pd

from alpha_x.modeling.policy import build_policy_signal_frame, get_policy_variants


def _build_test_predictions() -> pd.DataFrame:
    timestamps = [index * 3_600_000 for index in range(4)]
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "datetime": pd.to_datetime(timestamps, unit="ms", utc=True),
            "close": [100.0, 101.0, 102.0, 103.0],
            "predicted_proba": [0.59, 0.61, 0.66, 0.72],
            "regime": [
                "trend_up_high_vol",
                "range_low_vol",
                "trend_up_high_vol",
                "trend_up_high_vol",
            ],
        }
    )


def test_policy_threshold_conversion_to_signal() -> None:
    variant = next(item for item in get_policy_variants() if item.policy_id == "policy_a_p065")

    signal_frame = build_policy_signal_frame(_build_test_predictions(), variant=variant)

    assert signal_frame["signal"].tolist() == [0, 0, 1, 1]


def test_policy_regime_filter_keeps_only_target_regime() -> None:
    variant = next(
        item for item in get_policy_variants() if item.policy_id == "policy_c_regime_p060"
    )

    signal_frame = build_policy_signal_frame(_build_test_predictions(), variant=variant)

    assert signal_frame["signal"].tolist() == [0, 0, 1, 1]
    assert signal_frame["regime_allowed"].tolist() == [True, False, True, True]
