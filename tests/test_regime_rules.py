from __future__ import annotations

import pandas as pd

from alpha_x.regime.catalog import get_default_regime_rule_set
from alpha_x.regime.rules import detect_regimes


def _build_feature_frame() -> pd.DataFrame:
    rows = 200
    timestamps = [index * 3_600_000 for index in range(rows)]
    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "datetime": pd.to_datetime(timestamps, unit="ms", utc=True),
            "close": [100.0 + index for index in range(rows)],
            "dist_sma_72": [0.02] * 150 + [-0.02] * 50,
            "sma_24_slope_4": [0.5] * 150 + [-0.5] * 50,
            "atr_pct_24": [0.02] * 180 + [0.08] * 20,
            "range_pct_24_rank_72": [0.1] * 100 + [0.9] * 100,
            "valid_feature_row": [False] * 95 + [True] * 105,
        }
    )
    return frame


def test_detect_regimes_assigns_expected_categories() -> None:
    result = detect_regimes(_build_feature_frame(), get_default_regime_rule_set())

    assert result.assigned_rows > 0
    valid = result.frame.loc[result.frame["regime_is_valid"]]
    assert set(valid["regime"].dropna().unique().tolist()).issubset(
        set(get_default_regime_rule_set().regime_descriptions.keys())
    )
    assert valid["compression_state"].isin(["compressed", "expanded", "normal"]).all()


def test_detect_regimes_marks_warmup_rows_as_invalid() -> None:
    result = detect_regimes(_build_feature_frame(), get_default_regime_rule_set())

    assert bool(result.frame.loc[0, "regime_is_valid"]) is False
    assert result.frame.loc[0, "regime_discard_reason"] == "feature_warmup"
    assert result.frame["regime_discard_reason"].isin(
        ["valid", "feature_warmup", "volatility_baseline_warmup"]
    ).all()
