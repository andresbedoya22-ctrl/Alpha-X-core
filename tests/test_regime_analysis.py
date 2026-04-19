from __future__ import annotations

import pandas as pd

from alpha_x.regime.analysis import build_regime_label_table


def test_build_regime_label_table_summarizes_label_distribution() -> None:
    frame = pd.DataFrame(
        {
            "regime": ["trend_up_low_vol", "trend_up_low_vol", "range_high_vol"],
            "regime_is_valid": [True, True, True],
            "tb_is_valid": [True, True, True],
            "tb_label": [1, -1, 0],
            "tb_event_return": [0.02, -0.01, 0.0],
        }
    )

    summary = build_regime_label_table(frame)

    assert len(summary) == 3
    assert set(summary["regime"].tolist()) == {"trend_up_low_vol", "range_high_vol"}
    assert summary["label_pct_within_regime"].between(0.0, 1.0).all()
