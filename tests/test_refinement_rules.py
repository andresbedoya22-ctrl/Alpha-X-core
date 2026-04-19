from __future__ import annotations

import pandas as pd

from alpha_x.refinements.confirmation import apply_entry_confirmation
from alpha_x.refinements.cooldown import apply_cooldown
from alpha_x.refinements.holding import apply_minimum_holding


def test_minimum_holding_keeps_position_open() -> None:
    signal = pd.Series([0, 1, 0, 0, 0], dtype="int64")

    result = apply_minimum_holding(signal, min_hold_bars=3)

    assert result.tolist() == [0, 1, 1, 1, 0]


def test_cooldown_blocks_immediate_reentry() -> None:
    signal = pd.Series([0, 1, 0, 1, 1, 0], dtype="int64")

    result = apply_cooldown(signal, cooldown_bars=2)

    assert result.tolist() == [0, 1, 0, 0, 0, 0]


def test_confirmation_requires_two_bars_before_entry() -> None:
    signal = pd.Series([0, 1, 1, 1, 0], dtype="int64")

    result = apply_entry_confirmation(signal, confirmation_bars=2)

    assert result.tolist() == [0, 0, 1, 1, 0]
