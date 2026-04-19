from __future__ import annotations

import pandas as pd


def apply_cooldown(signal: pd.Series, *, cooldown_bars: int) -> pd.Series:
    if cooldown_bars < 0:
        raise ValueError("cooldown_bars must be non-negative.")
    if cooldown_bars == 0:
        return signal.astype("int64")

    raw = signal.astype("int64").tolist()
    output: list[int] = []
    previous_output = 0
    cooldown_remaining = 0

    for value in raw:
        if cooldown_remaining > 0 and value == 1:
            current = 0
            cooldown_remaining -= 1
        else:
            current = value
            if cooldown_remaining > 0:
                cooldown_remaining -= 1

        if previous_output == 1 and current == 0:
            cooldown_remaining = cooldown_bars

        output.append(current)
        previous_output = current

    return pd.Series(output, index=signal.index, dtype="int64")
