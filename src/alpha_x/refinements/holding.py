from __future__ import annotations

import pandas as pd


def apply_minimum_holding(signal: pd.Series, *, min_hold_bars: int) -> pd.Series:
    if min_hold_bars < 0:
        raise ValueError("min_hold_bars must be non-negative.")
    if min_hold_bars == 0:
        return signal.astype("int64")

    raw = signal.astype("int64").tolist()
    output: list[int] = []
    in_position = False
    hold_bars = 0

    for value in raw:
        if not in_position:
            if value == 1:
                in_position = True
                hold_bars = 1
                output.append(1)
            else:
                output.append(0)
            continue

        if hold_bars < min_hold_bars:
            output.append(1)
            hold_bars += 1
            continue

        if value == 1:
            output.append(1)
            hold_bars += 1
        else:
            output.append(0)
            in_position = False
            hold_bars = 0

    return pd.Series(output, index=signal.index, dtype="int64")
