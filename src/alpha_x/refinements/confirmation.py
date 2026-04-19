from __future__ import annotations

import pandas as pd


def apply_entry_confirmation(signal: pd.Series, *, confirmation_bars: int) -> pd.Series:
    if confirmation_bars <= 0:
        raise ValueError("confirmation_bars must be positive.")
    if confirmation_bars == 1:
        return signal.astype("int64")

    raw = signal.astype("int64")
    confirmed = raw.rolling(window=confirmation_bars, min_periods=confirmation_bars).sum().eq(
        confirmation_bars
    )
    output: list[int] = []
    in_position = False

    for index, value in raw.items():
        if in_position:
            if value == 1:
                output.append(1)
            else:
                output.append(0)
                in_position = False
            continue

        if bool(confirmed.loc[index]):
            output.append(1)
            in_position = True
        else:
            output.append(0)

    return pd.Series(output, index=signal.index, dtype="int64")
