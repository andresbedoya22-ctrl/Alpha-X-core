from __future__ import annotations

import pandas as pd


def resample_1h_to_4h(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"timestamp", "datetime", "open", "high", "low", "close", "volume"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"Missing required columns for 4h resample: {missing}")
    if frame.empty:
        return frame.copy()

    columns = ["timestamp", "datetime", "open", "high", "low", "close", "volume"]
    prepared = frame.loc[:, columns].copy()
    prepared = prepared.sort_values("timestamp", ascending=True).reset_index(drop=True)
    prepared["bucket_start"] = prepared["datetime"].dt.floor("4h")
    prepared["bucket_end"] = prepared["bucket_start"] + pd.Timedelta(hours=3)
    prepared["expected_end"] = prepared["bucket_start"] + pd.Timedelta(hours=3)

    records: list[dict[str, float | int | pd.Timestamp]] = []
    for _, bucket in prepared.groupby("bucket_start", sort=True):
        if len(bucket) != 4:
            continue
        deltas = bucket["timestamp"].diff().dropna()
        if not deltas.eq(3_600_000).all():
            continue
        if bucket["datetime"].iloc[-1] != bucket["expected_end"].iloc[0]:
            continue

        records.append(
            {
                "timestamp": int(bucket["timestamp"].iloc[0]),
                "datetime": pd.Timestamp(bucket["bucket_start"].iloc[0]),
                "open": float(bucket["open"].iloc[0]),
                "high": float(bucket["high"].max()),
                "low": float(bucket["low"].min()),
                "close": float(bucket["close"].iloc[-1]),
                "volume": float(bucket["volume"].sum()),
            }
        )

    return pd.DataFrame.from_records(
        records,
        columns=["timestamp", "datetime", "open", "high", "low", "close", "volume"],
    )
