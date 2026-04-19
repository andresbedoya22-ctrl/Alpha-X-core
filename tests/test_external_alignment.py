from __future__ import annotations

import pandas as pd
import pytest

from alpha_x.external_data.alignment import (
    ETF_PROXY_ALIGNMENT_POLICY,
    FUNDING_ALIGNMENT_POLICY,
    AlignmentPolicy,
    align_external_to_ohlcv,
    compute_coverage_stats,
)

HOUR_MS = 3_600_000


def _make_ohlcv(timestamps_ms: list[int]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": timestamps_ms,
            "open": [100.0] * len(timestamps_ms),
            "high": [101.0] * len(timestamps_ms),
            "low": [99.0] * len(timestamps_ms),
            "close": [100.0] * len(timestamps_ms),
            "volume": [1.0] * len(timestamps_ms),
        }
    )


def test_funding_alignment_is_backward_only() -> None:
    ohlcv = _make_ohlcv([0, HOUR_MS, 2 * HOUR_MS, 3 * HOUR_MS, 4 * HOUR_MS])
    funding = pd.DataFrame({"timestamp_ms": [4 * HOUR_MS], "funding_rate": [0.001]})
    result = align_external_to_ohlcv(ohlcv, funding, FUNDING_ALIGNMENT_POLICY)

    assert result.loc[result["timestamp"] == 3 * HOUR_MS, "funding_rate"].isna().all()
    assert result.loc[result["timestamp"] == 4 * HOUR_MS, "funding_rate"].iloc[0] == pytest.approx(
        0.001
    )


def test_etf_alignment_waits_until_effective_next_day() -> None:
    ohlcv = _make_ohlcv([0, 12 * HOUR_MS, 24 * HOUR_MS, 25 * HOUR_MS])
    etf = pd.DataFrame(
        {
            "timestamp_ms": [0],
            "effective_timestamp_ms": [24 * HOUR_MS],
            "btc_etf_flow_usd": [123.0],
        }
    )
    result = align_external_to_ohlcv(ohlcv, etf, ETF_PROXY_ALIGNMENT_POLICY)

    assert result.loc[result["timestamp"] == 12 * HOUR_MS, "btc_etf_flow_usd"].isna().all()
    assert result.loc[result["timestamp"] == 24 * HOUR_MS, "btc_etf_flow_usd"].iloc[
        0
    ] == pytest.approx(123.0)
    assert result.loc[result["timestamp"] == 25 * HOUR_MS, "btc_etf_flow_usd"].iloc[
        0
    ] == pytest.approx(123.0)


def test_forward_fill_limit_is_respected() -> None:
    policy = AlignmentPolicy(
        source_name="test",
        external_col="value",
        aligned_col="value",
        ts_col="timestamp_ms",
        ffill_limit=2,
        frequency_note="test",
    )
    ohlcv = _make_ohlcv([0, HOUR_MS, 2 * HOUR_MS, 3 * HOUR_MS])
    external = pd.DataFrame({"timestamp_ms": [0], "value": [42.0]})
    result = align_external_to_ohlcv(ohlcv, external, policy)

    assert result.loc[result["timestamp"] == 2 * HOUR_MS, "value"].iloc[0] == pytest.approx(42.0)
    assert result.loc[result["timestamp"] == 3 * HOUR_MS, "value"].isna().all()


def test_compute_coverage_stats() -> None:
    stats = compute_coverage_stats(
        pd.DataFrame({"btc_etf_flow_usd": [1.0, None, 2.0]}), "btc_etf_flow_usd"
    )
    assert stats["total_rows"] == 3
    assert stats["rows_with_data"] == 2
    assert stats["rows_missing"] == 1
    assert stats["coverage_pct"] == pytest.approx(66.67, rel=1e-3)
