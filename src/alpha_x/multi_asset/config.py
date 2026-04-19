from __future__ import annotations

# Official market registry for the multi-asset research phase.
# All comparative analyses must use these markets consistently.
OFFICIAL_MARKETS: list[str] = ["BTC-EUR", "ETH-EUR", "XRP-EUR", "SOL-EUR"]

# Single timeframe for all multi-asset research (must match existing BTC-EUR data).
OFFICIAL_INTERVAL: str = "1h"

# Default backfill target. Matches existing BTC-EUR depth (~30k rows ≈ 3.4 years at 1h).
TARGET_ROWS_DEFAULT: int = 30_000

# Forward-fill limits for external sources merged onto 1h OHLCV.
# Prevents stale values from persisting across long gaps.
FUNDING_FFILL_LIMIT: int = 8  # funding is every 8h; max 1 period stale
ETF_FLOWS_FFILL_LIMIT: int = 168  # daily business series; cap at 7 days
