from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MarketInfo:
    """Official metadata for one research market."""

    market: str
    base_asset: str
    quote_asset: str
    exchange: str
    funding_symbol: str
    has_spot_etf: bool
    etf_ticker: str | None
    etf_notes: str


MARKET_REGISTRY: dict[str, MarketInfo] = {
    "BTC-EUR": MarketInfo(
        market="BTC-EUR",
        base_asset="BTC",
        quote_asset="EUR",
        exchange="bitvavo",
        funding_symbol="BTCUSDT",
        has_spot_etf=True,
        etf_ticker="BTC_SPOT_ETF",
        etf_notes=(
            "BTC ETF flows are used as a global institutional crypto context series. "
            "They are not EUR-spot specific."
        ),
    ),
    "ETH-EUR": MarketInfo(
        market="ETH-EUR",
        base_asset="ETH",
        quote_asset="EUR",
        exchange="bitvavo",
        funding_symbol="ETHUSDT",
        has_spot_etf=True,
        etf_ticker="ETH_SPOT_ETF",
        etf_notes=(
            "ETH spot ETFs exist but public free historical flow coverage is materially shorter "
            "and not fully ingested in this phase."
        ),
    ),
    "XRP-EUR": MarketInfo(
        market="XRP-EUR",
        base_asset="XRP",
        quote_asset="EUR",
        exchange="bitvavo",
        funding_symbol="XRPUSDT",
        has_spot_etf=False,
        etf_ticker=None,
        etf_notes=(
            "No equivalent spot ETF flow series. BTC ETF flows are reused only as a global "
            "institutional crypto context."
        ),
    ),
    "SOL-EUR": MarketInfo(
        market="SOL-EUR",
        base_asset="SOL",
        quote_asset="EUR",
        exchange="bitvavo",
        funding_symbol="SOLUSDT",
        has_spot_etf=False,
        etf_ticker=None,
        etf_notes=(
            "No equivalent spot ETF flow series. BTC ETF flows are reused only as a global "
            "institutional crypto context."
        ),
    ),
}


def get_market_info(market: str) -> MarketInfo:
    if market not in MARKET_REGISTRY:
        raise KeyError(
            f"Market '{market}' is not in the official registry. "
            f"Known markets: {sorted(MARKET_REGISTRY)}"
        )
    return MARKET_REGISTRY[market]


def get_official_markets() -> list[str]:
    return list(MARKET_REGISTRY)
