from __future__ import annotations

import pytest

from alpha_x.multi_asset.config import OFFICIAL_MARKETS
from alpha_x.multi_asset.markets import MARKET_REGISTRY, get_market_info


def test_official_registry_contains_four_markets() -> None:
    assert OFFICIAL_MARKETS == ["BTC-EUR", "ETH-EUR", "XRP-EUR", "SOL-EUR"]
    assert set(MARKET_REGISTRY) == set(OFFICIAL_MARKETS)


def test_market_metadata_is_complete() -> None:
    for market in OFFICIAL_MARKETS:
        info = get_market_info(market)
        assert info.market == market
        assert info.base_asset
        assert info.quote_asset == "EUR"
        assert info.exchange == "bitvavo"
        assert info.funding_symbol.endswith("USDT")


def test_unknown_market_raises() -> None:
    with pytest.raises(KeyError):
        get_market_info("DOGE-EUR")
