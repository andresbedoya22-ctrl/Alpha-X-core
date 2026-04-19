from __future__ import annotations

from dataclasses import dataclass

from alpha_x.data.bitvavo_client import BitvavoClient


@dataclass(frozen=True)
class UniverseAsset:
    market: str
    priority: int
    reserve: bool = False


OFFICIAL_UNIVERSE: tuple[str, ...] = (
    "BTC-EUR",
    "ETH-EUR",
    "SOL-EUR",
    "XRP-EUR",
    "LINK-EUR",
    "ADA-EUR",
    "AVAX-EUR",
    "DOT-EUR",
    "LTC-EUR",
    "UNI-EUR",
    "AAVE-EUR",
    "ATOM-EUR",
    "XLM-EUR",
    "BCH-EUR",
)

RESERVE_UNIVERSE: tuple[str, ...] = (
    "MATIC-EUR",
    "NEAR-EUR",
    "ALGO-EUR",
)


def build_official_universe() -> list[UniverseAsset]:
    assets = [
        UniverseAsset(market=market, priority=index + 1)
        for index, market in enumerate(OFFICIAL_UNIVERSE)
    ]
    assets.extend(
        UniverseAsset(market=market, priority=len(OFFICIAL_UNIVERSE) + index + 1, reserve=True)
        for index, market in enumerate(RESERVE_UNIVERSE)
    )
    return assets


def validate_pairs_available(
    client: BitvavoClient,
    markets: list[str] | tuple[str, ...],
) -> tuple[list[str], list[str]]:
    available = set(client.list_markets())
    valid = [market for market in markets if market.upper() in available]
    missing = [market for market in markets if market.upper() not in available]
    return valid, missing
