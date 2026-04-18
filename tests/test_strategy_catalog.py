from __future__ import annotations

from alpha_x.strategies.catalog import get_strategy_catalog


def test_strategy_catalog_contains_five_distinct_hypotheses() -> None:
    catalog = get_strategy_catalog()

    assert len(catalog) == 5
    assert len({strategy.strategy_id for strategy in catalog}) == 5
    assert len({strategy.family for strategy in catalog}) == 5
