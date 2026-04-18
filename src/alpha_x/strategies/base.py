from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import pandas as pd

StrategyBuilder = Callable[..., pd.DataFrame]


@dataclass(frozen=True)
class StrategyDefinition:
    strategy_id: str
    name: str
    family: str
    description: str
    parameters: dict[str, Any]
    builder: StrategyBuilder

    def build_signal(self, frame: pd.DataFrame) -> pd.DataFrame:
        return self.builder(frame, **self.parameters)
