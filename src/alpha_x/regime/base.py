from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class RegimeRuleSet:
    rule_set_id: str
    name: str
    description: str
    parameters: dict[str, Any]
    regime_descriptions: dict[str, str]


@dataclass(frozen=True)
class RegimeDetectionResult:
    frame: pd.DataFrame
    assigned_rows: int
    discarded_rows: int
    discard_pct: float
    regime_names: list[str]
    rules_used: dict[str, Any]

