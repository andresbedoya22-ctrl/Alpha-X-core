from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class TruthMetrics:
    name: str
    source_type: str
    cagr: float | None
    sharpe: float | None
    sortino: float | None
    calmar: float | None
    max_drawdown: float
    total_return: float
    turnover: float
    fee_drag: float
    average_exposure: float
    rebalance_count: int
    trade_count: int
    final_equity: float


def calculate_truth_metrics(
    equity_curve: pd.DataFrame,
    *,
    name: str,
    source_type: str,
    rebalance_count: int,
    trade_count: int,
) -> TruthMetrics:
    returns = pd.to_numeric(equity_curve["bar_return"], errors="coerce").fillna(0.0)
    equity = pd.to_numeric(equity_curve["equity"], errors="coerce").ffill()
    final_equity = float(equity.iloc[-1])
    total_return = final_equity - 1.0
    drawdown = (equity / equity.cummax()) - 1.0
    max_drawdown = float(drawdown.min()) if not drawdown.empty else 0.0
    periods = max(len(equity) - 1, 1)
    years = periods / 365.25
    cagr = ((final_equity / float(equity.iloc[0])) ** (1.0 / years) - 1.0) if years > 0 else None

    volatility = float(returns.std(ddof=0))
    downside = returns.where(returns < 0.0, 0.0)
    downside_volatility = float((downside.pow(2).mean()) ** 0.5)
    sharpe = (float(returns.mean()) / volatility * np.sqrt(365.25)) if volatility > 0 else None
    sortino = (
        float(returns.mean()) / downside_volatility * np.sqrt(365.25)
        if downside_volatility > 0
        else None
    )
    calmar = (cagr / abs(max_drawdown)) if cagr is not None and max_drawdown < 0 else None
    turnover = float(
        pd.to_numeric(equity_curve.get("turnover", 0.0), errors="coerce").fillna(0.0).sum()
    )
    fee_drag = float(
        pd.to_numeric(equity_curve.get("trade_fee", 0.0), errors="coerce").fillna(0.0).sum()
    )
    average_exposure = float(
        pd.to_numeric(equity_curve.get("position", 0.0), errors="coerce").fillna(0.0).mean()
    )

    return TruthMetrics(
        name=name,
        source_type=source_type,
        cagr=cagr,
        sharpe=sharpe,
        sortino=sortino,
        calmar=calmar,
        max_drawdown=max_drawdown,
        total_return=total_return,
        turnover=turnover,
        fee_drag=fee_drag,
        average_exposure=average_exposure,
        rebalance_count=rebalance_count,
        trade_count=trade_count,
        final_equity=final_equity,
    )
