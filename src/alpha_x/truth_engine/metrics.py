from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class TruthMetrics:
    name: str
    source_type: str
    equity_base_type: str
    initial_equity: float
    capital_base: float
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
    capital_base: float | None = None,
    cash_flow_strategy: bool = False,
    equity_base_type: str = "nominal",
) -> TruthMetrics:
    returns = pd.to_numeric(equity_curve["bar_return"], errors="coerce").fillna(0.0)
    equity = pd.to_numeric(equity_curve["equity"], errors="coerce").ffill()
    if equity.empty:
        raise ValueError("Truth metrics require a non-empty equity curve.")

    initial_equity = float(equity.iloc[0])
    resolved_capital_base = float(capital_base) if capital_base is not None else initial_equity
    if resolved_capital_base <= 0:
        raise ValueError("capital_base must be positive.")

    final_equity = float(equity.iloc[-1])
    total_return = (final_equity / resolved_capital_base) - 1.0
    drawdown = (equity / equity.cummax()) - 1.0
    max_drawdown = float(drawdown.min()) if not drawdown.empty else 0.0
    periods = max(len(equity) - 1, 1)
    years = periods / 365.25
    cagr = (
        None
        if cash_flow_strategy or years <= 0
        else ((final_equity / resolved_capital_base) ** (1.0 / years) - 1.0)
    )

    volatility = float(returns.std(ddof=0))
    downside = returns.where(returns < 0.0, 0.0)
    downside_volatility = float((downside.pow(2).mean()) ** 0.5)
    sharpe = (
        None
        if cash_flow_strategy or volatility <= 0
        else float(returns.mean()) / volatility * np.sqrt(365.25)
    )
    sortino = (
        None
        if cash_flow_strategy or downside_volatility <= 0
        else float(returns.mean()) / downside_volatility * np.sqrt(365.25)
    )
    calmar = (
        cagr / abs(max_drawdown)
        if cagr is not None and max_drawdown < 0 and not cash_flow_strategy
        else None
    )
    turnover = float(
        pd.to_numeric(equity_curve.get("turnover", 0.0), errors="coerce").fillna(0.0).sum()
    )
    fee_paid = float(
        pd.to_numeric(equity_curve.get("trade_fee", 0.0), errors="coerce").fillna(0.0).sum()
    )
    fee_drag = fee_paid / resolved_capital_base
    average_exposure = float(
        pd.to_numeric(equity_curve.get("position", 0.0), errors="coerce").fillna(0.0).mean()
    )

    return TruthMetrics(
        name=name,
        source_type=source_type,
        equity_base_type=equity_base_type,
        initial_equity=initial_equity,
        capital_base=resolved_capital_base,
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
