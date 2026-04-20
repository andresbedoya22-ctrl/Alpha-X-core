from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from alpha_x.truth_engine.weighting import WeightingConfig, compute_target_weights


@dataclass(frozen=True)
class RebalanceConfig:
    review_weekday: int = 6
    min_net_advantage: float = 0.0025
    no_trade_buffer: float = 0.05
    turnover_cap: float = 0.60
    fee_rate: float = 0.0025
    slippage_rate: float = 0.0005


@dataclass(frozen=True)
class PortfolioSimulationResult:
    equity_curve: pd.DataFrame
    weights: pd.DataFrame
    decisions: pd.DataFrame
    metadata: dict[str, float | int | str]


def simulate_family_portfolio(
    score_panel: pd.DataFrame,
    return_frame: pd.DataFrame,
    *,
    score_column: str,
    family_name: str,
    weighting_config: WeightingConfig,
    rebalance_config: RebalanceConfig,
    initial_capital: float = 1.0,
    gross_exposure_column: str = "risk_multiplier",
) -> PortfolioSimulationResult:
    if score_panel.empty:
        raise ValueError("Portfolio simulation requires a non-empty score panel.")
    if initial_capital <= 0:
        raise ValueError("initial_capital must be positive.")

    dates = (
        score_panel.loc[:, ["timestamp", "datetime"]]
        .drop_duplicates()
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    assets = sorted(score_panel["market"].unique().tolist())
    desired_weights = pd.DataFrame(0.0, index=dates.index, columns=assets)
    decision_rows: list[dict[str, object]] = []
    current_weights = {asset: 0.0 for asset in assets}
    current_score = 0.0
    one_way_cost = rebalance_config.fee_rate + rebalance_config.slippage_rate
    date_to_index = {int(row.timestamp): int(row.Index) for row in dates.itertuples(index=True)}

    for timestamp, group in score_panel.groupby("timestamp", sort=True):
        date_index = date_to_index[int(timestamp)]
        dt_value = pd.Timestamp(group["datetime"].iloc[0])
        desired_weights.iloc[date_index] = pd.Series(current_weights)

        if dt_value.weekday() != rebalance_config.review_weekday:
            continue

        gross_exposure = (
            float(group[gross_exposure_column].iloc[0]) if gross_exposure_column in group else 1.0
        )
        proposed_weights = compute_target_weights(
            group,
            score_column=score_column,
            config=weighting_config,
            gross_exposure=gross_exposure,
        )
        proposed_vector = {asset: proposed_weights.get(asset, 0.0) for asset in assets}
        proposed_score = sum(
            proposed_vector[row.market] * float(row._asdict()[score_column])
            for row in group.itertuples(index=False)
        )
        turnover = sum(abs(proposed_vector[asset] - current_weights[asset]) for asset in assets)
        score_gain = proposed_score - current_score
        friction = turnover * one_way_cost
        should_trade = turnover > rebalance_config.no_trade_buffer and score_gain > (
            rebalance_config.min_net_advantage + friction
        )
        executed_turnover = turnover

        if turnover > rebalance_config.turnover_cap:
            scale = rebalance_config.turnover_cap / turnover
            proposed_vector = {
                asset: current_weights[asset]
                + (proposed_vector[asset] - current_weights[asset]) * scale
                for asset in assets
            }
            executed_turnover = rebalance_config.turnover_cap
            proposed_score = sum(
                proposed_vector[row.market] * float(row._asdict()[score_column])
                for row in group.itertuples(index=False)
            )
            score_gain = proposed_score - current_score
            friction = executed_turnover * one_way_cost
            should_trade = executed_turnover > rebalance_config.no_trade_buffer and score_gain > (
                rebalance_config.min_net_advantage + friction
            )

        decision_rows.append(
            {
                "timestamp": int(timestamp),
                "datetime": dt_value,
                "family": family_name,
                "score_column": score_column,
                "current_score": current_score,
                "proposed_score": proposed_score,
                "score_gain": score_gain,
                "turnover": executed_turnover,
                "friction_estimate": friction,
                "should_trade": should_trade,
                "regime": group["operating_regime"].iloc[0]
                if "operating_regime" in group
                else None,
            }
        )

        if should_trade:
            current_weights = proposed_vector
            current_score = proposed_score
            desired_weights.iloc[date_index] = pd.Series(current_weights)

    weights = dates.copy()
    weights.loc[:, assets] = desired_weights.values
    active_weights = weights.loc[:, assets].shift(1).fillna(0.0)
    active_returns = return_frame.loc[:, assets].fillna(0.0)
    turnover = active_weights.diff().abs().sum(axis=1).fillna(active_weights.abs().sum(axis=1))
    cost_rate = turnover * one_way_cost
    gross_bar_return = (active_weights * active_returns).sum(axis=1)
    net_bar_return = gross_bar_return - cost_rate
    equity = initial_capital * (1.0 + net_bar_return).cumprod()
    previous_equity = equity.shift(1).fillna(initial_capital)

    equity_curve = dates.copy()
    equity_curve["gross_return"] = gross_bar_return
    equity_curve["bar_return"] = net_bar_return
    equity_curve["equity"] = equity
    equity_curve["position"] = active_weights.sum(axis=1)
    equity_curve["turnover"] = turnover
    equity_curve["trade_fee"] = cost_rate * previous_equity

    return PortfolioSimulationResult(
        equity_curve=equity_curve,
        weights=weights,
        decisions=pd.DataFrame(decision_rows),
        metadata={
            "family_name": family_name,
            "score_column": score_column,
            "review_weekday": rebalance_config.review_weekday,
            "fee_rate": rebalance_config.fee_rate,
            "slippage_rate": rebalance_config.slippage_rate,
            "initial_capital": initial_capital,
            "capital_base": initial_capital,
            "equity_base_type": "nominal",
            "trade_count": int((turnover > 0).sum()),
            "rebalance_count": int(len(decision_rows)),
            "avg_exposure": float(active_weights.sum(axis=1).mean()),
            "avg_turnover": float(turnover.mean()),
        },
    )
