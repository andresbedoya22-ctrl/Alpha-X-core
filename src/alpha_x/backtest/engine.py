from __future__ import annotations

from dataclasses import asdict

import pandas as pd

from alpha_x.backtest.models import BacktestConfig, BacktestResult, BacktestTrade

REQUIRED_COLUMNS = ("timestamp", "close", "signal")


def _normalize_signal(series: pd.Series) -> pd.Series:
    normalized = pd.to_numeric(series, errors="raise").fillna(0.0)
    unique_values = set(normalized.astype("int64").unique().tolist())
    if not unique_values.issubset({0, 1}):
        raise ValueError("Signal column must contain only 0/1 values for long/flat backtests.")
    return normalized.astype("int64")


def _validate_inputs(frame: pd.DataFrame, config: BacktestConfig) -> pd.DataFrame:
    missing_columns = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing_columns:
        raise ValueError(f"Missing required backtest columns: {missing_columns}")
    if frame.empty:
        raise ValueError("Backtest requires a non-empty dataset.")
    if config.initial_capital <= 0:
        raise ValueError("Initial capital must be positive.")
    if config.fee_rate < 0:
        raise ValueError("Fee rate must be non-negative.")
    if config.slippage_rate < 0:
        raise ValueError("Slippage rate must be non-negative.")

    columns = list(REQUIRED_COLUMNS)
    if "datetime" in frame.columns:
        columns.append("datetime")
    prepared = frame.loc[:, columns].copy()
    prepared["timestamp"] = pd.to_numeric(prepared["timestamp"], errors="raise").astype("int64")
    prepared["close"] = pd.to_numeric(prepared["close"], errors="raise").astype("float64")
    prepared["signal"] = _normalize_signal(prepared["signal"])
    prepared = prepared.sort_values("timestamp", ascending=True).reset_index(drop=True)
    if "datetime" not in prepared.columns:
        prepared["datetime"] = pd.to_datetime(prepared["timestamp"], unit="ms", utc=True)
    return prepared.loc[:, ["timestamp", "datetime", "close", "signal"]]


def run_long_flat_backtest(
    frame: pd.DataFrame,
    *,
    initial_capital: float,
    fee_rate: float,
    slippage_rate: float,
    signal_column: str = "signal",
    name: str = "Long/Flat Strategy",
) -> BacktestResult:
    if signal_column != "signal":
        renamed = frame.rename(columns={signal_column: "signal"})
    else:
        renamed = frame

    config = BacktestConfig(
        initial_capital=initial_capital,
        fee_rate=fee_rate,
        slippage_rate=slippage_rate,
        signal_column=signal_column,
    )
    prepared = _validate_inputs(renamed, config)

    cash = float(initial_capital)
    units = 0.0
    current_position = 0
    fill_count = 0
    trade_records: list[BacktestTrade] = []
    open_trade: dict[str, float | int | pd.Timestamp] | None = None
    rows: list[dict[str, float | int | pd.Timestamp]] = []

    for index, row in prepared.iterrows():
        target_position = int(prepared["signal"].iloc[index - 1]) if index > 0 else 0
        close_price = float(row["close"])
        timestamp = int(row["timestamp"])
        dt_value = pd.Timestamp(row["datetime"])
        trade_fee = 0.0

        if target_position != current_position:
            fill_count += 1
            if target_position == 1:
                execution_price = close_price * (1.0 + config.slippage_rate)
                gross_notional = cash / (1.0 + config.fee_rate)
                trade_fee = cash - gross_notional
                units = gross_notional / execution_price
                open_trade = {
                    "entry_timestamp": timestamp,
                    "entry_datetime": dt_value,
                    "entry_price": execution_price,
                    "entry_fee": trade_fee,
                    "entry_capital": cash,
                    "entry_index": index,
                    "quantity": units,
                }
                cash = 0.0
            else:
                execution_price = close_price * (1.0 - config.slippage_rate)
                gross_proceeds = units * execution_price
                trade_fee = gross_proceeds * config.fee_rate
                net_proceeds = gross_proceeds - trade_fee
                entry_details = open_trade
                cash = net_proceeds
                if entry_details is not None:
                    entry_capital = float(entry_details["entry_capital"])
                    trade_records.append(
                        BacktestTrade(
                            entry_timestamp=int(entry_details["entry_timestamp"]),
                            exit_timestamp=timestamp,
                            entry_datetime=pd.Timestamp(entry_details["entry_datetime"]),
                            exit_datetime=dt_value,
                            entry_price=float(entry_details["entry_price"]),
                            exit_price=execution_price,
                            quantity=float(entry_details["quantity"]),
                            entry_fee=float(entry_details["entry_fee"]),
                            exit_fee=trade_fee,
                            gross_proceeds=gross_proceeds,
                            net_proceeds=net_proceeds,
                            net_pnl=net_proceeds - entry_capital,
                            return_pct=(net_proceeds / entry_capital) - 1.0,
                            bars_held=index - int(entry_details["entry_index"]),
                        )
                    )
                units = 0.0
                open_trade = None

            current_position = target_position

        equity = cash + (units * close_price)
        rows.append(
            {
                "timestamp": timestamp,
                "datetime": dt_value,
                "close": close_price,
                "signal": int(row["signal"]),
                "position": current_position,
                "trade_fee": trade_fee,
                "equity": equity,
            }
        )

    equity_curve = pd.DataFrame(rows)
    equity_curve["bar_return"] = equity_curve["equity"].pct_change().fillna(0.0)
    trades = pd.DataFrame([asdict(trade) for trade in trade_records])

    return BacktestResult(
        name=name,
        equity_curve=equity_curve,
        trades=trades,
        metadata={
            "initial_capital": initial_capital,
            "capital_base": initial_capital,
            "fee_rate": fee_rate,
            "slippage_rate": slippage_rate,
            "fill_count": fill_count,
            "trade_count": len(trade_records),
            "exposure": float(equity_curve["position"].mean()),
            "signal_column": signal_column,
            "execution_rule": "Signal observed on close[t], executed on close[t+1].",
            "has_open_position": bool(current_position),
            "last_signal_unexecuted": bool(prepared["signal"].iloc[-1] != current_position),
        },
    )
