from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from alpha_x.data.ohlcv_models import OHLCV_COLUMNS, normalize_ohlcv_frame
from alpha_x.data.ohlcv_validation import validate_temporal_integrity

OFFICIAL_1D_SMA_WINDOW = 125
OFFICIAL_BUFFER = 0.03
DEADBAND = 0.10
INITIAL_CAPITAL = 10_000.0
FOUR_HOURS_MS = 4 * 60 * 60 * 1000

SMA_WINDOWS_4H = (700, 750, 800)
BUFFERS_4H = (0.025, 0.030, 0.035)
PERSISTENCE_VALUES = (1, 2, 3)
BASE_TRADING_DAYS_PER_YEAR = 365.25


@dataclass(frozen=True)
class StrategyConfig:
    name: str
    timeframe: str
    sma_window: int
    buffer: float
    entry_persistence: int = 1
    exit_persistence: int = 1
    confirmation_price: str = "close"

    @property
    def label(self) -> str:
        return (
            f"{self.name} | SMA{self.sma_window} | buffer {self.buffer:.1%} | "
            f"entry p{self.entry_persistence} exit p{self.exit_persistence}"
        )


@dataclass(frozen=True)
class DataQuality:
    dataset_path: str
    source_timeframe: str
    target_timeframe: str
    row_count: int
    start: str
    end: str
    first_timestamp: int
    last_timestamp: int
    is_sorted: bool
    has_unique_timestamps: bool
    gap_count: int
    total_missing_intervals: int
    incomplete_4h_bins_dropped: int = 0
    source_gap_count: int = 0
    source_total_missing_intervals: int = 0
    bucket_alignment_utc: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CostModel:
    name: str
    fee_per_side: float
    slippage_per_side: float

    @property
    def cost_per_side(self) -> float:
        return self.fee_per_side + self.slippage_per_side


@dataclass(frozen=True)
class SimulationResult:
    name: str
    config: StrategyConfig
    equity_curve: pd.DataFrame
    metadata: dict[str, Any]


BASE_COST_MODEL = CostModel(
    name="bitvavo_retail_base",
    fee_per_side=0.0020,
    slippage_per_side=0.0005,
)
STRESS_COST_MODEL = CostModel(
    name="bitvavo_retail_stress",
    fee_per_side=0.0025,
    slippage_per_side=0.0008,
)
OFFICIAL_1D_CONFIG = StrategyConfig(
    name="Distance buffer 1D official",
    timeframe="1d",
    sma_window=OFFICIAL_1D_SMA_WINDOW,
    buffer=OFFICIAL_BUFFER,
)
BUY_HOLD_CONFIG_4H = StrategyConfig(
    name="Buy & Hold BTC",
    timeframe="4h",
    sma_window=1,
    buffer=0.0,
)


def load_ohlcv_frame(path: Path, timeframe: str) -> tuple[pd.DataFrame, DataQuality]:
    if not path.exists():
        raise FileNotFoundError(f"OHLCV dataset not found: {path}")
    frame = _with_datetime(normalize_ohlcv_frame(pd.read_csv(path)))
    report = validate_temporal_integrity(frame, timeframe)
    quality = _quality_from_report(
        frame,
        path=path,
        source_timeframe=timeframe,
        target_timeframe=timeframe,
        report=report,
    )
    return frame, quality


def derive_4h_from_1h(path: Path) -> tuple[pd.DataFrame, DataQuality]:
    if not path.exists():
        raise FileNotFoundError(f"Source 1H OHLCV dataset not found: {path}")

    source = normalize_ohlcv_frame(pd.read_csv(path))
    source_report = validate_temporal_integrity(source, "1h")
    working = source.copy()
    working["bucket"] = (working["timestamp"] // FOUR_HOURS_MS) * FOUR_HOURS_MS
    grouped = working.groupby("bucket", sort=True)

    incomplete_bins = int(grouped.size().ne(4).sum())
    rows: list[dict[str, float | int]] = []
    for bucket, group in grouped:
        ordered = group.sort_values("timestamp")
        expected = [int(bucket) + offset * 60 * 60 * 1000 for offset in range(4)]
        if len(ordered) != 4 or ordered["timestamp"].astype("int64").tolist() != expected:
            if len(ordered) == 4:
                incomplete_bins += 1
            continue
        rows.append(
            {
                "timestamp": int(bucket),
                "open": float(ordered["open"].iloc[0]),
                "high": float(ordered["high"].max()),
                "low": float(ordered["low"].min()),
                "close": float(ordered["close"].iloc[-1]),
                "volume": float(ordered["volume"].sum()),
            }
        )

    if not rows:
        raise ValueError("No complete 4H buckets can be derived from the 1H dataset.")

    frame = _with_datetime(normalize_ohlcv_frame(pd.DataFrame(rows)))
    report = validate_temporal_integrity(frame, "4h")
    quality = _quality_from_report(
        frame,
        path=path,
        source_timeframe="1h",
        target_timeframe="4h",
        report=report,
        incomplete_4h_bins_dropped=incomplete_bins,
        source_report=source_report,
        bucket_alignment_utc="Unix epoch anchored 00:00/04:00/08:00/12:00/16:00/20:00 UTC",
    )
    return frame, quality


def build_distance_buffer_targets(frame: pd.DataFrame, config: StrategyConfig) -> pd.DataFrame:
    if config.confirmation_price not in {"close", "high_low"}:
        raise ValueError(f"Unsupported confirmation price: {config.confirmation_price}")

    targets = frame.loc[:, ["timestamp", "datetime", "open", "high", "low", "close"]].copy()
    sma = targets["close"].rolling(config.sma_window, min_periods=config.sma_window).mean()
    threshold = sma * (1.0 + config.buffer)
    targets["sma"] = sma
    targets["threshold"] = threshold
    if config.confirmation_price == "high_low":
        entry_raw = (targets["high"] > threshold) & threshold.notna()
        exit_raw = (targets["low"] <= threshold) & threshold.notna()
    else:
        entry_raw = (targets["close"] > threshold) & threshold.notna()
        exit_raw = (targets["close"] <= threshold) & threshold.notna()

    targets["entry_condition"] = entry_raw
    targets["exit_condition"] = exit_raw
    targets["entry_streak"] = _streak(entry_raw)
    targets["exit_streak"] = _streak(exit_raw)
    state = 0.0
    exposure: list[float] = []
    for row in targets.itertuples(index=False):
        if state == 0.0 and int(row.entry_streak) >= config.entry_persistence:
            state = 1.0
        elif state == 1.0 and int(row.exit_streak) >= config.exit_persistence:
            state = 0.0
        exposure.append(state)
    targets["target_exposure"] = exposure
    return targets


def build_buy_and_hold_targets(frame: pd.DataFrame) -> pd.DataFrame:
    targets = frame.loc[:, ["timestamp", "datetime", "close"]].copy()
    targets["target_exposure"] = 1.0
    return targets


def simulate_exposure_strategy(
    frame: pd.DataFrame,
    targets: pd.DataFrame,
    *,
    config: StrategyConfig,
    initial_capital: float,
    cost_model: CostModel,
    dead_band: float,
) -> SimulationResult:
    if frame.empty:
        raise ValueError("Simulation requires a non-empty price frame.")
    if len(frame) != len(targets):
        raise ValueError("Price frame and targets must have the same length.")

    close = frame["close"].astype("float64").reset_index(drop=True)
    target_values = targets["target_exposure"].astype("float64").fillna(0.0).clip(0.0, 1.0)

    equity = float(initial_capital)
    current_exposure = 0.0
    total_cost = 0.0
    total_fee = 0.0
    total_slippage = 0.0
    turnover = 0.0
    rebalance_count = 0
    rows: list[dict[str, Any]] = []

    for index, price in enumerate(close):
        bar_return = 0.0
        if index > 0:
            bar_return = (float(price) / float(close.iloc[index - 1])) - 1.0
            equity *= 1.0 + current_exposure * bar_return

        scheduled_target = 0.0
        executed_target = current_exposure
        trade_size = 0.0
        trade_cost = 0.0
        fee_paid = 0.0
        slippage_paid = 0.0
        if index >= 1:
            scheduled_target = float(target_values.iloc[index - 1])
            delta = scheduled_target - current_exposure
            if abs(delta) > 0 and abs(delta) >= dead_band:
                trade_size = abs(delta)
                fee_paid = equity * trade_size * cost_model.fee_per_side
                slippage_paid = equity * trade_size * cost_model.slippage_per_side
                trade_cost = fee_paid + slippage_paid
                equity -= trade_cost
                current_exposure = scheduled_target
                executed_target = scheduled_target
                total_cost += trade_cost
                total_fee += fee_paid
                total_slippage += slippage_paid
                turnover += trade_size
                rebalance_count += 1

        rows.append(
            {
                "timestamp": int(frame["timestamp"].iloc[index]),
                "datetime": frame["datetime"].iloc[index],
                "close": float(price),
                "bar_return": bar_return,
                "signal_target_exposure": float(target_values.iloc[index]),
                "scheduled_target_exposure": scheduled_target,
                "position": current_exposure,
                "executed_target_exposure": executed_target,
                "trade_size": trade_size,
                "trade_cost": trade_cost,
                "fee_paid": fee_paid,
                "slippage_paid": slippage_paid,
                "equity": equity,
            }
        )

    equity_curve = pd.DataFrame(rows)
    return SimulationResult(
        name=config.name,
        config=config,
        equity_curve=equity_curve,
        metadata={
            "initial_capital": initial_capital,
            "capital_base": initial_capital,
            "cost_model": cost_model.name,
            "fee_per_side": cost_model.fee_per_side,
            "slippage_per_side": cost_model.slippage_per_side,
            "cost_per_side": cost_model.cost_per_side,
            "dead_band": dead_band,
            "execution_delay_bars": 1,
            "total_cost": total_cost,
            "total_fee": total_fee,
            "total_slippage": total_slippage,
            "fee_drag": total_cost / initial_capital,
            "turnover": turnover,
            "rebalance_count": rebalance_count,
            "avg_exposure": float(equity_curve["position"].mean()),
        },
    )


def run_config(
    frame: pd.DataFrame,
    config: StrategyConfig,
    *,
    cost_model: CostModel,
    initial_capital: float = INITIAL_CAPITAL,
) -> SimulationResult:
    if config.name == "Buy & Hold BTC":
        targets = build_buy_and_hold_targets(frame)
    else:
        targets = build_distance_buffer_targets(frame, config)
    return simulate_exposure_strategy(
        frame,
        targets,
        config=config,
        initial_capital=initial_capital,
        cost_model=cost_model,
        dead_band=DEADBAND,
    )


def calculate_metrics(
    result: SimulationResult,
    *,
    start: pd.Timestamp | None = None,
    end: pd.Timestamp | None = None,
    initial_capital: float = INITIAL_CAPITAL,
) -> dict[str, float | int | str | None]:
    curve = result.equity_curve.copy()
    curve["datetime"] = pd.to_datetime(curve["datetime"], utc=True)
    if start is not None:
        curve = curve[curve["datetime"] >= start]
    if end is not None:
        curve = curve[curve["datetime"] <= end]
    if len(curve) < 2:
        raise ValueError(f"Not enough rows to calculate metrics for {result.name}.")

    normalized = curve.copy().reset_index(drop=True)
    period_start_equity = float(normalized["equity"].iloc[0])
    scale = initial_capital / period_start_equity
    normalized["equity"] = normalized["equity"] * scale
    for column in ("trade_cost", "fee_paid", "slippage_paid"):
        normalized[column] = normalized[column] * scale

    final_equity = float(normalized["equity"].iloc[-1])
    total_return = final_equity / initial_capital - 1.0
    days = (
        pd.to_datetime(normalized["datetime"].iloc[-1], utc=True)
        - pd.to_datetime(normalized["datetime"].iloc[0], utc=True)
    ).total_seconds() / 86_400.0
    cagr = None if days <= 0 or total_return <= -1 else (1.0 + total_return) ** (
        BASE_TRADING_DAYS_PER_YEAR / days
    ) - 1
    max_drawdown = float((normalized["equity"] / normalized["equity"].cummax() - 1.0).min())
    returns = normalized["equity"].pct_change().dropna()
    std = float(returns.std(ddof=1)) if len(returns) > 1 else 0.0
    sharpe = None if std <= 0 else float(
        returns.mean() / std * np.sqrt(_bars_per_year(result.config.timeframe))
    )
    calmar = None if cagr is None or max_drawdown == 0 else float(cagr / abs(max_drawdown))

    return {
        "name": result.name,
        "timeframe": result.config.timeframe,
        "sma_window": result.config.sma_window,
        "buffer": result.config.buffer,
        "entry_persistence": result.config.entry_persistence,
        "exit_persistence": result.config.exit_persistence,
        "confirmation_price": result.config.confirmation_price,
        "start": str(pd.to_datetime(normalized["datetime"].iloc[0], utc=True)),
        "end": str(pd.to_datetime(normalized["datetime"].iloc[-1], utc=True)),
        "final_equity": final_equity,
        "total_return": total_return,
        "cagr": cagr,
        "sharpe": sharpe,
        "calmar": calmar,
        "max_drawdown": max_drawdown,
        "fee_drag": float(normalized["trade_cost"].sum() / initial_capital),
        "fee_only_drag": float(normalized["fee_paid"].sum() / initial_capital),
        "slippage_drag": float(normalized["slippage_paid"].sum() / initial_capital),
        "turnover": float(normalized["trade_size"].sum()),
        "rebalances": int((normalized["trade_size"] > 0).sum()),
        "avg_exposure": float(normalized["position"].mean()),
    }


def build_4h_grid_results(
    frame_4h: pd.DataFrame,
    *,
    cost_model: CostModel,
    start: pd.Timestamp | None = None,
    end: pd.Timestamp | None = None,
) -> pd.DataFrame:
    rows = []
    for sma_window in SMA_WINDOWS_4H:
        for buffer in BUFFERS_4H:
            for persistence in PERSISTENCE_VALUES:
                config = StrategyConfig(
                    name="Distance buffer 4H corrected",
                    timeframe="4h",
                    sma_window=sma_window,
                    buffer=buffer,
                    entry_persistence=persistence,
                    exit_persistence=persistence,
                )
                result = run_config(frame_4h, config, cost_model=cost_model)
                rows.append(calculate_metrics(result, start=start, end=end))
    ranking = pd.DataFrame(rows)
    return ranking.sort_values(
        ["calmar", "cagr", "max_drawdown", "rebalances"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)


def build_common_comparison_table(
    frame_4h: pd.DataFrame,
    frame_1d: pd.DataFrame,
    best_config_4h: StrategyConfig,
    *,
    cost_model: CostModel,
) -> pd.DataFrame:
    common_start = common_period_start(frame_4h, frame_1d, best_config_4h)
    common_end = min(_last_timestamp(frame_4h), _last_timestamp(frame_1d))
    results = [
        run_config(frame_4h, best_config_4h, cost_model=cost_model),
        run_config(frame_1d, OFFICIAL_1D_CONFIG, cost_model=cost_model),
        run_config(frame_4h, BUY_HOLD_CONFIG_4H, cost_model=cost_model),
    ]
    return pd.DataFrame(
        [calculate_metrics(result, start=common_start, end=common_end) for result in results]
    )


def build_temporal_robustness_table(
    frame_4h: pd.DataFrame,
    frame_1d: pd.DataFrame,
    best_config_4h: StrategyConfig,
    *,
    cost_model: CostModel,
    window_days: int = 365,
    step_days: int = 182,
) -> pd.DataFrame:
    common_start = common_period_start(frame_4h, frame_1d, best_config_4h)
    common_end = min(_last_timestamp(frame_4h), _last_timestamp(frame_1d))
    result_4h = run_config(frame_4h, best_config_4h, cost_model=cost_model)
    result_1d = run_config(frame_1d, OFFICIAL_1D_CONFIG, cost_model=cost_model)
    result_bh = run_config(frame_4h, BUY_HOLD_CONFIG_4H, cost_model=cost_model)

    rows: list[dict[str, Any]] = []
    start = common_start.floor("D")
    number = 1
    while start + pd.Timedelta(days=window_days) <= common_end:
        end = start + pd.Timedelta(days=window_days)
        metrics_4h = calculate_metrics(result_4h, start=start, end=end)
        metrics_1d = calculate_metrics(result_1d, start=start, end=end)
        metrics_bh = calculate_metrics(result_bh, start=start, end=end)
        rows.append(
            {
                "window": f"rolling_{number:02d}",
                "start": str(start.date()),
                "end": str(end.date()),
                "winner_by_calmar": _window_winner(metrics_4h, metrics_1d),
                "both_distance_buffers_negative": bool(
                    (metrics_4h["total_return"] or 0.0) < 0.0
                    and (metrics_1d["total_return"] or 0.0) < 0.0
                ),
                "4h_total_return": metrics_4h["total_return"],
                "1d_total_return": metrics_1d["total_return"],
                "buy_hold_total_return": metrics_bh["total_return"],
                "4h_cagr": metrics_4h["cagr"],
                "1d_cagr": metrics_1d["cagr"],
                "4h_calmar": metrics_4h["calmar"],
                "1d_calmar": metrics_1d["calmar"],
                "4h_max_drawdown": metrics_4h["max_drawdown"],
                "1d_max_drawdown": metrics_1d["max_drawdown"],
                "4h_rebalances": metrics_4h["rebalances"],
                "1d_rebalances": metrics_1d["rebalances"],
                "4h_fee_drag": metrics_4h["fee_drag"],
                "1d_fee_drag": metrics_1d["fee_drag"],
            }
        )
        start = start + pd.Timedelta(days=step_days)
        number += 1
    return pd.DataFrame(rows)


def build_expanding_robustness_table(
    frame_4h: pd.DataFrame,
    frame_1d: pd.DataFrame,
    best_config_4h: StrategyConfig,
    *,
    cost_model: CostModel,
    min_days: int = 365,
    step_days: int = 182,
) -> pd.DataFrame:
    common_start = common_period_start(frame_4h, frame_1d, best_config_4h)
    common_end = min(_last_timestamp(frame_4h), _last_timestamp(frame_1d))
    result_4h = run_config(frame_4h, best_config_4h, cost_model=cost_model)
    result_1d = run_config(frame_1d, OFFICIAL_1D_CONFIG, cost_model=cost_model)
    rows: list[dict[str, Any]] = []
    end = common_start.floor("D") + pd.Timedelta(days=min_days)
    number = 1
    while end <= common_end:
        metrics_4h = calculate_metrics(result_4h, start=common_start, end=end)
        metrics_1d = calculate_metrics(result_1d, start=common_start, end=end)
        rows.append(
            {
                "window": f"expanding_{number:02d}",
                "start": str(common_start.date()),
                "end": str(end.date()),
                "winner_by_calmar": _window_winner(metrics_4h, metrics_1d),
                "4h_cagr": metrics_4h["cagr"],
                "1d_cagr": metrics_1d["cagr"],
                "4h_calmar": metrics_4h["calmar"],
                "1d_calmar": metrics_1d["calmar"],
                "4h_max_drawdown": metrics_4h["max_drawdown"],
                "1d_max_drawdown": metrics_1d["max_drawdown"],
                "4h_rebalances": metrics_4h["rebalances"],
                "1d_rebalances": metrics_1d["rebalances"],
            }
        )
        end = end + pd.Timedelta(days=step_days)
        number += 1
    return pd.DataFrame(rows)


def build_block_bootstrap_table(
    frame_4h: pd.DataFrame,
    frame_1d: pd.DataFrame,
    best_config_4h: StrategyConfig,
    *,
    cost_model: CostModel,
    iterations: int = 500,
    block_days: int = 30,
    seed: int = 7,
) -> pd.DataFrame:
    common_start = common_period_start(frame_4h, frame_1d, best_config_4h)
    common_end = min(_last_timestamp(frame_4h), _last_timestamp(frame_1d))
    rows = []
    for result in (
        run_config(frame_4h, best_config_4h, cost_model=cost_model),
        run_config(frame_1d, OFFICIAL_1D_CONFIG, cost_model=cost_model),
    ):
        curve = result.equity_curve.copy()
        curve["datetime"] = pd.to_datetime(curve["datetime"], utc=True)
        curve = curve[(curve["datetime"] >= common_start) & (curve["datetime"] <= common_end)]
        returns = curve["equity"].pct_change().dropna().to_numpy()
        rows.append(
            {
                "name": result.name,
                "timeframe": result.config.timeframe,
                **_bootstrap_return_metrics(
                    returns,
                    bars_per_year=_bars_per_year(result.config.timeframe),
                    iterations=iterations,
                    block_bars=max(1, int(block_days / _bar_days(result.config.timeframe))),
                    seed=seed,
                ),
            }
        )
    return pd.DataFrame(rows)


def evaluate_pro_layer(
    frame_4h: pd.DataFrame,
    base_config: StrategyConfig,
    *,
    cost_model: CostModel,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame:
    candidates = [
        base_config,
        StrategyConfig(
            name="Distance buffer 4H corrected pro asymmetric_exit",
            timeframe="4h",
            sma_window=base_config.sma_window,
            buffer=base_config.buffer,
            entry_persistence=base_config.entry_persistence,
            exit_persistence=1,
        ),
    ]
    rows = []
    for config in candidates:
        result = run_config(frame_4h, config, cost_model=cost_model)
        row = calculate_metrics(result, start=start, end=end)
        row["layer"] = "base" if config == base_config else "pro_asymmetric_exit_p1"
        rows.append(row)
    return pd.DataFrame(rows)


def build_trade_log(
    frame: pd.DataFrame,
    config: StrategyConfig,
    *,
    cost_model: CostModel,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    signals = build_distance_buffer_targets(frame, config).copy().reset_index(drop=True)
    signals["datetime"] = pd.to_datetime(signals["datetime"], utc=True)
    signals["prior_target"] = signals["target_exposure"].shift(1, fill_value=0.0)
    signals["transition"] = "HOLD"
    signals.loc[
        (signals["prior_target"] == 0.0) & (signals["target_exposure"] == 1.0),
        "transition",
    ] = "OFF_TO_ON"
    signals.loc[
        (signals["prior_target"] == 1.0) & (signals["target_exposure"] == 0.0),
        "transition",
    ] = "ON_TO_OFF"

    trades: list[dict[str, Any]] = []
    notes: list[dict[str, Any]] = []
    open_entry: dict[str, Any] | None = None

    for signal_index, row in signals.iterrows():
        transition = str(row["transition"])
        if transition not in {"OFF_TO_ON", "ON_TO_OFF"}:
            continue
        execution_index = signal_index + 1
        if execution_index >= len(signals):
            notes.append(
                {
                    "signal_datetime": str(row["datetime"]),
                    "transition": transition,
                    "note": "Ignored transition because t+1 execution row is unavailable.",
                }
            )
            continue
        execution = signals.iloc[execution_index]
        if transition == "OFF_TO_ON":
            if open_entry is None:
                open_entry = {
                    "signal_entry_date": row["datetime"],
                    "execution_entry_date": execution["datetime"],
                    "entry_price_signal": float(row["close"]),
                    "entry_price_execution": float(execution["close"]),
                    "entry_execution_index": execution_index,
                }
            continue

        if open_entry is None:
            notes.append(
                {
                    "signal_datetime": str(row["datetime"]),
                    "transition": transition,
                    "note": "Ignored exit because no trade was open.",
                }
            )
            continue

        entry_price = float(open_entry["entry_price_execution"])
        exit_price = float(execution["close"])
        gross_return = exit_price / entry_price - 1.0
        net_return = (exit_price * (1.0 - cost_model.cost_per_side)) / (
            entry_price * (1.0 + cost_model.cost_per_side)
        ) - 1.0
        bars_held = execution_index - int(open_entry["entry_execution_index"])
        trades.append(
            {
                "trade_id": len(trades) + 1,
                "signal_entry_date": str(open_entry["signal_entry_date"]),
                "execution_entry_date": str(open_entry["execution_entry_date"]),
                "signal_exit_date": str(row["datetime"]),
                "execution_exit_date": str(execution["datetime"]),
                "entry_price_signal": float(open_entry["entry_price_signal"]),
                "entry_price_execution": entry_price,
                "exit_price_signal": float(row["close"]),
                "exit_price_execution": exit_price,
                "gross_return": gross_return,
                "net_return": net_return,
                "holding_bars": bars_held,
                "holding_days": bars_held * _bar_days(config.timeframe),
                "fees_paid": cost_model.fee_per_side * 2.0,
                "slippage_paid": cost_model.slippage_per_side * 2.0,
            }
        )
        open_entry = None

    if open_entry is not None:
        notes.append(
            {
                "signal_datetime": str(open_entry["signal_entry_date"]),
                "transition": "OFF_TO_ON",
                "note": "Open trade excluded from complete-trade statistics.",
            }
        )
    return pd.DataFrame(trades), {"notes": notes}


def summarize_trades(
    trades: pd.DataFrame,
    *,
    dataset_start: pd.Timestamp,
    dataset_end: pd.Timestamp,
) -> dict[str, Any]:
    if trades.empty:
        return {
            "complete_trades": 0,
            "trades_per_year": 0.0,
        }
    returns = trades["net_return"].astype("float64")
    winners = trades[trades["net_return"] > 0.0]
    losers = trades[trades["net_return"] <= 0.0]
    positive_sum = float(winners["net_return"].sum())
    negative_sum = float(losers["net_return"].sum())
    years = max(
        (dataset_end - dataset_start).total_seconds()
        / (BASE_TRADING_DAYS_PER_YEAR * 86_400.0),
        0.0,
    )
    return {
        "complete_trades": int(len(trades)),
        "trades_per_year": float(len(trades) / years) if years > 0 else None,
        "win_rate": float((returns > 0.0).mean()),
        "loss_rate": float((returns <= 0.0).mean()),
        "profit_factor": "inf" if negative_sum == 0.0 else positive_sum / abs(negative_sum),
        "expectancy_per_trade": float(returns.mean()),
        "mean_return_per_trade": float(returns.mean()),
        "median_return_per_trade": float(returns.median()),
        "std_return_per_trade": float(returns.std(ddof=1)) if len(returns) > 1 else 0.0,
        "winner_mean_holding_days": (
            float(winners["holding_days"].mean()) if not winners.empty else None
        ),
        "loser_mean_holding_days": (
            float(losers["holding_days"].mean()) if not losers.empty else None
        ),
        "best_trade": _clean_record(trades.loc[returns.idxmax()].to_dict()),
        "worst_trade": _clean_record(trades.loc[returns.idxmin()].to_dict()),
        "longest_winning_streak": _longest_streak(returns, win=True),
        "longest_losing_streak": _longest_streak(returns, win=False),
        "concentration": _trade_concentration(trades),
    }


def filter_trades_by_execution_period(
    trades: pd.DataFrame,
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame:
    if trades.empty:
        return trades.copy()
    filtered = trades.copy()
    filtered["execution_entry_date"] = pd.to_datetime(
        filtered["execution_entry_date"],
        utc=True,
    )
    filtered["execution_exit_date"] = pd.to_datetime(
        filtered["execution_exit_date"],
        utc=True,
    )
    return filtered[
        (filtered["execution_entry_date"] >= start)
        & (filtered["execution_exit_date"] <= end)
    ].reset_index(drop=True)


def run_monthly_contribution_simulation(
    frame: pd.DataFrame,
    config: StrategyConfig,
    *,
    cost_model: CostModel,
    start: str = "2025-01-01",
    end: str = "2025-12-31",
    initial_capital: float = 1_000.0,
    monthly_contribution: float = 500.0,
) -> dict[str, Any]:
    targets = (
        build_buy_and_hold_targets(frame)
        if config.name == "Buy & Hold BTC"
        else build_distance_buffer_targets(frame, config)
    )
    working = frame.loc[:, ["datetime", "close"]].copy()
    working["datetime"] = pd.to_datetime(working["datetime"], utc=True)
    working["target"] = targets["target_exposure"].astype(float)
    start_ts = pd.Timestamp(start, tz="UTC")
    end_ts = pd.Timestamp(end, tz="UTC") + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
    working = working[(working["datetime"] >= start_ts) & (working["datetime"] <= end_ts)]
    if working.empty:
        raise ValueError("No rows available for contribution simulation period.")

    cash = initial_capital
    btc = 0.0
    invested = initial_capital
    last_month = working["datetime"].iloc[0].strftime("%Y-%m")
    nav_rows = []
    current_target = 0.0
    fees = 0.0
    slippage = 0.0
    for index, row in working.reset_index(drop=True).iterrows():
        dt = row["datetime"]
        price = float(row["close"])
        month = dt.strftime("%Y-%m")
        if index > 0 and month != last_month:
            cash += monthly_contribution
            invested += monthly_contribution
            last_month = month

        if index >= 1:
            scheduled_target = float(working["target"].iloc[index - 1])
            nav_before_trade = cash + btc * price
            current_value_exposure = (
                0.0 if nav_before_trade <= 0 else (btc * price) / nav_before_trade
            )
            if abs(scheduled_target - current_target) >= DEADBAND:
                target_btc_value = nav_before_trade * scheduled_target
                delta_value = target_btc_value - btc * price
                cost_base = abs(delta_value)
                fee = cost_base * cost_model.fee_per_side
                slip = cost_base * cost_model.slippage_per_side
                if delta_value > 0:
                    buy_value = max(0.0, delta_value - fee - slip)
                    cash -= delta_value
                    btc += buy_value / price
                else:
                    btc_to_sell = min(btc, abs(delta_value) / price)
                    proceeds = btc_to_sell * price - fee - slip
                    btc -= btc_to_sell
                    cash += proceeds
                fees += fee
                slippage += slip
                current_target = scheduled_target
            elif current_value_exposure == 0.0 and scheduled_target == 0.0:
                current_target = scheduled_target

        nav = cash + btc * price
        nav_rows.append(
            {
                "datetime": dt,
                "nav": nav,
                "invested": invested,
                "fee": fees,
                "slippage": slippage,
            }
        )

    nav = pd.DataFrame(nav_rows)
    nav["tw_nav"] = nav["nav"] / nav["invested"]
    return {
        "name": config.name,
        "timeframe": config.timeframe,
        "final_value": float(nav["nav"].iloc[-1]),
        "invested_capital": float(nav["invested"].iloc[-1]),
        "return_on_contributions": float(nav["nav"].iloc[-1] / nav["invested"].iloc[-1] - 1.0),
        "time_weighted_nav_drawdown": float((nav["tw_nav"] / nav["tw_nav"].cummax() - 1.0).min()),
        "invested_capital_drawdown": float((nav["nav"] / nav["invested"] - 1.0).min()),
        "fee_drag": float(
            (nav["fee"].iloc[-1] + nav["slippage"].iloc[-1]) / nav["invested"].iloc[-1]
        ),
    }


def common_period_start(
    frame_4h: pd.DataFrame,
    frame_1d: pd.DataFrame,
    config_4h: StrategyConfig,
) -> pd.Timestamp:
    return max(
        _first_useful_execution(frame_4h, config_4h.sma_window),
        _first_useful_execution(frame_1d, OFFICIAL_1D_SMA_WINDOW),
    )


def config_from_metrics(
    row: pd.Series,
    *,
    name: str = "Distance buffer 4H corrected",
) -> StrategyConfig:
    return StrategyConfig(
        name=name,
        timeframe=str(row["timeframe"]),
        sma_window=int(row["sma_window"]),
        buffer=float(row["buffer"]),
        entry_persistence=int(row["entry_persistence"]),
        exit_persistence=int(row["exit_persistence"]),
        confirmation_price=str(row["confirmation_price"]),
    )


def write_report_markdown(
    path: Path,
    *,
    manifest: dict[str, Any],
    ranking_base: pd.DataFrame,
    ranking_stress: pd.DataFrame,
    common_base: pd.DataFrame,
    common_stress: pd.DataFrame,
    rolling: pd.DataFrame,
    expanding: pd.DataFrame,
    bootstrap: pd.DataFrame,
    pro_layer: pd.DataFrame,
    trade_summary_4h: dict[str, Any],
    trade_summary_1d: dict[str, Any],
    practical_2025: pd.DataFrame,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    verdict, reason = build_verdict(common_base, rolling, trade_summary_4h, trade_summary_1d)
    top_ranking = ranking_base.head(27)
    lines = [
        "# Distance Buffer 4H Corrected Study",
        "",
        "Research-only parallel study. The frozen 1D execution layer is not changed.",
        "",
        "## Assumptions",
        "",
        f"- Primary dataset: BTC/EUR Bitvavo from `{manifest['data']['4h']['dataset_path']}`.",
        (
            "- 1H -> 4H aggregation: open first, high max, low min, close last, "
            "volume sum; only complete 4-hour buckets retained."
        ),
        f"- 4H bucket alignment: {manifest['data']['4h']['bucket_alignment_utc']}.",
        (
            f"- 4H range: {manifest['data']['4h']['start']} -> {manifest['data']['4h']['end']} "
            f"({manifest['data']['4h']['row_count']} rows)."
        ),
        (
            f"- 4H gaps after aggregation: {manifest['data']['4h']['gap_count']} gaps, "
            f"{manifest['data']['4h']['total_missing_intervals']} missing 4H intervals; "
            f"{manifest['data']['4h']['incomplete_4h_bins_dropped']} incomplete buckets dropped."
        ),
        (
            f"- Source 1H gaps: {manifest['data']['4h']['source_gap_count']} gaps, "
            f"{manifest['data']['4h']['source_total_missing_intervals']} missing 1H intervals."
        ),
        (
            f"- 1D official reference dataset: `{manifest['data']['1d']['dataset_path']}`, "
            f"{manifest['data']['1d']['start']} -> {manifest['data']['1d']['end']}."
        ),
        "- Official 1D reference: close > SMA125 * 1.03, t+1, 100%/0%, deadband 10%.",
        "- Corrected 4H grid: SMA700/750/800, buffers 2.5%/3.0%/3.5%, persistence 1/2/3.",
        "- Base costs: 0.20% fee plus 0.05% slippage per side.",
        "- Stress costs: 0.25% fee plus 0.08% slippage per side.",
        "",
        "## Ranking 4H Base",
        "",
        _markdown_table(top_ranking),
        "",
        "## Ranking 4H Stress",
        "",
        _markdown_table(ranking_stress.head(10)),
        "",
        "## Best 4H vs 1D vs Buy & Hold Base",
        "",
        _markdown_table(common_base),
        "",
        "## Best 4H vs 1D vs Buy & Hold Stress",
        "",
        _markdown_table(common_stress),
        "",
        "## Rolling Robustness",
        "",
        _markdown_table(rolling),
        "",
        "## Expanding Robustness",
        "",
        _markdown_table(expanding),
        "",
        "## Block Bootstrap",
        "",
        _markdown_table(bootstrap),
        "",
        "## Pro Layer Test",
        "",
        _markdown_table(pro_layer),
        "",
        _pro_layer_read(pro_layer),
        "",
        "## Trade Forensics 4H",
        "",
        _summary_lines(trade_summary_4h),
        "",
        "## Trade Forensics 1D",
        "",
        _summary_lines(trade_summary_1d),
        "",
        "## Psychological Read",
        "",
        _psychological_read(common_base, trade_summary_4h, trade_summary_1d),
        "",
        "## Practical 2025 Simulation",
        "",
        _markdown_table(practical_2025),
        "",
        "## Verdict",
        "",
        f"**{verdict}**",
        "",
        reason,
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8", newline="\n")


def build_verdict(
    common_base: pd.DataFrame,
    rolling: pd.DataFrame,
    trade_summary_4h: dict[str, Any],
    trade_summary_1d: dict[str, Any],
) -> tuple[str, str]:
    metrics = common_base.set_index("name")
    m4h = metrics.loc["Distance buffer 4H corrected"]
    m1d = metrics.loc["Distance buffer 1D official"]
    cagr_delta = float(m4h["cagr"] - m1d["cagr"])
    calmar_delta = float(m4h["calmar"] - m1d["calmar"])
    rebalance_ratio = float(m4h["rebalances"] / max(float(m1d["rebalances"]), 1.0))
    rolling_wins = int((rolling["winner_by_calmar"] == "4H").sum()) if not rolling.empty else 0
    rolling_losses = int((rolling["winner_by_calmar"] == "1D").sum()) if not rolling.empty else 0
    trade_ratio = float(
        (trade_summary_4h.get("trades_per_year") or 0.0)
        / max(float(trade_summary_1d.get("trades_per_year") or 0.0), 1e-9)
    )
    if cagr_delta > 0.02 and calmar_delta > 0.10 and rolling_wins > rolling_losses:
        return (
            "La 4H corregida supera de forma defendible a la 1D y merece paper trading paralelo",
            (
                "Razon principal: mejora CAGR y Calmar en el periodo comun y gana mas "
                "ventanas temporales."
            ),
        )
    if cagr_delta > 0 and calmar_delta >= -0.10 and rebalance_ratio <= 3.0 and trade_ratio <= 3.0:
        return (
            "La 4H corregida es prometedora pero no suficiente para desplazar a la 1D",
            (
                "Razon principal: la ventaja no es lo bastante dominante frente a la "
                "carga operativa adicional."
            ),
        )
    return (
        "La 4H corregida no vale la pena frente a la 1D",
        "Razon principal: no mejora de forma robusta el binomio CAGR/Calmar despues de costes.",
    )


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(_json_clean(payload), handle, indent=2, sort_keys=True)
        handle.write("\n")


def _with_datetime(frame: pd.DataFrame) -> pd.DataFrame:
    enriched = frame.copy()
    enriched["datetime"] = pd.to_datetime(enriched["timestamp"], unit="ms", utc=True)
    return enriched.loc[:, ["timestamp", "datetime", *OHLCV_COLUMNS[1:]]]


def _quality_from_report(
    frame: pd.DataFrame,
    *,
    path: Path,
    source_timeframe: str,
    target_timeframe: str,
    report: Any,
    incomplete_4h_bins_dropped: int = 0,
    source_report: Any | None = None,
    bucket_alignment_utc: str | None = None,
) -> DataQuality:
    missing = [gap.missing_intervals for gap in report.gaps]
    source_missing = [gap.missing_intervals for gap in source_report.gaps] if source_report else []
    return DataQuality(
        dataset_path=str(path),
        source_timeframe=source_timeframe,
        target_timeframe=target_timeframe,
        row_count=len(frame),
        start=str(pd.to_datetime(frame["datetime"].iloc[0], utc=True)),
        end=str(pd.to_datetime(frame["datetime"].iloc[-1], utc=True)),
        first_timestamp=int(frame["timestamp"].iloc[0]),
        last_timestamp=int(frame["timestamp"].iloc[-1]),
        is_sorted=bool(report.is_sorted),
        has_unique_timestamps=bool(report.has_unique_timestamps),
        gap_count=len(report.gaps),
        total_missing_intervals=int(sum(missing)),
        incomplete_4h_bins_dropped=incomplete_4h_bins_dropped,
        source_gap_count=0 if source_report is None else len(source_report.gaps),
        source_total_missing_intervals=int(sum(source_missing)),
        bucket_alignment_utc=bucket_alignment_utc,
    )


def _first_useful_execution(frame: pd.DataFrame, sma_window: int) -> pd.Timestamp:
    if len(frame) <= sma_window:
        raise ValueError(f"Not enough rows for SMA{sma_window} plus t+1 execution.")
    return pd.to_datetime(frame["datetime"].iloc[sma_window], utc=True)


def _last_timestamp(frame: pd.DataFrame) -> pd.Timestamp:
    return pd.to_datetime(frame["datetime"].iloc[-1], utc=True)


def _streak(condition: pd.Series) -> pd.Series:
    groups = condition.ne(condition.shift()).cumsum()
    return condition.astype(int).groupby(groups).cumsum()


def _bars_per_year(timeframe: str) -> float:
    if timeframe == "4h":
        return BASE_TRADING_DAYS_PER_YEAR * 6.0
    if timeframe == "1d":
        return BASE_TRADING_DAYS_PER_YEAR
    raise ValueError(f"Unsupported metrics timeframe: {timeframe}")


def _bar_days(timeframe: str) -> float:
    if timeframe == "4h":
        return 4.0 / 24.0
    if timeframe == "1d":
        return 1.0
    raise ValueError(f"Unsupported trade timeframe: {timeframe}")


def _window_winner(metrics_4h: dict[str, Any], metrics_1d: dict[str, Any]) -> str:
    calmar_4h = metrics_4h["calmar"]
    calmar_1d = metrics_1d["calmar"]
    if calmar_4h is None and calmar_1d is None:
        return "none"
    if calmar_1d is None or (calmar_4h is not None and calmar_4h > calmar_1d):
        return "4H"
    if calmar_4h is None or calmar_1d > calmar_4h:
        return "1D"
    return "tie"


def _bootstrap_return_metrics(
    returns: np.ndarray,
    *,
    bars_per_year: float,
    iterations: int,
    block_bars: int,
    seed: int,
) -> dict[str, float]:
    rng = np.random.default_rng(seed)
    if len(returns) < 2:
        return {
            "sharpe_p05": np.nan,
            "sharpe_median": np.nan,
            "sharpe_p95": np.nan,
            "calmar_p05": np.nan,
            "calmar_median": np.nan,
            "calmar_p95": np.nan,
        }
    sharpes = []
    calmars = []
    starts = np.arange(0, len(returns))
    for _ in range(iterations):
        sampled: list[float] = []
        while len(sampled) < len(returns):
            start = int(rng.choice(starts))
            block = returns[start : min(start + block_bars, len(returns))]
            sampled.extend(block.tolist())
        sample = np.asarray(sampled[: len(returns)], dtype=float)
        std = float(sample.std(ddof=1))
        sharpes.append(np.nan if std <= 0 else float(sample.mean() / std * np.sqrt(bars_per_year)))
        equity = np.cumprod(1.0 + sample)
        total_return = float(equity[-1] - 1.0)
        years = len(sample) / bars_per_year
        cagr = (
            np.nan
            if years <= 0 or total_return <= -1
            else (1.0 + total_return) ** (1.0 / years) - 1.0
        )
        max_drawdown = float((equity / np.maximum.accumulate(equity) - 1.0).min())
        calmars.append(np.nan if max_drawdown == 0 or np.isnan(cagr) else cagr / abs(max_drawdown))
    return {
        "sharpe_p05": float(np.nanpercentile(sharpes, 5)),
        "sharpe_median": float(np.nanpercentile(sharpes, 50)),
        "sharpe_p95": float(np.nanpercentile(sharpes, 95)),
        "calmar_p05": float(np.nanpercentile(calmars, 5)),
        "calmar_median": float(np.nanpercentile(calmars, 50)),
        "calmar_p95": float(np.nanpercentile(calmars, 95)),
    }


def _longest_streak(returns: pd.Series, *, win: bool) -> int:
    mask = returns > 0.0 if win else returns <= 0.0
    longest = 0
    current = 0
    for value in mask:
        if bool(value):
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def _trade_concentration(trades: pd.DataFrame) -> list[dict[str, Any]]:
    total = float(trades["net_return"].sum()) if not trades.empty else 0.0
    ordered = trades.sort_values("net_return", ascending=False)
    rows = []
    for top_n in (1, 3, 5, 10):
        available = min(top_n, len(ordered))
        top_sum = float(ordered.head(available)["net_return"].sum()) if available else 0.0
        rows.append(
            {
                "top_n": top_n,
                "available_trades": available,
                "net_return_sum": top_sum,
                "share_of_total_net_return_sum": None if total == 0 else top_sum / total,
            }
        )
    return rows


def _clean_record(record: dict[str, Any]) -> dict[str, Any]:
    cleaned = {}
    for key, value in record.items():
        if isinstance(value, np.integer):
            cleaned[key] = int(value)
        elif isinstance(value, np.floating):
            cleaned[key] = float(value)
        else:
            cleaned[key] = value
    return cleaned


def _markdown_table(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "_No rows._"
    display = frame.copy()
    for column in display.columns:
        if pd.api.types.is_float_dtype(display[column]):
            display[column] = display[column].map(_format_float)
    headers = [str(column) for column in display.columns]
    rows = [[str(value) for value in row] for row in display.to_numpy()]
    separator = ["---"] * len(headers)
    table_rows = [headers, separator, *rows]
    return "\n".join("| " + " | ".join(row) + " |" for row in table_rows)


def _format_float(value: float) -> str:
    return "" if pd.isna(value) else f"{value:.6f}"


def _summary_lines(summary: dict[str, Any]) -> str:
    simple_keys = [
        "complete_trades",
        "trades_per_year",
        "win_rate",
        "loss_rate",
        "profit_factor",
        "expectancy_per_trade",
        "mean_return_per_trade",
        "median_return_per_trade",
        "std_return_per_trade",
        "winner_mean_holding_days",
        "loser_mean_holding_days",
        "longest_winning_streak",
        "longest_losing_streak",
    ]
    lines = []
    for key in simple_keys:
        value = summary.get(key)
        if isinstance(value, float):
            lines.append(f"- {key}: {value:.6f}")
        else:
            lines.append(f"- {key}: {value}")
    for label in ("best_trade", "worst_trade"):
        trade = summary.get(label, {})
        if trade:
            lines.append(
                f"- {label}: trade {trade.get('trade_id')} "
                f"net_return={float(trade.get('net_return')):.6f}, "
                f"holding_days={float(trade.get('holding_days')):.2f}"
            )
    for row in summary.get("concentration", []):
        share = row.get("share_of_total_net_return_sum")
        share_text = "None" if share is None else f"{float(share):.6f}"
        lines.append(
            f"- top_{row['top_n']}_concentration: "
            f"net_sum={float(row['net_return_sum']):.6f}, share={share_text}"
        )
    return "\n".join(lines)


def _psychological_read(
    common_base: pd.DataFrame,
    trade_summary_4h: dict[str, Any],
    trade_summary_1d: dict[str, Any],
) -> str:
    metrics = common_base.set_index("name")
    m4h = metrics.loc["Distance buffer 4H corrected"]
    m1d = metrics.loc["Distance buffer 1D official"]
    rebalance_ratio = float(m4h["rebalances"] / max(float(m1d["rebalances"]), 1.0))
    trades_ratio = float(
        (trade_summary_4h.get("trades_per_year") or 0.0)
        / max(float(trade_summary_1d.get("trades_per_year") or 0.0), 1e-9)
    )
    if rebalance_ratio > 1.5 or trades_ratio > 1.5:
        hardness = "4H is psychologically harder than 1D."
    else:
        hardness = "4H is not materially harder than 1D by trade count."
    return "\n".join(
        [
            f"- {hardness}",
            (
                f"- 4H rebalances/common period: {int(m4h['rebalances'])} "
                f"vs 1D {int(m1d['rebalances'])}; ratio {rebalance_ratio:.2f}."
            ),
            f"- Complete-trade frequency ratio 4H/1D: {trades_ratio:.2f}.",
            (
                "- Manual or semi-manual paper trading is only reasonable with "
                "disciplined 4H close checks."
            ),
        ]
    )


def _pro_layer_read(pro_layer: pd.DataFrame) -> str:
    if pro_layer.empty or len(pro_layer) < 2:
        return "Pro layer was not evaluated."
    indexed = pro_layer.set_index("layer")
    base = indexed.loc["base"]
    pro = indexed.loc["pro_asymmetric_exit_p1"]
    improves = (
        float(pro["cagr"]) > float(base["cagr"])
        and float(pro["calmar"]) > float(base["calmar"])
        and float(pro["max_drawdown"]) >= float(base["max_drawdown"])
        and int(pro["rebalances"]) <= int(base["rebalances"]) + 2
    )
    if improves:
        return (
            "Pro layer retained for consideration: asymmetric exit persistence p1 improved "
            "CAGR, Calmar and drawdown without materially worsening turnover."
        )
    return (
        "Pro layer discarded: asymmetric exit persistence p1 did not improve CAGR, Calmar, "
        "drawdown and trade profile together."
    )


def _json_clean(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _json_clean(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_clean(item) for item in value]
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        value = float(value)
    if isinstance(value, float):
        if np.isnan(value):
            return None
        if np.isinf(value):
            return "inf" if value > 0 else "-inf"
    return value
