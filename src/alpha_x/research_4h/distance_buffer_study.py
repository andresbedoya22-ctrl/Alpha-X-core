from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from alpha_x.data.ohlcv_models import OHLCV_COLUMNS, normalize_ohlcv_frame
from alpha_x.data.ohlcv_validation import validate_temporal_integrity

SMA_WINDOW = 125
BUFFER = 0.03
DEADBAND = 0.10
INITIAL_CAPITAL = 10_000.0
FOUR_HOURS_MS = 4 * 60 * 60 * 1000


@dataclass(frozen=True)
class DataQuality:
    dataset_path: str
    source_timeframe: str
    target_timeframe: str
    row_count: int
    start: str
    end: str
    first_useful_signal: str | None
    first_useful_execution: str | None
    is_sorted: bool
    has_unique_timestamps: bool
    gap_count: int
    total_missing_intervals: int
    incomplete_4h_bins_dropped: int = 0
    source_gap_count: int = 0
    source_total_missing_intervals: int = 0

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

    complete_buckets = grouped.filter(lambda group: len(group) == 4).copy()
    incomplete_bins = int(grouped.size().ne(4).sum())
    if complete_buckets.empty:
        raise ValueError("No complete 4H buckets can be derived from the 1H dataset.")

    rows: list[dict[str, float | int]] = []
    for bucket, group in complete_buckets.groupby("bucket", sort=True):
        ordered = group.sort_values("timestamp")
        expected = [int(bucket) + offset * 60 * 60 * 1000 for offset in range(4)]
        if ordered["timestamp"].astype("int64").tolist() != expected:
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
    )
    return frame, quality


def build_distance_buffer_3pct_targets(frame: pd.DataFrame) -> pd.DataFrame:
    targets = frame.loc[:, ["timestamp", "datetime", "close"]].copy()
    sma = targets["close"].rolling(SMA_WINDOW, min_periods=SMA_WINDOW).mean()
    targets["target_exposure"] = (
        (targets["close"] > sma * (1.0 + BUFFER)) & sma.notna()
    ).astype("float64")
    return targets


def build_buy_and_hold_targets(frame: pd.DataFrame) -> pd.DataFrame:
    targets = frame.loc[:, ["timestamp", "datetime", "close"]].copy()
    targets["target_exposure"] = 1.0
    return targets


def simulate_exposure_strategy(
    frame: pd.DataFrame,
    targets: pd.DataFrame,
    *,
    name: str,
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
        if index >= 1:
            scheduled_target = float(target_values.iloc[index - 1])
            delta = scheduled_target - current_exposure
            if abs(delta) > 0 and abs(delta) >= dead_band:
                trade_size = abs(delta)
                trade_cost = equity * trade_size * cost_model.cost_per_side
                equity -= trade_cost
                current_exposure = scheduled_target
                executed_target = scheduled_target
                total_cost += trade_cost
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
                "equity": equity,
            }
        )

    equity_curve = pd.DataFrame(rows)
    return SimulationResult(
        name=name,
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
            "fee_drag": total_cost / initial_capital,
            "turnover": turnover,
            "rebalance_count": rebalance_count,
            "avg_exposure": float(equity_curve["position"].mean()),
        },
    )


def run_strategy(
    frame: pd.DataFrame,
    *,
    strategy_name: str,
    cost_model: CostModel,
    initial_capital: float = INITIAL_CAPITAL,
) -> SimulationResult:
    if strategy_name == "Buy & Hold BTC":
        targets = build_buy_and_hold_targets(frame)
    elif strategy_name in {"Distance buffer 4H", "Distance buffer 1D official"}:
        targets = build_distance_buffer_3pct_targets(frame)
    else:
        raise ValueError(f"Unknown strategy: {strategy_name}")
    return simulate_exposure_strategy(
        frame,
        targets,
        name=strategy_name,
        initial_capital=initial_capital,
        cost_model=cost_model,
        dead_band=DEADBAND,
    )


def calculate_metrics(
    result: SimulationResult,
    *,
    timeframe: str,
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
    normalized["trade_cost"] = normalized["trade_cost"] * scale
    final_equity = float(normalized["equity"].iloc[-1])
    total_return = final_equity / initial_capital - 1.0
    days = (
        pd.to_datetime(normalized["datetime"].iloc[-1], utc=True)
        - pd.to_datetime(normalized["datetime"].iloc[0], utc=True)
    ).total_seconds() / 86_400.0
    cagr = None if days <= 0 or total_return <= -1 else (1.0 + total_return) ** (365.25 / days) - 1
    max_drawdown = float((normalized["equity"] / normalized["equity"].cummax() - 1.0).min())
    returns = normalized["equity"].pct_change().dropna()
    std = float(returns.std(ddof=1)) if len(returns) > 1 else 0.0
    sharpe = None if std <= 0 else float(returns.mean() / std * np.sqrt(_bars_per_year(timeframe)))
    calmar = None if cagr is None or max_drawdown == 0 else float(cagr / abs(max_drawdown))

    return {
        "name": result.name,
        "timeframe": timeframe,
        "start": str(pd.to_datetime(normalized["datetime"].iloc[0], utc=True)),
        "end": str(pd.to_datetime(normalized["datetime"].iloc[-1], utc=True)),
        "final_equity": final_equity,
        "total_return": total_return,
        "cagr": cagr,
        "sharpe": sharpe,
        "calmar": calmar,
        "max_drawdown": max_drawdown,
        "fee_drag": float(normalized["trade_cost"].sum() / initial_capital),
        "turnover": float(normalized["trade_size"].sum()),
        "rebalances": int((normalized["trade_size"] > 0).sum()),
        "avg_exposure": float(normalized["position"].mean()),
    }


def build_full_sample_table(
    frame_4h: pd.DataFrame,
    frame_1d: pd.DataFrame,
    *,
    cost_model: CostModel,
) -> pd.DataFrame:
    results = [
        (run_strategy(frame_4h, strategy_name="Distance buffer 4H", cost_model=cost_model), "4h"),
        (
            run_strategy(
                frame_1d,
                strategy_name="Distance buffer 1D official",
                cost_model=cost_model,
            ),
            "1d",
        ),
        (run_strategy(frame_4h, strategy_name="Buy & Hold BTC", cost_model=cost_model), "4h"),
    ]
    return pd.DataFrame(
        [calculate_metrics(result, timeframe=timeframe) for result, timeframe in results]
    )


def build_common_comparison_table(
    frame_4h: pd.DataFrame,
    frame_1d: pd.DataFrame,
    *,
    cost_model: CostModel,
) -> pd.DataFrame:
    common_start = max(_first_useful_execution(frame_4h), _first_useful_execution(frame_1d))
    common_end = min(_last_timestamp(frame_4h), _last_timestamp(frame_1d))
    results = [
        (run_strategy(frame_4h, strategy_name="Distance buffer 4H", cost_model=cost_model), "4h"),
        (
            run_strategy(
                frame_1d,
                strategy_name="Distance buffer 1D official",
                cost_model=cost_model,
            ),
            "1d",
        ),
        (run_strategy(frame_4h, strategy_name="Buy & Hold BTC", cost_model=cost_model), "4h"),
    ]
    rows = [
        calculate_metrics(result, timeframe=timeframe, start=common_start, end=common_end)
        for result, timeframe in results
    ]
    return pd.DataFrame(rows)


def build_temporal_robustness_table(
    frame_4h: pd.DataFrame,
    frame_1d: pd.DataFrame,
    *,
    cost_model: CostModel,
    window_days: int = 182,
) -> pd.DataFrame:
    common_start = max(_first_useful_execution(frame_4h), _first_useful_execution(frame_1d))
    common_end = min(_last_timestamp(frame_4h), _last_timestamp(frame_1d))
    result_4h = run_strategy(frame_4h, strategy_name="Distance buffer 4H", cost_model=cost_model)
    result_1d = run_strategy(
        frame_1d,
        strategy_name="Distance buffer 1D official",
        cost_model=cost_model,
    )
    result_bh = run_strategy(frame_4h, strategy_name="Buy & Hold BTC", cost_model=cost_model)

    rows: list[dict[str, Any]] = []
    start = common_start.floor("D")
    number = 1
    while start + pd.Timedelta(days=window_days) <= common_end:
        end = start + pd.Timedelta(days=window_days)
        metrics_4h = calculate_metrics(result_4h, timeframe="4h", start=start, end=end)
        metrics_1d = calculate_metrics(result_1d, timeframe="1d", start=start, end=end)
        metrics_bh = calculate_metrics(result_bh, timeframe="4h", start=start, end=end)
        winner = _window_winner(metrics_4h, metrics_1d)
        both_fail = bool(
            (metrics_4h["total_return"] or 0.0) < 0.0 and (metrics_1d["total_return"] or 0.0) < 0.0
        )
        rows.append(
            {
                "window": f"w_{number:02d}",
                "start": str(start.date()),
                "end": str(end.date()),
                "winner_by_calmar": winner,
                "both_distance_buffers_negative": both_fail,
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
        start = end
        number += 1
    return pd.DataFrame(rows)


def build_trade_log(
    frame: pd.DataFrame,
    *,
    timeframe: str,
    cost_model: CostModel,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    signals = build_distance_buffer_3pct_targets(frame).copy().reset_index(drop=True)
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
    total_cost_per_side = cost_model.cost_per_side

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
                    "signal_entry_datetime": row["datetime"],
                    "execution_entry_datetime": execution["datetime"],
                    "entry_signal_price": float(row["close"]),
                    "entry_execution_price": float(execution["close"]),
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

        entry_price = float(open_entry["entry_execution_price"])
        exit_price = float(execution["close"])
        net_return = (exit_price * (1.0 - total_cost_per_side)) / (
            entry_price * (1.0 + total_cost_per_side)
        ) - 1.0
        bars_held = execution_index - int(open_entry["entry_execution_index"])
        trades.append(
            {
                "trade_id": len(trades) + 1,
                "signal_entry_datetime": str(open_entry["signal_entry_datetime"]),
                "execution_entry_datetime": str(open_entry["execution_entry_datetime"]),
                "signal_exit_datetime": str(row["datetime"]),
                "execution_exit_datetime": str(execution["datetime"]),
                "entry_signal_price": float(open_entry["entry_signal_price"]),
                "entry_execution_price": entry_price,
                "exit_signal_price": float(row["close"]),
                "exit_execution_price": exit_price,
                "gross_return": exit_price / entry_price - 1.0,
                "net_return": net_return,
                "holding_bars": bars_held,
                "holding_days": bars_held * _bar_days(timeframe),
                "round_trip_cost": total_cost_per_side * 2.0,
            }
        )
        open_entry = None

    if open_entry is not None:
        notes.append(
            {
                "signal_datetime": str(open_entry["signal_entry_datetime"]),
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
    years = max((dataset_end - dataset_start).total_seconds() / (365.25 * 86_400.0), 0.0)
    return {
        "complete_trades": int(len(trades)),
        "trades_per_year": float(len(trades) / years) if years > 0 else None,
        "win_rate": float((returns > 0.0).mean()),
        "loss_rate": float((returns <= 0.0).mean()),
        "profit_factor": "inf" if negative_sum == 0.0 else positive_sum / abs(negative_sum),
        "expectancy_per_trade": float(returns.mean()),
        "mean_return_per_trade": float(returns.mean()),
        "median_return_per_trade": float(returns.median()),
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


def write_report_markdown(
    path: Path,
    *,
    manifest: dict[str, Any],
    full_base: pd.DataFrame,
    full_stress: pd.DataFrame,
    common_base: pd.DataFrame,
    robustness: pd.DataFrame,
    trade_summary_4h: dict[str, Any],
    trade_summary_1d: dict[str, Any],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    verdict, reason = build_verdict(common_base, robustness)
    lines = [
        "# Distance buffer 4H research study",
        "",
        "Research-only study. The frozen 1D execution layer is not changed.",
        "",
        "## Assumptions",
        "",
        (
            f"- Dataset 4H: derived from `{manifest['data']['4h']['dataset_path']}` "
            "complete 1H buckets."
        ),
        f"- 4H useful execution starts: {manifest['data']['4h']['first_useful_execution']}.",
        (
            f"- 4H derived gaps: {manifest['data']['4h']['gap_count']} gaps, "
            f"{manifest['data']['4h']['total_missing_intervals']} missing 4H intervals; "
            f"{manifest['data']['4h']['incomplete_4h_bins_dropped']} incomplete bins dropped."
        ),
        f"- Dataset 1D: `{manifest['data']['1d']['dataset_path']}`.",
        f"- 1D useful execution starts: {manifest['data']['1d']['first_useful_execution']}.",
        (
            f"- 1D gaps: {manifest['data']['1d']['gap_count']} gaps, "
            f"{manifest['data']['1d']['total_missing_intervals']} missing daily intervals."
        ),
        "- Signal: close > SMA125 * 1.03, otherwise OFF.",
        "- Execution: t+1 next candle close.",
        "- Deadband: 10%.",
        "- Base costs: 0.20% fee plus 0.05% slippage per side.",
        "- Stress costs: 0.25% fee plus 0.08% slippage per side.",
        "",
        "## Full Sample Base",
        "",
        _markdown_table(full_base),
        "",
        "## Full Sample Stress",
        "",
        _markdown_table(full_stress),
        "",
        "## Common Period Base Comparison",
        "",
        _markdown_table(common_base),
        "",
        "## Temporal Robustness",
        "",
        _markdown_table(robustness),
        "",
        "## Trade Forensics 4H",
        "",
        _summary_lines(trade_summary_4h),
        "",
        "## Trade Forensics 1D",
        "",
        _summary_lines(trade_summary_1d),
        "",
        "## Operating Read",
        "",
        _operating_read(common_base, trade_summary_4h, trade_summary_1d),
        "",
        "## Verdict",
        "",
        f"**{verdict}**",
        "",
        reason,
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8", newline="\n")


def build_verdict(common_base: pd.DataFrame, robustness: pd.DataFrame) -> tuple[str, str]:
    metrics = common_base.set_index("name")
    m4h = metrics.loc["Distance buffer 4H"]
    m1d = metrics.loc["Distance buffer 1D official"]
    cagr_delta = float(m4h["cagr"] - m1d["cagr"])
    calmar_delta = float(m4h["calmar"] - m1d["calmar"])
    rebalance_ratio = float(m4h["rebalances"] / max(float(m1d["rebalances"]), 1.0))
    if cagr_delta > 0.02 and calmar_delta > 0 and rebalance_ratio <= 2.0:
        return (
            "La version 4H mejora materialmente a la 1D y merece investigacion adicional",
            "Razon principal: mejora CAGR y Calmar sin multiplicar de forma extrema la operativa.",
        )
    if cagr_delta > 0 and calmar_delta >= -0.05:
        return (
            "La version 4H es interesante pero no supera lo suficiente a la 1D",
            "Razon principal: la mejora no compensa claramente la frecuencia operativa adicional.",
        )
    return (
        "La version 4H no vale la pena frente a la 1D",
        (
            "Razon principal: no mejora suficientemente el binomio CAGR/Calmar "
            "frente al ruido operativo."
        ),
    )


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
        first_useful_signal=_useful_datetime(frame, SMA_WINDOW - 1),
        first_useful_execution=_useful_datetime(frame, SMA_WINDOW),
        is_sorted=bool(report.is_sorted),
        has_unique_timestamps=bool(report.has_unique_timestamps),
        gap_count=len(report.gaps),
        total_missing_intervals=int(sum(missing)),
        incomplete_4h_bins_dropped=incomplete_4h_bins_dropped,
        source_gap_count=0 if source_report is None else len(source_report.gaps),
        source_total_missing_intervals=int(sum(source_missing)),
    )


def _first_useful_execution(frame: pd.DataFrame) -> pd.Timestamp:
    if len(frame) <= SMA_WINDOW:
        raise ValueError("Not enough rows for SMA125 plus t+1 execution.")
    return pd.to_datetime(frame["datetime"].iloc[SMA_WINDOW], utc=True)


def _last_timestamp(frame: pd.DataFrame) -> pd.Timestamp:
    return pd.to_datetime(frame["datetime"].iloc[-1], utc=True)


def _useful_datetime(frame: pd.DataFrame, index: int) -> str | None:
    if len(frame) <= index:
        return None
    return str(pd.to_datetime(frame["datetime"].iloc[index], utc=True))


def _bars_per_year(timeframe: str) -> float:
    if timeframe == "4h":
        return 365.25 * 6.0
    if timeframe == "1d":
        return 365.25
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
                f"net_return={float(trade.get('net_return')):.6f}"
            )
    for row in summary.get("concentration", []):
        share = row.get("share_of_total_net_return_sum")
        share_text = "None" if share is None else f"{float(share):.6f}"
        lines.append(
            f"- top_{row['top_n']}_concentration: "
            f"net_sum={float(row['net_return_sum']):.6f}, share={share_text}"
        )
    return "\n".join(lines)


def _operating_read(
    common_base: pd.DataFrame,
    trade_summary_4h: dict[str, Any],
    trade_summary_1d: dict[str, Any],
) -> str:
    metrics = common_base.set_index("name")
    m4h = metrics.loc["Distance buffer 4H"]
    m1d = metrics.loc["Distance buffer 1D official"]
    rebalance_ratio = float(m4h["rebalances"] / max(float(m1d["rebalances"]), 1.0))
    trades_ratio = float(
        (trade_summary_4h.get("trades_per_year") or 0.0)
        / max(float(trade_summary_1d.get("trades_per_year") or 0.0), 1e-9)
    )
    return "\n".join(
        [
            (
                f"- 4H rebalances/common period: {int(m4h['rebalances'])} "
                f"vs 1D {int(m1d['rebalances'])}."
            ),
            f"- Rebalance ratio 4H/1D: {rebalance_ratio:.2f}.",
            f"- Complete-trade frequency ratio 4H/1D: {trades_ratio:.2f}.",
            (
                "- Paper/manual execution is only reasonable if the operator can "
                "review 4H closes consistently."
            ),
            (
                "- Any CAGR edge must be discounted for missed 4H closes, "
                "weekend attention and higher decision fatigue."
            ),
        ]
    )


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(_json_clean(payload), handle, indent=2, sort_keys=True)
        handle.write("\n")


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
