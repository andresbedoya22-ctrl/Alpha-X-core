from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path

import pandas as pd

STRATEGY_ID = "distance_buffer_v1"
MARKET = "BTC-EUR"
EXCHANGE = "bitvavo"
TIMEFRAME = "1d"
SMA_WINDOW = 125
BUFFER = 0.03
DEADBAND = 0.10
BASE_FEE_PER_SIDE = 0.0020
BASE_SLIPPAGE_PER_SIDE = 0.0005
STRESS_FEE_PER_SIDE = 0.0025
STRESS_SLIPPAGE_PER_SIDE = 0.0008

JOURNAL_COLUMNS = [
    "signal_date",
    "execution_date",
    "action",
    "target_exposure",
    "prior_exposure",
    "close_signal_day",
    "close_execution_day",
    "assumed_fee",
    "assumed_slippage",
    "rationale",
    "executed_yes_no",
    "notes",
]

PAPER_TRACKING_COLUMNS = [
    "date",
    "theoretical_equity",
    "paper_equity",
    "theoretical_exposure",
    "paper_exposure",
    "daily_divergence",
    "cumulative_divergence",
    "notes",
]


@dataclass(frozen=True)
class ExecutionState:
    last_signal_date: str | None
    current_exposure: float
    last_rebalance_date: str | None
    reference_price: float | None
    last_action: str | None
    last_reason: str | None
    updated_at_utc: str | None


@dataclass(frozen=True)
class OperationalSignal:
    signal_date: str
    execution_date: str
    regime: str
    target_exposure: float
    current_exposure: float
    action: str
    reason: str
    close: float
    sma125: float
    threshold: float
    assumed_fee: float
    assumed_slippage: float
    last_signal_date: str | None


def default_state() -> ExecutionState:
    return ExecutionState(
        last_signal_date=None,
        current_exposure=0.0,
        last_rebalance_date=None,
        reference_price=None,
        last_action=None,
        last_reason=None,
        updated_at_utc=None,
    )


def read_state(path: Path) -> ExecutionState:
    if not path.exists():
        return default_state()
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return ExecutionState(
        last_signal_date=payload.get("last_signal_date"),
        current_exposure=float(payload.get("current_exposure", 0.0)),
        last_rebalance_date=payload.get("last_rebalance_date"),
        reference_price=(
            None if payload.get("reference_price") is None else float(payload["reference_price"])
        ),
        last_action=payload.get("last_action"),
        last_reason=payload.get("last_reason"),
        updated_at_utc=payload.get("updated_at_utc"),
    )


def write_state(path: Path, state: ExecutionState) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(asdict(state), handle, indent=2, sort_keys=True)
        handle.write("\n")
    return path


def calculate_distance_buffer_signal(
    frame: pd.DataFrame,
    *,
    state: ExecutionState | None = None,
    assumed_fee: float = BASE_FEE_PER_SIDE,
    assumed_slippage: float = BASE_SLIPPAGE_PER_SIDE,
) -> OperationalSignal:
    if frame.empty:
        raise ValueError("Distance buffer signal requires a non-empty frame.")
    required = ("timestamp", "datetime", "close")
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"Frame missing required columns: {missing}")
    if len(frame) < SMA_WINDOW:
        raise ValueError(f"At least {SMA_WINDOW} rows are required to compute SMA125.")

    working = frame.loc[:, ["timestamp", "datetime", "close"]].copy().reset_index(drop=True)
    working["sma125"] = working["close"].rolling(SMA_WINDOW, min_periods=SMA_WINDOW).mean()
    latest = working.iloc[-1]
    if pd.isna(latest["sma125"]):
        raise ValueError("Latest row does not have a valid SMA125.")

    active_state = state or default_state()
    close = float(latest["close"])
    sma125 = float(latest["sma125"])
    threshold = sma125 * (1.0 + BUFFER)
    target_exposure = 1.0 if close > threshold else 0.0
    current_exposure = float(active_state.current_exposure)
    delta = abs(target_exposure - current_exposure)
    signal_date = str(pd.to_datetime(latest["datetime"], utc=True).date())
    execution_date = str(
        (pd.to_datetime(latest["datetime"], utc=True) + pd.Timedelta(days=1)).date()
    )
    regime = "ON" if target_exposure == 1.0 else "OFF"

    if active_state.last_signal_date is not None and signal_date <= active_state.last_signal_date:
        action = "NO_NEW_SIGNAL"
        reason = "latest signal date has already been processed"
    elif delta >= DEADBAND:
        action = "REBALANCE"
        comparator = ">" if target_exposure == 1.0 else "<="
        reason = f"close {comparator} SMA125 * 1.03 and delta >= deadband"
    else:
        action = "HOLD"
        comparator = ">" if target_exposure == 1.0 else "<="
        reason = f"close {comparator} SMA125 * 1.03 but delta < deadband"

    return OperationalSignal(
        signal_date=signal_date,
        execution_date=execution_date,
        regime=regime,
        target_exposure=target_exposure,
        current_exposure=current_exposure,
        action=action,
        reason=reason,
        close=close,
        sma125=sma125,
        threshold=threshold,
        assumed_fee=assumed_fee,
        assumed_slippage=assumed_slippage,
        last_signal_date=active_state.last_signal_date,
    )


def state_from_signal(signal: OperationalSignal, *, mark_executed: bool = False) -> ExecutionState:
    exposure = (
        signal.target_exposure
        if mark_executed and signal.action == "REBALANCE"
        else (signal.current_exposure)
    )
    rebalance_date = (
        signal.execution_date if mark_executed and signal.action == "REBALANCE" else None
    )
    return ExecutionState(
        last_signal_date=signal.signal_date,
        current_exposure=exposure,
        last_rebalance_date=rebalance_date,
        reference_price=signal.close,
        last_action=signal.action,
        last_reason=signal.reason,
        updated_at_utc=str(pd.Timestamp.now(tz="UTC").floor("s")),
    )


def ensure_journal(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=JOURNAL_COLUMNS)
            writer.writeheader()
    return path


def append_signal_to_journal(path: Path, signal: OperationalSignal) -> Path:
    ensure_journal(path)
    with path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=JOURNAL_COLUMNS)
        writer.writerow(
            {
                "signal_date": signal.signal_date,
                "execution_date": signal.execution_date,
                "action": signal.action,
                "target_exposure": signal.target_exposure,
                "prior_exposure": signal.current_exposure,
                "close_signal_day": signal.close,
                "close_execution_day": "",
                "assumed_fee": signal.assumed_fee,
                "assumed_slippage": signal.assumed_slippage,
                "rationale": signal.reason,
                "executed_yes_no": "NO",
                "notes": "",
            }
        )
    return path


def ensure_paper_tracking(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=PAPER_TRACKING_COLUMNS)
            writer.writeheader()
    return path


def format_signal(signal: OperationalSignal) -> str:
    target_pct = int(round(signal.target_exposure * 100))
    current_pct = int(round(signal.current_exposure * 100))
    return "\n".join(
        [
            f"SIGNAL_DATE: {signal.signal_date}",
            f"EXECUTION_DATE: {signal.execution_date}",
            f"REGIME: {signal.regime}",
            f"TARGET_EXPOSURE: {target_pct}%",
            f"CURRENT_EXPOSURE: {current_pct}%",
            f"ACTION: {signal.action}",
            f"REASON: {signal.reason}",
            f"CLOSE: {signal.close:.2f}",
            f"SMA125: {signal.sma125:.2f}",
            f"THRESHOLD_SMA125_X_1.03: {signal.threshold:.2f}",
            f"ASSUMED_FEE_PER_SIDE: {signal.assumed_fee:.4%}",
            f"ASSUMED_SLIPPAGE_PER_SIDE: {signal.assumed_slippage:.4%}",
        ]
    )
