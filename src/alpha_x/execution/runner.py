from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from alpha_x.config.settings import Settings, get_settings
from alpha_x.data.ohlcv_models import OHLCV_COLUMNS, normalize_ohlcv_frame
from alpha_x.data.ohlcv_storage import build_ohlcv_csv_path
from alpha_x.execution.distance_buffer_v1 import (
    BASE_FEE_PER_SIDE,
    BASE_SLIPPAGE_PER_SIDE,
    STRESS_FEE_PER_SIDE,
    STRESS_SLIPPAGE_PER_SIDE,
    ExecutionState,
    OperationalSignal,
    append_signal_to_journal,
    calculate_distance_buffer_signal,
    read_state,
    write_state,
)
from alpha_x.execution.messages import format_no_operation_message, format_rebalance_message
from alpha_x.execution.telegram_bot import TelegramBot, TelegramConfig

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class DailyRunResult:
    signal: OperationalSignal
    state: ExecutionState
    message: str
    journal_written: bool
    telegram_sent: bool


def load_distance_buffer_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Distance buffer dataset not found: {path}")
    normalized = normalize_ohlcv_frame(pd.read_csv(path))
    frame = normalized.copy()
    frame["datetime"] = pd.to_datetime(frame["timestamp"], unit="ms", utc=True)
    return frame.loc[:, ["timestamp", "datetime", *OHLCV_COLUMNS[1:]]]


def cost_rates(cost_scenario: str) -> tuple[float, float]:
    if cost_scenario == "base":
        return BASE_FEE_PER_SIDE, BASE_SLIPPAGE_PER_SIDE
    if cost_scenario == "stress":
        return STRESS_FEE_PER_SIDE, STRESS_SLIPPAGE_PER_SIDE
    raise ValueError("cost_scenario must be 'base' or 'stress'.")


def build_dataset_path(
    settings: Settings,
    *,
    market: str = "BTC-EUR",
    timeframe: str = "1d",
) -> Path:
    return Path(
        build_ohlcv_csv_path(
            raw_data_dir=settings.raw_data_dir,
            exchange="bitvavo",
            market=market,
            timeframe=timeframe,
        )
    )


def build_signal(
    *,
    settings: Settings | None = None,
    state_path: Path | None = None,
    dataset_path: Path | None = None,
) -> tuple[OperationalSignal, ExecutionState]:
    active_settings = settings or get_settings()
    active_state_path = state_path or active_settings.distance_buffer_state_path
    active_dataset_path = dataset_path or build_dataset_path(active_settings)
    state = read_state(active_state_path)
    frame = load_distance_buffer_frame(active_dataset_path)
    fee, slippage = cost_rates(active_settings.distance_buffer_cost_scenario)
    signal = calculate_distance_buffer_signal(
        frame,
        state=state,
        assumed_fee=fee,
        assumed_slippage=slippage,
    )
    return signal, state


def build_status_text(
    *,
    settings: Settings | None = None,
    state_path: Path | None = None,
    dataset_path: Path | None = None,
) -> str:
    from alpha_x.execution.messages import format_status_message

    signal, state = build_signal(
        settings=settings,
        state_path=state_path,
        dataset_path=dataset_path,
    )
    return format_status_message(signal, state)


def state_after_review(signal: OperationalSignal, previous: ExecutionState) -> ExecutionState:
    is_rebalance = signal.action == "REBALANCE"
    return ExecutionState(
        last_signal_date=(
            previous.last_signal_date if signal.action == "NO_NEW_SIGNAL" else signal.signal_date
        ),
        current_exposure=signal.target_exposure if is_rebalance else previous.current_exposure,
        last_rebalance_date=signal.execution_date if is_rebalance else previous.last_rebalance_date,
        reference_price=signal.close,
        last_action=signal.action,
        last_reason=signal.reason,
        updated_at_utc=str(pd.Timestamp.now(tz="UTC").floor("s")),
    )


def run_daily_review(
    *,
    settings: Settings | None = None,
    state_path: Path | None = None,
    journal_path: Path | None = None,
    dataset_path: Path | None = None,
    send_telegram: bool = True,
    dry_run: bool = False,
) -> DailyRunResult:
    active_settings = settings or get_settings()
    active_state_path = state_path or active_settings.distance_buffer_state_path
    active_journal_path = journal_path or active_settings.distance_buffer_journal_path
    signal, previous_state = build_signal(
        settings=active_settings,
        state_path=active_state_path,
        dataset_path=dataset_path,
    )
    next_state = state_after_review(signal, previous_state)
    message = (
        format_rebalance_message(signal)
        if signal.action == "REBALANCE"
        else format_no_operation_message(signal)
    )

    journal_written = False
    if signal.action != "NO_NEW_SIGNAL":
        append_signal_to_journal(active_journal_path, signal)
        journal_written = True
    write_state(active_state_path, next_state)

    telegram_sent = False
    if send_telegram and not dry_run:
        config = TelegramConfig.from_values(
            bot_token=active_settings.telegram_bot_token,
            chat_id=active_settings.telegram_chat_id,
        )
        TelegramBot(config).send_message(message)
        telegram_sent = True

    LOGGER.info(
        "Distance buffer daily review completed action=%s signal_date=%s telegram_sent=%s",
        signal.action,
        signal.signal_date,
        telegram_sent,
    )
    return DailyRunResult(
        signal=signal,
        state=next_state,
        message=message,
        journal_written=journal_written,
        telegram_sent=telegram_sent,
    )
