from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd

from alpha_x.execution.distance_buffer_v1 import (
    JOURNAL_COLUMNS,
    calculate_distance_buffer_signal,
    default_state,
    ensure_journal,
    read_state,
    state_from_signal,
    write_state,
)
from alpha_x.execution.messages import (
    format_no_operation_message,
    format_rebalance_message,
    format_status_message,
)
from alpha_x.execution.runner import run_daily_review
from alpha_x.execution.telegram_bot import TelegramConfig, TelegramConfigurationError


def _frame(close: float = 150.0) -> pd.DataFrame:
    closes = [100.0] * 124 + [close]
    timestamps = [index * 86_400_000 for index in range(len(closes))]
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "datetime": pd.to_datetime(timestamps, unit="ms", utc=True),
            "close": closes,
        }
    )


def test_distance_buffer_signal_uses_sma125_buffer_and_deadband() -> None:
    signal = calculate_distance_buffer_signal(_frame(), state=default_state())

    assert signal.regime == "ON"
    assert signal.target_exposure == 1.0
    assert signal.current_exposure == 0.0
    assert signal.action == "REBALANCE"
    assert signal.threshold == 103.412


def test_distance_buffer_state_roundtrip(tmp_path: Path) -> None:
    signal = calculate_distance_buffer_signal(_frame(), state=default_state())
    state_path = tmp_path / "state.json"

    write_state(state_path, state_from_signal(signal, mark_executed=True))
    state = read_state(state_path)

    assert state.last_signal_date == signal.signal_date
    assert state.current_exposure == 1.0
    assert state.last_rebalance_date == signal.execution_date


def test_distance_buffer_journal_template(tmp_path: Path) -> None:
    journal_path = tmp_path / "journal.csv"

    ensure_journal(journal_path)

    with journal_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader)
    assert header == JOURNAL_COLUMNS


def test_distance_buffer_message_formatters_include_operational_context() -> None:
    signal = calculate_distance_buffer_signal(_frame(), state=default_state())

    rebalance_message = format_rebalance_message(signal)
    no_operation_message = format_no_operation_message(signal)
    status_message = format_status_message(signal, default_state())

    assert "REBALANCE" in rebalance_message
    assert "Ejecucion t+1" in rebalance_message
    assert "sin operacion" in no_operation_message
    assert "Threshold" in no_operation_message
    assert "Colchon sobre trigger" in no_operation_message
    assert "status" in status_message
    assert "SMA125" in status_message


def test_telegram_config_requires_token_and_chat_id() -> None:
    try:
        TelegramConfig.from_values(bot_token=None, chat_id="123")
    except TelegramConfigurationError as error:
        assert "TELEGRAM_BOT_TOKEN" in str(error)
    else:
        raise AssertionError("Expected TelegramConfigurationError.")


def test_daily_runner_writes_state_and_journal_without_telegram(tmp_path: Path) -> None:
    dataset_path = tmp_path / "btc-eur_1d.csv"
    state_path = tmp_path / "state.json"
    journal_path = tmp_path / "journal.csv"
    frame = _frame()
    frame.loc[:, ["timestamp", "close"]].assign(
        open=frame["close"],
        high=frame["close"],
        low=frame["close"],
        volume=1.0,
    ).loc[:, ["timestamp", "open", "high", "low", "close", "volume"]].to_csv(
        dataset_path,
        index=False,
    )

    result = run_daily_review(
        state_path=state_path,
        journal_path=journal_path,
        dataset_path=dataset_path,
        send_telegram=False,
    )

    state = read_state(state_path)
    assert result.signal.action == "REBALANCE"
    assert result.journal_written is True
    assert result.telegram_sent is False
    assert state.current_exposure == 1.0
    assert state.last_action == "REBALANCE"

    with journal_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    assert rows[0]["action"] == "REBALANCE"


def test_daily_runner_no_new_signal_updates_review_state_without_duplicate_journal(
    tmp_path: Path,
) -> None:
    dataset_path = tmp_path / "btc-eur_1d.csv"
    state_path = tmp_path / "state.json"
    journal_path = tmp_path / "journal.csv"
    frame = _frame()
    frame.loc[:, ["timestamp", "close"]].assign(
        open=frame["close"],
        high=frame["close"],
        low=frame["close"],
        volume=1.0,
    ).loc[:, ["timestamp", "open", "high", "low", "close", "volume"]].to_csv(
        dataset_path,
        index=False,
    )

    first = run_daily_review(
        state_path=state_path,
        journal_path=journal_path,
        dataset_path=dataset_path,
        send_telegram=False,
    )
    second = run_daily_review(
        state_path=state_path,
        journal_path=journal_path,
        dataset_path=dataset_path,
        send_telegram=False,
    )

    state = read_state(state_path)
    assert first.signal.action == "REBALANCE"
    assert second.signal.action == "NO_NEW_SIGNAL"
    assert second.journal_written is False
    assert state.last_signal_date == first.signal.signal_date
    assert state.current_exposure == 1.0
    assert state.last_action == "NO_NEW_SIGNAL"

    with journal_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
