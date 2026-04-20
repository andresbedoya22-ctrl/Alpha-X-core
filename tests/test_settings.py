from pathlib import Path

from alpha_x.config.settings import Settings


def test_settings_defaults(tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,
        LOG_DIR=tmp_path / "logs",
        REPORTS_DIR=tmp_path / "reports",
        RAW_DATA_DIR=tmp_path / "raw",
        EXTERNAL_DATA_DIR=tmp_path / "external",
        PROCESSED_DATA_DIR=tmp_path / "processed",
        DISTANCE_BUFFER_STATE_PATH=tmp_path / "state.json",
        DISTANCE_BUFFER_JOURNAL_PATH=tmp_path / "journal.csv",
        DISTANCE_BUFFER_COST_SCENARIO="stress",
        DISTANCE_BUFFER_DAILY_RUN_TIME="21:45",
        TELEGRAM_BOT_TOKEN="token",
        TELEGRAM_CHAT_ID="chat",
    )

    settings.ensure_directories()

    assert settings.environment == "development"
    assert settings.system_symbol == "BTC/EUR"
    assert settings.log_dir.exists()
    assert settings.reports_dir.exists()
    assert settings.raw_data_dir.exists()
    assert settings.external_data_dir.exists()
    assert settings.processed_data_dir.exists()
    assert settings.distance_buffer_state_path == tmp_path / "state.json"
    assert settings.distance_buffer_journal_path == tmp_path / "journal.csv"
    assert settings.distance_buffer_cost_scenario == "stress"
    assert settings.distance_buffer_daily_run_time == "21:45"
    assert settings.telegram_bot_token == "token"
    assert settings.telegram_chat_id == "chat"
