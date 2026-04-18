from pathlib import Path

from alpha_x.config.settings import Settings


def test_settings_defaults(tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,
        LOG_DIR=tmp_path / "logs",
        REPORTS_DIR=tmp_path / "reports",
        RAW_DATA_DIR=tmp_path / "raw",
        PROCESSED_DATA_DIR=tmp_path / "processed",
    )

    settings.ensure_directories()

    assert settings.environment == "development"
    assert settings.system_symbol == "BTC/EUR"
    assert settings.log_dir.exists()
    assert settings.reports_dir.exists()
    assert settings.raw_data_dir.exists()
    assert settings.processed_data_dir.exists()
