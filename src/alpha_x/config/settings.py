from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="ALPHA_X_",
        case_sensitive=False,
        extra="ignore",
    )

    environment: str = Field(default="development", alias="ENV")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_dir: Path = Field(default=Path("logs"), alias="LOG_DIR")
    reports_dir: Path = Field(default=Path("reports"), alias="REPORTS_DIR")
    raw_data_dir: Path = Field(default=Path("data/raw"), alias="RAW_DATA_DIR")
    processed_data_dir: Path = Field(default=Path("data/processed"), alias="PROCESSED_DATA_DIR")
    system_symbol: str = Field(default="BTC/EUR", alias="SYSTEM_SYMBOL")
    bitvavo_base_url: str = Field(default="https://api.bitvavo.com/v2", alias="BITVAVO_BASE_URL")
    bitvavo_market: str = Field(default="BTC-EUR", alias="BITVAVO_MARKET")
    ohlcv_default_interval: str = Field(default="1h", alias="OHLCV_DEFAULT_INTERVAL")
    ohlcv_default_limit: int = Field(default=1000, alias="OHLCV_DEFAULT_LIMIT")

    def ensure_directories(self) -> None:
        for directory in (
            self.log_dir,
            self.reports_dir,
            self.raw_data_dir,
            self.processed_data_dir,
        ):
            directory.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.ensure_directories()
    return settings
