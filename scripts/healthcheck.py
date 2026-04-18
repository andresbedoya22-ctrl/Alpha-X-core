from __future__ import annotations

from alpha_x.config.settings import get_settings
from alpha_x.utils.logging_utils import configure_logging


def main() -> None:
    settings = get_settings()
    logger = configure_logging(settings.log_dir, settings.log_level)

    logger.info("ALPHA-X CORE healthcheck started")
    logger.info("Environment: %s", settings.environment)
    logger.info("System symbol: %s", settings.system_symbol)
    logger.info("Raw data dir: %s", settings.raw_data_dir.resolve())
    logger.info("Processed data dir: %s", settings.processed_data_dir.resolve())
    logger.info("Reports dir: %s", settings.reports_dir.resolve())
    logger.info("Healthcheck completed")


if __name__ == "__main__":
    main()
