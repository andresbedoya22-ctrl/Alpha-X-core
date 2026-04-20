from __future__ import annotations

from dataclasses import dataclass

import requests


class TelegramConfigurationError(RuntimeError):
    pass


@dataclass(frozen=True)
class TelegramConfig:
    bot_token: str
    chat_id: str

    @classmethod
    def from_values(
        cls,
        *,
        bot_token: str | None,
        chat_id: str | None,
    ) -> TelegramConfig:
        missing = []
        if not bot_token:
            missing.append("TELEGRAM_BOT_TOKEN")
        if not chat_id:
            missing.append("TELEGRAM_CHAT_ID")
        if missing:
            raise TelegramConfigurationError(
                "Missing Telegram configuration: " + ", ".join(missing)
            )
        return cls(bot_token=bot_token, chat_id=chat_id)


class TelegramBot:
    def __init__(self, config: TelegramConfig, *, timeout: int = 30) -> None:
        self.config = config
        self.timeout = timeout

    def send_message(self, text: str) -> None:
        url = f"https://api.telegram.org/bot{self.config.bot_token}/sendMessage"
        payload = {"chat_id": self.config.chat_id, "text": text}
        try:
            response = requests.post(url, json=payload, timeout=self.timeout)
            response.raise_for_status()
        except requests.HTTPError as error:
            message = response.text.strip() or str(error)
            raise RuntimeError(f"Telegram sendMessage failed: {message}") from error
        except requests.RequestException as error:
            raise RuntimeError(f"Telegram sendMessage failed: {error}") from error
